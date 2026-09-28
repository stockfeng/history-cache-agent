#include "history_cache/local_store.h"

#include "binary.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <sys/file.h>

namespace history_cache {
namespace {

constexpr const char* kMarker = "history-cache-local-v1\n";
constexpr const char* kCurrent = "current.json";
std::atomic<uint64_t> temp_counter{0};

std::filesystem::path temp_path(const std::filesystem::path& root) {
    return root / "tmp" / ("write-" + std::to_string(::getpid()) + "-" +
                            std::to_string(temp_counter.fetch_add(1)) + ".part");
}

class Temporary {
public:
    explicit Temporary(std::filesystem::path path) : path_(std::move(path)) {}
    ~Temporary() {
        if (owned_) { std::error_code ignored; std::filesystem::remove(path_, ignored); }
    }
    void mark_owned() { owned_ = true; }
    const std::filesystem::path& path() const { return path_; }
private:
    std::filesystem::path path_;
    bool owned_ = false;
};

class WriterLock {
public:
    explicit WriterLock(const std::filesystem::path& root)
        : file_(root / "writer.lock", O_RDWR | O_CREAT) {
        if (::flock(file_.fd(), LOCK_EX | LOCK_NB) != 0)
            throw Error(ErrorCode::conflict, "another local publisher holds the writer lock");
    }
    ~WriterLock() { (void)::flock(file_.fd(), LOCK_UN); }
private:
    detail::File file_;
};

std::string text(const Bytes& bytes) { return std::string(bytes.begin(), bytes.end()); }

void link_immutable(const std::filesystem::path& temporary, const std::filesystem::path& destination,
                    const Digest& expected_hash, uint64_t max_bytes) {
    if (::link(temporary.c_str(), destination.c_str()) != 0) {
        if (errno != EEXIST) throw Error(ErrorCode::io, "cannot install immutable object");
        detail::require(file_sha256(destination, max_bytes) == expected_hash,
                        "immutable key already contains different bytes", ErrorCode::conflict);
    }
    detail::sync_directory(destination.parent_path());
}

void atomic_write(const std::filesystem::path& root, const char* name, const std::string& bytes) {
    Temporary temporary(temp_path(root));
    write_new_file(temporary.path(), bytes);
    temporary.mark_owned();
    const auto destination = root / name;
    detail::reject_symlinks(destination);
    detail::require(::rename(temporary.path().c_str(), destination.c_str()) == 0,
                    "cannot replace local pointer/checkpoint", ErrorCode::io);
    detail::sync_directory(root);
    detail::sync_directory(root / "tmp");
}

void inject(FailPoint actual, FailPoint target) {
    if (actual == target) throw Error(ErrorCode::io, "injected publisher failure");
}

}  // namespace

void LocalStore::initialize(const std::filesystem::path& root) {
    create_new_directory(root);
    for (const auto* subdir : {"data", "data/v1", "manifests", "manifests/v1", "tmp"}) {
        create_new_directory(root / subdir);
    }
    write_new_file(root / ".history-cache-local-v1", std::string(kMarker));
    detail::sync_directory(root);
    detail::sync_directory(root.parent_path().empty() ? "." : root.parent_path());
}

LocalStore::LocalStore(std::filesystem::path root) : root_(std::filesystem::absolute(std::move(root))) {
    detail::reject_symlinks(root_);
    detail::require(text(read_file(root_ / ".history-cache-local-v1", 128)) == kMarker,
                    "directory is not an initialized local fixture store", ErrorCode::invalid);
    const auto permissions = std::filesystem::status(root_).permissions();
    detail::require((permissions & (std::filesystem::perms::group_write | std::filesystem::perms::others_write)) ==
                    std::filesystem::perms::none, "local fixture store must not be writable by other users",
                    ErrorCode::invalid);
}

std::filesystem::path LocalStore::object_path(const std::string& key) const {
    (void)parse_immutable_key(key);
    const auto path = root_ / key;
    detail::reject_symlinks(path);
    return path;
}

Bytes LocalStore::get(const std::string& key, uint64_t max_bytes) const {
    detail::require(max_bytes <= kMaxObjectBytes, "invalid get budget", ErrorCode::resource_limit);
    return read_file(object_path(key), max_bytes);
}

RangeReader LocalStore::open_range(const std::string& key, uint64_t expected_size) const {
    return file_range_reader(object_path(key), expected_size);
}

void LocalStore::put(const std::string& key, const Bytes& bytes) {
    const auto destination = object_path(key);
    const uint64_t limit = key.rfind("manifests/v1/", 0) == 0 ? kMaxMetadataBytes : kMaxObjectBytes;
    detail::require(bytes.size() <= limit, "put exceeds object budget", ErrorCode::resource_limit);
    const auto hash = sha256(bytes);
    detail::require(destination.stem().string() == hex(hash), "object key does not match content hash",
                    ErrorCode::invalid);
    Temporary temporary(temp_path(root_));
    write_new_file(temporary.path(), bytes);
    temporary.mark_owned();
    link_immutable(temporary.path(), destination, hash, limit);
}

void LocalStore::put_file(const std::string& key, const std::filesystem::path& source,
                          const Digest& expected_hash) {
    const auto destination = object_path(key);
    detail::require(key.compare(0, 8, "data/v1/") == 0 && destination.stem().string() == hex(expected_hash),
                    "invalid data object key", ErrorCode::invalid);
    detail::File input(source, O_RDONLY);
    const auto size = input.size();
    detail::require(size > 0 && size <= kMaxObjectBytes, "input pack exceeds budget", ErrorCode::resource_limit);
    Temporary temporary(temp_path(root_));
    {
        detail::File output(temporary.path(), O_WRONLY | O_CREAT | O_EXCL);
        temporary.mark_owned();
        Sha256 hash;
        for (uint64_t offset = 0; offset < size;) {
            auto chunk = input.read(offset, std::min<uint64_t>(65536, size - offset));
            output.write(offset, chunk);
            hash.update(chunk);
            offset += chunk.size();
        }
        detail::require(hash.finish() == expected_hash, "input pack hash mismatch");
        output.sync();
    }
    link_immutable(temporary.path(), destination, expected_hash, kMaxObjectBytes);
}

Snapshot LocalStore::load_snapshot(uint64_t min_publication_seq) const {
    const auto pointer = parse_pointer(text(read_file(root_ / kCurrent, 4096)));
    detail::require(pointer.publication_seq >= min_publication_seq,
                    "pointer publication sequence decreased", ErrorCode::conflict);
    const auto bytes = get(pointer.manifest_key, kMaxMetadataBytes);
    detail::require(sha256(bytes) == pointer.manifest_sha256, "manifest SHA-256 mismatch");
    auto manifest = parse_manifest(text(bytes));
    detail::require(manifest.dataset_epoch == pointer.dataset_epoch, "pointer/manifest epoch mismatch");
    return {pointer, std::move(manifest)};
}

Pointer LocalStore::publish(const Manifest& manifest, uint64_t expected_seq, FailPoint fail_point) {
    return commit(manifest, expected_seq, fail_point, false);
}

Pointer LocalStore::rollback(const std::string& manifest_key, const Digest& manifest_hash,
                             uint64_t expected_seq) {
    detail::require(manifest_key == "manifests/v1/" + hex(manifest_hash) + ".json",
                    "rollback manifest reference mismatch", ErrorCode::invalid);
    const auto bytes = get(manifest_key, kMaxMetadataBytes);
    detail::require(sha256(bytes) == manifest_hash, "rollback manifest hash mismatch");
    return commit(parse_manifest(text(bytes)), expected_seq, FailPoint::none, true);
}

Pointer LocalStore::commit(const Manifest& manifest, uint64_t expected_seq,
                           FailPoint fail_point, bool rollback_requested) {
    const auto manifest_text = serialize_manifest(manifest);
    const auto hash = sha256(manifest_text);
    WriterLock writer(root_);
    std::optional<Snapshot> current;
    if (std::filesystem::exists(root_ / kCurrent)) current = load_snapshot();
    const uint64_t current_seq = current ? current->pointer.publication_seq : 0;
    detail::require(expected_seq < UINT64_MAX, "publication sequence exhausted", ErrorCode::resource_limit);
    if (current && current_seq == expected_seq + 1 && current->pointer.manifest_sha256 == hash) {
        atomic_write(root_, "checkpoint.json", serialize_pointer(current->pointer));
        return current->pointer;
    }
    detail::require(current_seq == expected_seq, "publication compare-and-swap conflict", ErrorCode::conflict);
    if (current) {
        detail::require(current->pointer.dataset_epoch == manifest.dataset_epoch,
                        "epoch migration requires a separate store", ErrorCode::conflict);
        if (!rollback_requested) validate_publication_transition(current->manifest, manifest);
    }
    for (const auto& entry : manifest.entries)
        if (entry.pack) verify_pack(object_path(entry.pack->key), *entry.pack, pack_metadata(entry));
    inject(fail_point, FailPoint::after_objects);
    const std::string key = "manifests/v1/" + hex(hash) + ".json";
    put(key, Bytes(manifest_text.begin(), manifest_text.end()));
    detail::require(sha256(get(key, kMaxMetadataBytes)) == hash, "stored manifest verification failed");
    inject(fail_point, FailPoint::after_manifest);
    Pointer pointer{manifest.dataset_epoch, expected_seq + 1, key, hash};
    atomic_write(root_, kCurrent, serialize_pointer(pointer));
    inject(fail_point, FailPoint::after_pointer);
    atomic_write(root_, "checkpoint.json", serialize_pointer(pointer));
    return pointer;
}

}  // namespace history_cache
