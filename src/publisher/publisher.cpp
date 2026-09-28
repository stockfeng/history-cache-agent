#include "history_cache/publisher.h"

#include "binary.h"

namespace history_cache {
namespace {

struct Current {
    Pointer pointer;
    std::string etag;
};

std::optional<Current> read_current(const ObjectStore& store) {
    const auto object = store.read_current();
    if (!object) return std::nullopt;
    detail::require(!object->bytes.empty() && object->bytes.size() <= kMaxPointerBytes,
                    "pointer exceeds read budget", ErrorCode::resource_limit);
    validate_etag(object->etag);
    return Current{parse_pointer(std::string(object->bytes.begin(), object->bytes.end())), object->etag};
}

Snapshot snapshot_from(const ObjectStore& store, const Current& current) {
    const auto bytes = store.get(current.pointer.manifest_key, kMaxMetadataBytes);
    detail::require(!bytes.empty() && bytes.size() <= kMaxMetadataBytes,
                    "manifest exceeds read budget", ErrorCode::resource_limit);
    detail::require(sha256(bytes) == current.pointer.manifest_sha256, "manifest SHA-256 mismatch");
    auto manifest = parse_manifest(std::string(bytes.begin(), bytes.end()));
    detail::require(manifest.dataset_epoch == current.pointer.dataset_epoch, "pointer/manifest epoch mismatch");
    return {current.pointer, std::move(manifest)};
}

bool is_target(const Current& current, const Pointer& target) {
    return current.pointer.publication_seq == target.publication_seq &&
           current.pointer.dataset_epoch == target.dataset_epoch &&
           current.pointer.manifest_key == target.manifest_key &&
           current.pointer.manifest_sha256 == target.manifest_sha256;
}

}  // namespace

Snapshot load_snapshot(const ObjectStore& store, uint64_t min_publication_seq) {
    const auto current = read_current(store);
    detail::require(bool(current), "current pointer is missing", ErrorCode::missing);
    detail::require(current->pointer.publication_seq >= min_publication_seq,
                    "pointer publication sequence decreased", ErrorCode::conflict);
    return snapshot_from(store, *current);
}

PublishResult ConditionalPublisher::publish(const Manifest& manifest, uint64_t expected_seq) {
    detail::require(expected_seq < UINT64_MAX, "publication sequence exhausted", ErrorCode::resource_limit);
    const auto manifest_text = serialize_manifest(manifest);
    const auto hash = sha256(manifest_text);
    const Pointer target{manifest.dataset_epoch, expected_seq + 1, "manifests/v1/" + hex(hash) + ".json", hash};
    const auto current = read_current(store_);
    if (current && is_target(*current, target)) {
        (void)snapshot_from(store_, *current);
        return {PublishOutcome::committed, target, true};
    }
    if ((current ? current->pointer.publication_seq : 0) != expected_seq)
        return {PublishOutcome::conflict, target, false};
    if (current) validate_publication_transition(snapshot_from(store_, *current).manifest, manifest);

    for (const auto& entry : manifest.entries) {
        if (entry.pack) {
            const auto read = store_.open_range(entry.pack->key, entry.pack->bytes);
            verify_pack(read, *entry.pack, pack_metadata(entry));
        }
    }
    const auto stored = put_immutable(store_, target.manifest_key, Bytes(manifest_text.begin(), manifest_text.end()));
    if (stored != WriteOutcome::applied) {
        const auto outcome = stored == WriteOutcome::not_applied ? PublishOutcome::not_applied :
                             stored == WriteOutcome::precondition_failed ? PublishOutcome::conflict :
                             PublishOutcome::indeterminate;
        return {outcome, target, false};
    }
    const auto pointer_text = serialize_pointer(target);
    const std::optional<std::string> etag = current ? std::optional<std::string>(current->etag) : std::nullopt;
    WriteOutcome written;
    try {
        written = store_.write_current(Bytes(pointer_text.begin(), pointer_text.end()), etag);
    } catch (const Error& error) {
        if (error.code() != ErrorCode::io) throw;
        written = WriteOutcome::indeterminate;
    }
    if (written == WriteOutcome::applied) return {PublishOutcome::committed, target, false};
    if (written == WriteOutcome::not_applied) return {PublishOutcome::not_applied, target, false};
    try {
        const auto observed = read_current(store_);
        if (observed && is_target(*observed, target)) return {PublishOutcome::committed, target, true};
    } catch (const Error&) {
        return {PublishOutcome::indeterminate, target, false};
    }
    // An unchanged GET cannot rule out a timed-out request that is still in flight.
    // A newer pointer also cannot prove whether our write committed before it.
    return {written == WriteOutcome::precondition_failed ? PublishOutcome::conflict : PublishOutcome::indeterminate,
            target, false};
}

}  // namespace history_cache
