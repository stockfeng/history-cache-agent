#include "history_cache/journal.h"

#include "binary.h"

#include <cerrno>
#include <sys/file.h>

namespace history_cache {
namespace {

constexpr size_t kIntentHeader = 52;
constexpr size_t kEventBytes = 80;
constexpr size_t kMaxEvents = 128;
constexpr const char* kReady = "history-publish-journal-v1\n";
enum class Event : uint8_t { attempt = 1, not_applied = 2, conflict = 3, unknown = 4, committed = 5 };

class Fd {
public:
    explicit Fd(int value = -1) : value_(value) {}
    ~Fd() { if (value_ >= 0) ::close(value_); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : value_(other.value_) { other.value_ = -1; }
    Fd& operator=(Fd&& other) noexcept {
        if (this != &other) {
            if (value_ >= 0) ::close(value_);
            value_ = other.value_; other.value_ = -1;
        }
        return *this;
    }
    int get() const { return value_; }
    void sync() const { detail::require(::fsync(value_) == 0, "journal fsync failed", ErrorCode::io); }
private:
    int value_;
};

Fd directory(const std::filesystem::path& input) {
    const auto path = std::filesystem::absolute(input);
    Fd current(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    detail::require(current.get() >= 0, "cannot open journal path root", ErrorCode::io);
    for (const auto& part : path.relative_path()) {
        detail::require(part != "..", "parent traversal in journal path", ErrorCode::invalid);
        if (part == "." || part.empty()) continue;
        Fd next(::openat(current.get(), part.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        detail::require(next.get() >= 0, "journal directory path is missing or unsafe", ErrorCode::invalid);
        current = std::move(next);
    }
    return current;
}

void private_directory(int fd) {
    struct stat status{};
    detail::require(::fstat(fd, &status) == 0 && S_ISDIR(status.st_mode) && status.st_uid == ::geteuid() &&
                    (status.st_mode & 07777) == 0700, "journal requires an owned 0700 directory", ErrorCode::invalid);
}

Fd file(int root, const char* name, int flags) {
    const int opened = ::openat(root, name, flags | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK, 0600);
    if (opened < 0) throw Error(errno == EEXIST ? ErrorCode::conflict : ErrorCode::io, "cannot open journal file");
    Fd result(opened);
    struct stat status{};
    detail::require(::fstat(opened, &status) == 0 && S_ISREG(status.st_mode) && status.st_uid == ::geteuid() &&
                    (status.st_mode & 07777) == 0600 && status.st_nlink == 1,
                    "journal requires owned 0600 single-link regular files", ErrorCode::invalid);
    return result;
}

Bytes read(int fd, size_t limit) {
    struct stat status{};
    detail::require(::fstat(fd, &status) == 0 && status.st_size >= 0, "cannot stat journal file", ErrorCode::io);
    detail::require(static_cast<uint64_t>(status.st_size) <= limit, "journal file exceeds budget", ErrorCode::resource_limit);
    Bytes output(static_cast<size_t>(status.st_size));
    size_t done = 0;
    while (done < output.size()) {
        const auto count = ::pread(fd, output.data() + done, output.size() - done, static_cast<off_t>(done));
        if (count < 0 && errno == EINTR) continue;
        detail::require(count > 0, "short journal read", ErrorCode::io);
        done += static_cast<size_t>(count);
    }
    return output;
}

void write(int fd, const Bytes& bytes, size_t offset = 0) {
    size_t done = 0;
    while (done < bytes.size()) {
        const auto count = ::pwrite(fd, bytes.data() + done, bytes.size() - done, static_cast<off_t>(offset + done));
        if (count < 0 && errno == EINTR) continue;
        detail::require(count > 0, "journal write failed", ErrorCode::io);
        done += static_cast<size_t>(count);
    }
}

void lock(int fd) {
    detail::require(::flock(fd, LOCK_EX | LOCK_NB) == 0, "another local publisher holds this journal",
                    ErrorCode::conflict);
}

void notify(const JournalHook& hook, JournalStep step) { if (hook) hook(step); }

Pointer make_target(const Manifest& candidate, uint64_t expected_seq) {
    detail::require(expected_seq < UINT64_MAX, "journal publication sequence exhausted", ErrorCode::resource_limit);
    const auto hash = sha256(serialize_manifest(candidate));
    return {candidate.dataset_epoch, expected_seq + 1, "manifests/v1/" + hex(hash) + ".json", hash};
}

Bytes intent(const Digest& scope, const Manifest& candidate, uint64_t expected_seq) {
    detail::require(scope != Digest{}, "empty journal scope", ErrorCode::invalid);
    (void)make_target(candidate, expected_seq);
    const auto manifest = serialize_manifest(candidate);
    Bytes result{'R', '2', 'J', 'I', '0', '0', '0', '1'};
    result.insert(result.end(), scope.begin(), scope.end());
    result.resize(kIntentHeader);
    detail::put_be(result, 40, expected_seq, 8);
    detail::put_be(result, 48, manifest.size(), 4);
    result.insert(result.end(), manifest.begin(), manifest.end());
    const auto hash = sha256(result);
    result.insert(result.end(), hash.begin(), hash.end());
    return result;
}

Bytes dispatch_floor(size_t records, const Digest& tail) {
    Bytes result(kEventBytes, 0);
    result[0] = 'J'; result[1] = 'F'; result[2] = 'L'; result[3] = '1';
    detail::put_be(result, 8, records, 8);
    std::copy(tail.begin(), tail.end(), result.begin() + 16);
    const auto hash = sha256(Bytes(result.begin(), result.begin() + 48));
    std::copy(hash.begin(), hash.end(), result.begin() + 48);
    return result;
}

}  // namespace

struct JournaledPublisher::State {
    ObjectStore& store;
    Fd root, writer, events, floor;
    Manifest candidate;
    uint64_t expected_seq = 0;
    Pointer target;
    Digest tail{};
    size_t records = 0;
    bool pending = false;
    bool historical_unknown = false;
    bool committed = false;
    bool poisoned = false;

    State(const std::filesystem::path& path, ObjectStore& storage) : store(storage), root(directory(path)) {
        private_directory(root.get());
        writer = file(root.get(), "writer.lock", O_RDWR);
        lock(writer.get());
        (void)read(writer.get(), 0);
        const auto ready = file(root.get(), "ready", O_RDONLY);
        const auto marker = read(ready.get(), 64);
        detail::require(std::string(marker.begin(), marker.end()) == kReady, "journal is not initialized");
        const auto source = file(root.get(), "intent.r2j", O_RDONLY);
        auto bytes = read(source.get(), kMaxMetadataBytes + kIntentHeader + 32);
        detail::require(bytes.size() > kIntentHeader + 32 &&
                        std::string(bytes.begin(), bytes.begin() + 8) == "R2JI0001", "invalid journal intent header");
        const auto length = detail::be(bytes, 48, 4);
        detail::require(length <= kMaxMetadataBytes && bytes.size() == kIntentHeader + length + 32,
                        "truncated journal intent");
        const auto hash = detail::fixed<32>(bytes, bytes.size() - 32);
        tail = sha256(bytes);
        const auto intent_hash = tail;
        bytes.resize(bytes.size() - 32);
        detail::require(sha256(bytes) == hash, "journal intent checksum mismatch");
        detail::require(detail::fixed<32>(bytes, 8) == store.scope_id(), "journal target scope mismatch", ErrorCode::conflict);
        expected_seq = detail::be(bytes, 40, 8);
        candidate = parse_manifest(std::string(bytes.begin() + kIntentHeader, bytes.end()));
        target = make_target(candidate, expected_seq);
        events = file(root.get(), "events.r2j", O_RDWR);
        const auto log = read(events.get(), kEventBytes * kMaxEvents);
        detail::require(log.size() % kEventBytes == 0, "journal has a torn event; refusing automatic repair");
        for (size_t offset = 0; offset < log.size(); offset += kEventBytes) {
            const Bytes record(log.begin() + static_cast<std::ptrdiff_t>(offset),
                               log.begin() + static_cast<std::ptrdiff_t>(offset + kEventBytes));
            detail::require(record[0] == 'J' && record[1] == 'E' && record[2] == 'V' && record[3] == '1' &&
                            record[5] == 0 && record[6] == 0 && record[7] == 0 &&
                            detail::be(record, 8, 8) == records + 1 && detail::fixed<32>(record, 16) == tail &&
                            sha256(Bytes(record.begin(), record.begin() + 48)) == detail::fixed<32>(record, 48),
                            "journal event chain is corrupt");
            apply(static_cast<Event>(record[4]));
            tail = detail::fixed<32>(record, 48);
            ++records;
        }
        floor = file(root.get(), "dispatch.r2j", O_RDWR);
        const auto anchor = read(floor.get(), kEventBytes);
        detail::require(anchor.size() == kEventBytes, "truncated journal dispatch floor");
        const auto index = detail::be(anchor, 8, 8);
        detail::require(index <= records &&
                        anchor == dispatch_floor(index, index == 0 ? intent_hash :
                                                  detail::fixed<32>(log, static_cast<size_t>(index) * kEventBytes - 32)) &&
                        (index == 0 || log[(static_cast<size_t>(index) - 1) * kEventBytes + 4] == static_cast<uint8_t>(Event::attempt)),
                        "journal dispatch floor or event suffix is corrupt");
        // An ACK record left readable by a failed fsync is trusted only after a
        // successful recovery fsync. This is not a power-loss filesystem proof.
        events.sync();
        floor.sync();
        ready.sync();
        source.sync();
        root.sync();
        auto parent = directory(std::filesystem::absolute(path).parent_path());
        parent.sync();
    }

    void apply(Event event) {
        detail::require(!committed, "journal has records after commitment");
        if (event == Event::attempt) {
            historical_unknown = historical_unknown || pending;
            pending = true;
        } else {
            detail::require(pending, "journal result without an attempt");
            detail::require(event == Event::not_applied || event == Event::conflict ||
                            event == Event::unknown || event == Event::committed, "invalid journal event kind");
            pending = false;
            historical_unknown = historical_unknown || event == Event::unknown;
            committed = event == Event::committed;
            if (committed) historical_unknown = false;
        }
    }

    void append(Event event, const JournalHook& hook, JournalStep before_sync) {
        detail::require(records < kMaxEvents, "journal event budget exhausted", ErrorCode::resource_limit);
        Bytes record(kEventBytes, 0);
        record[0] = 'J'; record[1] = 'E'; record[2] = 'V'; record[3] = '1'; record[4] = static_cast<uint8_t>(event);
        detail::put_be(record, 8, records + 1, 8);
        std::copy(tail.begin(), tail.end(), record.begin() + 16);
        const auto hash = sha256(Bytes(record.begin(), record.begin() + 48));
        std::copy(hash.begin(), hash.end(), record.begin() + 48);
        try {
            write(events.get(), record, records * kEventBytes);
            notify(hook, before_sync);
            events.sync();
        } catch (...) { poisoned = true; throw; }
        apply(event);
        tail = hash;
        ++records;
    }

    void authorize_dispatch(const JournalHook& hook) {
        try {
            write(floor.get(), dispatch_floor(records, tail));
            notify(hook, JournalStep::before_dispatch_sync);
            floor.sync();
        } catch (...) { poisoned = true; throw; }
    }
};

void JournaledPublisher::prepare(const std::filesystem::path& root, const Digest& scope,
                                 const Manifest& candidate, uint64_t expected_seq, const JournalHook& hook) {
    const auto bytes = intent(scope, candidate, expected_seq);
    const auto path = std::filesystem::absolute(root);
    detail::require(!path.filename().empty() && path.filename() != "." && path.filename() != "..",
                    "a new journal leaf directory is required", ErrorCode::invalid);
    auto parent = directory(path.parent_path());
    if (::mkdirat(parent.get(), path.filename().c_str(), 0700) != 0)
        throw Error(errno == EEXIST ? ErrorCode::conflict : ErrorCode::io, "cannot create new journal directory");
    Fd dir(::openat(parent.get(), path.filename().c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    private_directory(dir.get());
    const auto writer = file(dir.get(), "writer.lock", O_RDWR | O_CREAT | O_EXCL);
    lock(writer.get());
    const auto source = file(dir.get(), "intent.r2j", O_WRONLY | O_CREAT | O_EXCL);
    write(source.get(), bytes);
    notify(hook, JournalStep::before_intent_sync);
    source.sync();
    const auto events = file(dir.get(), "events.r2j", O_RDWR | O_CREAT | O_EXCL);
    events.sync();
    const auto floor = file(dir.get(), "dispatch.r2j", O_RDWR | O_CREAT | O_EXCL);
    write(floor.get(), dispatch_floor(0, sha256(bytes)));
    floor.sync();
    writer.sync();
    dir.sync();
    const auto ready = file(dir.get(), "ready", O_WRONLY | O_CREAT | O_EXCL);
    const std::string marker(kReady);
    write(ready.get(), Bytes(marker.begin(), marker.end()));
    ready.sync();
    dir.sync();
    parent.sync();
}

JournaledPublisher::JournaledPublisher(const std::filesystem::path& root, ObjectStore& store)
    : state_(std::make_unique<State>(root, store)) {}
JournaledPublisher::~JournaledPublisher() = default;

Pointer JournaledPublisher::target() const { return state_->target; }
bool JournaledPublisher::unresolved() const { return state_->pending || state_->historical_unknown || state_->poisoned; }

PublishResult JournaledPublisher::resume(const JournalHook& hook) {
    auto& state = *state_;
    detail::require(!state.poisoned, "reopen journal after local persistence failure", ErrorCode::io);
    if (state.committed) return {PublishOutcome::committed, state.target, true};
    detail::require(state.records + 2 <= kMaxEvents, "journal event budget exhausted", ErrorCode::resource_limit);
    const bool prior_unknown = unresolved();
    state.append(Event::attempt, hook, JournalStep::before_attempt_sync);
    state.authorize_dispatch(hook);
    PublishResult result{PublishOutcome::indeterminate, state.target, false};
    try {
        notify(hook, JournalStep::attempt_durable);
        result = ConditionalPublisher(state.store).publish(state.candidate, state.expected_seq);
    } catch (...) {}
    if (prior_unknown && result.outcome != PublishOutcome::committed) result.outcome = PublishOutcome::indeterminate;
    const auto event = result.outcome == PublishOutcome::committed ? Event::committed :
                       result.outcome == PublishOutcome::not_applied ? Event::not_applied :
                       result.outcome == PublishOutcome::conflict ? Event::conflict : Event::unknown;
    try {
        state.append(event, hook, JournalStep::before_result_sync);
        notify(hook, JournalStep::result_durable);
    } catch (...) {
        state.poisoned = true;
        return {PublishOutcome::indeterminate, state.target, false};
    }
    return result;
}

}  // namespace history_cache
