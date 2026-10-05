#include "history_cache/journal.h"
#include "history_cache/factor_publisher.h"
#include <nlohmann/json.hpp>

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

Pointer factor_target(const Bytes& bytes, const std::string& symbol, const std::string& market,
                      const std::optional<Pointer>& base) {
    const auto parsed = parse_factor_snapshot(bytes, sha256(bytes), symbol, market, 0, FactorReadPolicy::structural_only);
    const auto seq = base ? base->publication_seq : 0;
    detail::require(seq < UINT64_MAX, "factor sequence exhausted", ErrorCode::resource_limit);
    const Pointer target{parsed.source_epoch, seq + 1, "manifests/v1/" + hex(sha256(bytes)) + ".json", sha256(bytes)};
    (void)serialize_pointer(target);
    return target;
}

Bytes intent(const Digest& scope, const Manifest& candidate, uint64_t expected_seq,
             const std::optional<Pointer>& epoch_base) {
    detail::require(scope != Digest{}, "empty journal scope", ErrorCode::invalid);
    (void)make_target(candidate, expected_seq);
    const auto manifest = serialize_manifest(candidate);
    Bytes result{'R', '2', 'J', 'I', '0', '0', '0', '1'};
    result.insert(result.end(), scope.begin(), scope.end());
    result.resize(kIntentHeader);
    detail::put_be(result, 40, expected_seq, 8);
    detail::put_be(result, 48, manifest.size(), 4);
    result.insert(result.end(), manifest.begin(), manifest.end());
    if (epoch_base) {
        detail::require(epoch_base->publication_seq == expected_seq &&
                        epoch_base->dataset_epoch != candidate.dataset_epoch,
                        "invalid journal epoch migration base", ErrorCode::conflict);
        result[7] = '2';
        const auto pointer = serialize_pointer(*epoch_base);
        result.insert(result.end(), pointer.begin(), pointer.end());
    }
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

void prepare_bytes(const std::filesystem::path& root, const Bytes& bytes, const JournalHook& hook);

}  // namespace

struct JournaledPublisher::State {
    ObjectStore& store;
    Fd root, writer, events, floor;
    Manifest candidate;
    Bytes factor_bytes;
    std::string factor_symbol, factor_market;
    std::optional<Pointer> factor_base;
    std::function<int64_t()> now_ms;
    uint64_t expected_seq = 0;
    Pointer target;
    std::optional<Pointer> epoch_base;
    Digest tail{};
    size_t records = 0;
    bool pending = false;
    bool historical_unknown = false;
    bool committed = false;
    bool poisoned = false;

    State(const std::filesystem::path& path, ObjectStore& storage, std::function<int64_t()> clock)
        : store(storage), root(directory(path)), now_ms(std::move(clock)) {
        if (!now_ms) now_ms = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count(); };
        private_directory(root.get());
        writer = file(root.get(), "writer.lock", O_RDWR);
        lock(writer.get());
        (void)read(writer.get(), 0);
        const auto ready = file(root.get(), "ready", O_RDONLY);
        const auto marker = read(ready.get(), 64);
        detail::require(std::string(marker.begin(), marker.end()) == kReady, "journal is not initialized");
        const auto source = file(root.get(), "intent.r2j", O_RDONLY);
        auto bytes = read(source.get(), kMaxMetadataBytes + kMaxPointerBytes + kIntentHeader + 32);
        const std::string magic(bytes.begin(), bytes.begin() + std::min<size_t>(8, bytes.size()));
        detail::require(bytes.size() > kIntentHeader + 32 &&
                        (magic == "R2JI0001" || magic == "R2JI0002" || magic == "R2JI0003"), "invalid journal intent header");
        const auto length = detail::be(bytes, 48, 4);
        detail::require(length <= kMaxMetadataBytes &&
                        (magic != "R2JI0002" ? bytes.size() == kIntentHeader + length + 32 :
                         bytes.size() > kIntentHeader + length + 32 &&
                         bytes.size() <= kIntentHeader + length + kMaxPointerBytes + 32),
                        "truncated journal intent");
        const auto hash = detail::fixed<32>(bytes, bytes.size() - 32);
        tail = sha256(bytes);
        const auto intent_hash = tail;
        bytes.resize(bytes.size() - 32);
        detail::require(sha256(bytes) == hash, "journal intent checksum mismatch");
        detail::require(detail::fixed<32>(bytes, 8) == store.scope_id(), "journal target scope mismatch", ErrorCode::conflict);
        expected_seq = detail::be(bytes, 40, 8);
        if (magic == "R2JI0003") {
            const auto body = nlohmann::json::parse(bytes.begin() + kIntentHeader, bytes.end());
            detail::require(body.is_object() && body.size() == 4 && body.contains("candidate") &&
                body.contains("symbol") && body.contains("market") && body.contains("base"), "invalid factor intent");
            const auto encoded = body.at("candidate").get<std::string>();
            factor_bytes.assign(encoded.begin(), encoded.end());
            factor_symbol = body.at("symbol").get<std::string>();
            factor_market = body.at("market").get<std::string>();
            if (!body.at("base").is_null()) factor_base = parse_pointer(body.at("base").get<std::string>());
            detail::require(expected_seq == (factor_base ? factor_base->publication_seq : 0), "factor base sequence differs");
            target = factor_target(factor_bytes, factor_symbol, factor_market, factor_base);
        } else {
            candidate = parse_manifest(std::string(bytes.begin() + kIntentHeader, bytes.begin() + kIntentHeader + length));
            target = make_target(candidate, expected_seq);
        }
        if (magic == "R2JI0002") {
            epoch_base = parse_pointer(std::string(bytes.begin() + kIntentHeader + length, bytes.end()));
            detail::require(epoch_base->publication_seq == expected_seq &&
                            epoch_base->dataset_epoch != candidate.dataset_epoch,
                            "journal migration base differs", ErrorCode::conflict);
        }
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
                                 const Manifest& candidate, uint64_t expected_seq, const JournalHook& hook,
                                 const std::optional<Pointer>& epoch_base) {
    const auto bytes = intent(scope, candidate, expected_seq, epoch_base);
    prepare_bytes(root, bytes, hook);
}

void JournaledPublisher::prepare_factors(const std::filesystem::path& root, const Digest& scope,
    const Bytes& candidate, const std::string& symbol, const std::string& market,
    const std::optional<Pointer>& base, int64_t now_ms, const JournalHook& hook) {
    detail::require(scope != Digest{}, "empty journal scope", ErrorCode::invalid);
    (void)parse_factor_snapshot(candidate, sha256(candidate), symbol, market, now_ms);
    (void)factor_target(candidate, symbol, market, base);
    const nlohmann::json body = {{"candidate", std::string(candidate.begin(), candidate.end())},
        {"symbol", symbol}, {"market", market}, {"base", base ? nlohmann::json(serialize_pointer(*base)) : nlohmann::json(nullptr)}};
    const auto encoded = body.dump();
    detail::require(encoded.size() <= kMaxMetadataBytes, "factor intent too large", ErrorCode::resource_limit);
    Bytes bytes{'R', '2', 'J', 'I', '0', '0', '0', '3'};
    bytes.insert(bytes.end(), scope.begin(), scope.end());
    bytes.resize(kIntentHeader);
    detail::put_be(bytes, 40, base ? base->publication_seq : 0, 8);
    detail::put_be(bytes, 48, encoded.size(), 4);
    bytes.insert(bytes.end(), encoded.begin(), encoded.end());
    const auto hash = sha256(bytes);
    bytes.insert(bytes.end(), hash.begin(), hash.end());
    prepare_bytes(root, bytes, hook);
}

namespace {
void prepare_bytes(const std::filesystem::path& root, const Bytes& bytes, const JournalHook& hook) {
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
}  // namespace

JournaledPublisher::JournaledPublisher(const std::filesystem::path& root, ObjectStore& store,
                                     std::function<int64_t()> now_ms)
    : state_(std::make_unique<State>(root, store, std::move(now_ms))) {}
JournaledPublisher::~JournaledPublisher() = default;

Pointer JournaledPublisher::target() const { return state_->target; }
std::optional<Pointer> JournaledPublisher::epoch_base() const { return state_->epoch_base; }
bool JournaledPublisher::unresolved() const { return state_->pending || state_->historical_unknown || state_->poisoned; }
bool JournaledPublisher::committed() const { return state_->committed; }

PublishResult JournaledPublisher::resume(const JournalHook& hook, bool retry_fresh_factors) {
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
        result = state.factor_bytes.empty()
            ? ConditionalPublisher(state.store).publish(state.candidate, state.expected_seq, state.epoch_base)
            : publish_factors(state.store, state.factor_bytes, state.factor_symbol, state.factor_market,
                              state.factor_base, state.now_ms, prior_unknown && !retry_fresh_factors);
    } catch (const Error& error) {
        result.error_code = error.code();
    } catch (...) {
        result.error_code = ErrorCode::invalid;
    }
    if (result.outcome == PublishOutcome::conflict) result.error_code = ErrorCode::conflict;
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
