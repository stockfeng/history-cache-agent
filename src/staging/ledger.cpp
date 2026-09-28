#include "internal.h"

#include <cerrno>
#include <sys/file.h>

namespace history_cache::staging {
namespace {

class Fd {
public:
    explicit Fd(int value = -1) : value_(value) {}
    ~Fd() { if (value_ >= 0) ::close(value_); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : value_(std::exchange(other.value_, -1)) {}
    Fd& operator=(Fd&& other) noexcept {
        if (this != &other) { if (value_ >= 0) ::close(value_); value_ = std::exchange(other.value_, -1); }
        return *this;
    }
    int get() const { return value_; }
    void sync() const { require(::fsync(value_) == 0, "staging fsync failed", ErrorCode::io); }
private:
    int value_;
};

Fd directory(const std::filesystem::path& input) {
    Fd root(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    require(root.get() >= 0, "cannot open staging path root", ErrorCode::io);
    for (const auto& part : std::filesystem::absolute(input).relative_path()) {
        require(part != "..", "staging path traversal", ErrorCode::invalid);
        if (part.empty() || part == ".") continue;
        Fd next(::openat(root.get(), part.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        require(next.get() >= 0, "staging directory missing or unsafe", ErrorCode::invalid);
        root = std::move(next);
    }
    return root;
}

void owner_directory(int fd) {
    struct stat info{};
    require(::fstat(fd, &info) == 0 && S_ISDIR(info.st_mode) && info.st_uid == ::geteuid() &&
            (info.st_mode & 07777) == 0700, "owned 0700 staging directory required", ErrorCode::invalid);
}

Fd file(int root, const std::string& name, int flags, bool private_file = true) {
    require(!name.empty() && name != "." && name != ".." && name.find('/') == std::string::npos, "invalid staging file name");
    const auto fd = ::openat(root, name.c_str(), flags | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC, 0600);
    require(fd >= 0, "cannot open staging file", ErrorCode::io);
    Fd result(fd);
    struct stat info{};
    require(::fstat(fd, &info) == 0 && S_ISREG(info.st_mode) && info.st_nlink == 1 &&
            (!private_file || (info.st_uid == ::geteuid() && (info.st_mode & 07777) == 0600)),
            "unsafe staging file", ErrorCode::invalid);
    return result;
}

Bytes read(int fd, uint64_t limit) {
    struct stat info{};
    require(::fstat(fd, &info) == 0 && info.st_size >= 0 && static_cast<uint64_t>(info.st_size) <= limit,
            "staging file exceeds limit", ErrorCode::resource_limit);
    Bytes result(static_cast<size_t>(info.st_size));
    size_t done = 0;
    while (done < result.size()) {
        const auto count = ::pread(fd, result.data() + done, result.size() - done, static_cast<off_t>(done));
        if (count < 0 && errno == EINTR) continue;
        require(count > 0, "short staging file read", ErrorCode::io);
        done += static_cast<size_t>(count);
    }
    return result;
}

void write(int fd, const Bytes& data, uint64_t offset = 0) {
    size_t done = 0;
    while (done < data.size()) {
        const auto count = ::pwrite(fd, data.data() + done, data.size() - done, static_cast<off_t>(offset + done));
        if (count < 0 && errno == EINTR) continue;
        require(count > 0, "staging file write failed", ErrorCode::io);
        done += static_cast<size_t>(count);
    }
}

void lock(int fd) { require(::flock(fd, LOCK_EX | LOCK_NB) == 0, "staging run already locked", ErrorCode::conflict); }

Fd new_directory(const std::filesystem::path& input) {
    const auto path = std::filesystem::absolute(input);
    auto parent = directory(path.parent_path());
    const auto name = path.filename();
    require(!name.empty() && name != "." && name != "..", "new staging leaf required");
    require(::mkdirat(parent.get(), name.c_str(), 0700) == 0, "staging leaf already exists or is unavailable", ErrorCode::conflict);
    auto child = directory(path);
    owner_directory(child.get());
    child.sync(); parent.sync();
    return child;
}

Bytes floor_record(uint64_t records, const Digest& hash) {
    Bytes result(80, 0);
    result[0] = 'S'; result[1] = 'F'; result[2] = 'L'; result[3] = '1';
    detail::put_be(result, 8, records, 8);
    std::copy(hash.begin(), hash.end(), result.begin() + 16);
    const auto digest = sha256(Bytes(result.begin(), result.begin() + 48));
    std::copy(digest.begin(), digest.end(), result.begin() + 48);
    return result;
}

}  // namespace

Bytes read_input(const std::filesystem::path& input, uint64_t limit) {
    const auto path = std::filesystem::absolute(input);
    auto parent = directory(path.parent_path());
    auto source = file(parent.get(), path.filename().string(), O_RDONLY, false);
    return read(source.get(), limit);
}

struct Ledger::State {
    Fd root, writer, log, floor;
    Json intent, events = Json::array();
    LedgerHook hook;
    Digest tail{};
    uint64_t offset = 0;
    TransferUsage usage;
    std::map<std::string, Owned> objects;
    std::map<std::string, Json> planned_objects;
    std::optional<Json> pending;
    uint64_t started_seconds = 0, review_due_seconds = 0;
    bool poisoned = false, finished = false, successful = false;

    void call(LedgerStep step) const { if (hook) hook(step); }
    void open_files() {
        owner_directory(root.get());
        writer = file(root.get(), "run.lock", O_RDWR); lock(writer.get()); (void)read(writer.get(), 0);
        log = file(root.get(), "events.r2s", O_RDWR);
        floor = file(root.get(), "dispatch.r2s", O_RDWR);
    }
    void initialize_objects() {
        for (const auto& scenario : intent.at("scenarios")) {
            const std::string prefix = scenario.at("namespace");
            if (retains_objects(intent)) {
                require(scenario.at("cleanup").empty(), "retention profile forbids cleanup");
                for (const auto& object : scenario.at("objects")) {
                    const auto key = prefix + object.at("relative_key").get<std::string>();
                    require(objects.emplace(key, Owned{key, {}, {}, false, false, false, false}).second, "duplicate retained key");
                    (void)number(object.at("max_body_bytes"), 65536);
                    (void)parse_digest(object.at("final_body_sha256").get<std::string>());
                    planned_objects.emplace(key, object);
                }
                continue;
            }
            for (const auto& step : scenario.at("cleanup")) {
                if (step.at("method") != "DELETE") continue;
                const auto key = prefix + step.at("relative_key").get<std::string>();
                objects.emplace(key, Owned{key, {}, {}, false, false, false, false});
            }
        }
        require(objects.size() == 9, "invalid run ownership set");
        if (retains_objects(intent)) {
            uint64_t stored = 0;
            for (const auto& [key, object] : planned_objects) { (void)key; stored += object.at("max_body_bytes").get<uint64_t>(); }
            require(stored == intent.at("retention").at("planned_body_bytes") &&
                    stored <= intent.at("config").at("budget").at("max_stored_body_bytes").get<uint64_t>() &&
                    intent.at("config").at("retention").at("max_objects") == objects.size(), "retained object budget mismatch");
        }
    }
    void apply(const Json& event) {
        require(!finished, "events after run completion");
        const auto type = event.at("type").get<std::string>();
        if (type == "started") {
            require(retains_objects(intent) && events.empty(), "unexpected staging start event");
            fields(event, {"type", "unix_seconds", "review_due_unix_seconds"});
            started_seconds = number(event.at("unix_seconds"), UINT64_MAX - 7 * 24 * 3600);
            review_due_seconds = event.at("review_due_unix_seconds").get<uint64_t>();
            require(review_due_seconds == started_seconds + number(intent.at("config").at("retention").at("review_after_seconds"), 7 * 24 * 3600),
                    "retention review deadline mismatch");
        } else if (type == "request") {
            require(!pending && event.at("index") == usage.requests + 1, "invalid request ledger ordering");
            const auto key = event.at("key").get<std::string>();
            require(objects.count(key) != 0, "request outside ownership set");
            if (retains_objects(intent)) {
                require(started_seconds != 0 && (event.at("method") == "GET" || event.at("method") == "PUT") &&
                        event.at("phase") == "business", "retention profile forbids non-business or DELETE requests");
                require(event.at("upload_bytes").get<uint64_t>() <= planned_objects.at(key).at("max_body_bytes").get<uint64_t>(),
                        "retained write exceeds object budget");
            }
            const auto upload = event.at("upload_bytes").get<uint64_t>();
            const auto download = event.at("download_reserved_bytes").get<uint64_t>();
            const auto& budget = intent.at("config").at("budget");
            require(usage.requests < budget.at("max_reserved_requests").get<uint64_t>() &&
                    upload <= budget.at("max_upload_body_bytes").get<uint64_t>() - usage.upload_reserved &&
                    download <= budget.at("max_download_reserved_bytes").get<uint64_t>() - usage.download_reserved,
                    "aggregate staging budget exhausted", ErrorCode::resource_limit);
            ++usage.requests; usage.upload_reserved += upload; usage.download_reserved += download;
            pending = event;
        } else if (type == "result") {
            require(pending && event.at("index") == usage.requests, "result without staging request");
            const auto key = pending->at("key").get<std::string>();
            auto& object = objects.at(key);
            const auto method = pending->at("method").get<std::string>();
            const auto delivery = event.at("delivery").get<std::string>();
            const unsigned status = event.at("status").get<unsigned>();
            const bool verified = event.at("verified").get<bool>();
            if (method == "PUT" && verified && status == 200) {
                const bool create = pending->at("condition") == "create";
                if (create && !object.owned) object.owned = true;
                if (object.owned) {
                    object.etag = event.at("etag").get<std::string>();
                    object.sha256 = pending->at("upload_sha256").get<std::string>();
                    object.body_bytes = pending->at("upload_bytes").get<uint64_t>();
                    object.absent = false;
                }
            } else if (method == "DELETE" && verified && status == 204) {
                require(object.owned, "delete without ownership"); object.deleted = true;
            } else if ((method == "PUT" || method == "DELETE") && delivery != "not_sent" && !(verified && status == 412)) {
                object.uncertain = true;
            }
            if (method == "GET" && verified && status == 404) object.absent = true;
            if (method == "GET" && verified && (status == 200 || status == 206)) {
                object.absent = false;
                if (retains_objects(intent) && object.owned && (event.at("etag") != object.etag ||
                    (status == 200 && event.at("response_sha256") != object.sha256))) object.identity_changed = true;
            }
            if (method == "PUT" && pending->at("condition") == "create" && verified && status == 412) object.absent = false;
            object.touched = true;
            pending.reset();
        } else if (type == "finished") {
            require(!pending, "cannot finish with pending dispatch");
            successful = event.at("success").get<bool>();
            if (retains_objects(intent) && successful) {
                require(started_seconds != 0, "retention start missing");
                for (const auto& [key, object] : objects)
                    require(object.owned && !object.absent && !object.deleted && !object.uncertain && !object.identity_changed &&
                            object.sha256 == planned_objects.at(key).at("final_body_sha256"), "successful run lacks retained ownership");
            }
            finished = true;
        } else throw Error(ErrorCode::corrupt, "invalid staging ledger event");
        events.push_back(event);
    }
    void append(const Json& event, LedgerStep before_sync, bool dispatch) {
        require(!poisoned && events.size() < 512, "staging ledger unavailable", ErrorCode::resource_limit);
        const auto payload = canonical(event);
        require(payload.size() <= 4096, "staging event too large");
        const auto encoded = bytes(payload);
        Bytes record(44, 0);
        record[0] = 'S'; record[1] = 'E'; record[2] = 'V'; record[3] = '1';
        detail::put_be(record, 4, encoded.size(), 4);
        detail::put_be(record, 8, events.size() + 1, 4);
        std::copy(tail.begin(), tail.end(), record.begin() + 12);
        record.insert(record.end(), encoded.begin(), encoded.end());
        const auto hash = sha256(record);
        record.insert(record.end(), hash.begin(), hash.end());
        try {
            // Validate the transition before permitting a durable request.
            const auto saved_usage = usage;
            const auto saved_pending = pending;
            if (event.at("type") == "request") {
                apply(event); events.erase(events.size() - 1); usage = saved_usage; pending = saved_pending;
            }
            write(log.get(), record, offset); call(before_sync); log.sync();
            if (dispatch) { write(floor.get(), floor_record(events.size() + 1, hash)); call(LedgerStep::before_dispatch_sync); floor.sync(); }
            apply(event); offset += record.size(); tail = hash;
        } catch (...) { poisoned = true; throw; }
    }
};

Ledger::Ledger(const std::filesystem::path& path, const Json& intent, const LedgerHook& hook)
    : state_(std::make_unique<State>()) {
    auto& s = *state_; s.intent = intent; s.hook = hook; s.root = new_directory(path);
    auto writer = file(s.root.get(), "run.lock", O_RDWR | O_CREAT | O_EXCL); lock(writer.get()); writer.sync();
    auto source = file(s.root.get(), "intent.json", O_WRONLY | O_CREAT | O_EXCL);
    const auto raw = bytes(canonical(intent)); write(source.get(), raw); s.call(LedgerStep::before_intent_sync); source.sync();
    auto log = file(s.root.get(), "events.r2s", O_RDWR | O_CREAT | O_EXCL); log.sync();
    auto floor = file(s.root.get(), "dispatch.r2s", O_RDWR | O_CREAT | O_EXCL);
    s.tail = sha256(raw); write(floor.get(), floor_record(0, s.tail)); floor.sync();
    s.root.sync(); auto parent = directory(std::filesystem::absolute(path).parent_path()); parent.sync();
    s.writer = std::move(writer); s.log = std::move(log); s.floor = std::move(floor); s.initialize_objects();
    if (retains_objects(intent)) {
        const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        require(now > 0, "invalid local retention clock");
        const auto start = static_cast<uint64_t>(now);
        s.append({{"type", "started"}, {"unix_seconds", start},
                  {"review_due_unix_seconds", start + intent.at("config").at("retention").at("review_after_seconds").get<uint64_t>()}},
                 LedgerStep::before_start_sync, false);
    }
}

Ledger::Ledger(const std::filesystem::path& path) : state_(std::make_unique<State>()) {
    auto& s = *state_; s.root = directory(path); s.open_files();
    auto source = file(s.root.get(), "intent.json", O_RDONLY);
    const auto raw = read(source.get(), 256 * 1024);
    s.intent = parse(text(raw), 256 * 1024); require(canonical(s.intent) == text(raw), "noncanonical staging intent");
    s.tail = sha256(raw); s.initialize_objects();
    const auto content = read(s.log.get(), 512 * 4172);
    std::vector<Digest> hashes{s.tail};
    std::vector<bool> dispatches{false};
    while (s.offset < content.size()) {
        require(content.size() - s.offset >= 76, "torn staging event");
        const auto position = static_cast<size_t>(s.offset);
        const auto size = detail::be(content, position + 4, 4);
        require(size > 0 && size <= 4096 && size + 76 <= content.size() - s.offset &&
                detail::be(content, position, 4) == 0x53455631 && detail::be(content, position + 8, 4) == s.events.size() + 1 &&
                detail::fixed<32>(content, position + 12) == s.tail && s.events.size() < 512, "corrupt staging event");
        const auto end = position + 44 + static_cast<size_t>(size);
        const auto hash = sha256(Bytes(content.begin() + static_cast<std::ptrdiff_t>(position), content.begin() + static_cast<std::ptrdiff_t>(end)));
        require(hash == detail::fixed<32>(content, end), "staging event checksum mismatch");
        const auto payload = std::string(content.begin() + static_cast<std::ptrdiff_t>(position + 44), content.begin() + static_cast<std::ptrdiff_t>(end));
        const auto event = parse(payload, 4096); require(canonical(event) == payload, "noncanonical staging event");
        s.apply(event); s.tail = hash; s.offset = end + 32;
        hashes.push_back(hash); dispatches.push_back(event.at("type") == "request");
    }
    const auto floor = read(s.floor.get(), 80); require(floor.size() == 80, "truncated staging dispatch floor");
    const auto count = detail::be(floor, 8, 8);
    require(count < hashes.size() && floor == floor_record(count, hashes.at(static_cast<size_t>(count))) &&
            (count == 0 || dispatches.at(static_cast<size_t>(count))), "lost staging dispatch suffix");
    s.log.sync(); s.floor.sync(); source.sync(); s.root.sync();
}

Ledger::~Ledger() = default;
void Ledger::request(const Json& event) { state_->append(event, LedgerStep::before_request_sync, true); }
void Ledger::result(const Json& event) { state_->append(event, LedgerStep::before_result_sync, false); }
void Ledger::finish(const Json& event) { state_->append(event, LedgerStep::before_finish_sync, false); }
bool Ledger::poisoned() const { return state_->poisoned; }
const TransferUsage& Ledger::usage() const { return state_->usage; }

Json Ledger::report() const {
    const auto& s = *state_;
    Json objects = Json::array();
    for (const auto& [key, value] : s.objects) {
        const bool pending = s.pending && s.pending->at("key") == key && s.pending->at("method") != "GET";
        objects.push_back({{"key", key}, {"owned", value.owned}, {"deleted_ack", value.deleted},
                           {"absence_verified", value.absent && !value.uncertain && !pending},
                           {"uncertain", value.uncertain || pending}, {"last_etag", value.etag},
                           {"last_body_sha256", value.sha256}});
    }
    Json report{{"schema_version", 1}, {"intent_sha256", hex(sha256(canonical(s.intent)))},
            {"status", s.poisoned ? "INDETERMINATE_LOCAL_PERSISTENCE" : s.finished ? s.successful ? "PASS" : "FAIL" : "INTERRUPTED_NO_REEXECUTION"},
            {"usage", {{"reserved_requests", s.usage.requests}, {"upload_body_bytes", s.usage.upload_reserved},
                       {"download_reserved_bytes", s.usage.download_reserved}}},
            {"objects", objects}, {"events", s.events}, {"pending_request", s.pending ? *s.pending : Json(nullptr)},
            {"adapter_usage", s.finished ? s.events.back().value("adapter_usage", Json(nullptr)) : Json(nullptr)},
            {"durable_budget_matches_adapter", s.finished ? s.events.back().value("durable_budget_matches_adapter", Json(nullptr)) : Json(nullptr)},
            {"automatic_resume_supported", false}, {"billing_guarantee", false}};
    if (!retains_objects(s.intent)) return report;
    report["schema_version"] = 2;
    report["profile"] = s.intent.at("profile");
    if (s.finished && !s.poisoned) report["status"] = s.successful ? "PASS_STAGING_RETAINED" : "FAIL_STAGING_RETAINED";
    Json counts = Json::object();
    for (const auto* name : {"retained_owned", "unknown_write", "identity_changed", "observed_absent", "unowned_observed", "unobserved"}) counts[name] = 0;
    uint64_t acknowledged_bytes = 0;
    for (auto& object : report["objects"]) {
        const auto key = object.at("key").get<std::string>();
        const auto& value = s.objects.at(key);
        const bool unknown = object.at("uncertain").get<bool>();
        const bool absent = object.at("absence_verified").get<bool>();
        const std::string disposition = unknown ? "unknown_write" : value.identity_changed ? "identity_changed" :
            absent ? "observed_absent" : value.owned ? "retained_owned" : value.touched ? "unowned_observed" : "unobserved";
        counts[disposition] = counts[disposition].get<uint64_t>() + 1;
        object["retention_state"] = disposition;
        object["identity_changed"] = value.identity_changed;
        object["last_acknowledged_body_bytes"] = value.body_bytes;
        object["planned_max_body_bytes"] = s.planned_objects.at(key).at("max_body_bytes");
        object["role"] = s.planned_objects.at(key).at("role");
        if (disposition == "retained_owned") acknowledged_bytes += value.body_bytes;
    }
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    report["retention"] = {{"policy", s.intent.at("config").at("retention")}, {"state_counts", counts},
        {"planned_objects", 9}, {"planned_body_bytes", s.intent.at("retention").at("planned_body_bytes")},
        {"retained_owned_body_bytes", acknowledged_bytes}, {"inventory_is_current_cloud_state", false},
        {"started_unix_seconds", s.started_seconds ? Json(s.started_seconds) : Json(nullptr)},
        {"review_due_unix_seconds", s.review_due_seconds ? Json(s.review_due_seconds) : Json(nullptr)},
        {"review_overdue_by_local_clock", s.review_due_seconds && now > 0 ? Json(static_cast<uint64_t>(now) >= s.review_due_seconds) : Json(nullptr)},
        {"automatic_delete", false}, {"bucket_lifecycle_modified", false}, {"cleanup_performed", false},
        {"cleanup_requires_separate_authorization", true}, {"review_deadline_is_not_object_expiry", true}};
    const auto capability = [&](const std::string& step_id) {
        Json observations = Json::array();
        bool passed = false, failed = false;
        std::optional<Json> request;
        for (const auto& event : s.events) {
            if (event.at("type") == "request") request = event;
            else if (event.at("type") == "result" && request) {
                if (request->at("step") == step_id && event.at("backend_dispatched").get<bool>()) {
                    const bool complete = event.at("delivery") == "complete" && event.at("verified").get<bool>();
                    const bool matches = complete && event.at("expected_response").get<bool>();
                    passed = passed || matches; failed = failed || !matches;
                    observations.push_back({{"request_index", request->at("index")}, {"status", event.at("status")},
                        {"verified_response", complete}, {"matches_plan", matches}});
                }
                request.reset();
            }
        }
        return Json{{"state", failed ? "observed_failure_or_indeterminate" : passed ? "observed_match" : "not_observed"},
                    {"observations", observations}};
    };
    report["capabilities"] = {{"evidence_scope", "this_run_recorded_transport_responses_only"},
        {"endpoint", s.intent.at("endpoint")}, {"bucket", s.intent.at("config").at("bucket")},
        {"general_endpoint_verification", false}, {"create_only_pack", capability("create-pack")},
        {"create_only_pack_conflict", capability("duplicate-pack-create-only")},
        {"create_only_manifest", capability("publish-1-create-manifest")},
        {"create_only_manifest_conflict", capability("publish-2-create-manifest")},
        {"pointer_create", capability("publish-1-write-pointer")}, {"pointer_cas", capability("publish-2-write-pointer")},
        {"stale_pointer_cas_rejected", capability("stale-etag-cannot-write-seq-3")},
        {"range_header_206", capability("read-rows-header")},
        {"pinned_range_206", capability("read-rows-toc")},
        {"conditional_delete", {{"state", "not_tested_not_required"}, {"verified", false}}},
        {"publication_read_profile_completed", s.finished && s.successful && !s.poisoned}};
    return report;
}

Json inspect_run(const std::filesystem::path& root) { return Ledger(root).report(); }

void register_namespaces(const std::filesystem::path& path, const Json& intent) {
    auto root = directory(path); owner_directory(root.get());
    auto writer = file(root.get(), "registry.lock", O_RDWR | O_CREAT); lock(writer.get()); (void)read(writer.get(), 0);
    for (const auto& scenario : intent.at("scenarios")) {
        const auto scope = scenario.at("scope_sha256").get<std::string>(); (void)parse_digest(scope);
        auto marker = file(root.get(), scope + ".used", O_WRONLY | O_CREAT | O_EXCL);
        write(marker.get(), bytes(canonical(Json{{"scope_sha256", scope}, {"intent_sha256", hex(sha256(canonical(intent)))}})));
        marker.sync(); root.sync();
    }
    writer.sync(); root.sync();
}

void write_report(const std::filesystem::path& path, const Json& report) {
    auto root = directory(path); owner_directory(root.get());
    auto output = file(root.get(), "report.json", O_WRONLY | O_CREAT | O_EXCL);
    write(output.get(), bytes(canonical(report))); output.sync(); root.sync();
}

}  // namespace history_cache::staging
