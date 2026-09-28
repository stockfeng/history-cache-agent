#include "internal.h"

#include "history_cache/journal.h"
#include "history_cache/reader.h"

#include <mutex>

namespace history_cache::staging {

bool no_such_key(const HttpResponse& response) {
    if (response.status != 404 || response.body.size() > 4096) return false;
    auto body = text(response.body);
    const auto trim = [&] {
        const auto first = body.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) { body.clear(); return; }
        body = body.substr(first, body.find_last_not_of(" \t\r\n") - first + 1);
    };
    trim();
    if (body.rfind("<?xml ", 0) == 0) {
        const auto end = body.find("?>");
        if (end == std::string::npos || end > 128 || body.substr(1, end - 1).find('<') != std::string::npos) return false;
        body.erase(0, end + 2); trim();
    }
    if (body.size() < 15 || body.rfind("<Error>", 0) != 0 || body.substr(body.size() - 8) != "</Error>") return false;
    body = body.substr(7, body.size() - 15);
    std::set<std::string> seen;
    bool missing = false;
    while (true) {
        trim(); if (body.empty()) return missing;
        const auto end = body.find('>'); if (body.front() != '<' || end == std::string::npos) return false;
        const auto name = body.substr(1, end - 1);
        if (name != "Code" && name != "Message" && name != "Key" && name != "RequestId" && name != "HostId" &&
            name != "Resource" && name != "BucketName") return false;
        if (!seen.insert(name).second) return false;
        const auto next = body.find('<', end + 1); const auto close = "</" + name + '>';
        if (next == std::string::npos || body.compare(next, close.size(), close) != 0) return false;
        if (name == "Code") missing = body.substr(end + 1, next - end - 1) == "NoSuchKey";
        body.erase(0, next + close.size());
    }
}

namespace {

std::string header(const HttpResponse& response, const std::string& name) {
    const auto found = response.headers.find(name);
    require(found != response.headers.end(), "missing staging response header");
    return found->second;
}

bool verified(const HttpResult& result, const HttpRequest& request) {
    try {
        require(result.delivery == HttpDelivery::complete && result.response.tls_verified, "incomplete staging response");
        const auto& response = result.response;
        require(response.status >= 200 && response.status <= 599 &&
                response.body.size() <= (response.status >= 300 ? 4096 : request.response_limit), "response exceeds request limit");
        if (response.headers.count("content-length")) require(http_unsigned(header(response, "content-length")) == response.body.size(), "response length mismatch");
        if (response.headers.count("transfer-encoding")) require(header(response, "transfer-encoding") == "chunked" &&
                                                               !response.headers.count("content-length"), "ambiguous response framing");
        if (response.headers.count("content-encoding")) require(header(response, "content-encoding") == "identity", "encoded staging response");
        if (response.headers.count("age")) require(http_unsigned(header(response, "age")) == 0, "cached staging response");
        if (response.status == 404) require(no_such_key(response), "not a missing key response");
        if ((response.status == 200 || response.status == 206) && request.method != HttpMethod::erase) validate_etag(header(response, "etag"));
        if ((request.method != HttpMethod::get || !request.headers.count("range")) && response.status < 300)
            require(!response.headers.count("content-range"), "unexpected partial response");
        return true;
    } catch (...) { return false; }
}

class Gate final : public HttpTransport {
public:
    Gate(Ledger& ledger, const Scenario& scenario, std::shared_ptr<HttpTransport> backend, SteadyClock::time_point total_deadline)
        : ledger_(ledger), scenario_(scenario), backend_(std::move(backend)), total_deadline_(total_deadline) {
        const auto& c = scenario.config;
        base_ = "https://" + c.account_id + (c.jurisdiction == "eu" ? ".eu" : "") + ".r2.cloudflarestorage.com/" + c.bucket + '/' + c.key_prefix;
        for (const auto& object : scenario.objects) {
            owned_.emplace(object.key, Owned{object.key, {}, {}, false, false, false, false});
        }
    }
    bool healthy() const { return healthy_ && !ledger_.poisoned(); }
    bool complete() const { return healthy() && cursor_ == scenario_.business.size() && !optional_; }
    bool retained_complete() const {
        return complete() && std::all_of(scenario_.objects.begin(), scenario_.objects.end(), [&](const auto& planned) {
            const auto& value = owned_.at(planned.key);
            return value.owned && !value.absent && !value.deleted && !value.uncertain && !value.identity_changed &&
                   value.sha256 == planned.final_sha256;
        });
    }
    bool uncertain() const {
        return ledger_.poisoned() || std::any_of(owned_.begin(), owned_.end(), [](const auto& item) { return item.second.uncertain; });
    }
    void cleanup_mode() { cleanup_ = true; optional_.reset(); cursor_ = 0; }
    void skip_cleanup_object() { cursor_ += 3; }
    const Owned& object(const std::string& key) const { return owned_.at(key); }
    bool safe_dependencies() const {
        const auto& pointer = owned_.at("current.json");
        return pointer.absent && !uncertain();
    }
    std::string etag(const std::string& name) const { return etags_.at(name); }

    HttpResult perform(const HttpRequest& request) override {
        std::lock_guard<std::mutex> guard(mutex_);
        if (ledger_.poisoned()) return {HttpDelivery::not_sent, {}};
        if (scenario_.retain_objects && (cleanup_ || request.method == HttpMethod::erase)) {
            healthy_ = false; return {HttpDelivery::not_sent, {}};
        }
        const bool stopped = !cleanup_ && !healthy_ && !optional_;
        const Step* step = nullptr;
        bool extra = false;
        if (optional_) { step = &*optional_; extra = true; }
        else {
            const auto& steps = cleanup_ ? scenario_.cleanup : scenario_.business;
            if (cursor_ >= steps.size()) { healthy_ = false; return {HttpDelivery::not_sent, {}}; }
            step = &steps[cursor_];
        }
        const Step expected = *step;
        try {
            require(request.method == expected.method && request.url == base_ + expected.key && request.body == expected.body &&
                    request.response_limit == expected.limit, "request is not the next approved staging operation");
            HttpHeaders actual;
            for (const auto* name : {"range", "if-match", "if-none-match"}) {
                const auto found = request.headers.find(name); if (found != request.headers.end()) actual[name] = found->second;
            }
            auto headers = expected.headers;
            for (auto& [name, value] : headers) { (void)name; if (!value.empty() && value.front() == '@') value = etags_.at(value.substr(1)); }
            require(actual == headers && request.headers.count("authorization") && request.cancelled,
                    "staging conditional gate failed");
            if (cleanup_) {
                const auto& object = owned_.at(expected.key);
                require(object.owned && !object.uncertain && !uncertain(), "cleanup ownership or quiescence unproven");
                if (expected.key != "current.json") require(safe_dependencies(), "pointer absence required before deleting dependencies");
            }
        } catch (...) { healthy_ = false; return {HttpDelivery::not_sent, {}}; }
        const auto method = method_name(request.method);
        const auto condition = request.headers.count("if-none-match") ? "create" : request.headers.count("if-match") ? "etag" : "none";
        const auto index = ledger_.usage().requests + 1;
        HttpHeaders conditions;
        for (const auto* name : {"if-match", "if-none-match", "range"}) {
            if (request.headers.count(name)) conditions.emplace(name, request.headers.at(name));
        }
        const auto expired = [&] { return request.cancelled->load() || SteadyClock::now() >= request.deadline ||
                                         SteadyClock::now() >= total_deadline_; };
        const bool local_block = stopped || expected.suppress_dispatch || expired();
        try {
            ledger_.request({{"type", "request"}, {"index", index}, {"scenario", scenario_.name}, {"step", expected.id},
                             {"phase", cleanup_ ? "cleanup" : "business"}, {"key", scenario_.config.key_prefix + expected.key},
                             {"method", method}, {"condition", condition}, {"upload_bytes", request.body.size()},
                             {"conditional_headers", conditions}, {"response_limit", request.response_limit},
                             {"upload_sha256", hex(sha256(request.body))}, {"download_reserved_bytes", std::max<uint64_t>(4096, request.response_limit)},
                             {"local_no_dispatch", local_block}});
        } catch (...) { healthy_ = false; return {HttpDelivery::not_sent, {}}; }
        HttpResult result;
        // Persistence can outlast a request deadline. Never dispatch on a stale
        // pre-fsync timing check, even when an injected backend ignores deadlines.
        const bool dispatched = !local_block && !expired();
        if (!dispatched) result = {HttpDelivery::not_sent, {}};
        else {
            try { result = backend_->perform(request); }
            catch (...) { result = {HttpDelivery::indeterminate, {}}; }
        }
        const bool verified_result = verified(result, request);
        const auto status = result.response.status;
        auto& object = owned_.at(expected.key);
        std::string response_etag;
        if (verified_result && result.response.headers.count("etag")) response_etag = header(result.response, "etag");
        if (request.method == HttpMethod::put && verified_result && status == 200) {
            if (condition == std::string("create")) object.owned = true;
            if (object.owned) { object.etag = response_etag; object.sha256 = hex(sha256(request.body)); object.absent = false; }
        } else if (request.method == HttpMethod::erase && verified_result && status == 204) object.deleted = true;
        else if (request.method != HttpMethod::get && result.delivery != HttpDelivery::not_sent && !(verified_result && status == 412))
            object.uncertain = true;
        if (request.method == HttpMethod::get && verified_result) {
            if (scenario_.retain_objects && object.owned && (status == 200 || status == 206) &&
                (response_etag != object.etag || (status == 200 && hex(sha256(result.response.body)) != object.sha256)))
                object.identity_changed = true;
            if (status == 404) object.absent = true;
            else if (status == 200 || status == 206) object.absent = false;
        }
        if (request.method == HttpMethod::put && condition == std::string("create") && verified_result && status == 412)
            object.absent = false;
        bool matches = (!stopped && expected.suppress_dispatch) || (verified_result && status == expected.status);
        if (scenario_.retain_objects && object.identity_changed) matches = false;
        try {
            if (matches && !expected.suppress_dispatch) {
                if (expected.response_hash) require(sha256(result.response.body) == *expected.response_hash, "staging response hash mismatch");
                if (!expected.content_range.empty()) require(header(result.response, "content-range") == expected.content_range &&
                                                            result.response.body.size() == expected.limit, "staging Range mismatch");
                if (!cleanup_ && expected.headers.count("range") && expected.headers.count("if-match"))
                    require(response_etag == request.headers.at("if-match"), "Range identity changed");
                if (cleanup_ && request.method == HttpMethod::get && status == 200)
                    require(response_etag == object.etag && hex(sha256(result.response.body)) == object.sha256, "owned object identity changed");
                if (!expected.save_etag.empty() && (status == 200 || status == 206)) etags_[expected.save_etag] = response_etag;
            }
        } catch (...) { matches = false; }
        try {
            ledger_.result({{"type", "result"}, {"index", index}, {"delivery", result.delivery == HttpDelivery::complete ? "complete" :
                              result.delivery == HttpDelivery::not_sent ? "not_sent" : "indeterminate"},
                            {"status", status}, {"verified", verified_result}, {"expected_response", matches},
                            {"backend_dispatched", dispatched},
                            {"etag", response_etag}, {"response_bytes", result.response.body.size()},
                            {"response_sha256", hex(sha256(result.response.body))},
                            {"ack_suppressed_locally", matches && expected.suppress_ack}});
        } catch (...) { healthy_ = false; object.uncertain = request.method != HttpMethod::get || object.uncertain; return {HttpDelivery::indeterminate, {}}; }
        if (extra) optional_.reset(); else ++cursor_;
        if (!matches) {
            healthy_ = false;
            if (!cleanup_ && scenario_.recovery.count(expected.id)) optional_ = scenario_.recovery.at(expected.id);
        }
        if (expected.suppress_ack && matches) return {HttpDelivery::indeterminate, {}};
        if (!matches && request.method == HttpMethod::get) return {HttpDelivery::indeterminate, {}};
        return result;
    }
private:
    Ledger& ledger_;
    const Scenario& scenario_;
    std::shared_ptr<HttpTransport> backend_;
    SteadyClock::time_point total_deadline_;
    std::string base_;
    std::map<std::string, Owned> owned_;
    std::map<std::string, std::string> etags_;
    std::optional<Step> optional_;
    size_t cursor_ = 0;
    bool healthy_ = true, cleanup_ = false;
    std::mutex mutex_;
};

class ReadOnlyResume final : public ObjectStore {
public:
    ReadOnlyResume(ObjectStore& store, const Pointer& target) : store_(store), target_(target) {}
    Digest scope_id() const override { return store_.scope_id(); }
    std::optional<VersionedObject> read_current() const override {
        require(reads_++ == 0, "duplicate recovery current read");
        auto result = store_.read_current();
        exact_ = result && text(result->bytes) == serialize_pointer(target_);
        return result;
    }
    Bytes get(const std::string& key, uint64_t max) const override {
        require(exact_ && !manifest_read_ && key == target_.manifest_key && max == kMaxMetadataBytes, "unapproved recovery manifest");
        manifest_read_ = true; return store_.get(key, max);
    }
    RangeReader open_range(const std::string&, uint64_t) const override { throw Error(ErrorCode::io, "read-only recovery forbids Range"); }
    WriteOutcome create(const std::string&, const Bytes&) override { throw Error(ErrorCode::io, "read-only recovery forbids create"); }
    WriteOutcome write_current(const Bytes&, const std::optional<std::string>&) override { throw Error(ErrorCode::io, "read-only recovery forbids pointer write"); }
private:
    ObjectStore& store_;
    Pointer target_;
    mutable unsigned reads_ = 0;
    mutable bool exact_ = false, manifest_read_ = false;
};

void expect_missing(const std::function<void()>& call) {
    try { call(); } catch (const Error& error) { if (error.code() == ErrorCode::missing) return; throw; }
    throw Error(ErrorCode::conflict, "staging key already exists");
}

TransferLimits limits(const Json& config, SteadyClock::time_point deadline, uint64_t requests, uint64_t upload, uint64_t download) {
    TransferLimits result;
    result.deadline = deadline; result.max_requests = requests; result.max_upload_bytes = upload; result.max_download_bytes = download;
    result.request_timeout = std::chrono::seconds(config.at("budget").at("request_timeout_seconds").get<int>());
    result.connect_timeout = std::chrono::seconds(config.at("budget").at("connect_timeout_seconds").get<int>());
    return result;
}

void business(const Scenario& scenario, const Manifest& candidate, const Bytes& pack, const std::filesystem::path& root,
              S3Store& store, Gate& gate) {
    const auto& entry = candidate.entries.front();
    const auto hash = sha256(serialize_manifest(candidate));
    const auto key = "manifests/v1/" + hex(hash) + ".json";
    require(!store.read_current(), "staging pointer exists");
    expect_missing([&] { (void)store.get(key, kMaxMetadataBytes); });
    expect_missing([&] { (void)store.get(entry.pack->key, entry.pack->bytes); });
    require(gate.healthy(), "staging preflight failed");
    require(put_immutable(store, entry.pack->key, pack) == WriteOutcome::applied && gate.healthy(), "pack seed failed");
    const auto first = root / (scenario.name + "-seq-1");
    JournaledPublisher::prepare(first, store.scope_id(), candidate, 0);
    PublishResult published;
    {
        JournaledPublisher publisher(first, store); published = publisher.resume();
    }
    if (scenario.name == "local-ack-loss-read-blocked") {
        require(published.outcome == PublishOutcome::indeterminate && gate.healthy(), "planned local recovery fault did not occur");
        ReadOnlyResume guarded(store, published.target);
        JournaledPublisher publisher(first, guarded); published = publisher.resume();
    }
    require(published.outcome == PublishOutcome::committed && gate.healthy(), "first publication did not commit safely");
    if (scenario.name == "baseline") {
        const auto old = store.read_current(); require(bool(old) && gate.healthy(), "missing first pointer");
        const auto second = root / (scenario.name + "-seq-2");
        JournaledPublisher::prepare(second, store.scope_id(), candidate, 1);
        {
            JournaledPublisher publisher(second, store);
            require(publisher.resume().outcome == PublishOutcome::committed && gate.healthy(), "second publication failed");
        }
        const Pointer stale{candidate.dataset_epoch, 3, key, hash};
        require(store.write_current(bytes(serialize_pointer(stale)), old->etag) == WriteOutcome::precondition_failed && gate.healthy(), "stale CAS was not rejected");
        if (!scenario.retain_objects)
            require(store.erase_staging_object("current.json", old->etag) == WriteOutcome::precondition_failed && gate.healthy(), "stale DELETE was not rejected");
        require(store.create(entry.pack->key, pack) == WriteOutcome::precondition_failed && gate.healthy(), "pack overwrite was not rejected");
    }
    {
        const auto before = store.usage().requests;
        JournaledPublisher publisher(first, store); const auto historical = publisher.resume();
        require(historical.outcome == PublishOutcome::committed && historical.target.publication_seq == 1 &&
                store.usage().requests == before, "committed journal dispatched another request");
    }
    auto snapshot = std::make_shared<Snapshot>(load_snapshot(store, scenario.final_seq));
    require(snapshot->pointer.publication_seq == scenario.final_seq && gate.healthy(), "wrong final pointer");
    const Query query{entry.identity, entry.data_version, entry.coverage, entry.row_count};
    const auto result = read_plan(store, plan_query(snapshot, query, true, true));
    require(result.rows == 2050 && hex(result.rows_sha256) == "a68c9717a5516aa23fc228ed8b203e8cc4da0bc4e968601eaa1db640c703a370" && gate.complete(),
            "staging row read or request sequence mismatch");
}

bool cleanup(const Scenario& scenario, S3Store& store, Gate& gate) {
    gate.cleanup_mode();
    if (gate.uncertain()) return false;
    for (size_t i = 0; i < scenario.cleanup.size(); i += 3) {
        const auto& check = scenario.cleanup[i];
        const auto& object = gate.object(check.key);
        if (!object.owned) {
            if (!object.absent || object.uncertain) return false;
            gate.skip_cleanup_object(); continue;
        }
        if (check.key != "current.json" && !gate.safe_dependencies()) return false;
        try {
            if (check.key == "current.json") require(bool(store.read_current()), "owned pointer unexpectedly missing");
            else (void)store.get(check.key, check.limit);
            if (store.erase_staging_object(check.key, gate.etag(check.save_etag)) != WriteOutcome::applied) return false;
            if (check.key == "current.json") require(!store.read_current(), "pointer delete not verified");
            else expect_missing([&] { (void)store.get(check.key, check.limit); });
            require(gate.object(check.key).absent, "cleanup absence not recorded");
        } catch (...) { return false; }
    }
    return true;
}

}  // namespace

Json Executor::run(const Plan& plan, const Approval& approval, const std::filesystem::path& evidence,
                   const std::filesystem::path& registry, const CredentialsLoader& credentials,
                   const TransportFactory& transport, const LedgerHook& hook) {
    const auto& state = *plan.state_;
    require(approval.execute && approval.exclusive_namespace && approval.config_sha256 == plan.config_hash() &&
            approval.intent_sha256 == plan.intent_hash(), "explicit matching staging approval required", ErrorCode::invalid);
    for (const auto& value : state.config.at("confirmations")) require(value.get<bool>(), "new staging resource confirmations required");
    require(credentials && transport, "staging dependencies missing");
    const auto total_deadline = SteadyClock::now() + std::chrono::seconds(state.config.at("budget").at("max_run_seconds").get<int>());
    Ledger ledger(evidence, state.intent, hook);
    register_namespaces(registry, state.intent);
    bool success = true;
    std::string failure = "none";
    TransferUsage adapter_usage;
    const auto add_usage = [&](const TransferUsage& usage) {
        adapter_usage.requests += usage.requests;
        adapter_usage.upload_reserved += usage.upload_reserved;
        adapter_usage.download_reserved += usage.download_reserved;
    };
    const auto accounted = [&] { return adapter_usage.requests == ledger.usage().requests &&
                                       adapter_usage.upload_reserved == ledger.usage().upload_reserved &&
                                       adapter_usage.download_reserved == ledger.usage().download_reserved; };
    try {
        require(SteadyClock::now() < total_deadline, "staging setup deadline expired");
        const auto secrets = credentials(); require(bool(secrets), "staging credentials unavailable");
        const auto backend = transport(); require(bool(backend), "staging backend unavailable");
        for (const auto& scenario : state.scenarios) {
            const auto start = SteadyClock::now();
            auto gate = std::make_shared<Gate>(ledger, scenario, backend, total_deadline);
            const auto business_deadline = std::min(total_deadline, start + std::chrono::seconds(state.config.at("budget").at("business_seconds_per_namespace").get<int>()));
            auto budget = limits(state.config, business_deadline, scenario.worst.requests, scenario.worst.upload_reserved, 40 * 1024 * 1024);
            S3Store store(scenario.config, gate, secrets, budget);
            bool completed = false;
            try { business(scenario, state.candidate, state.pack, evidence, store, *gate); completed = true; }
            catch (...) { success = false; failure = "business_failed_or_indeterminate"; }
            budget.cancelled->store(true);
            add_usage(store.usage());
            require(accounted(), "staging adapter reservations do not match durable ledger");
            if (scenario.retain_objects) {
                if (!completed) break;
                require(gate->retained_complete(), "retained object ownership or identity is unverified");
                continue;
            }
            const auto cleanup_deadline = std::min(total_deadline, SteadyClock::now() + std::chrono::seconds(state.config.at("budget").at("cleanup_seconds_per_namespace").get<int>()));
            S3Store cleaner(scenario.config, gate, secrets, limits(state.config, cleanup_deadline, 9, 0, 65536));
            const bool cleaned = cleanup(scenario, cleaner, *gate);
            add_usage(cleaner.usage());
            require(accounted(), "cleanup adapter reservations do not match durable ledger");
            if (!cleaned) { success = false; failure = "cleanup_unverified_or_residue"; }
            if (!completed || !cleaned) break;
        }
    } catch (...) { success = false; failure = "local_setup_or_persistence_failure"; }
    try { ledger.finish({{"type", "finished"}, {"success", success}, {"failure", failure},
                          {"durable_budget_matches_adapter", accounted()},
                          {"adapter_usage", {{"reserved_requests", adapter_usage.requests}, {"upload_body_bytes", adapter_usage.upload_reserved},
                                             {"download_reserved_bytes", adapter_usage.download_reserved}}}}); }
    catch (...) { success = false; }
    auto report = ledger.report();
    report["status"] = ledger.poisoned() ? "INDETERMINATE_LOCAL_PERSISTENCE" : success ?
        (retains_objects(state.intent) ? "PASS_STAGING_RETAINED" : "PASS_STAGING_PROFILE") :
        (retains_objects(state.intent) ? "FAIL_STAGING_RETAINED" : "FAIL_STAGING_PROFILE");
    report["failure"] = failure;
    report["local_faults_are_real_network_faults"] = false;
    try { write_report(evidence, report); }
    catch (...) { report["status"] = "INDETERMINATE_REPORT_PERSISTENCE"; }
    return report;
}

}  // namespace history_cache::staging
