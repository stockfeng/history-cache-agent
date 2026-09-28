#include "internal.h"

#include "history_cache/columns.h"

#include <regex>

namespace history_cache::staging {

Json parse(const std::string& raw, uint64_t limit) {
    require(!raw.empty() && raw.size() <= limit, "staging JSON exceeds limit", ErrorCode::resource_limit);
    std::vector<std::set<std::string>> keys;
    size_t items = 0;
    const auto callback = [&](int depth, Json::parse_event_t event, Json& value) {
        require(depth <= 16 && ++items <= 100000, "staging JSON complexity exceeds limit");
        if (event == Json::parse_event_t::object_start) keys.emplace_back();
        if (event == Json::parse_event_t::key) require(!keys.empty() && keys.back().insert(value.get<std::string>()).second,
                                                     "duplicate staging JSON field");
        if (event == Json::parse_event_t::object_end) keys.pop_back();
        return true;
    };
    try { return Json::parse(raw, callback); }
    catch (...) { throw Error(ErrorCode::invalid, "invalid staging JSON"); }
}

void fields(const Json& value, std::initializer_list<const char*> names) {
    require(value.is_object() && value.size() == names.size(), "invalid staging config fields");
    for (const auto* name : names) require(value.contains(name), "missing staging config field");
}

uint64_t number(const Json& value, uint64_t cap) {
    require(value.is_number_unsigned(), "staging integer required");
    const auto result = value.get<uint64_t>();
    require(result > 0 && result <= cap, "staging integer outside limits");
    return result;
}

Json Step::json() const {
    Json result{{"id", id}, {"phase", phase}, {"relative_key", key}, {"method", method_name(method)},
                {"upload_bytes", body.size()}, {"upload_sha256", hex(sha256(body))},
                {"response_limit", limit}, {"download_reserved_bytes", std::max<uint64_t>(4096, limit)},
                {"expected_status", suppress_dispatch ? Json(nullptr) : Json(status)}, {"headers", headers},
                {"max_wire_requests", suppress_dispatch ? 0 : 1}, {"suppress_ack_locally", suppress_ack},
                {"recovery", recovery}};
    if (response_hash) result["response_sha256"] = hex(*response_hash);
    if (!save_etag.empty()) result["save_etag_as"] = save_etag;
    if (!content_range.empty()) result["content_range"] = content_range;
    return result;
}

namespace {

std::string string(const Json& value, size_t max) {
    require(value.is_string(), "staging string required");
    auto result = value.get<std::string>();
    require(result.size() <= max, "staging string exceeds limit");
    return result;
}

void validate_config(const Json& config) {
    const bool retain = retains_objects(config);
    if (retain) fields(config, {"schema_version", "profile", "environment", "account_id", "bucket", "jurisdiction", "key_prefix",
                               "credentials", "confirmations", "budget", "cleanup", "retention"});
    else fields(config, {"schema_version", "profile", "environment", "account_id", "bucket", "jurisdiction", "key_prefix",
                         "credentials", "confirmations", "budget", "cleanup"});
    require(number(config.at("schema_version"), retain ? 2 : 1) == (retain ? 2U : 1U) &&
            (retain || config.at("profile") == "cpp-staging-2050-v1") && config.at("environment") == "staging" &&
            config.at("key_prefix") == "r2-history-staging/" &&
            config.at("cleanup") == (retain ? "disabled_retain_objects" : "verified_created_objects_only"), "unsupported staging profile");
    require(std::regex_match(string(config.at("account_id"), 32), std::regex("[0-9a-f]{32}")), "invalid staging account");
    const auto bucket = string(config.at("bucket"), 63);
    const auto segments = '-' + bucket + '-';
    require(std::regex_match(bucket, std::regex("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")) &&
            segments.find("-staging-") != std::string::npos && segments.find("-prod-") == std::string::npos &&
            segments.find("-production-") == std::string::npos, "invalid staging bucket");
    require(config.at("jurisdiction") == "default" || config.at("jurisdiction") == "eu", "invalid jurisdiction");
    const Json credentials{{"provider", "environment"}, {"access_key_id_env", "R2_STAGING_ACCESS_KEY_ID"},
                           {"secret_access_key_env", "R2_STAGING_SECRET_ACCESS_KEY"}};
    require(config.at("credentials") == credentials, "unsupported staging credential source");
    const auto& confirmations = config.at("confirmations");
    if (retain) fields(confirmations, {"dedicated_bucket", "private_bucket", "bucket_scoped_credentials", "authorized_credentials_ready",
                                      "new_cpp_budget_approved", "retention_owner_ready", "retained_objects_approved",
                                      "manual_review_without_auto_delete_approved"});
    else fields(confirmations, {"dedicated_bucket", "private_bucket", "bucket_scoped_credentials", "fresh_credentials_ready",
                                "new_cpp_budget_approved", "cleanup_owner_ready"});
    for (const auto& value : confirmations) require(value.is_boolean(), "invalid staging confirmation");
    const auto& budget = config.at("budget");
    if (retain) fields(budget, {"max_reserved_requests", "max_upload_body_bytes", "max_download_reserved_bytes", "max_stored_body_bytes",
                               "max_run_seconds", "business_seconds_per_namespace", "request_timeout_seconds", "connect_timeout_seconds", "max_cost_usd"});
    else fields(budget, {"max_reserved_requests", "max_upload_body_bytes", "max_download_reserved_bytes", "max_stored_body_bytes",
                         "max_run_seconds", "business_seconds_per_namespace", "cleanup_seconds_per_namespace",
                         "request_timeout_seconds", "connect_timeout_seconds", "max_cost_usd"});
    (void)number(budget.at("max_reserved_requests"), 128);
    (void)number(budget.at("max_upload_body_bytes"), 65536);
    (void)number(budget.at("max_download_reserved_bytes"), 128 * 1024 * 1024);
    (void)number(budget.at("max_stored_body_bytes"), 65536);
    const auto total = number(budget.at("max_run_seconds"), retain ? 360 : 540);
    const auto business = number(budget.at("business_seconds_per_namespace"), 120);
    const auto cleanup = retain ? 0 : number(budget.at("cleanup_seconds_per_namespace"), 60);
    const auto request = number(budget.at("request_timeout_seconds"), 10);
    const auto connect = number(budget.at("connect_timeout_seconds"), 3);
    require(connect <= request && request <= (retain ? business : std::min(business, cleanup)) && 3 * (business + cleanup) <= total,
            "inconsistent staging deadlines");
    const auto cost = string(budget.at("max_cost_usd"), 8);
    require(std::regex_match(cost, std::regex("0\\.[0-9]{1,4}|1(\\.0{1,4})?")) &&
            cost.find_first_of("123456789") != std::string::npos, "invalid staging cost proposal");
    if (retain) {
        const auto& policy = config.at("retention");
        fields(policy, {"owner", "review_after_seconds", "max_objects", "max_cost_usd_through_review", "expiry"});
        require(std::regex_match(string(policy.at("owner"), 64), std::regex("[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}")), "retention owner required");
        (void)number(policy.at("review_after_seconds"), 7 * 24 * 3600);
        require(number(policy.at("max_objects"), 9) == 9 && policy.at("expiry") == "manual_review_only_no_auto_delete",
                "unsupported retention policy");
        const auto retained_cost = string(policy.at("max_cost_usd_through_review"), 8);
        require(std::regex_match(retained_cost, std::regex("0\\.[0-9]{1,4}|1(\\.0{1,4})?")) &&
                retained_cost.find_first_of("123456789") != std::string::npos, "invalid retention cost proposal");
    }
}

Digest fixture_rows_hash() {
    Sha256 hash;
    for (int64_t index = 0; index < 2050; ++index) {
        const float price = 100 + static_cast<float>(index % 1000) * 0.125F;
        hash.update(canonical_row({1704067200000LL + index * 60000, price, price + 1, price - 1, price + 0.5F, index * 17 + 1}));
    }
    return hash.finish();
}

void add_usage(TransferUsage& usage, const Step& step) {
    ++usage.requests;
    usage.upload_reserved += step.body.size();
    usage.download_reserved += std::max<uint64_t>(4096, step.limit);
}

}  // namespace

Plan Plan::prepare(const std::string& config_raw, const std::string& manifest_raw, const Bytes& pack,
                   const std::string& run_id) {
    auto state = std::make_shared<State>();
    state->config = parse(config_raw, 16384);
    validate_config(state->config);
    const bool retain = retains_objects(state->config);
    require(std::regex_match(run_id, std::regex("[a-z0-9][a-z0-9-]{2,55}")), "invalid staging run ID");
    state->config_hash = sha256(config_raw);
    state->candidate = parse_manifest(manifest_raw);
    const auto& candidate = state->candidate;
    require(candidate.dataset_epoch == "fixture-epoch-1" && candidate.entries.size() == 1, "not a staging fixture");
    const auto& entry = candidate.entries.front();
    const SeriesIdentity expected{"synthetic", "TEST", "FIXTURE", 60, "none"};
    require(canonical_identity(entry.identity) == canonical_identity(expected) && entry.row_count == 2050 &&
            entry.source_version == 1 && entry.coverage.start_ms == 1704067200000LL &&
            entry.coverage.end_ms == 1704190200000LL && entry.data_version == sha256(std::string("synthetic-none-1m-v1\n")) &&
            entry.pack && !pack.empty() && pack.size() <= 65536 && pack.size() == entry.pack->bytes, "invalid small staging fixture");
    state->pack = pack;
    state->manifest = bytes(manifest_raw);
    const RangeReader read = [&](uint64_t start, uint64_t size) {
        require(start <= pack.size() && size <= pack.size() - start, "fixture Range outside object");
        return Bytes(pack.begin() + static_cast<std::ptrdiff_t>(start), pack.begin() + static_cast<std::ptrdiff_t>(start + size));
    };
    verify_pack(read, *entry.pack, pack_metadata(entry));
    const auto index = read_pack_index(read, *entry.pack, pack_metadata(entry));
    Sha256 row_hash;
    for (const auto& block : index.blocks) for (const auto& row : read_pack_block(read, block)) row_hash.update(canonical_row(row));
    require(row_hash.finish() == fixture_rows_hash(), "staging rows are not the fixed synthetic fixture");
    const auto manifest_hash = sha256(manifest_raw);
    const auto manifest_key = "manifests/v1/" + hex(manifest_hash) + ".json";
    const auto pointer = [&](uint64_t seq) { return bytes(serialize_pointer({candidate.dataset_epoch, seq, manifest_key, manifest_hash})); };
    std::vector<std::pair<uint64_t, uint64_t>> ranges{{0, 160}, {160, index.blocks.size() * 80}};
    for (const auto& block : index.blocks) ranges.emplace_back(block.offset, block.compressed_bytes);
    const std::string account = state->config.at("account_id"), bucket = state->config.at("bucket"), jurisdiction = state->config.at("jurisdiction");
    const std::string endpoint = "https://" + account + (jurisdiction == "eu" ? ".eu" : "") + ".r2.cloudflarestorage.com";
    TransferUsage total;
    Json scenarios = Json::array();
    for (unsigned scenario_index = 0; scenario_index < 3; ++scenario_index) {
        Scenario scenario;
        const std::string suffix = scenario_index == 0 ? "base" : scenario_index == 1 ? "ack" : "resume";
        scenario.name = scenario_index == 0 ? "baseline" : scenario_index == 1 ? "local-ack-loss" : "local-ack-loss-read-blocked";
        scenario.config = {account, bucket, "r2-history-staging/" + run_id + '-' + suffix + '/', jurisdiction};
        scenario.final_seq = scenario_index == 0 ? 2 : 1;
        scenario.retain_objects = retain;
        scenario.objects = {{"current.json", "pointer", hex(sha256(pointer(scenario.final_seq))), pointer(3).size()},
                            {manifest_key, "manifest", hex(manifest_hash), state->manifest.size()},
                            {entry.pack->key, "pack", hex(entry.pack->sha256), pack.size()}};
        auto add = [&](const std::string& id, const std::string& phase, HttpMethod method, const std::string& key,
                       unsigned status, uint64_t limit, Bytes body = {}, HttpHeaders headers = {}) -> Step& {
            Step step;
            step.id = id; step.phase = phase; step.method = method; step.key = key;
            step.status = status; step.limit = limit; step.body = std::move(body); step.headers = std::move(headers);
            scenario.business.push_back(std::move(step));
            return scenario.business.back();
        };
        auto current = [&](const std::string& id, const std::string& phase, uint64_t seq) -> Step& {
            auto& step = add(id, phase, HttpMethod::get, "current.json", seq == 0 ? 404 : 200, kMaxPointerBytes);
            if (seq) step.response_hash = sha256(pointer(seq));
            return step;
        };
        auto manifest = [&](const std::string& id, const std::string& phase) -> Step& {
            auto& step = add(id, phase, HttpMethod::get, manifest_key, 200, kMaxMetadataBytes);
            step.response_hash = manifest_hash;
            return step;
        };
        auto range_steps = [&](const std::string& phase) {
            size_t i = 0;
            for (const auto& [start, size] : ranges) {
                const auto label = i == 0 ? "header" : i == 1 ? "toc" : "block-" + std::to_string(i - 2);
                const auto interval = std::to_string(start) + '-' + std::to_string(start + size - 1);
                HttpHeaders headers{{"range", "bytes=" + interval}};
                if (i > 0) headers["if-match"] = "@" + phase + "-pack-etag";
                auto& step = add(phase + '-' + label, phase, HttpMethod::get, entry.pack->key, 206, size, {}, headers);
                step.response_hash = sha256(read(start, size));
                step.content_range = "bytes " + interval + '/' + std::to_string(pack.size());
                if (i == 0) step.save_etag = phase + "-pack-etag";
                ++i;
            }
        };
        current("pointer-must-be-missing", "preflight", 0);
        add("manifest-must-be-missing", "preflight", HttpMethod::get, manifest_key, 404, kMaxMetadataBytes);
        add("pack-must-be-missing", "preflight", HttpMethod::get, entry.pack->key, 404, pack.size());
        add("create-pack", "seed-pack", HttpMethod::put, entry.pack->key, 200, 0, pack, {{"if-none-match", "*"}});
        add("read-back-pack", "seed-pack", HttpMethod::get, entry.pack->key, 200, kMaxObjectBytes).response_hash = entry.pack->sha256;
        auto publish = [&](uint64_t seq) {
            const auto phase = "publish-" + std::to_string(seq);
            current(phase + "-current", phase, seq - 1).save_etag = phase + "-current-etag";
            if (seq > 1) manifest(phase + "-transition-manifest", phase);
            range_steps(phase);
            add(phase + "-create-manifest", phase, HttpMethod::put, manifest_key, seq == 1 ? 200 : 412, 0,
                state->manifest, {{"if-none-match", "*"}});
            manifest(phase + "-read-back-manifest", phase);
            const HttpHeaders condition = seq == 1 ? HttpHeaders{{"if-none-match", "*"}} : HttpHeaders{{"if-match", "@" + phase + "-current-etag"}};
            auto& step = add(phase + "-write-pointer", phase, HttpMethod::put, "current.json", 200, 0, pointer(seq), condition);
            step.suppress_ack = scenario_index != 0;
        };
        publish(1);
        if (scenario_index == 0) {
            current("save-seq-1-etag", "capture-etag-1", 1).save_etag = "stale-seq-1-etag";
            publish(2);
            add("stale-etag-cannot-write-seq-3", "reject-stale-cas", HttpMethod::put, "current.json", 412, 0,
                pointer(3), {{"if-match", "@stale-seq-1-etag"}});
            if (!retain) add("stale-etag-cannot-delete-pointer", "reject-stale-delete", HttpMethod::erase, "current.json", 412, 0,
                             {}, {{"if-match", "@stale-seq-1-etag"}});
            add("duplicate-pack-create-only", "reject-pack-overwrite", HttpMethod::put, entry.pack->key, 412, 0,
                pack, {{"if-none-match", "*"}});
            for (uint64_t seq : {1, 2}) {
                Step recovery;
                recovery.id = "publish-" + std::to_string(seq) + "-optional-recovery";
                recovery.phase = "publish-" + std::to_string(seq);
                recovery.key = "current.json"; recovery.status = 200; recovery.limit = kMaxPointerBytes;
                recovery.response_hash = sha256(pointer(seq)); recovery.recovery = true;
                scenario.recovery.emplace(recovery.phase + "-write-pointer", recovery);
            }
        } else {
            auto& recovery = current("one-authoritative-recovery", "publish-1", 1);
            recovery.recovery = true;
            recovery.suppress_dispatch = scenario_index == 2;
            if (scenario_index == 2) {
                current("resume-original-target", "resume-read-only", 1);
                manifest("resume-original-manifest", "resume-read-only");
            }
        }
        current("load-current", "read-snapshot", scenario.final_seq);
        manifest("load-manifest", "read-snapshot");
        range_steps("read-rows");
        for (const auto& key : {std::string("current.json"), manifest_key, entry.pack->key}) {
            if (retain) break;
            const auto label = key == "current.json" ? "pointer" : key == manifest_key ? "manifest" : "pack";
            const auto limit = key == "current.json" ? kMaxPointerBytes : key == manifest_key ? state->manifest.size() : pack.size();
            Step check;
            check.id = std::string("check-owned-") + label; check.phase = "cleanup"; check.key = key;
            check.limit = limit; check.status = 200;
            check.response_hash = key == "current.json" ? sha256(pointer(scenario.final_seq)) : key == manifest_key ? manifest_hash : entry.pack->sha256;
            check.save_etag = std::string("cleanup-") + label + "-etag";
            scenario.cleanup.push_back(check);
            Step erase;
            erase.id = std::string("delete-owned-") + label; erase.phase = "cleanup"; erase.key = key;
            erase.method = HttpMethod::erase; erase.status = 204; erase.headers = {{"if-match", "@" + check.save_etag}};
            scenario.cleanup.push_back(erase);
            Step missing;
            missing.id = std::string("verify-missing-") + label; missing.phase = "cleanup"; missing.key = key;
            missing.limit = limit; missing.status = 404;
            scenario.cleanup.push_back(missing);
        }
        Json business = Json::array(), recovery = Json::array(), cleanup = Json::array();
        for (const auto& step : scenario.business) { business.push_back(step.json()); add_usage(scenario.worst, step); }
        for (const auto& [after, step] : scenario.recovery) {
            auto item = step.json(); item["insert_after"] = after; recovery.push_back(item); add_usage(scenario.worst, step);
        }
        for (const auto& step : scenario.cleanup) { cleanup.push_back(step.json()); add_usage(total, step); }
        total.requests += scenario.worst.requests;
        total.upload_reserved += scenario.worst.upload_reserved;
        total.download_reserved += scenario.worst.download_reserved;
        require(scenario.worst.download_reserved <= 40 * 1024 * 1024, "business reservation exceeds phase budget");
        scenarios.push_back({{"name", scenario.name}, {"namespace", scenario.config.key_prefix},
                             {"scope_sha256", hex(sha256("history-s3-scope-v1\n" + endpoint + '\n' + bucket + '\n' + scenario.config.key_prefix + '\n'))},
                             {"business", business}, {"optional_recovery", recovery}, {"cleanup", cleanup},
                             {"business_max_requests", scenario.worst.requests}, {"final_pointer_seq", scenario.final_seq}});
        if (retain) {
            Json objects = Json::array();
            for (const auto& object : scenario.objects) objects.push_back({{"relative_key", object.key}, {"role", object.role},
                {"max_body_bytes", object.max_bytes}, {"final_body_sha256", object.final_sha256}});
            scenarios.back()["objects"] = std::move(objects);
        }
        state->scenarios.push_back(std::move(scenario));
    }
    const auto& budget = state->config.at("budget");
    require(total.requests <= budget.at("max_reserved_requests").get<uint64_t>() &&
            total.upload_reserved <= budget.at("max_upload_body_bytes").get<uint64_t>() &&
            total.download_reserved <= budget.at("max_download_reserved_bytes").get<uint64_t>() &&
            3ULL * (40 * 1024 * 1024 + (retain ? 0 : 65536)) <= budget.at("max_download_reserved_bytes").get<uint64_t>() &&
            3 * (pack.size() + state->manifest.size() + pointer(1).size()) <= budget.at("max_stored_body_bytes").get<uint64_t>(),
            "staging workload exceeds configured budget", ErrorCode::resource_limit);
    state->intent = {{"schema_version", retain ? 2 : 1}, {"profile", state->config.at("profile")}, {"run_id", run_id},
                     {"config", state->config}, {"config_sha256", hex(state->config_hash)}, {"endpoint", endpoint},
                     {"manifest_sha256", hex(manifest_hash)}, {"pack_sha256", hex(entry.pack->sha256)},
                     {"rows_sha256", hex(fixture_rows_hash())}, {"scenarios", scenarios},
                     {"planned", {{"reserved_requests", total.requests}, {"upload_body_bytes", total.upload_reserved},
                                  {"download_reserved_bytes", total.download_reserved}}},
                     {"automatic_retries", 0}, {"cleanup_conditional_if_match", !retain},
                     {"stale_delete_precondition_probe", !retain},
                     {"external_restart_execution_supported", false}, {"local_faults_are_network_failures", false}};
    if (retain) {
        state->intent["retention"] = {{"policy", state->config.at("retention")}, {"planned_objects", 9},
            {"planned_body_bytes", 3 * (pack.size() + state->manifest.size() + pointer(3).size())},
            {"automatic_delete", false}, {"bucket_lifecycle_modified", false}, {"cleanup_requires_separate_authorization", true},
            {"review_deadline_is_not_object_expiry", true}};
        state->intent["required_capabilities"] = {{"create_only_put", true}, {"pointer_put_cas", true},
                                                 {"range_206", true}, {"conditional_delete", false}};
    }
    return Plan(std::move(state));
}

Json Plan::intent() const { return state_->intent; }
Digest Plan::intent_hash() const { return sha256(canonical(state_->intent)); }
Digest Plan::config_hash() const { return state_->config_hash; }

}  // namespace history_cache::staging
