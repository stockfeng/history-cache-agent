#include "transport_fixture.h"
#include "history_cache/staging.h"
#include "../src/staging/internal.h"

#include <sys/stat.h>

using namespace test;
namespace st = hc::staging;
using Json = nlohmann::json;

namespace {

Json configuration(bool confirmed = true) {
    return {{"schema_version", 1}, {"profile", "cpp-staging-2050-v1"}, {"environment", "staging"},
            {"account_id", std::string(32, 'a')}, {"bucket", "history-cache-staging"}, {"jurisdiction", "default"},
            {"key_prefix", "r2-history-staging/"},
            {"credentials", {{"provider", "environment"}, {"access_key_id_env", "R2_STAGING_ACCESS_KEY_ID"},
                              {"secret_access_key_env", "R2_STAGING_SECRET_ACCESS_KEY"}}},
            {"confirmations", {{"dedicated_bucket", confirmed}, {"private_bucket", confirmed}, {"bucket_scoped_credentials", confirmed},
                               {"fresh_credentials_ready", confirmed}, {"new_cpp_budget_approved", confirmed}, {"cleanup_owner_ready", confirmed}}},
            {"budget", {{"max_reserved_requests", 112}, {"max_upload_body_bytes", 65536}, {"max_download_reserved_bytes", 134217728},
                        {"max_stored_body_bytes", 65536}, {"max_run_seconds", 540}, {"business_seconds_per_namespace", 120},
                        {"cleanup_seconds_per_namespace", 60}, {"request_timeout_seconds", 10}, {"connect_timeout_seconds", 3}, {"max_cost_usd", "0.10"}}},
            {"cleanup", "verified_created_objects_only"}};
}

Json retention_configuration(bool confirmed = true) {
    auto config = Json::parse(hc::read_file(fs::path(HC_SOURCE_DIR) / "tests/staging-config-retain-v2.json", 16384));
    for (auto& value : config["confirmations"]) value = confirmed;
    config["retention"]["owner"] = "synthetic-test-owner";
    return config;
}

struct Fixture {
    Workspace workspace;
    hc::Manifest candidate;
    hc::Bytes pack;
    Fixture() {
        hc::CatalogEntry entry{identity(), hc::sha256(std::string("synthetic-none-1m-v1\n")), 1,
                               {1704067200000LL, 1704190200000LL}, 2050, std::nullopt};
        const auto path = workspace.root / "pack.r2b";
        entry.pack = hc::write_pack(path, hc::pack_metadata(entry), [](uint64_t offset, uint32_t count) {
            std::vector<hc::Row> rows;
            for (uint64_t index = offset; index < offset + count; ++index) {
                const auto price = 100 + static_cast<float>(index % 1000) * 0.125F;
                rows.push_back({1704067200000LL + static_cast<int64_t>(index) * 60000, price, price + 1, price - 1,
                               price + 0.5F, static_cast<int64_t>(index) * 17 + 1});
            }
            return rows;
        });
        candidate = {"fixture-epoch-1", {entry}}; pack = hc::read_file(path, 65536);
        check(::mkdir((workspace.root / "registry").c_str(), 0700) == 0, "registry mkdir failed");
    }
    st::Plan plan(Json config = configuration(), std::string run = "staging-unit-001") {
        return st::Plan::prepare(config.dump() + '\n', hc::serialize_manifest(candidate), pack, run);
    }
};

class Peer final : public hc::HttpTransport {
public:
    struct Object { hc::Bytes data; std::string etag; };
    std::map<std::string, Object> objects;
    std::vector<hc::HttpRequest> calls;
    uint64_t writes = 0, deletes = 0;
    std::function<std::optional<hc::HttpResult>(const hc::HttpRequest&)> before;
    std::function<void(const hc::HttpRequest&, hc::HttpResult&)> after;
    hc::HttpResult perform(const hc::HttpRequest& request) override {
        calls.push_back(request);
        if (before) { auto override = before(request); if (override) return *override; }
        auto result = exchange(request);
        if (after) after(request, result);
        return result;
    }
    hc::HttpResult exchange(const hc::HttpRequest& request) {
        check(request.url.find("/history-cache-staging/r2-history-staging/staging-unit-001-") != std::string::npos, "unscoped peer call");
        check(request.headers.count("authorization") && request.headers.at("x-amz-content-sha256") == hc::hex(hc::sha256(request.body)), "unsigned call");
        auto found = objects.find(request.url);
        if (request.method == hc::HttpMethod::put) {
            check(request.headers.count("if-none-match") + request.headers.count("if-match") == 1, "unconditional staging PUT");
            if (request.headers.count("if-none-match") && found != objects.end()) return response(412, {}, {}, request.response_limit);
            if (request.headers.count("if-match") && (found == objects.end() || found->second.etag != request.headers.at("if-match")))
                return response(412, {}, {}, request.response_limit);
            const auto tag = "\"staging-" + std::to_string(++writes) + "\"";
            objects[request.url] = {request.body, tag};
            return response(200, {}, {{"etag", tag}}, request.response_limit);
        }
        if (request.method == hc::HttpMethod::erase) {
            check(request.headers.count("if-match") && request.body.empty(), "DELETE lacks exact ETag");
            if (found == objects.end() || found->second.etag != request.headers.at("if-match")) return response(412, {}, {}, 0);
            objects.erase(found); ++deletes;
            return response(204, {}, {}, 0);
        }
        if (found == objects.end()) return response(404, bytes("<Error><Code>NoSuchKey</Code></Error>"), {}, request.response_limit);
        const auto& object = found->second;
        if (request.headers.count("if-match") && request.headers.at("if-match") != object.etag) return response(412, {}, {}, request.response_limit);
        if (request.headers.count("range")) {
            const auto range = request.headers.at("range").substr(6); const auto dash = range.find('-');
            const auto start = hc::http_unsigned(range.substr(0, dash)), end = hc::http_unsigned(range.substr(dash + 1));
            check(start <= end && end < object.data.size(), "invalid synthetic Range");
            return response(206, hc::Bytes(object.data.begin() + static_cast<std::ptrdiff_t>(start), object.data.begin() + static_cast<std::ptrdiff_t>(end + 1)),
                            {{"etag", object.etag}, {"content-range", "bytes " + range + '/' + std::to_string(object.data.size())}}, request.response_limit);
        }
        return response(200, object.data, {{"etag", object.etag}}, request.response_limit);
    }
};

Json execute(Fixture& fixture, const st::Plan& plan, const std::shared_ptr<Peer>& peer, const st::LedgerHook& hook = {}, std::string leaf = "run") {
    return st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()}, fixture.workspace.root / leaf,
                            fixture.workspace.root / "registry", credentials, [peer] { return peer; }, hook);
}

void rejects_any(const std::function<void()>& call) {
    bool rejected = false; try { call(); } catch (...) { rejected = true; } check(rejected, "expected staging rejection");
}

void staging_plan() {
    run_case("cpp_plan_small_fixture_exact_sequence_and_reservations", [] {
        Fixture f; const auto plan = f.plan(); const auto intent = plan.intent();
        check(intent.at("planned").at("reserved_requests") == 110 && intent.at("planned").at("download_reserved_bytes") == 113678069 &&
              intent.at("planned").at("upload_body_bytes") == 39288, "C++ request proposal changed");
        check(intent.at("scenarios").size() == 3 && intent.at("scenarios")[0].at("business").size() == 35 &&
              intent.at("scenarios")[1].at("business").size() == 22 && intent.at("scenarios")[2].at("business").size() == 24, "request sequence count");
        check(f.plan(configuration(false)).intent_hash() != plan.intent_hash(), "confirmation not bound to intent");
    });
    run_case("cpp_config_strict_fields_confirmations_and_budgets", [] {
        Fixture f;
        for (const auto& [name, value] : std::vector<std::pair<std::string, Json>>{{"account_id", "bad"}, {"bucket", "history-prod-staging"},
                 {"environment", "production"}, {"endpoint", "https://arbitrary.invalid"}, {"schema_version", true}, {"cleanup", "recursive"}}) {
            auto config = configuration(); config[name] = value; rejects_any([&] { (void)f.plan(config); });
        }
        auto config = configuration(); config["budget"]["max_download_reserved_bytes"] = 1024; rejects_any([&] { (void)f.plan(config); });
        config = configuration(); config["credentials"]["secret_access_key_env"] = "AWS_SECRET_ACCESS_KEY"; rejects_any([&] { (void)f.plan(config); });
        rejects_any([&] { (void)st::Plan::prepare("{\"x\":1,\"x\":2}", hc::serialize_manifest(f.candidate), f.pack, "unit"); });
    });
    run_case("cpp_fixture_content_and_run_path_guards", [] {
        Fixture f; auto corrupted = f.pack; corrupted.back() ^= 1;
        rejects_any([&] { (void)st::Plan::prepare(configuration().dump(), hc::serialize_manifest(f.candidate), corrupted, "unit"); });
        for (const auto* run : {"../bad", "ab", "x/y", "ALLCAPS"}) rejects_any([&] { (void)f.plan(configuration(), run); });
        auto candidate = f.candidate; candidate.entries[0].identity.dataset = "real";
        rejects_any([&] { (void)st::Plan::prepare(configuration().dump(), hc::serialize_manifest(candidate), f.pack, "unit"); });
    });
    run_case("approval_gate_precedes_credentials_transport_and_output", [] {
        Fixture f; const auto plan = f.plan(); unsigned called = 0;
        for (const auto& approval : std::vector<st::Approval>{{}, {true, false, plan.config_hash(), plan.intent_hash()},
               {true, true, {}, plan.intent_hash()}, {true, true, plan.config_hash(), {}}}) {
            rejects_any([&] { (void)st::Executor::run(plan, approval, f.workspace.root / "run", f.workspace.root / "registry",
                [&] { ++called; return credentials(); }, [&] { ++called; return std::make_shared<Peer>(); }); });
        }
        const auto pending = f.plan(configuration(false));
        rejects_any([&] { (void)st::Executor::run(pending, {true, true, pending.config_hash(), pending.intent_hash()}, f.workspace.root / "run", f.workspace.root / "registry",
            [&] { ++called; return credentials(); }, [&] { ++called; return std::make_shared<Peer>(); }); });
        check(called == 0 && !fs::exists(f.workspace.root / "run"), "approval gate had side effects");
    });
    run_case("input_symlink_fifo_and_parent_traversal_rejected", [] {
        Fixture f; fs::create_symlink(f.workspace.root / "pack.r2b", f.workspace.root / "link");
        check(::mkfifo((f.workspace.root / "fifo").c_str(), 0600) == 0, "mkfifo");
        for (const auto& path : {f.workspace.root / "link", f.workspace.root / "fifo", f.workspace.root / "../bad"})
            rejects_any([&] { (void)st::read_input(path, 65536); });
    });
}

void staging_execution() {
    run_case("three_scenarios_execute_and_cleanup_with_no_credential_logging", [] {
        Fixture f; const auto plan = f.plan(); auto peer = std::make_shared<Peer>();
        const auto report = execute(f, plan, peer);
        check(report.at("status") == "PASS_STAGING_PROFILE", report.dump());
        check(peer->objects.empty() && peer->deletes == 9 && peer->writes == 10 && peer->calls.size() == 107, "profile did not create/clean exact objects");
        check(report.at("usage").at("reserved_requests") == 108 && report.at("usage").at("download_reserved_bytes") == 113669877, "aggregate counters reset");
        check(report.at("durable_budget_matches_adapter") == true && report.at("adapter_usage") == report.at("usage"), "adapter and durable budgets differ");
        check(report.dump().find("SYNTHETICKEYID") == std::string::npos && report.dump().find("authorization") == std::string::npos, "secret or auth logged");
        const auto reopened = st::inspect_run(f.workspace.root / "run");
        check(reopened.at("usage") == report.at("usage") && reopened.at("status") == "PASS", "inspect changed usage");
    });
    run_case("namespace_registry_prevents_replay_in_new_output_before_loading_keys", [] {
        Fixture f; const auto plan = f.plan(); auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        (void)execute(f, plan, peer);
        unsigned loaded = 0;
        rejects_any([&] { (void)st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()}, f.workspace.root / "run", f.workspace.root / "registry",
            [&] { ++loaded; return credentials(); }, [peer] { return peer; }); });
        fs::create_symlink(f.workspace.root / "run", f.workspace.root / "output-link");
        rejects_any([&] { (void)st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()}, f.workspace.root / "output-link", f.workspace.root / "registry",
            [&] { ++loaded; return credentials(); }, [peer] { return peer; }); });
        rejects_any([&] { (void)st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()}, f.workspace.root / "other", f.workspace.root / "registry",
            [&] { ++loaded; return credentials(); }, [peer] { return peer; }); });
        check(loaded == 0, "replay loaded credentials");
    });
    run_case("existing_objects_stop_without_put_or_delete", [] {
        Fixture f; const auto plan = f.plan(); auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            return response(200, bytes("not-a-pointer"), {{"etag", "\"existing\""}}, request.response_limit);
        };
        const auto report = execute(f, plan, peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->calls.size() == 1 && peer->deletes == 0 && peer->writes == 0, "adopted existing namespace");
    });
    run_case("unknown_pack_write_is_not_owned_or_deleted_even_on_matching_readback", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::put) result = {hc::HttpDelivery::indeterminate, {}};
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->calls.size() == 4 && peer->deletes == 0 && peer->objects.size() == 1, "unknown object cleaned or read past stop");
        check(report.at("usage").at("reserved_requests") == 5 && report.at("durable_budget_matches_adapter") == true,
              "locally blocked helper read was not reserved");
        bool unknown = false; for (const auto& object : report.at("objects")) unknown = unknown || object.at("uncertain").get<bool>();
        check(unknown, "unknown write lost from evidence");
    });
    run_case("unexpected_manifest_412_cannot_be_adopted_by_immutable_helper", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            if (request.method == hc::HttpMethod::put && request.url.find("/manifests/") != std::string::npos)
                return response(412, {}, {}, request.response_limit);
            return std::nullopt;
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->writes == 1 && peer->deletes == 0 &&
              report.at("durable_budget_matches_adapter") == true, "unowned manifest adopted or pointer dependencies deleted");
    });
    run_case("delete_ack_loss_preserves_pointer_dependencies_and_residue", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::erase && result.response.status == 204) result = {hc::HttpDelivery::indeterminate, {}};
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->deletes == 1 && peer->objects.size() == 2, "dependencies deleted after unknown pointer DELETE");
    });
    run_case("cleanup_if_match_rejects_changed_object_without_delete", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [raw = peer.get()](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            if (request.method == hc::HttpMethod::erase && raw->objects.at(request.url).etag == request.headers.at("if-match"))
                raw->objects.at(request.url).etag = "\"changed\"";
            return std::nullopt;
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->deletes == 0 && peer->objects.size() == 3, "conditional DELETE degraded to overwrite");
    });
    run_case("business_cancellation_keeps_separate_cleanup_budget", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::get && request.response_limit == hc::kMaxObjectBytes && result.response.status == 200)
                request.cancelled->store(true);
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->writes == 1 && peer->deletes == 1 && peer->objects.empty(), "cleanup reused cancelled store");
    });
    run_case("short_range_stops_publication_and_cleans_only_owned_pack", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.headers.count("range")) result.response.body.pop_back();
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->writes == 1 && peer->deletes == 1 &&
              peer->objects.empty() && report.at("durable_budget_matches_adapter") == true, "short Range was accepted or owned pack not cleaned");
    });
    run_case("ignored_delete_precondition_stops_without_deleting_dependencies", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [raw = peer.get()](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            if (request.method != hc::HttpMethod::erase) return std::nullopt;
            raw->objects.erase(request.url); ++raw->deletes;
            return response(204, {}, {}, 0);
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->deletes == 1 && peer->objects.size() == 2 &&
              peer->writes == 4 && report.at("durable_budget_matches_adapter") == true, "ignored stale DELETE condition was accepted");
    });
    run_case("baseline_unknown_pointer_only_reads_original_target_and_stops", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::put && request.url.find("current.json") != std::string::npos)
                result = {hc::HttpDelivery::indeterminate, {}};
        };
        const auto report = execute(f, f.plan(), peer);
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->calls.size() == 15 && peer->writes == 3 &&
              peer->deletes == 0 && report.at("durable_budget_matches_adapter") == true, "baseline recovery retried or resumed business");
        const auto& last = peer->calls.back();
        check(last.method == hc::HttpMethod::get && last.url.find("current.json") != std::string::npos, "recovery did not read original pointer");
    });
    for (const auto* fault : {"missing", "newer"}) {
        run_case(std::string("read_only_resume_") + fault + "_target_never_writes_or_reads_manifest", [fault] {
            Fixture f; auto peer = std::make_shared<Peer>();
            unsigned resume_current = 0;
            peer->after = [&](const hc::HttpRequest& request, hc::HttpResult& result) {
                if (request.method != hc::HttpMethod::get || request.url.find("-resume/current.json") == std::string::npos) return;
                if (++resume_current < 3) return;
                if (std::string(fault) == "missing") result = response(404, bytes("<Error><Code>NoSuchKey</Code></Error>"), {}, request.response_limit);
                else {
                    auto value = hc::parse_pointer(text(result.response.body)); value.publication_seq = 2;
                    result = response(200, bytes(hc::serialize_pointer(value)), {{"etag", "\"newer\""}}, request.response_limit);
                }
            };
            const auto report = execute(f, f.plan(), peer);
            check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->writes == 10 && peer->deletes == 6 &&
                  report.at("durable_budget_matches_adapter") == true, "non-target resume wrote or deleted dependencies");
            std::vector<hc::HttpRequest> calls;
            for (const auto& request : peer->calls) if (request.url.find("-resume/") != std::string::npos) calls.push_back(request);
            check(calls.size() == 16 && calls.at(14).url.find("current.json") != std::string::npos &&
                  calls.at(15).url.find("current.json") != std::string::npos &&
                  calls.at(14).method == hc::HttpMethod::get && calls.at(15).method == hc::HttpMethod::get,
                  "non-target resume exceeded one GET before cleanup identity check");
        });
    }
}

void staging_ledger() {
    run_case("intent_fsync_failure_prevents_credentials_and_dispatch", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); unsigned loaded = 0; const auto plan = f.plan();
        rejects_any([&] { (void)st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()}, f.workspace.root / "run", f.workspace.root / "registry",
            [&] { ++loaded; return credentials(); }, [peer] { return peer; }, [](st::LedgerStep step) {
                if (step == st::LedgerStep::before_intent_sync) throw std::runtime_error("synthetic fsync error");
            }); });
        check(loaded == 0 && peer->calls.empty(), "intent error dispatched");
    });
    run_case("request_and_dispatch_floor_fail_before_network", [] {
        for (const auto point : {st::LedgerStep::before_request_sync, st::LedgerStep::before_dispatch_sync}) {
            Fixture f; auto peer = std::make_shared<Peer>();
            const auto report = execute(f, f.plan(), peer, [point](st::LedgerStep step) { if (step == point) throw std::runtime_error("synthetic sync failure"); });
            check(report.at("status") == "INDETERMINATE_LOCAL_PERSISTENCE" && peer->calls.empty(), "dispatch before durable reservation");
        }
    });
    run_case("result_checkpoint_error_preserves_pending_dispatch_after_reopen", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); unsigned results = 0;
        const auto report = execute(f, f.plan(), peer, [&](st::LedgerStep step) {
            if (step == st::LedgerStep::before_result_sync && ++results == 4) throw std::runtime_error("synthetic ACK checkpoint error");
        });
        check(report.at("status") == "INDETERMINATE_LOCAL_PERSISTENCE" && peer->calls.size() == 4 && peer->deletes == 0, "checkpoint error cleaned uncertain write");
        const auto inspection = st::inspect_run(f.workspace.root / "run");
        check(inspection.at("usage").at("reserved_requests") == 4 && inspection.at("automatic_resume_supported") == false,
              "reopen replenished request budget");
    });
    run_case("ledger_corruption_and_lost_dispatch_suffix_fail_closed", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        (void)execute(f, f.plan(), peer);
        const auto path = f.workspace.root / "run/events.r2s";
        check(::truncate(path.c_str(), 0) == 0, "truncate synthetic ledger failed");
        rejects_any([&] { (void)st::inspect_run(f.workspace.root / "run"); });
    });
    run_case("ledger_lock_and_permissions_fail_closed", [] {
        Fixture f; const auto plan = f.plan();
        st::Ledger ledger(f.workspace.root / "run", plan.intent(), {});
        rejects_any([&] { (void)st::inspect_run(f.workspace.root / "run"); });
        check(::chmod((f.workspace.root / "run").c_str(), 0755) == 0, "chmod");
        rejects_any([&] { (void)st::inspect_run(f.workspace.root / "run"); });
    });
    run_case("result_suffix_loss_preserves_pending_without_refunding_usage", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        (void)execute(f, f.plan(), peer);
        const auto path = f.workspace.root / "run/events.r2s";
        const auto raw = st::read_input(path, 65536);
        const auto request_bytes = hc::detail::be(raw, 4, 4) + 76;
        check(::truncate(path.c_str(), static_cast<off_t>(request_bytes)) == 0, "truncate result suffix");
        const auto report = st::inspect_run(f.workspace.root / "run");
        check(report.at("status") == "INTERRUPTED_NO_REEXECUTION" && report.at("pending_request").is_object() &&
              report.at("usage").at("reserved_requests") == 1, "lost result refunded or erased pending request");
    });
    run_case("ledger_byte_corruption_symlink_and_hardlink_are_rejected", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        (void)execute(f, f.plan(), peer);
        const auto path = f.workspace.root / "run/events.r2s";
        fs::create_hard_link(path, f.workspace.root / "extra-link");
        rejects_any([&] { (void)st::inspect_run(f.workspace.root / "run"); });
        fs::remove(f.workspace.root / "extra-link");
        const int fd = ::open(path.c_str(), O_RDWR | O_NOFOLLOW); check(fd >= 0, "open test ledger");
        unsigned char bad = 0;
        const auto wrote = ::pwrite(fd, &bad, 1, 0); ::close(fd); check(wrote == 1, "corrupt test ledger");
        rejects_any([&] { (void)st::inspect_run(f.workspace.root / "run"); });
        fs::create_symlink(f.workspace.root / "run", f.workspace.root / "run-link");
        rejects_any([&] { (void)st::inspect_run(f.workspace.root / "run-link"); });
    });
    run_case("request_deadline_expiring_during_fsync_never_dispatches", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); auto config = configuration();
        config["budget"]["connect_timeout_seconds"] = 1; config["budget"]["request_timeout_seconds"] = 1;
        bool delayed = false;
        const auto report = execute(f, f.plan(config), peer, [&](st::LedgerStep step) {
            if (!delayed && step == st::LedgerStep::before_dispatch_sync) {
                delayed = true; const struct timespec pause{1, 100000000}; ::nanosleep(&pause, nullptr);
            }
        });
        check(report.at("status") == "FAIL_STAGING_PROFILE" && peer->calls.empty() && report.at("usage").at("reserved_requests") == 1 &&
              report.at("events")[1].at("backend_dispatched") == false, "expired reservation reached backend");
    });
    run_case("finish_checkpoint_failure_never_reports_completed_run", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        const auto report = execute(f, f.plan(), peer, [](st::LedgerStep step) {
            if (step == st::LedgerStep::before_finish_sync) throw std::runtime_error("synthetic finish failure");
        });
        check(report.at("status") == "INDETERMINATE_LOCAL_PERSISTENCE" && peer->calls.size() == 1, "finish fsync failure hidden");
    });
}

void staging_delete() {
    run_case("delete_is_exact_conditional_single_request_and_conservative", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        for (const auto status : {204U, 412U, 200U, 403U, 500U}) {
            peer->handler = [status](const hc::HttpRequest& request) { return response(status, {}, {}, request.response_limit); };
            const auto before = peer->calls.size();
            const auto result = store.erase_staging_object("current.json", "\"Exact/ETag\"");
            check(result == (status == 204 ? hc::WriteOutcome::applied : status == 412 ? hc::WriteOutcome::precondition_failed : hc::WriteOutcome::indeterminate), "DELETE outcome");
            check(peer->calls.size() == before + 1 && peer->calls.back().method == hc::HttpMethod::erase &&
                  peer->calls.back().headers.at("if-match") == "\"Exact/ETag\"", "DELETE retried or lost exact ETag");
        }
        rejects_any([&] { (void)store.erase_staging_object("../other", "\"tag\""); });
        rejects_any([&] { (void)store.erase_staging_object("current.json", "*"); });
    });
    run_case("delete_timeout_unverified_and_nonempty_ack_are_unknown", [] {
        auto peer = std::make_shared<ScriptedHttp>(); hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        peer->handler = [](const hc::HttpRequest&) { return hc::HttpResult{hc::HttpDelivery::indeterminate, {}}; };
        check(store.erase_staging_object("current.json", "\"tag\"") == hc::WriteOutcome::indeterminate, "timeout DELETE");
        peer->handler = [](const hc::HttpRequest&) { return hc::HttpResult{hc::HttpDelivery::complete, {204, {}, {}, false}}; };
        check(store.erase_staging_object("current.json", "\"tag\"") == hc::WriteOutcome::indeterminate, "unverified DELETE");
        peer->handler = [](const hc::HttpRequest&) { return hc::HttpResult{hc::HttpDelivery::complete, {204, {{"content-length", "1"}}, bytes("x"), true}}; };
        check(store.erase_staging_object("current.json", "\"tag\"") == hc::WriteOutcome::indeterminate, "nonempty DELETE ACK");
        peer->handler = [](const hc::HttpRequest&) { return hc::HttpResult{hc::HttpDelivery::complete, {204, {{"content-range", "bytes 0-0/1"}}, {}, true}}; };
        check(store.erase_staging_object("current.json", "\"tag\"") == hc::WriteOutcome::indeterminate, "partial DELETE ACK");
        peer->handler = [](const hc::HttpRequest&) { return hc::HttpResult{hc::HttpDelivery::not_sent, {}}; };
        check(store.erase_staging_object("current.json", "\"tag\"") == hc::WriteOutcome::not_applied, "not-sent DELETE");
    });
    run_case("delete_sigv4_independent_vector_and_disabled_curl", [] {
        hc::HttpRequest request;
        const auto host = config().account_id + ".r2.cloudflarestorage.com";
        const auto path = '/' + config().bucket + '/' + config().key_prefix + "current.json";
        request.method = hc::HttpMethod::erase; request.url = "https://" + host + path;
        request.headers = {{"if-match", "\"opaque/Case-123\""}};
        hc::sign_s3_request(request, *credentials(), host, path, signing_time());
        const auto golden = Json::parse(hc::read_file(fs::path(HC_SOURCE_DIR) / "tests/golden/sigv4-delete-v1.json", 16384));
        check(request.headers.at("authorization") == golden.at("authorization"), "DELETE signature differs from independent oracle");
        check(hc::CurlHttpTransport(false).perform(request).delivery == hc::HttpDelivery::not_sent, "disabled DELETE transport dispatched");
    });
}

void no_deletes(const Json& report, const Peer& peer) {
    check(peer.deletes == 0, "retention mode deleted an object");
    for (const auto& request : peer.calls) check(request.method != hc::HttpMethod::erase, "retention mode dispatched DELETE");
    for (const auto& event : report.at("events")) if (event.at("type") == "request")
        check(event.at("method") != "DELETE" && event.at("phase") == "business", "retention mode reserved cleanup");
}

Json retained_role(const Json& report, const std::string& role, const std::string& scope = "-base/") {
    for (const auto& object : report.at("objects"))
        if (object.at("role") == role && object.at("key").get<std::string>().find(scope) != std::string::npos) return object;
    throw std::runtime_error("retained role missing");
}

void staging_retention() {
    run_case("retention_plan_excludes_all_deletes_and_bounds_inventory", [] {
        Fixture f; const auto plan = f.plan(retention_configuration()); const auto intent = plan.intent();
        check(intent.at("schema_version") == 2 && intent.at("planned") == Json({{"reserved_requests", 82},
            {"upload_body_bytes", 39288}, {"download_reserved_bytes", 113535349}}), "retention reservation drift");
        check(!intent.at("stale_delete_precondition_probe").get<bool>() && intent.at("retention").at("planned_objects") == 9 &&
              intent.at("retention").at("planned_body_bytes") == 29271, "retention inventory drift");
        for (const auto& scenario : intent.at("scenarios")) {
            check(scenario.at("cleanup").empty() && scenario.at("objects").size() == 3, "retention cleanup or object set");
            for (const auto& step : scenario.at("business")) check(step.at("method") != "DELETE", "retention DELETE plan");
        }
        check(f.plan().intent().at("stale_delete_precondition_probe") == true && f.plan().intent_hash() != plan.intent_hash(), "v1 silently changed profile");
    });
    run_case("retention_config_is_explicit_versioned_and_strict", [] {
        Fixture f;
        for (const auto& [name, value] : std::vector<std::pair<std::string, Json>>{{"schema_version", 1}, {"cleanup", "verified_created_objects_only"},
                {"profile", "cpp-staging-2050-v1"}}) {
            auto config = retention_configuration(); config[name] = value; rejects_any([&] { (void)f.plan(config); });
        }
        for (const auto& [name, value] : std::vector<std::pair<std::string, Json>>{{"owner", ""}, {"owner", "../owner"},
                {"review_after_seconds", 0}, {"review_after_seconds", 604801}, {"max_objects", 8},
                {"max_cost_usd_through_review", "0.00"}, {"expiry", "automatic"}}) {
            auto config = retention_configuration(); config["retention"][name] = value; rejects_any([&] { (void)f.plan(config); });
        }
        for (const auto& [name, value] : std::vector<std::pair<std::string, Json>>{{"max_reserved_requests", 81},
                {"max_stored_body_bytes", 29270}, {"max_run_seconds", 361}, {"cleanup_seconds_per_namespace", 60}}) {
            auto config = retention_configuration(); config["budget"][name] = value; rejects_any([&] { (void)f.plan(config); });
        }
        auto config = retention_configuration(); config["confirmations"]["fresh_credentials_ready"] = true;
        rejects_any([&] { (void)f.plan(config); });
    });
    run_case("retention_approval_owner_and_budget_changes_require_new_hashes", [] {
        Fixture f; unsigned loaded = 0; const auto original = f.plan(retention_configuration());
        auto changed = retention_configuration(); changed["retention"]["owner"] = "different-owner";
        const auto plan = f.plan(changed);
        check(original.intent_hash() != plan.intent_hash(), "owner not bound");
        rejects_any([&] { (void)st::Executor::run(plan, {true, true, original.config_hash(), original.intent_hash()},
            f.workspace.root / "run", f.workspace.root / "registry", [&] { ++loaded; return credentials(); }, [] { return std::make_shared<Peer>(); }); });
        const auto confirmations = retention_configuration().at("confirmations");
        for (const auto& item : confirmations.items()) {
            auto config = retention_configuration(); config["confirmations"][item.key()] = false;
            const auto unconfirmed = f.plan(config);
            rejects_any([&] { (void)st::Executor::run(unconfirmed, {true, true, unconfirmed.config_hash(), unconfirmed.intent_hash()},
                f.workspace.root / "run", f.workspace.root / "registry", [&] { ++loaded; return credentials(); }, [] { return std::make_shared<Peer>(); }); });
        }
        check(!loaded && !fs::exists(f.workspace.root / "run"), "retention confirmation had side effects");
    });
    run_case("retention_three_scenarios_pass_with_nine_owned_objects_and_no_delete", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "PASS_STAGING_RETAINED" && peer->calls.size() == 79 && peer->writes == 10 && peer->objects.size() == 9,
              "retention nominal trace mismatch");
        check(report.at("usage") == Json({{"reserved_requests", 80}, {"upload_body_bytes", 39288}, {"download_reserved_bytes", 113527157}}) &&
              report.at("adapter_usage") == report.at("usage") && report.at("durable_budget_matches_adapter") == true, "retention budget mismatch");
        check(report.at("retention").at("retained_owned_body_bytes") == 29271 &&
              report.at("retention").at("state_counts").at("retained_owned") == 9, "retained inventory hidden");
        for (const auto& object : report.at("objects")) check(object.at("owned") == true && object.at("deleted_ack") == false &&
              object.at("absence_verified") == false && object.at("uncertain") == false, "false cleared/unknown inventory");
        const auto reopened = st::inspect_run(f.workspace.root / "run");
        check(reopened.at("status") == "PASS_STAGING_RETAINED" && reopened.at("objects") == report.at("objects") &&
              reopened.at("usage") == report.at("usage") && reopened.at("capabilities") == report.at("capabilities"), "retention inspect changed evidence");
        no_deletes(report, *peer);
    });
    run_case("retention_capabilities_are_observations_not_delete_verification", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); const auto report = execute(f, f.plan(retention_configuration()), peer);
        const auto& caps = report.at("capabilities");
        for (const auto* name : {"create_only_pack", "create_only_pack_conflict", "create_only_manifest", "create_only_manifest_conflict",
                                "pointer_create", "pointer_cas", "stale_pointer_cas_rejected", "range_header_206", "pinned_range_206"})
            check(caps.at(name).at("state") == "observed_match", "capability observation missing");
        check(caps.at("conditional_delete").at("state") == "not_tested_not_required" && caps.at("conditional_delete").at("verified") == false &&
              caps.at("general_endpoint_verification") == false && caps.at("publication_read_profile_completed") == true, "capability overclaim");
        no_deletes(report, *peer);
    });
    run_case("retention_unknown_pack_write_never_adopts_or_cleans", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::put) result = {hc::HttpDelivery::indeterminate, {}};
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->calls.size() == 4 && peer->objects.size() == 1 &&
              report.at("usage").at("reserved_requests") == 5 && report.at("durable_budget_matches_adapter") == true, "unknown write trace mismatch");
        const auto& pack = retained_role(report, "pack");
        check(pack.at("retention_state") == "unknown_write" && pack.at("owned") == false && pack.at("absence_verified") == false &&
              report.at("retention").at("retained_owned_body_bytes") == 0, "unknown write adopted");
        check(report.at("capabilities").at("create_only_pack").at("state") == "observed_failure_or_indeterminate", "unknown capability passed");
        no_deletes(report, *peer);
    });
    run_case("retention_unknown_pointer_matching_recovery_stops_without_adoption", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::put && request.url.find("current.json") != std::string::npos)
                result = {hc::HttpDelivery::indeterminate, {}};
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        const auto& pointer = retained_role(report, "pointer");
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->calls.size() == 15 && peer->objects.size() == 3 &&
              pointer.at("owned") == false && pointer.at("retention_state") == "unknown_write" &&
              report.at("retention").at("retained_owned_body_bytes") == 9497, "matching recovery adopted uncertain pointer");
        no_deletes(report, *peer);
    });
    run_case("retention_preexisting_key_reports_unowned_without_mutation", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            return response(200, bytes("not-a-pointer"), {{"etag", "\"existing\""}}, request.response_limit);
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->calls.size() == 1 && peer->writes == 0 &&
              retained_role(report, "pointer").at("retention_state") == "unowned_observed" &&
              report.at("retention").at("state_counts").at("unobserved") == 8, "preexisting objects adopted or treated absent");
        no_deletes(report, *peer);
    });
    run_case("retention_manifest_conflict_invalidates_prior_absence", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            if (request.method == hc::HttpMethod::put && request.url.find("/manifests/") != std::string::npos) return response(412);
            return std::nullopt;
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->writes == 1 &&
              retained_role(report, "manifest").at("retention_state") == "unowned_observed" &&
              retained_role(report, "manifest").at("absence_verified") == false &&
              report.at("retention").at("retained_owned_body_bytes") == 8768, "conflict adopted or absence persisted");
        no_deletes(report, *peer);
    });
    run_case("retention_short_range_stops_without_cleanup", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.headers.count("range")) result.response.body.pop_back();
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->writes == 1 && peer->objects.size() == 1 &&
              retained_role(report, "pack").at("retention_state") == "retained_owned", "short Range cleaned or passed");
        no_deletes(report, *peer);
    });
    run_case("retention_changed_identity_cannot_be_reported_as_owned_retention", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::get && request.response_limit == hc::kMaxObjectBytes && result.response.status == 200)
                result.response.headers["etag"] = "\"changed\"";
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && retained_role(report, "pack").at("retention_state") == "identity_changed" &&
              report.at("retention").at("retained_owned_body_bytes") == 0, "changed identity counted as retained ownership");
        no_deletes(report, *peer);
    });
    run_case("retention_ignored_put_condition_fails_without_profile_fallback", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->before = [raw = peer.get()](const hc::HttpRequest& request) -> std::optional<hc::HttpResult> {
            if (request.method == hc::HttpMethod::put && request.headers.count("if-match") &&
                raw->objects.at(request.url).etag != request.headers.at("if-match")) {
                raw->objects[request.url] = {request.body, "\"ignored-cas\""}; ++raw->writes;
                return response(200, {}, {{"etag", "\"ignored-cas\""}}, 0);
            }
            return std::nullopt;
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->calls.size() == 26 && peer->objects.size() == 3 &&
              report.at("capabilities").at("stale_pointer_cas_rejected").at("state") == "observed_failure_or_indeterminate", "ignored PUT condition accepted");
        no_deletes(report, *peer);
    });
    run_case("retention_cancellation_keeps_owned_data_without_cleanup_budget", [] {
        Fixture f; auto peer = std::make_shared<Peer>();
        peer->after = [](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method == hc::HttpMethod::get && request.response_limit == hc::kMaxObjectBytes && result.response.status == 200)
                request.cancelled->store(true);
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->objects.size() == 1 && peer->writes == 1 &&
              report.at("durable_budget_matches_adapter") == true, "cancel triggered cleanup or budget loss");
        no_deletes(report, *peer);
    });
    for (const auto* fault : {"missing", "newer"}) run_case(std::string("retention_read_only_resume_") + fault + "_retains_prior_scenarios", [fault] {
        Fixture f; auto peer = std::make_shared<Peer>(); unsigned reads = 0;
        peer->after = [&](const hc::HttpRequest& request, hc::HttpResult& result) {
            if (request.method != hc::HttpMethod::get || request.url.find("-resume/current.json") == std::string::npos || ++reads < 3) return;
            if (std::string(fault) == "missing") result = response(404, bytes("<Error><Code>NoSuchKey</Code></Error>"), {}, request.response_limit);
            else {
                auto value = hc::parse_pointer(text(result.response.body)); value.publication_seq = 2;
                result = response(200, bytes(hc::serialize_pointer(value)), {{"etag", "\"newer\""}}, request.response_limit);
            }
        };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        check(report.at("status") == "FAIL_STAGING_RETAINED" && peer->writes == 10 && peer->objects.size() == 9 &&
              report.at("durable_budget_matches_adapter") == true, "resume fault lost prior scenarios");
        const auto& object = retained_role(report, "pointer", "-resume/");
        check(object.at("retention_state") == (std::string(fault) == "missing" ? "observed_absent" : "identity_changed"), "resume observation misreported");
        check(peer->calls.back().method == hc::HttpMethod::get && peer->calls.back().url.find("current.json") != std::string::npos, "resume continued past target");
        no_deletes(report, *peer);
    });
    run_case("retention_registry_cannot_be_reused_by_other_profile", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); unsigned loaded = 0;
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        const auto report = execute(f, f.plan(retention_configuration()), peer);
        for (const auto& config : {configuration(), retention_configuration()}) {
            const auto plan = f.plan(config);
            rejects_any([&] { (void)st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()},
                f.workspace.root / (config.at("schema_version") == 1 ? "v1-retry" : "v2-retry"), f.workspace.root / "registry",
                [&] { ++loaded; return credentials(); }, [peer] { return peer; }); });
        }
        check(!loaded && peer->calls.size() == 1, "profile switch reused namespace");
        no_deletes(report, *peer);
    });
    run_case("retention_pending_write_reopens_unknown_without_budget_refund", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); unsigned results = 0;
        const auto report = execute(f, f.plan(retention_configuration()), peer, [&](st::LedgerStep step) {
            if (step == st::LedgerStep::before_result_sync && ++results == 4) throw std::runtime_error("synthetic ACK checkpoint failure");
        });
        check(report.at("status") == "INDETERMINATE_LOCAL_PERSISTENCE" && peer->calls.size() == 4, "checkpoint failure continued");
        const auto path = f.workspace.root / "run/events.r2s";
        const auto raw = st::read_input(path, 65536);
        size_t offset = 0;
        while (offset < raw.size()) {
            const auto size = static_cast<size_t>(hc::detail::be(raw, offset + 4, 4));
            const auto event = Json::parse(raw.begin() + static_cast<std::ptrdiff_t>(offset + 44), raw.begin() + static_cast<std::ptrdiff_t>(offset + 44 + size));
            offset += size + 76;
            if (event.at("type") == "request" && event.at("index") == 4) break;
        }
        check(::truncate(path.c_str(), static_cast<off_t>(offset)) == 0, "truncate retention result");
        const auto reopened = st::inspect_run(f.workspace.root / "run");
        check(reopened.at("status") == "INTERRUPTED_NO_REEXECUTION" && reopened.at("pending_request").at("index") == 4 &&
              reopened.at("usage").at("reserved_requests") == 4 && reopened.at("usage").at("upload_body_bytes") == 8768 &&
              retained_role(reopened, "pack").at("retention_state") == "unknown_write" &&
              retained_role(reopened, "pack").at("owned") == false, "pending write refunded or adopted on inspect");
        no_deletes(reopened, *peer);
    });
    run_case("retention_review_deadline_is_persistent_not_automatic_expiry", [] {
        Fixture f; auto peer = std::make_shared<Peer>(); auto config = retention_configuration();
        config["retention"]["review_after_seconds"] = 1;
        peer->before = [](const hc::HttpRequest&) -> std::optional<hc::HttpResult> { return response(403); };
        const auto report = execute(f, f.plan(config), peer);
        const auto before = st::read_input(f.workspace.root / "run/events.r2s", 65536);
        const struct timespec pause{1, 100000000}; ::nanosleep(&pause, nullptr);
        const auto reopened = st::inspect_run(f.workspace.root / "run");
        check(reopened.at("retention").at("review_due_unix_seconds") == report.at("retention").at("review_due_unix_seconds") &&
              reopened.at("retention").at("review_overdue_by_local_clock") == true &&
              reopened.at("retention").at("automatic_delete") == false &&
              st::read_input(f.workspace.root / "run/events.r2s", 65536) == before && peer->calls.size() == 1, "review reset or triggered I/O");
        no_deletes(reopened, *peer);
    });
    run_case("retention_start_checkpoint_failure_precedes_credentials", [] {
        Fixture f; const auto plan = f.plan(retention_configuration()); unsigned loaded = 0;
        rejects_any([&] { (void)st::Executor::run(plan, {true, true, plan.config_hash(), plan.intent_hash()},
            f.workspace.root / "run", f.workspace.root / "registry", [&] { ++loaded; return credentials(); },
            [] { return std::make_shared<Peer>(); }, [](st::LedgerStep step) {
                if (step == st::LedgerStep::before_start_sync) throw std::runtime_error("synthetic start checkpoint failure");
            }); });
        check(!loaded, "retention start failure loaded secrets");
    });
    run_case("retention_ledger_independently_rejects_delete_and_oversized_write", [] {
        for (const bool erase : {true, false}) {
            Fixture f; const auto plan = f.plan(retention_configuration()); const auto intent = plan.intent();
            st::Ledger ledger(f.workspace.root / "run", intent, {});
            const std::string key = intent.at("scenarios")[0].at("namespace").get<std::string>() + "current.json";
            rejects_any([&] { ledger.request({{"type", "request"}, {"index", 1}, {"key", key}, {"phase", "business"},
                {"method", erase ? "DELETE" : "PUT"}, {"upload_bytes", erase ? 0 : 65536}, {"download_reserved_bytes", 4096}}); });
            check(ledger.poisoned() && ledger.usage().requests == 0, "ledger accepted forbidden retention request");
        }
    });
}

}  // namespace

int main(int argc, char** argv) {
    try {
        std::cout << std::unitbuf;
        check(argc == 2 || argc == 3, "choose staging test suite and optional exact case");
        const std::string suite = argv[1];
        if (argc == 3) case_filter = argv[2];
        if (suite == "staging_plan") staging_plan();
        else if (suite == "staging_execution") staging_execution();
        else if (suite == "staging_ledger") staging_ledger();
        else if (suite == "staging_delete") staging_delete();
        else if (suite == "staging_retention") staging_retention();
        else throw std::runtime_error("unknown suite");
        check(cases > 0, "unknown staging test case");
        std::cout << "PASS " << suite << " cases=" << cases << '\n'; return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
