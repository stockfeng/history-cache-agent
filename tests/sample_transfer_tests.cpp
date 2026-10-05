#include "transport_fixture.h"
#include "../tools/sample_transfer.h"
#include "../tools/snapshot_diagnostics.h"
#include "../tools/verified_object_store.h"
#include "history_cache/journal.h"

using namespace test;

int main() {
    try {
        run_case("verified_remote_objects_reused_but_not_current_or_creates", [] {
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store remote(config(), peer, credentials(), limits(), signing_time);
            hc::sample::VerifiedObjectStore cache(remote, 1024);
            const auto body = bytes("verified immutable contents");
            const auto receipt = hc::sample::ensure_object(cache, key(body), body);
            check(receipt.readback == body && peer->calls.size() == 3, "must perform remote readback");
            check(cache.get(key(body), body.size()) == body && peer->calls.size() == 3, "GET was not reused");
            const auto range = cache.open_range(key(body), body.size());
            check(range(1, 4) == hc::Bytes(body.begin() + 1, body.begin() + 5) &&
                  peer->calls.size() == 3, "range did not use verified remote bytes");
            rejects(hc::ErrorCode::resource_limit, [&] { (void)cache.get(key(body), 1); });
            rejects(hc::ErrorCode::corrupt, [&] { (void)cache.open_range(key(body), 1); });
            rejects(hc::ErrorCode::invalid, [&] { (void)range(UINT64_MAX, 1); });
            (void)cache.read_current();
            (void)cache.read_current();
            check(peer->calls.size() == 5, "mutable current was cached");
            (void)cache.create(key(body), body);
            check(cache.bytes() == 0, "create did not invalidate prior read");
            (void)cache.get(key(body), body.size());
            check(peer->calls.size() == 7, "create bypassed fresh readback");
            hc::sample::VerifiedObjectStore restarted(remote, 1024);
            (void)restarted.get(key(body), body.size());
            check(peer->calls.size() == 8, "new invocation reused old observations");
        });
        run_case("verified_cache_budget_and_corruption_fail_closed", [] {
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store remote(config(), peer, credentials(), limits(), signing_time);
            hc::sample::VerifiedObjectStore cache(remote, 1);
            const auto body = bytes("does not fit");
            (void)hc::sample::ensure_object(cache, key(body), body);
            check(cache.bytes() == 0, "cache exceeded cap");
            (void)cache.get(key(body), body.size());
            check(peer->calls.size() == 4, "oversized object cached");
            peer->objects.begin()->second.body[0] ^= 1;
            rejects(hc::ErrorCode::corrupt, [&] { (void)cache.get(key(body), body.size()); });
            check(cache.bytes() == 0, "corrupt bytes cached");
        });
        run_case("verified_cache_preserves_unknown_pointer_recovery", [] {
            Workspace workspace;
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store remote(config(), peer, credentials(), limits(), signing_time);
            hc::sample::VerifiedObjectStore cache(remote, 1024 * 1024);
            const auto path = workspace.root / "journal";
            hc::JournaledPublisher::prepare(path, cache.scope_id(), manifest(), 0);
            peer->fault = PointerFault::unknown;
            {
                hc::JournaledPublisher journal(path, cache);
                check(journal.resume().outcome == hc::PublishOutcome::indeterminate,
                      "cache hid uncertain pointer outcome");
            }
            hc::sample::VerifiedObjectStore reopened(remote, 1024 * 1024);
            hc::JournaledPublisher journal(path, reopened);
            check(journal.resume().outcome == hc::PublishOutcome::committed &&
                  journal.target().publication_seq == 1 && peer->writes == 2,
                  "recovery changed target or repeated pointer write");
        });
        run_case("diagnostics_bounded_redacted_and_no_retry", [] {
            auto peer = std::make_shared<ScriptedHttp>();
            hc::sample::SnapshotDiagnostics trace(peer);
            trace.stage = "journal_resume";
            hc::HttpRequest request;
            request.method = hc::HttpMethod::put;
            request.url = "https://SENSITIVE_HOST/SECRET_PREFIX/current.json?SECRET_QUERY";
            request.headers["authorization"] = "SECRET_HEADER";
            request.body = bytes("SECRET_BODY");
            peer->handler = [](const auto&) {
                hc::HttpResult result;
                result.failure = hc::HttpFailure::tls;
                result.diagnostics = {60, 20};
                return result;
            };
            check(trace.perform(request).failure == hc::HttpFailure::tls, "diagnostics changed TLS failure");
            const auto tls = trace.summary().at("recent").back();
            check(tls.at("stage") == "journal_resume" && tls.at("method") == "PUT" &&
                  tls.at("object_kind") == "current" && tls.at("http_status") == 0 &&
                  tls.at("failure") == "tls" && tls.at("curl_code") == 60 &&
                  tls.at("tls_verify_result") == 20, "lost TLS diagnosis");
            for (const unsigned status : {403, 412}) {
                peer->handler = [status](const auto&) { return response(status, bytes("SECRET_RESPONSE")); };
                check(trace.perform(request).response.status == status, "changed HTTP failure");
                check(trace.summary().at("recent").back().at("http_status") == status, "lost HTTP status");
                check(trace.summary().at("recent").back().at("curl_code") == -1, "invented curl diagnosis");
            }
            peer->handler = [](const auto&) -> hc::HttpResult {
                throw hc::Error(hc::ErrorCode::io, "SECRET_EXCEPTION");
            };
            for (unsigned i = 0; i < 40; ++i)
                rejects(hc::ErrorCode::io, [&] { (void)trace.perform(request); });
            const auto summary = trace.summary();
            check(summary.at("total") == 43 && summary.at("dropped") == 11 &&
                  summary.at("recent").size() == 32 && peer->calls.size() == 43,
                  "unbounded diagnostics or hidden retries");
            check(summary.at("recent").back().at("transport_exception") == true &&
                  summary.dump().find("SECRET") == std::string::npos &&
                  summary.dump().find("SENSITIVE") == std::string::npos, "diagnostics leaked payload");
        });
        run_case("diagnosed_journal_unknown_recovers_same_target", [] {
            Workspace workspace;
            auto peer = std::make_shared<FakeS3>();
            auto trace = std::make_shared<hc::sample::SnapshotDiagnostics>(peer);
            trace->stage = "journal_resume";
            hc::S3Store store(config(), trace, credentials(), limits(), signing_time);
            const auto journal_path = workspace.root / "journal";
            hc::JournaledPublisher::prepare(journal_path, store.scope_id(), manifest(), 0);
            peer->fault = PointerFault::unknown;
            {
                hc::JournaledPublisher journal(journal_path, store);
                check(journal.resume().outcome == hc::PublishOutcome::indeterminate, "expected unresolved publication");
            }
            bool found = false;
            const auto diagnosis = trace->summary();
            for (const auto& event : diagnosis.at("recent"))
                found |= event.at("method") == "PUT" && event.at("object_kind") == "current" &&
                         event.at("delivery") == "indeterminate";
            check(found, "unknown pointer write not diagnosed");
            hc::JournaledPublisher resumed(journal_path, store);
            check(resumed.resume().outcome == hc::PublishOutcome::committed &&
                  resumed.target().publication_seq == 1 && peer->writes == 2,
                  "diagnostics changed recovery identity or caused extra writes");
        });
        run_case("create_then_resume_is_read_only", [] {
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto body = bytes("observation transfer test");
            const auto first = hc::sample::ensure_object(store, key(body), body);
            check(!first.already_present && first.write == hc::WriteOutcome::applied &&
                  first.readback == body && peer->calls.size() == 3 && peer->writes == 1, "first transfer");
            const auto next = hc::sample::ensure_object(store, key(body), body);
            check(next.already_present && !next.write && next.readback == body &&
                  peer->calls.size() == 4 && peer->writes == 1, "resume repeated a successful PUT");
        });
        run_case("ack_loss_resolves_by_exact_readback", [] {
            auto peer = std::make_shared<FakeS3>();
            auto adapter = std::make_shared<ScriptedHttp>();
            adapter->handler = [peer](const auto& request) {
                auto result = peer->perform(request);
                if (request.method == hc::HttpMethod::put) return hc::HttpResult{hc::HttpDelivery::indeterminate, {}};
                return result;
            };
            hc::S3Store store(config(), adapter, credentials(), limits(), signing_time);
            const auto body = bytes("ack loss");
            const auto receipt = hc::sample::ensure_object(store, key(body), body);
            check(receipt.write == hc::WriteOutcome::indeterminate && receipt.readback == body && peer->writes == 1,
                  "ACK loss was not reconciled");
        });
        run_case("unresolved_write_stops_without_retry", [] {
            auto peer = std::make_shared<ScriptedHttp>();
            peer->handler = [](const auto& request) {
                if (request.method == hc::HttpMethod::put) return hc::HttpResult{hc::HttpDelivery::indeterminate, {}};
                return response(404, bytes("<Error><Code>NoSuchKey</Code></Error>"), {}, request.response_limit);
            };
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto body = bytes("not yet observed");
            rejects(hc::ErrorCode::missing, [&] { (void)hc::sample::ensure_object(store, key(body), body); });
            check(peer->calls.size() == 3, "unresolved write retried");
        });
        run_case("permission_failure_never_becomes_absence", [] {
            auto peer = std::make_shared<ScriptedHttp>();
            peer->handler = [](const auto& request) { return response(403, {}, {}, request.response_limit); };
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto body = bytes("forbidden");
            rejects(hc::ErrorCode::io, [&] { (void)hc::sample::ensure_object(store, key(body), body); });
            check(peer->calls.size() == 1 && peer->calls.front().method == hc::HttpMethod::get, "unsafe create after 403");
        });
        run_case("racing_identical_create_does_not_claim_ownership", [] {
            auto peer = std::make_shared<FakeS3>();
            auto adapter = std::make_shared<ScriptedHttp>();
            adapter->handler = [peer](const auto& request) {
                if (request.method == hc::HttpMethod::put)
                    peer->objects[request.url] = {request.body, "\"other-writer\""};
                return peer->perform(request);
            };
            hc::S3Store store(config(), adapter, credentials(), limits(), signing_time);
            const auto body = bytes("racing create");
            const auto receipt = hc::sample::ensure_object(store, key(body), body);
            check(receipt.write == hc::WriteOutcome::precondition_failed && receipt.readback == body && peer->writes == 0,
                  "racing create overwrote data or claimed ACK");
        });
        run_case("corrupt_existing_object_is_not_replaced", [] {
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto body = bytes("correct");
            (void)hc::sample::ensure_object(store, key(body), body);
            peer->objects.begin()->second.body[0] ^= 1;
            rejects(hc::ErrorCode::corrupt, [&] { (void)hc::sample::ensure_object(store, key(body), body); });
            check(peer->writes == 1, "replaced corrupt remote data");
        });
        run_case("resume_partial_pair_only_creates_missing_object", [] {
            auto peer = std::make_shared<FakeS3>();
            const auto one = bytes("first object"), two = bytes("second object");
            {
                hc::S3Store previous(config(), peer, credentials(), limits(), signing_time);
                check(previous.create(key(one), one) == hc::WriteOutcome::applied, "seed failed");
            }
            hc::S3Store resumed(config(), peer, credentials(), limits(), signing_time);
            const auto first = hc::sample::ensure_object(resumed, key(one), one);
            const auto second = hc::sample::ensure_object(resumed, key(two), two);
            check(first.already_present && !first.write && second.write == hc::WriteOutcome::applied &&
                  peer->writes == 2 && resumed.usage().requests == 4, "partial pair recovery");
        });
        std::cout << "PASS sample_transfer cases=" << cases << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL sample_transfer: " << error.what() << '\n';
        return 1;
    }
}
