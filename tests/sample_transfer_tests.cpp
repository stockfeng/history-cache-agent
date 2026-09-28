#include "transport_fixture.h"
#include "../tools/sample_transfer.h"

using namespace test;

int main() {
    try {
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
