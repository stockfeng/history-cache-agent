#include "transport_fixture.h"
#include "history_cache/publisher.h"
#include "history_cache/reader.h"

using namespace test;

namespace {

void http_protocol() {
    run_case("fragmented_body_interim_headers_and_opaque_etag", [] {
        hc::HttpResponseBuffer parser(5);
        parser.header("HTTP/1.1 100 Continue\r\n"); parser.header("\r\n");
        parser.header("HTTP/1.1 103 Early Hints\r\n"); parser.header("Link: </synthetic>\r\n"); parser.header("\r\n");
        parser.header("HTTP/1.1 200 OK\r\n"); parser.header("ETag: \"opaque/Case\"\r\n");
        parser.header("Content-Length: 5\r\n"); parser.header("\r\n");
        const auto body = bytes("hello");
        for (const auto& ch : body) parser.body(&ch, 1);
        const auto result = parser.finish();
        check(text(result.body) == "hello" && result.headers.at("etag") == "\"opaque/Case\"" && !result.tls_verified,
              "parser invented TLS proof or changed opaque ETag");
        rejects(hc::ErrorCode::corrupt, [&] { (void)parser.finish(); });
    });
    run_case("content_length_and_chunked_framing_are_unambiguous", [] {
        for (const auto& fields : {hc::HttpHeaders{{"Content-Length", "2"}, {"content-length", "2"}},
                                  hc::HttpHeaders{{"content-length", "2"}, {"transfer-encoding", "chunked"}},
                                  hc::HttpHeaders{{"transfer-encoding", "gzip, chunked"}},
                                  hc::HttpHeaders{{"content-encoding", "gzip"}},
                                  hc::HttpHeaders{{"etag", "\"a\""}, {"ETag", "\"b\""}}})
            rejects(hc::ErrorCode::corrupt, [&] { (void)response(200, bytes("hi"), fields); });
        auto result = response(200, bytes("decoded-chunks"), {{"transfer-encoding", "chunked"}});
        check(text(result.response.body) == "decoded-chunks", "curl-decoded chunk body rejected");
    });
    run_case("receive_budgets_apply_before_body_allocation", [] {
        rejects(hc::ErrorCode::resource_limit, [] { (void)response(200, bytes("large"), {}, 2); });
        rejects(hc::ErrorCode::resource_limit, [] { (void)response(503, hc::Bytes(4097), {}, 0); });
        hc::HttpResponseBuffer parser(3);
        parser.header("HTTP/1.1 200 OK\r\n"); parser.header("\r\n");
        auto body = bytes("four");
        rejects(hc::ErrorCode::resource_limit, [&] { parser.body(body.data(), body.size()); });
        hc::HttpResponseBuffer headers(0);
        headers.header("HTTP/1.1 200 OK\r\n");
        rejects(hc::ErrorCode::resource_limit, [&] { headers.header("x: " + std::string(32768, 'a') + "\r\n"); });
    });
    run_case("short_reads_trailers_and_invalid_statuses_fail_closed", [] {
        rejects(hc::ErrorCode::corrupt, [] { (void)response(200, bytes("one"), {{"content-length", "4"}}); });
        hc::HttpResponseBuffer parser(0);
        parser.header("HTTP/1.1 200 OK\r\n"); parser.header("\r\n");
        rejects(hc::ErrorCode::corrupt, [&] { parser.header("etag: \"trailer\"\r\n"); });
        for (const auto& line : {"HTTP/2 200 OK\r\n", "HTTP/1.1 101 Switching\r\n", "HTTP/1.1 200 OK\n", "HTTP/1.1 200 OK\rBAD\r\n"})
            rejects(hc::ErrorCode::corrupt, [&] { hc::HttpResponseBuffer test(0); test.header(line); });
        for (const auto& value : {"-1", "+1", " 1", "1x", "18446744073709551616", ""})
            rejects(hc::ErrorCode::corrupt, [&] { (void)hc::http_unsigned(value); });
        hc::HttpResponseBuffer interim(0);
        interim.header("HTTP/1.1 100 Continue\r\n"); interim.header("\r\n");
        rejects(hc::ErrorCode::corrupt, [&] { (void)interim.finish(); });
    });
}

void sigv4() {
    run_case("python_independent_sigv4_golden_vectors", [] {
        const auto golden = nlohmann::json::parse(hc::read_file(fs::path(HC_SOURCE_DIR) / "tests/golden/sigv4-v1.json", hc::kMaxMetadataBytes));
        auto keys = credentials();
        for (const auto& value : golden.at("cases")) {
            hc::HttpRequest request;
            request.method = value.at("method") == "GET" ? hc::HttpMethod::get : hc::HttpMethod::put;
            const auto host = value.at("host").get<std::string>(), path = value.at("path").get<std::string>();
            request.url = "https://" + host + path;
            request.body = hc::unhex(value.at("body_hex").get<std::string>());
            request.headers = value.at("headers").get<hc::HttpHeaders>();
            hc::sign_s3_request(request, *keys, host, path, signing_time());
            check(request.headers.at("authorization") == value.at("authorization") &&
                  request.headers.at("x-amz-content-sha256") == value.at("payload_sha256"), "SigV4 golden mismatch");
        }
    });
    run_case("signing_rejects_ambiguous_paths_headers_and_calendar_dates", [] {
        auto keys = credentials();
        const auto host = config().account_id + ".r2.cloudflarestorage.com";
        for (const auto& path : {"/../x", "/x/..", "/./x", "/x//y", "/x?query", "/x%2Fy", "/x/.", "/x y"}) {
            hc::HttpRequest request; request.url = "https://" + host + path;
            rejects(hc::ErrorCode::invalid, [&] { hc::sign_s3_request(request, *keys, host, path, signing_time()); });
        }
        for (const auto& date : {"20260230T010203Z", "20261301T010203Z", "20260921T250203Z", "20260921T0102xxZ"}) {
            hc::HttpRequest request; request.url = "https://" + host + "/safe";
            rejects(hc::ErrorCode::invalid, [&] { hc::sign_s3_request(request, *keys, host, "/safe", date); });
        }
        for (const auto& header : {hc::HttpHeaders{{"Host", host}}, hc::HttpHeaders{{"range", "bytes=0-1\r\nX:x"}},
                                  hc::HttpHeaders{{"range", " bytes=0-1"}}, hc::HttpHeaders{{"authorization", "old"}}}) {
            hc::HttpRequest request; request.url = "https://" + host + "/safe"; request.headers = header;
            rejects(hc::ErrorCode::invalid, [&] { hc::sign_s3_request(request, *keys, host, "/safe", signing_time()); });
        }
    });
    run_case("credentials_validated_without_echoing_sensitive_input", [] {
        for (const auto& value : {std::string("bad\ncredential"), std::string(129, 'x'), std::string()}) {
            try { hc::S3Credentials test("SYNTHETIC", value); throw std::runtime_error("accepted invalid secret"); }
            catch (const hc::Error& error) {
                check(std::string(error.what()) == "invalid S3 credential shape", "credential leaked in error");
            }
        }
    });
}

void s3_protocol() {
    run_case("isolated_staging_profiles_do_not_expand_production_or_allow_nested_paths", [] {
        Workspace workspace;
        auto profile = nlohmann::json{{"version", 1}, {"environment", "staging"},
            {"account_id", std::string(32, 'a')}, {"bucket", "history-cache-staging"},
            {"prefix", "r2-history-staging/isolated-compact-001/"},
            {"jurisdiction", "default"}, {"role", "reader"}};
        const auto path = workspace.root / "isolated.json";
        hc::write_new_file(path, profile.dump());
        auto cfg = hc::load_storage_profile(path, "reader");
        cfg.key_prefix += "factors-aapl-us-v1/";
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store reader(cfg, peer, credentials(), limits(), signing_time);
        auto original = config();
        original.key_prefix = "r2-history-staging/factors-aapl-us-v1/";
        hc::S3Store other(original, peer, credentials(), limits(), signing_time);
        check(reader.scope_id() != other.scope_id(), "isolated scope aliases original");
        unsigned i = 0;
        for (const auto& prefix : {"r2-history-staging/isolated-/", "r2-history-staging/isolated-a/b/",
                "r2-history-staging/isolated-../", "r2-history-staging/other/",
                "r2-history-production/isolated-a/"}) {
            profile["prefix"] = prefix;
            if (std::string(prefix).find("production") != std::string::npos) {
                profile["environment"] = "production"; profile["bucket"] = "history-cache-production";
            }
            const auto invalid = workspace.root / ("invalid-" + std::to_string(i++) + ".json");
            hc::write_new_file(invalid, profile.dump());
            rejects(hc::ErrorCode::invalid, [&] { (void)hc::load_storage_profile(invalid, "reader"); });
        }
        cfg.key_prefix += "nested/";
        rejects(hc::ErrorCode::invalid, [&] { hc::S3Store invalid(cfg, peer, credentials()); });
        check(peer->calls.empty(), "scope validation made network calls");
    });
    run_case("production_profile_is_explicit_reader_cannot_write_or_delete", [] {
        Workspace workspace;
        const auto path = workspace.root / "storage.json";
        auto profile = nlohmann::json{{"version", 1}, {"environment", "production"},
            {"account_id", std::string(32, 'a')}, {"bucket", "history-cache-production"},
            {"prefix", "r2-history-production/"}, {"jurisdiction", "default"}, {"role", "reader"}};
        hc::write_new_file(path, profile.dump());
        auto cfg = hc::load_storage_profile(path, "reader");
        cfg.key_prefix += "history-test-001/";
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store reader(cfg, peer, credentials(), limits(), signing_time);
        rejects(hc::ErrorCode::invalid, [&] { reader.create(key(bytes("body")), bytes("body")); });
        rejects(hc::ErrorCode::invalid, [&] { reader.write_current(pointer(), std::nullopt); });
        rejects(hc::ErrorCode::invalid, [&] { reader.erase_staging_object("current.json", "\"etag\""); });
        rejects(hc::ErrorCode::invalid, [&] { (void)hc::load_storage_profile(path, "publisher"); });
        cfg.read_only = false;
        hc::S3Store writer(cfg, peer, credentials(), limits(), signing_time);
        rejects(hc::ErrorCode::invalid, [&] { writer.erase_staging_object("current.json", "\"etag\""); });
        cfg.environment = "staging";
        rejects(hc::ErrorCode::invalid, [&] { hc::S3Store wrong(cfg, peer, credentials()); });
        check(peer->calls.empty(), "scope validation performed network I/O");
    });
    run_case("staging_scope_is_validated_and_credential_independent", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store one(config(), peer, credentials(), limits(), signing_time);
        hc::S3Store rotated(config(), peer, std::make_shared<const hc::S3Credentials>("ROTATED", "ROTATEDSECRET"), limits(), signing_time);
        check(one.scope_id() == rotated.scope_id(), "credential rotation changed journal scope");
        auto cfg = config(); cfg.jurisdiction = "eu";
        hc::S3Store eu(cfg, peer, credentials(), limits(), signing_time);
        check(eu.scope_id() != one.scope_id(), "jurisdiction not scoped");
        for (const auto& bucket : {"history-prod-staging", "history-production", "bucket", "staging.with.dot", "STAGING", "-staging"}) {
            cfg = config(); cfg.bucket = bucket;
            rejects(hc::ErrorCode::invalid, [&] { hc::S3Store bad(cfg, peer, credentials()); });
        }
        for (const auto& prefix : {"r2-history-staging/", "r2-history-staging/x/", "r2-history-staging/../../", "other/run-001/"}) {
            cfg = config(); cfg.key_prefix = prefix;
            rejects(hc::ErrorCode::invalid, [&] { hc::S3Store bad(cfg, peer, credentials()); });
        }
        cfg = config(); cfg.account_id[0] = 'G';
        rejects(hc::ErrorCode::invalid, [&] { hc::S3Store bad(cfg, peer, credentials()); });
        check(peer->calls.empty(), "invalid config reached HTTP");
    });
    run_case("pointer_body_and_etag_come_from_one_authoritative_get", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        peer->handler = [](const auto& req) { return response(200, pointer(), {{"etag", "\"opaque/Case\""}}, req.response_limit); };
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto observed = store.read_current();
        check(observed && observed->bytes == pointer() && observed->etag == "\"opaque/Case\"" && peer->calls.size() == 1,
              "pointer GET was split or altered");
        const auto& req = peer->calls[0];
        check(req.method == hc::HttpMethod::get && req.headers.at("cache-control") == "no-cache, no-store" &&
              req.headers.at("accept-encoding") == "identity" && !req.headers.count("if-match"), "non-authoritative GET");
    });
    run_case("only_verified_no_such_key_means_missing", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        for (const auto& body : {"<Error><Code>NoSuchKey</Code></Error>", "<?xml version=\"1.0\"?>\n<Error><Message>Absent</Message><Code>NoSuchKey</Code></Error>\n"}) {
            peer->handler = [body](const auto& req) { return response(404, bytes(body), {}, req.response_limit); };
            check(!store.read_current(), "definite missing not accepted");
        }
        for (const auto& body : {"", "<Error><Code>NoSuchBucket</Code></Error>", "<Error><Message><Code>NoSuchKey</Code></Message></Error>",
                                "<Error><Code>NoSuchKey</Code><Code>AccessDenied</Code></Error>", "junk<Error><Code>NoSuchKey</Code></Error>"}) {
            peer->handler = [body](const auto& req) { return response(404, bytes(body), {}, req.response_limit); };
            rejects(hc::ErrorCode::io, [&] { (void)store.read_current(); });
        }
        peer->handler = [](const auto& req) { return response(403, bytes("<Error><Code>AccessDenied</Code></Error>"), {}, req.response_limit); };
        rejects(hc::ErrorCode::io, [&] { (void)store.read_current(); });
        peer->handler = [](const auto&) { return hc::HttpResult{hc::HttpDelivery::indeterminate, {}}; };
        rejects(hc::ErrorCode::io, [&] { (void)store.read_current(); });
    });
    run_case("unverified_cached_weak_or_partial_pointer_is_rejected", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        for (const auto& fields : {hc::HttpHeaders{}, hc::HttpHeaders{{"etag", "W/\"weak\""}},
                                  hc::HttpHeaders{{"etag", "\"good\""}, {"age", "1"}},
                                  hc::HttpHeaders{{"etag", "\"good\""}, {"content-range", "bytes 0-1/10"}}}) {
            peer->handler = [fields](const auto& req) { return response(200, pointer(), fields, req.response_limit); };
            rejects(hc::ErrorCode::corrupt, [&] { (void)store.read_current(); });
        }
        peer->handler = [](const auto& req) {
            auto result = response(200, pointer(), {{"etag", "\"good\""}}, req.response_limit);
            result.response.tls_verified = false; return result;
        };
        rejects(hc::ErrorCode::io, [&] { (void)store.read_current(); });
    });
    run_case("immutable_create_is_content_addressed_and_create_only", [] {
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto body = bytes(hc::serialize_manifest(manifest()));
        check(hc::put_immutable(store, key(body), body) == hc::WriteOutcome::applied, "create/readback failed");
        check(hc::put_immutable(store, key(body), body) == hc::WriteOutcome::applied && peer->writes == 1, "immutable overwritten");
        const auto before = peer->calls.size();
        rejects(hc::ErrorCode::invalid, [&] { (void)store.create(key(body), bytes("different")); });
        rejects(hc::ErrorCode::invalid, [&] { (void)store.get("../current.json", 4096); });
        check(peer->calls.size() == before, "invalid object reached network");
        peer->objects.begin()->second.body[0] ^= 1;
        rejects(hc::ErrorCode::corrupt, [&] { (void)store.get(key(body), hc::kMaxMetadataBytes); });
    });
    run_case("write_outcomes_are_conservative_and_never_retry", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const std::vector<std::pair<hc::HttpResult, hc::WriteOutcome>> results = {
            {response(200, {}, {{"etag", "\"new\""}}, 0), hc::WriteOutcome::applied},
            {response(412, {}, {}, 0), hc::WriteOutcome::precondition_failed},
            {response(403, {}, {}, 0), hc::WriteOutcome::indeterminate},
            {response(503, {}, {}, 0), hc::WriteOutcome::indeterminate},
            {response(307, {}, {}, 0), hc::WriteOutcome::indeterminate},
            {response(200, {}, {}, 0), hc::WriteOutcome::indeterminate},
            {{hc::HttpDelivery::not_sent, {}}, hc::WriteOutcome::not_applied},
            {{hc::HttpDelivery::indeterminate, {}}, hc::WriteOutcome::indeterminate},
        };
        for (const auto& [result, expected] : results) {
            peer->handler = [result](const auto&) { return result; };
            const auto before = peer->calls.size();
            check(store.write_current(pointer(), "\"exact/Case\"") == expected && peer->calls.size() == before + 1,
                  "write result wrong or implicitly retried");
            check(peer->calls.back().headers.at("if-match") == "\"exact/Case\"", "ETag changed");
        }
        peer->handler = [](const auto&) -> hc::HttpResult { throw std::runtime_error("transport threw after dispatch"); };
        check(store.write_current(pointer(), std::nullopt) == hc::WriteOutcome::indeterminate, "exception erased ambiguity");
        check(peer->calls.back().headers.at("if-none-match") == "*", "first pointer not create-only");
    });
    run_case("range_exact_interval_total_and_etag_are_pinned", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto data_key = "data/v1/" + hc::hex(hc::sha256(std::string("abcdefgh"))) + ".r2b";
        auto range = store.open_range(data_key, 8);
        peer->handler = [](const auto& req) { return response(206, bytes("ab"), {{"etag", "\"original\""}, {"content-range", "bytes 0-1/8"}}, req.response_limit); };
        check(text(range(0, 2)) == "ab" && !peer->calls.back().headers.count("if-match"), "first Range pin");
        peer->handler = [](const auto& req) { return response(206, bytes("cd"), {{"etag", "\"original\""}, {"content-range", "bytes 2-3/8"}}, req.response_limit); };
        check(text(range(2, 2)) == "cd" && peer->calls.back().headers.at("if-match") == "\"original\"", "later Range lost pin");
        peer->handler = [](const auto& req) { return response(206, bytes("cd"), {{"etag", "\"changed\""}, {"content-range", "bytes 2-3/8"}}, req.response_limit); };
        rejects(hc::ErrorCode::conflict, [&] { (void)range(2, 2); });
        const auto before = peer->calls.size();
        rejects(hc::ErrorCode::resource_limit, [&] { (void)range(UINT64_MAX, 2); });
        rejects(hc::ErrorCode::resource_limit, [&] { (void)range(0, 0); });
        rejects(hc::ErrorCode::resource_limit, [&] { (void)range(0, hc::kMaxBlockBytes + 1); });
        check(peer->calls.size() == before, "invalid Range dispatched");
    });
    run_case("range_200_short_wrong_total_or_identity_is_not_accepted", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto data_key = "data/v1/" + hc::hex(hc::sha256(std::string("abcdefgh"))) + ".r2b";
        auto range = store.open_range(data_key, 8);
        for (const auto& content_range : {"bytes 1-2/8", "bytes 0-1/9", "bytes 0-1/*", "bytes 0-2/8", "bytes=0-1/8"}) {
            peer->handler = [content_range](const auto& req) { return response(206, bytes("ab"), {{"etag", "\"e\""}, {"content-range", content_range}}, req.response_limit); };
            rejects(hc::ErrorCode::corrupt, [&] { (void)range(0, 2); });
        }
        peer->handler = [](const auto& req) { return response(206, bytes("a"), {{"etag", "\"e\""}, {"content-range", "bytes 0-1/8"}}, req.response_limit); };
        rejects(hc::ErrorCode::corrupt, [&] { (void)range(0, 2); });
        peer->handler = [](const auto& req) { return response(200, bytes("ab"), {{"etag", "\"e\""}}, req.response_limit); };
        rejects(hc::ErrorCode::io, [&] { (void)range(0, 2); });
    });
    run_case("budgets_deadline_and_cancellation_prevent_dispatch", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        auto budget = limits(); budget.max_requests = 1;
        hc::S3Store store(config(), peer, credentials(), budget, signing_time);
        (void)store.write_current(pointer(), std::nullopt);
        check(store.write_current(pointer(), std::nullopt) == hc::WriteOutcome::not_applied && peer->calls.size() == 1,
              "request budget bypassed");
        check(store.usage().download_reserved == 4096 && store.usage().upload_reserved == pointer().size(), "error body not reserved");
        for (unsigned mode = 0; mode < 4; ++mode) {
            budget = limits();
            if (mode == 0) budget.max_upload_bytes = pointer().size() - 1;
            if (mode == 1) budget.max_download_bytes = 4095;
            if (mode == 2) budget.cancelled->store(true);
            if (mode == 3) budget.deadline = hc::SteadyClock::now();
            hc::S3Store limited(config(), peer, credentials(), budget, signing_time);
            check(limited.write_current(pointer(), std::nullopt) == hc::WriteOutcome::not_applied && limited.usage().requests == 0,
                  "budget/cancellation/expiry was dispatched");
        }
        check(peer->calls.size() == 1, "forbidden dispatch");
        for (unsigned mode = 0; mode < 3; ++mode) {
            budget = limits();
            if (mode == 0) budget.max_requests = 0;
            if (mode == 1) budget.deadline = hc::SteadyClock::now();
            if (mode == 2) budget.cancelled->store(true);
            hc::S3Store limited(config(), peer, credentials(), budget, signing_time);
            try {
                limited.read_current();
                check(false, "limited read succeeded");
            } catch (const hc::Error& error) {
                const std::string message = error.what();
                check(message.find("TLS") == std::string::npos, "local rejection misreported as TLS");
                check(message.find(mode == 0 ? "budget" : mode == 1 ? "deadline" : "cancelled") != std::string::npos,
                      "local rejection reason lost");
            }
        }
        check(peer->calls.size() == 1, "limited read reached transport");
    });
    run_case("post_dispatch_cancel_or_bad_response_preserves_uncertainty", [] {
        auto peer = std::make_shared<ScriptedHttp>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        peer->handler = [](const auto& req) {
            auto result = response(200, {}, {{"etag", "\"new\""}}, req.response_limit);
            result.response.tls_verified = false; return result;
        };
        check(store.write_current(pointer(), std::nullopt) == hc::WriteOutcome::indeterminate, "unverified ACK accepted");
        peer->handler = [](const auto& req) { return response(200, bytes("unexpected-body"), {{"etag", "\"new\""}}, req.response_limit); };
        check(store.write_current(pointer(), std::nullopt) == hc::WriteOutcome::indeterminate, "response overflow became non-delivery");
        peer->handler = [](const auto& req) {
            auto result = response(200, {}, {{"etag", "\"new\""}}, req.response_limit);
            req.cancelled->store(true); return result;
        };
        check(store.write_current(pointer(), std::nullopt) == hc::WriteOutcome::indeterminate, "post-dispatch cancellation became non-delivery");
        check(peer->calls.size() == 3 && store.write_current(pointer(), std::nullopt) == hc::WriteOutcome::not_applied &&
              peer->calls.size() == 3, "cancelled operation dispatched again");
    });
    run_case("synthetic_http_upload_publish_and_cross_block_query", [] {
        PackFixture fixture;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto& entry = fixture.candidate.entries[0];
        check(hc::put_immutable(store, entry.pack->key, fixture.data) == hc::WriteOutcome::applied, "upload failed");
        const auto before = peer->calls.size();
        check(hc::ConditionalPublisher(store).publish(fixture.candidate, 0).outcome == hc::PublishOutcome::committed, "publish failed");
        uint64_t verified_bytes = 0;
        for (size_t i = before; i < peer->calls.size(); ++i) {
            if (peer->calls[i].headers.count("range")) verified_bytes += peer->calls[i].response_limit;
        }
        check(verified_bytes == fixture.data.size(), "pack verified in more than one range pass");
        const auto snapshot = std::make_shared<const hc::Snapshot>(hc::load_snapshot(store));
        const hc::Query query{identity(), version(), {fixture.rows[1000].timestamp_ms, fixture.rows[1100].timestamp_ms}, 100};
        std::vector<hc::Row> rows;
        const auto result = hc::read_plan(store, hc::plan_query(snapshot, query, true), [&](const auto& batch) {
            rows.insert(rows.end(), batch.begin(), batch.end());
        });
        check(result.rows == 100 && rows.size() == 100, "query row count");
        for (size_t i = 0; i < rows.size(); ++i) check(hc::same_row(rows[i], fixture.rows[1000 + i]), "cross-block rows differ");
        const auto writes = peer->writes;
        const auto recovered = hc::ConditionalPublisher(store).publish(fixture.candidate, 0);
        check(recovered.outcome == hc::PublishOutcome::committed && recovered.recovered && peer->writes == writes, "recovery republished");
    });
    run_case("lost_ack_and_unreadable_recovery_reuse_exact_target", [] {
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        peer->fault = PointerFault::lost_ack_unreadable;
        auto result = hc::ConditionalPublisher(store).publish(manifest(), 0);
        check(result.outcome == hc::PublishOutcome::indeterminate && peer->writes == 2, "lost ACK not unknown");
        result = hc::ConditionalPublisher(store).publish(manifest(), 0);
        check(result.outcome == hc::PublishOutcome::committed && result.recovered && result.target.publication_seq == 1 && peer->writes == 2,
              "exact target recovery advanced generation");
    });
}

void curl_offline() {
    run_case("curl_is_disabled_before_initialization_or_dns", [] {
        hc::CurlHttpTransport transport;
        hc::HttpRequest request;
        request.url = "https://offline.invalid/must-not-resolve";
        request.deadline = hc::SteadyClock::now() + std::chrono::seconds(1);
        check(transport.perform(request).delivery == hc::HttpDelivery::not_sent, "default transport enabled network");
        hc::CurlHttpTransport pooled(false, true);
        check(pooled.perform(request).delivery == hc::HttpDelivery::not_sent, "pool bypassed network gate");
        hc::CurlHttpTransport publisher(false, hc::CurlHttpTransport::Reuse::publication);
        check(publisher.perform(request).delivery == hc::HttpDelivery::not_sent, "publication pool bypassed network gate");
    });
    run_case("curl_expired_cancelled_or_non_https_never_dispatches", [] {
        hc::CurlHttpTransport transport(true);
        hc::HttpRequest request;
        request.url = "https://offline.invalid/must-not-resolve";
        check(transport.perform(request).delivery == hc::HttpDelivery::not_sent, "expired request dispatched");
        request.deadline = hc::SteadyClock::now() + std::chrono::seconds(1);
        request.cancelled = std::make_shared<std::atomic_bool>(true);
        check(transport.perform(request).delivery == hc::HttpDelivery::not_sent, "cancelled request dispatched");
        request.cancelled->store(false); request.url = "http://offline.invalid/must-not-connect";
        check(transport.perform(request).delivery == hc::HttpDelivery::not_sent, "HTTP permitted");
        hc::CurlHttpTransport pooled(true, true);
        check(pooled.perform(request).delivery == hc::HttpDelivery::not_sent, "pool allowed HTTP");
        request.url = "https://offline.invalid/must-not-connect";
        request.headers["invalid header"] = "test";
        for (int i = 0; i < 8; ++i) {
            const auto result = pooled.perform(request);
            check(result.delivery == hc::HttpDelivery::not_sent && result.failure != hc::HttpFailure::resource_limit,
                  "invalid request leaked pool slot");
        }
    });
    run_case("curl_build_feature_and_runtime_are_reported", [] {
#ifdef HC_HAS_CURL
        check(hc::CurlHttpTransport::runtime_version() == HC_CURL_VERSION, "curl headers/runtime mismatch");
        std::cout << "CURL_RUNTIME " << hc::CurlHttpTransport::runtime_version() << '\n';
#else
        check(hc::CurlHttpTransport::runtime_version() == "not-built", "OFF build unexpectedly links curl");
        std::cout << "CURL_RUNTIME not-built\n";
#endif
    });
}

}  // namespace

int main(int argc, char** argv) {
    try {
        check(argc == 2, "usage: history-cache-transport-tests SUITE");
        const std::string suite = argv[1];
        if (suite == "http_protocol") http_protocol();
        else if (suite == "sigv4") sigv4();
        else if (suite == "s3_protocol") s3_protocol();
        else if (suite == "curl_offline") curl_offline();
        else throw std::runtime_error("unknown suite");
        std::cout << "PASS " << suite << " cases=" << cases << '\n';
        return 0;
    } catch (const std::exception& error) { std::cerr << "FAIL " << error.what() << '\n'; return 1; }
}
