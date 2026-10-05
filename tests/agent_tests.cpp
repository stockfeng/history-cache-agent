#include "history_cache/agent.h"
#include "history_cache/maintenance.h"
#include "history_cache/market_clock.h"
#include "transport_fixture.h"

#include <atomic>
#include <mutex>
#include <ctime>
#include <thread>

namespace {
namespace hc = history_cache;
using Json = nlohmann::json;
using test::check;

class Peer final : public hc::HttpTransport {
public:
    std::map<std::string, hc::Bytes> objects;
    std::mutex mutex;
    uint64_t calls = 0;
    hc::HttpFailure failure = hc::HttpFailure::none;
    bool corrupt_manifest = false;
    std::vector<hc::SteadyClock::time_point> deadlines;
    std::vector<uint64_t> rates;

    hc::HttpResult perform(const hc::HttpRequest& request) override {
        std::lock_guard<std::mutex> guard(mutex);
        ++calls;
        deadlines.push_back(request.deadline);
        rates.push_back(request.receive_bytes_per_second);
        check(request.method == hc::HttpMethod::get, "agent issued a write");
        if (failure != hc::HttpFailure::none) return {hc::HttpDelivery::indeterminate, {}, failure};
        const std::string prefix = "https://" + std::string(32, 'a') +
            ".r2.cloudflarestorage.com/history-cache-staging/r2-history-staging/";
        check(request.url.rfind(prefix, 0) == 0, "unexpected scope");
        const auto key = request.url.substr(prefix.size());
        const auto found = objects.find(key);
        if (found == objects.end()) return test::response(404,
            test::bytes("<Error><Code>NoSuchKey</Code></Error>"), {}, request.response_limit);
        const auto& body = found->second;
        if (request.headers.count("range")) {
            const auto text = request.headers.at("range").substr(6);
            const auto dash = text.find('-');
            const auto start = std::stoull(text.substr(0, dash)), end = std::stoull(text.substr(dash + 1));
            check(end < body.size(), "bad fixture range");
            return test::response(206, hc::Bytes(body.begin() + static_cast<std::ptrdiff_t>(start),
                body.begin() + static_cast<std::ptrdiff_t>(end + 1)),
                {{"etag", "\"pack\""}, {"content-range", "bytes " + text + "/" + std::to_string(body.size())}},
                request.response_limit);
        }
        if (corrupt_manifest && key.find("manifests/") != std::string::npos)
            return test::response(200, test::bytes("{}"), {{"etag", "\"bad\""}}, request.response_limit);
        return test::response(200, body, {{"etag", "\"object\""}}, request.response_limit);
    }
};

hc::AgentConfig config() {
    hc::AgentConfig result;
    result.account_id = std::string(32, 'a');
    result.access_key_id = "SYNTHETICKEYID";
    result.secret_access_key = "SYNTHETICSECRET";
    result.foreground_network = true;
    return result;
}

constexpr int64_t boundary = 1790784000000LL; // 2026-10-01 00:00 Asia/Shanghai.
Json request(int64_t start = boundary, int64_t end = boundary + 60000) {
    return {{"symbol", "000001.SZ"}, {"start_ms", start}, {"end_ms", end}, {"max_rows", 5},
            {"protocol_version", 1}, {"timestamp_semantics", "utc-instant-ms"}, {"range_semantics", "half-open"}};
}

Json factor_fixture(const std::string& symbol = "000001.SZ", const std::string& market = "SZ") {
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto day = (boundary + 28800000) / 86400000;
    return {{"schema_version", 1}, {"kind", "ddb-adjustment-snapshot-v1"},
        {"algorithm", "upcloud-adjustment-v1"}, {"symbol", symbol}, {"market", market}, {"model", "cumulative"},
        {"first_day", day}, {"end_day", day + 31}, {"coverage_complete", true}, {"allow_empty", false},
        {"observed_at_ms", now - 1000}, {"valid_until_ms", now + 60000}, {"source_epoch", "factor-fixture-1"},
        {"source_proof_sha256", hc::hex(hc::sha256("synthetic-proof"))},
        {"date_encoding", "exchange-civil-days-since-1970"},
        {"rows", Json::array({{{"code", symbol}, {"ex_date", day}, {"ex_factor", 2.0}, {"cum_factor", 2.0}, {"update_time", 1}},
                              {{"code", symbol}, {"ex_date", day + 1}, {"ex_factor", 2.0}, {"cum_factor", 4.0}, {"update_time", 2}}})}};
}

void publish_factors(Peer& peer, const Json& value, uint64_t seq = 1, const std::string& slug = "000001sz") {
    const auto bytes = test::bytes(value.dump());
    const auto digest = hc::sha256(bytes);
    const auto key = "manifests/v1/" + hc::hex(digest) + ".json";
    const auto prefix = "factors-" + slug + "-v1/";
    std::lock_guard<std::mutex> guard(peer.mutex);
    peer.objects[prefix + key] = bytes;
    peer.objects[prefix + "current.json"] = test::bytes(hc::serialize_pointer(
        {value.at("source_epoch"), seq, key, digest}));
}

void publish(Peer& peer, const std::string& month, hc::Coverage coverage, uint64_t seq = 1, bool empty = false,
             std::string symbol = "000001.SZ", std::vector<hc::Row> rows = {}, bool full = false,
             bool native = false, bool intraday = false, std::string epoch = "") {
    test::Workspace directory;
    if (!empty && rows.empty()) rows.push_back({coverage.start_ms, 10, 11, 9, 10, 100});
    if (native) for (auto& row : rows)
        row.native = hc::Row::NativeFields{{10, 11, 9, 10}, 100, 101};
    const auto market = symbol == "000001.SZ" ? "SZ" : symbol == "00700.HK" ? "HK" :
        symbol == "AP612C8000.CZC" ? "CZC" : "US";
    hc::CatalogEntry entry{{native ? "ddb-history-native64" : full ? "ddb-history-kline48" : "ddb-history-snapshot", market, symbol, 60, "none"},
        test::version(), seq, coverage, rows.size(), std::nullopt};
    hc::Bytes pack;
    if (!empty) {
        const auto file = directory.root / "pack.r2b";
        entry.pack = hc::write_pack(file, hc::pack_metadata(entry), [&](uint64_t offset, uint32_t count) {
            return std::vector<hc::Row>(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                rows.begin() + static_cast<std::ptrdiff_t>(offset + count));
        });
        pack = hc::read_file(file, hc::kMaxObjectBytes);
    }
    if (epoch.empty()) epoch = "epoch-" + month;
    const auto manifest = test::bytes(hc::serialize_manifest({epoch, {entry}}));
    const auto key = test::key(manifest);
    const auto pointer = test::bytes(hc::serialize_pointer({epoch, seq, key, hc::sha256(manifest)}));
    std::string slug;
    for (unsigned char c : symbol) if (c != '.') slug.push_back(static_cast<char>(std::tolower(c)));
    if (market == std::string("US")) slug += "-us";
    const auto prefix = std::string(intraday ? "intraday-" : "history-") + slug + "-" + month +
        (native ? "-native64-001/" : full ? "-kline48-001/" : "-001/");
    std::lock_guard<std::mutex> guard(peer.mutex);
    if (entry.pack) peer.objects[prefix + entry.pack->key] = pack;
    peer.objects[prefix + key] = manifest;
    peer.objects[prefix + "current.json"] = pointer;
}

void adjustment_service_tests() {
    constexpr int64_t day_ms = 86400000;
    auto peer = std::make_shared<Peer>();
    std::vector<hc::Row> raw{{boundary, 10, 11, 9, 10, 9007199254740993LL},
                            {boundary + day_ms, 10, 11, 9, 10, 9007199254740995LL}};
    publish(*peer, "202610", {boundary, boundary + 2 * day_ms}, 1, false, "000001.SZ", raw, false, true);
    publish_factors(*peer, factor_fixture());
    auto input = request(boundary, boundary + 2 * day_ms);
    input["row_encoding"] = "le-ddb-native64-v1";
    input["adjust"] = "forward";
    hc::Agent disabled(config(), peer);
    check(disabled.handle_query(input).at("status") == "MISS" && peer->calls == 0, "adjustment not default-off");
    auto enabled = config(); enabled.enable_adjustment = true;
    hc::Agent agent(enabled, peer);
    const auto cold = agent.handle_query(input);
    check(cold.at("status") == "HIT" && cold.at("rows") == 2 && peer->calls == 5, "factor cold HIT failed");
    auto expected = raw;
    expected[0].native = hc::Row::NativeFields{{5, 5.5, 4.5, 5}, 100, 101};
    expected[1].native = hc::Row::NativeFields{{10, 11, 9, 10}, 100, 101};
    hc::Bytes wanted;
    for (const auto& row : expected) {
        const auto bytes = hc::canonical_native(row); wanted.insert(wanted.end(), bytes.begin(), bytes.end());
    }
    check(hc::unhex(cold.at("data")) == wanted && cold.at("rows_sha256") == hc::hex(hc::sha256(wanted)),
          "adjustment lost native bytes or hash");
    const auto calls = peer->calls;
    check(agent.handle_query(input).at("data") == cold.at("data") && peer->calls == calls, "factor hot HIT fetched network");
    input["max_rows"] = 1;
    auto short_result = agent.handle_query(input);
    check(short_result.at("adjustment").at("anchor_day") == (boundary + 28800000) / day_ms,
          "truncated anchor used request end");
    check(short_result.at("data") != cold.at("data").get<std::string>().substr(0, 128), "truncated anchor not applied");
    input["max_rows"] = 5;
    input["adjust"] = "backward";
    check(agent.handle_query(input).at("status") == "HIT", "backward HIT failed");
    input["end_ms"] = boundary + 3 * day_ms;
    input["allow_partial"] = true;
    check(agent.handle_query(input).at("reason") == "adjustment_requires_complete_range", "adjusted partial escaped");
    auto composite = input;
    composite["op"] = "adjust_rows";
    composite["allow_partial"] = false;
    composite["input_adjust"] = "none";
    composite["prefix_end_ms"] = boundary + day_ms;
    composite["prefix_rows"] = 1;
    composite["end_ms"] = boundary + 2 * day_ms;
    composite["adjust"] = "forward";
    hc::Bytes combined;
    for (auto row : raw) {
        row.native = hc::Row::NativeFields{{10, 11, 9, 10}, 100, 101};
        const auto bytes = hc::canonical_native(row);
        combined.insert(combined.end(), bytes.begin(), bytes.end());
    }
    composite["data"] = hc::hex(combined.data(), combined.size());
    composite["rows_sha256"] = hc::hex(hc::sha256(combined));
    const auto composite_calls = peer->calls;
    const auto merged = agent.handle_query(composite);
    check(merged.at("status") == "HIT" && merged.at("data") == cold.at("data") &&
        merged.at("result_kind") == "gateway-raw-composite-v1" && peer->calls == composite_calls,
        "composite differs from full HIT or fetched K packs");
    composite["prefix_rows"] = 2;
    composite["prefix_end_ms"] = boundary + 2 * day_ms;
    check(agent.handle_query(composite).at("data") == cold.at("data"), "empty tail changed anchor");
    composite["data"] = composite.at("data").get<std::string>().substr(0, 128);
    composite["rows_sha256"] = hc::hex(hc::sha256(hc::unhex(composite.at("data"))));
    composite["prefix_rows"] = 1;
    composite["max_rows"] = 1;
    check(agent.handle_query(composite).at("data") == short_result.at("data"), "composite max_rows anchor incorrect");
    auto precision = composite;
    hc::Row precise{boundary, 0, 0, 0, 0, 9007199254740993LL};
    precise.native = hc::Row::NativeFields{{10.00499999, 10.00499999, 10.00499999, 10.00499999}, 100, 101};
    const auto precise_bytes = hc::canonical_native(precise);
    precision["data"] = hc::hex(precise_bytes);
    precision["rows_sha256"] = hc::hex(hc::sha256(hc::Bytes(precise_bytes.begin(), precise_bytes.end())));
    precise.native->prices.fill(10.0);
    check(agent.handle_query(precision).at("data") == hc::hex(hc::canonical_native(precise)),
        "composite rounded native DOUBLE through float first");
    auto deadline_request = composite;
    deadline_request["deadline_mono_ms"] = 1;
    check(agent.handle_query(deadline_request).at("reason") == "query_budget_exhausted", "expired composite ran");
    deadline_request["deadline_mono_ms"] = "1";
    check(agent.handle_query(deadline_request).at("reason") == "invalid_request", "invalid deadline accepted");
    for (int mutation = 0; mutation < 6; ++mutation) {
        auto invalid = composite;
        if (mutation == 0) invalid["rows_sha256"] = std::string(64, '0');
        if (mutation == 1) invalid["prefix_rows"] = 2;
        if (mutation == 2) invalid["prefix_end_ms"] = boundary;
        if (mutation == 3) invalid["input_adjust"] = "forward";
        if (mutation == 4) invalid["data"] = std::string(256, '0');
        if (mutation == 5) invalid["prefix_rows"] = 0;
        check(agent.handle_query(invalid).at("status") == "ERROR", "invalid composite accepted");
    }
    input["end_ms"] = boundary + 2 * day_ms;
    auto strict = enabled; strict.manifest_ttl_seconds = 0;
    hc::Agent refreshing(strict, peer);
    const auto before = refreshing.handle_query(input);
    check(before.at("status") == "HIT", "initial refreshing factors failed");
    auto revised = factor_fixture(); revised["rows"][0]["cum_factor"] = 3.0;
    publish_factors(*peer, revised, 2);
    const auto after = refreshing.handle_query(input);
    check(after.at("status") == "HIT" && before.at("data") != after.at("data"), "factor revision did not invalidate result");
    {
        auto data = revised;
        for (const auto* key : {"observed_at_ms", "valid_until_ms", "source_proof_sha256"}) data.erase(key);
        data["schema_version"] = 2; data["kind"] = "ddb-adjustment-data-v2";
        const auto payload = test::bytes(data.dump());
        auto ref = revised;
        ref.erase("rows"); ref["schema_version"] = 2; ref["kind"] = "ddb-adjustment-reference-v2";
        ref["factor_data_sha256"] = hc::hex(hc::sha256(payload)); ref["factor_data_bytes"] = payload.size();
        const auto key = "factors-000001sz-v1/manifests/v1/" + hc::hex(hc::sha256(payload)) + ".json";
        peer->objects[key] = payload;
        publish_factors(*peer, ref, 3);
        hc::Agent compact(strict, peer);
        const auto result = compact.handle_query(input);
        check(result.at("status") == "HIT" && result.at("data") == after.at("data"), "compact factor changed result");
        check(result.at("adjustment").at("factor_set_hash") == hc::hex(hc::sha256(payload)), "reference-v2 data version differs");
        ref["observed_at_ms"] = ref.at("observed_at_ms").get<int64_t>() + 1;
        publish_factors(*peer, ref, 4);
        const auto renewed = compact.handle_query(input);
        check(renewed.at("data") == after.at("data") &&
            renewed.at("adjustment").at("factor_set_hash") == result.at("adjustment").at("factor_set_hash"),
            "check-only renewal changed data version or result");
        auto scheduled = ref;
        const auto observed = ref.at("observed_at_ms").get<int64_t>();
        const auto check_day = (observed + 28800000) / 86400000 - 1;
        const auto until = (check_day + 3) * 86400000;
        scheduled.update(Json{{"schema_version", 3}, {"kind", "ddb-adjustment-reference-v3"},
            {"valid_until_ms", until}, {"verification", {
                {"policy", "market-source-check-v1"}, {"exchange", "XSHE"},
                {"calendar_sha256", hc::hex(hc::sha256("calendar"))}, {"trading_day", check_day},
                {"next_trading_day", check_day + 3}, {"check_minute", 480}, {"source_revision", 1},
                {"source_completed_at_ms", check_day * 86400000}, {"next_check_ms", until},
                {"config_sha256", hc::hex(hc::sha256("config"))}, {"workload_sha256", hc::hex(hc::sha256("workload"))},
                {"source_receipt_sha256", hc::hex(hc::sha256("receipt-1"))}}}});
        publish_factors(*peer, scheduled, 5);
        const auto checked = compact.handle_query(input);
        check(checked.at("status") == "HIT" && checked.at("data") == after.at("data"), "market check changed prices");
        check(checked.at("adjustment").at("factor_set_hash") == hc::hex(hc::sha256(payload)), "market data version is not stable");
        scheduled["verification"]["source_revision"] = 2;
        scheduled["verification"]["source_receipt_sha256"] = hc::hex(hc::sha256("receipt-2"));
        publish_factors(*peer, scheduled, 6);
        check(compact.handle_query(input).at("adjustment").at("factor_set_hash") == checked.at("adjustment").at("factor_set_hash"),
              "unchanged data got a new factor version");
        scheduled["verification"]["source_receipt_sha256"] = hc::hex(hc::sha256("same-revision-tamper"));
        publish_factors(*peer, scheduled, 7);
        check(compact.handle_query(input).at("status") == "ERROR", "source receipt changed at same revision");
        peer->objects.erase(key);
        publish_factors(*peer, scheduled, 8);
        check(compact.handle_query(input).at("status") == "ERROR", "missing compact data reused stale factors");
        publish_factors(*peer, revised, 2);
    }
    auto tampered = revised; tampered["rows"][0]["cum_factor"] = 5.0;
    publish_factors(*peer, tampered, 2);
    check(refreshing.handle_query(input).at("status") == "ERROR", "same-sequence factor replacement accepted");
    publish_factors(*peer, revised, 1);
    check(refreshing.handle_query(input).at("status") == "ERROR", "factor seq rollback accepted");
    auto expired = factor_fixture(); expired["valid_until_ms"] = expired.at("observed_at_ms");
    publish_factors(*peer, expired, 3);
    check(refreshing.handle_query(input).at("status") == "ERROR", "expired factors accepted");
    {
        std::lock_guard<std::mutex> guard(peer->mutex);
        peer->objects.erase("factors-000001sz-v1/current.json");
    }
    check(refreshing.handle_query(input).at("status") == "ERROR", "missing factors silently used old snapshot");
    input["adjust"] = "none";
    check(refreshing.handle_query(input).at("status") == "HIT", "missing factors broke unadjusted request");

    // UTC Oct 2 00:30 is still Oct 1 in New York, before the Oct 2 event.
    hc::NewYorkClock clock;
    const int64_t us_wall = boundary + 28800000 + 20 * 3600000 + 30 * 60000;
    auto us_peer = std::make_shared<Peer>();
    publish(*us_peer, "202610", {us_wall, us_wall + 60000}, 1, false, "AAPL", {}, false, true);
    auto us_factors = factor_fixture("AAPL", "US");
    us_factors["model"] = "futu_ab";
    us_factors["rows"] = Json::array({{{"code", "AAPL"},
        {"ex_div_date", (boundary + 28800000) / day_ms + 1},
        {"forward_adj_factorA", 0.5}, {"forward_adj_factorB", 0.12345},
        {"backward_adj_factorA", 2.0}, {"backward_adj_factorB", nullptr}, {"update_time", 1}}});
    publish_factors(*us_peer, us_factors, 1, "aapl-us");
    hc::Agent us_agent(enabled, us_peer);
    auto us_input = request(clock.to_utc(us_wall), clock.to_utc(us_wall + 60000));
    us_input["symbol"] = "AAPL";
    us_input["row_encoding"] = "le-ddb-native64-v1";
    us_input["adjust"] = "forward";
    const auto us_result = us_agent.handle_query(us_input);
    hc::Row us_expected{clock.to_utc(us_wall), 10, 11, 9, 10, 100};
    us_expected.native = hc::Row::NativeFields{{5.12345, 5.62345, 4.62345, 5.12345}, 100, 101};
    auto us_bytes = hc::canonical_native(us_expected);
    check(us_result.at("status") == "HIT" && us_result.at("data") == hc::hex(us_bytes.data(), us_bytes.size()),
          "US adjustment used UTC event date or rounded affine prices");
    us_input["adjust"] = "backward";
    us_expected.native = hc::Row::NativeFields{{10, 11, 9, 10}, 100, 101};
    us_bytes = hc::canonical_native(us_expected);
    check(us_agent.handle_query(us_input).at("data") == hc::hex(us_bytes.data(), us_bytes.size()),
          "US backward applied a future event");
    us_input["op"] = "adjust_rows";
    us_input["input_adjust"] = "none";
    us_input["prefix_rows"] = 0;
    us_input["start_ms"] = clock.to_utc(us_wall) - 1;
    us_input["prefix_end_ms"] = clock.to_utc(us_wall);
    us_input["data"] = hc::hex(us_bytes);
    us_input["rows_sha256"] = hc::hex(hc::sha256(hc::Bytes(us_bytes.begin(), us_bytes.end())));
    us_input["adjust"] = "forward";
    check(us_agent.handle_query(us_input).at("data") == us_result.at("data"), "composite US civil day differs");
    std::cout << "PASS agent_adjustment_us local_date affine_native same_sequence_guard\n";
    std::cout << "PASS agent_adjustment default_off cold5 hot0 native64 anchor revision rollback expiry partial_reject\n";
}

void versioned_factor_tests() {
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    for (const std::string symbol : {"000001.SZ", "00700.HK", "AAPL"}) {
        const bool us = symbol == "AAPL";
        const auto market = us ? "US" : symbol == "00700.HK" ? "HK" : "SZ";
        const auto slug = us ? "aapl-us" : symbol == "00700.HK" ? "00700hk" : "000001sz";
        auto peer = std::make_shared<Peer>();
        const int64_t stored = us ? boundary + 28800000 : boundary;
        hc::NewYorkClock clock;
        const int64_t start = us ? clock.to_utc(stored) : stored;
        publish(*peer, "202610", {stored, stored + 60000}, 1, false, symbol, {}, false, true);
        auto factor = factor_fixture(symbol, market);
        if (us) {
            factor["model"] = "futu_ab";
            factor["rows"] = Json::array({{{"code", symbol}, {"ex_div_date", (stored / 86400000) + 1},
                {"forward_adj_factorA", 0.5}, {"forward_adj_factorB", 0.125},
                {"backward_adj_factorA", 2.0}, {"backward_adj_factorB", 0.0}, {"update_time", 1}}});
        }
        auto cfg = config(); cfg.enable_adjustment = true;
        publish_factors(*peer, factor, 1, slug);
        auto input = request(start, start + 60000);
        input["symbol"] = symbol; input["row_encoding"] = "le-ddb-native64-v1";
        std::vector<Json> expected;
        hc::Agent fresh(cfg, peer);
        for (const std::string mode : {"forward", "backward"}) {
            input["adjust"] = mode;
            expected.push_back(fresh.handle_query(input));
            check(expected.back().at("status") == "HIT", "fresh reference failed");
        }
        // A genuine old observation, not an invalid zero-length interval.
        factor["observed_at_ms"] = now - 30LL * 86400000;
        factor["valid_until_ms"] = now - 30LL * 86400000 + 3600000;
        publish_factors(*peer, factor, 2, slug);
        hc::Agent cold(cfg, peer);
        auto strict = cfg; strict.manifest_ttl_seconds = 0;
        hc::Agent recheck(strict, peer);
        for (size_t index = 0; index < 2; ++index) {
            input["adjust"] = index == 0 ? "forward" : "backward";
            const auto result = cold.handle_query(input);
            check(result.at("status") == "HIT" && result.at("data") == expected[index].at("data"),
                  "verification age changed adjusted rows");
            const auto& meta = result.at("adjustment");
            check(meta.at("factor_freshness_policy") == "versioned-v1" && meta.at("factor_check_status") == "overdue" &&
                  meta.at("factor_observed_at_ms") == factor.at("observed_at_ms") &&
                  meta.at("factor_next_check_ms") == factor.at("valid_until_ms"), "lost check status or rewrote time");
            const auto calls = peer->calls;
            check(cold.handle_query(input).at("data") == result.at("data") && peer->calls == calls,
                  "overdue hot read bypassed normal TTL");
            check(recheck.handle_query(input).at("data") == result.at("data") &&
                  recheck.handle_query(input).at("data") == result.at("data"), "unchanged current rejected on recheck");
            hc::Row raw{start, 10, 11, 9, 10, 100};
            raw.native = hc::Row::NativeFields{{10, 11, 9, 10}, 100, 101};
            const auto bytes = hc::canonical_native(raw);
            auto composite = input;
            composite.update(Json{{"op", "adjust_rows"}, {"input_adjust", "none"}, {"prefix_rows", 1},
                {"prefix_end_ms", start + 60000}, {"data", hc::hex(bytes)},
                {"rows_sha256", hc::hex(hc::sha256(hc::Bytes(bytes.begin(), bytes.end())))}});
            check(cold.handle_query(composite).at("data") == result.at("data"), "overdue composite failed");
        }
        auto correction = factor;
        correction["rows"][0][us ? "forward_adj_factorA" : "cum_factor"] = 3.0;
        publish_factors(*peer, correction, 3, slug);
        input["adjust"] = us ? "forward" : "backward";
        check(recheck.handle_query(input).at("data") != expected[us ? 0 : 1].at("data"), "new version was not applied");
        peer->failure = hc::HttpFailure::deadline;
        check(recheck.handle_query(input).at("status") == "ERROR", "network error silently reused old version");
        peer->failure = hc::HttpFailure::none;
        auto future = correction;
        future["observed_at_ms"] = now + 3600000; future["valid_until_ms"] = now + 7200000;
        publish_factors(*peer, future, 4, slug);
        check(recheck.handle_query(input).at("status") == "ERROR", "future observation accepted");
    }
    std::cout << "PASS versioned_factors AH_US cold hot recheck composite correction clock network\n";
}

Json intraday_tests() {
    Json vectors = Json::array();
    auto peer = std::make_shared<Peer>();
    publish(*peer, "202610", {boundary, boundary + 60000}, 1, false, "000001.SZ", {}, false, true);
    publish(*peer, "20261001", {boundary, boundary + 120000}, 1, false, "000001.SZ",
            {{boundary, 10, 11, 9, 10, 100}, {boundary + 60000, 10, 11, 9, 10, 100}}, false, true, true);
    hc::Agent agent(config(), peer);
    auto input = request(boundary, boundary + 180000);
    input["row_encoding"] = "le-ddb-native64-v1";
    input["include_intraday"] = true;
    check(agent.handle_query(input).at("status") == "MISS", "partial data became implicit HIT");
    input["allow_partial"] = true;
    const auto result = agent.handle_query(input);
    vectors.push_back({{"request", input}, {"response", result}});
    check(result.at("status") == "PARTIAL" && result.at("rows") == 2 &&
          result.at("coverage_end_ms") == boundary + 120000 && result.at("finalized_through_ms").is_null(),
          "intraday prefix coverage or overlap clipping failed");
    input["end_ms"] = boundary + 120000;
    check(agent.handle_query(input).at("status") == "HIT", "complete intraday range missed");
    input["row_encoding"] = "le-kline48-v1";
    check(agent.handle_query(input).at("status") == "ERROR", "legacy partial contract admitted");
    auto midnight_peer = std::make_shared<Peer>();
    publish(*midnight_peer, "20260930", {boundary - 60000, boundary}, 1, false, "000001.SZ", {}, false, true, true);
    publish(*midnight_peer, "20261001", {boundary, boundary + 60000}, 1, false, "000001.SZ", {}, false, true, true);
    hc::Agent midnight(config(), midnight_peer);
    input = request(boundary - 60000, boundary + 60000);
    input["row_encoding"] = "le-ddb-native64-v1";
    input["include_intraday"] = true;
    check(midnight.handle_query(input).at("rows") == 2, "daily rollover lost a prefix");
    auto gap_peer = std::make_shared<Peer>();
    publish(*gap_peer, "202610", {boundary, boundary + 60000}, 1, false, "000001.SZ", {}, false, true);
    publish(*gap_peer, "20261001", {boundary + 120000, boundary + 180000}, 1, false,
            "000001.SZ", {}, false, true, true);
    hc::Agent gap(config(), gap_peer);
    input = request(boundary, boundary + 180000);
    input["row_encoding"] = "le-ddb-native64-v1";
    input["include_intraday"] = true;
    input["allow_partial"] = true;
    const auto missing = gap.handle_query(input);
    check(missing.at("status") == "PARTIAL" && missing.at("coverage_end_ms") == boundary + 60000 &&
          missing.at("rows") == 1, "internal gap silently skipped");
    auto limited = config(); limited.max_requests = 1;
    hc::Agent budget(limited, gap_peer);
    check(budget.handle_query(input).at("status") == "ERROR", "daily probing escaped request budget");
    auto us_peer = std::make_shared<Peer>();
    const int64_t wall = boundary + 28800000;
    publish(*us_peer, "20261001", {wall, wall + 60000}, 1, false, "AAPL", {}, false, true, true);
    hc::NewYorkClock clock;
    hc::Agent us(config(), us_peer);
    input = request(clock.to_utc(wall), clock.to_utc(wall + 120000));
    input["symbol"] = "AAPL";
    input["row_encoding"] = "le-ddb-native64-v1";
    input["include_intraday"] = true;
    input["allow_partial"] = true;
    const auto american = us.handle_query(input);
    vectors.push_back({{"request", input}, {"response", american}});
    check(american.at("status") == "PARTIAL" && american.at("first_ms") == clock.to_utc(wall) &&
          american.at("coverage_end_ms") == clock.to_utc(wall + 60000), "US partial coverage is not UTC");
    std::cout << "PASS agent_intraday explicit_partial overlap_clip native_only\n";
    return vectors;
}

void archive_handoff_tests() {
    auto peer = std::make_shared<Peer>();
    const std::vector<hc::Row> initial{{boundary, 10, 11, 9, 10, 100},
                                      {boundary + 60000, 10, 11, 9, 10, 100}};
    publish(*peer, "20261001", {boundary, boundary + 120000}, 1, false, "000001.SZ",
            initial, false, true, true);
    auto options = config(); options.manifest_ttl_seconds = 1;
    hc::Agent agent(options, peer);
    auto input = request(boundary, boundary + 180000);
    input["row_encoding"] = "le-ddb-native64-v1";
    input["include_intraday"] = true;
    input["allow_partial"] = true;
    const auto before = agent.handle_query(input);
    check(before.at("status") == "PARTIAL" && before.at("rows") == 2, "handoff initial prefix missing");
    auto revised = initial;
    revised[0].volume = 999;
    revised.push_back({boundary + 120000, 10, 11, 9, 10, 200});
    publish(*peer, "202610", {boundary, boundary + 180000}, 1, false, "000001.SZ", revised, false, true);
    const auto archived = agent.handle_query(input);
    auto history_input = input; history_input["include_intraday"] = false;
    hc::Agent history_only(config(), peer);
    const auto expected = history_only.handle_query(history_input);
    check(archived.at("status") == "HIT" && archived.at("rows") == 3 &&
          archived.at("data") == expected.at("data"), "archive did not replace overlapping intraday rows");
    // Even a later daily publication cannot override the monthly authority.
    publish(*peer, "20261001", {boundary, boundary + 180000}, 2, false, "000001.SZ", initial, false, true, true);
    check(agent.handle_query(input).at("data") == archived.at("data"), "daily pointer overrode archive");
    revised[1].volume = 777;
    publish(*peer, "202610", {boundary, boundary + 180000}, 2, false, "000001.SZ", revised, false, true);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    const auto refreshed = agent.handle_query(input);
    hc::Agent revised_only(config(), peer);
    check(refreshed.at("status") == "HIT" && refreshed.at("rows") == 3 &&
          refreshed.at("data") != archived.at("data") &&
          refreshed.at("data") == revised_only.handle_query(history_input).at("data"),
          "monthly TTL revision lost or duplicated rows");
    std::cout << "PASS agent_archive_handoff partial_to_hit monthly_authority revision_TTL\n";
}

void service_tests() {
    auto peer = std::make_shared<Peer>();
    publish(*peer, "202609", {boundary - 60000, boundary});
    publish(*peer, "202610", {boundary, boundary + 60000});
    hc::Agent agent(config(), peer);
    const auto result = agent.handle_query(request(boundary - 60000, boundary + 60000));
    check(result.at("status") == "HIT" && result.at("rows") == 2 && result.at("sources").size() == 2,
          "cross-month packs were not read from their own namespaces");
    const auto data = hc::unhex(result.at("data"));
    check(data.size() == 64 && hc::hex(hc::sha256(data)) == result.at("rows_sha256"), "bad canonical response");
    check(agent.handle_query(request()).at("rows") == 1, "Shanghai first eight hours routed to wrong month");
    auto invalid = request(); invalid["max_rows"] = 5001;
    const auto before = peer->calls;
    check(agent.handle_query(invalid).at("status") == "ERROR", "configured max_rows not enforced");
    invalid = request(); invalid["adjust"] = "qfq";
    check(agent.handle_query(invalid).at("status") == "MISS", "adjusted request admitted");
    check(peer->calls == before, "invalid request dispatched");
    for (const auto* symbol : {"XX612C8000.CZC", "ag2702C8000.SHF", "m2701-C-3000.DCE"}) {
        auto option = request(); option["symbol"] = symbol;
        check(agent.handle_query(option).at("status") == "ERROR" && peer->calls == before,
              "unsupported domestic option was dispatched as US stock");
    }
    auto legacy = request(); legacy.erase("protocol_version");
    check(agent.handle_query(legacy).at("status") == "ERROR" && peer->calls == before,
          "unversioned client was silently reinterpreted");
    auto missing = request(); missing["symbol"] = "600000.SH";
    check(agent.handle_query(missing).at("status") == "MISS", "verified missing pointer not MISS");
    publish(*peer, "202610", {boundary, boundary + 60000}, 1, false, "AP612C8000.CZC", {}, false, true);
    auto option = request(); option["symbol"] = "AP612C8000.CZC";
    option["row_encoding"] = "le-ddb-native64-v1";
    const auto option_result = agent.handle_query(option);
    check(option_result.at("status") == "HIT" && option_result.at("rows") == 1,
          "apple option native history did not use CZC namespace");
    auto adjusted_config = config(); adjusted_config.enable_adjustment = true;
    hc::Agent adjusted_agent(adjusted_config, peer);
    option["adjust"] = "forward";
    const auto option_calls = peer->calls;
    check(adjusted_agent.handle_query(option).at("reason") == "unsupported_adjustment_market" &&
          peer->calls == option_calls, "option adjustment must not read factors");

    auto failing = std::make_shared<Peer>();
    failing->failure = hc::HttpFailure::dns;
    hc::Agent broken(config(), failing);
    const auto failed = broken.handle_query(request());
    check(failed.at("status") == "ERROR" && failed.at("detail") == "S3 DNS lookup failed",
          "network error was hidden as coverage MISS");
    check(failing->calls == 3, "retry bound changed");

    auto corrupt = std::make_shared<Peer>();
    publish(*corrupt, "202610", {boundary, boundary + 60000});
    corrupt->corrupt_manifest = true;
    hc::Agent bad(config(), corrupt);
    check(bad.handle_query(request()).at("status") == "ERROR" && corrupt->calls == 2,
          "corruption should not retry or return MISS");

    auto limited_config = config(); limited_config.max_requests = 3;
    auto limited_peer = std::make_shared<Peer>();
    publish(*limited_peer, "202609", {boundary - 60000, boundary}, 1, true);
    publish(*limited_peer, "202610", {boundary, boundary + 60000}, 1, true);
    hc::Agent limited(limited_config, limited_peer);
    check(limited.handle_query(request(boundary - 60000, boundary + 60000)).at("status") == "ERROR",
          "aggregate query budget not enforced across namespaces");
    check(limited_peer->calls == 3, "aggregate request cap exceeded");

    auto empty_peer = std::make_shared<Peer>();
    publish(*empty_peer, "202610", {boundary, boundary + 60000}, 1, true);
    hc::Agent empty(config(), empty_peer);
    check(empty.handle_query(request()).at("status") == "HIT" &&
          empty.handle_query(request()).at("rows") == 0, "complete empty coverage became MISS");

    auto concurrent_config = config(); concurrent_config.manifest_ttl_seconds = 0;
    hc::Agent concurrent(concurrent_config, peer);
    std::atomic_bool good{true};
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) readers.emplace_back([&] {
        for (int j = 0; j < 30; ++j) if (concurrent.handle_query(request()).at("status") != "HIT") good = false;
    });
    for (uint64_t seq = 2; seq <= 10; ++seq) publish(*peer, "202610", {boundary, boundary + 60000}, seq);
    for (auto& thread : readers) thread.join();
    check(good, "concurrent refresh invalidated a reader snapshot");
    std::cout << "PASS agent_service cross_month empty_hit bounded_retry aggregate_budget concurrent_refresh\n";
}

void cache_tests() {
    {
        auto peer = std::make_shared<Peer>();
        auto cfg = config(); cfg.manifest_ttl_seconds = 0;
        hc::Agent agent(cfg, peer);
        const hc::Coverage coverage{boundary, boundary + 60000};
        publish(*peer, "202610", coverage, 1);
        check(agent.handle_query(request()).at("status") == "HIT", "epoch initial read");
        publish(*peer, "202610", coverage, 2, false, "000001.SZ", {}, false, false, false, "replacement");
        check(agent.handle_query(request()).at("status") == "HIT", "epoch refresh failed");
        publish(*peer, "202610", coverage, 1);
        check(agent.handle_query(request()).at("status") == "ERROR", "epoch rollback accepted");
        publish(*peer, "202610", coverage, 2);
        check(agent.handle_query(request()).at("status") == "ERROR", "same sequence epoch mutation accepted");
        std::cout << "PASS agent_epoch replacement rollback same_sequence_guard\n";
    }
    auto peer = std::make_shared<Peer>();
    publish(*peer, "202610", {boundary, boundary + 120000}, 1, false, "000001.SZ",
            {{boundary, 10, 11, 9, 10, 100}, {boundary + 60000, 10, 11, 9, 10, 200}});
    hc::Agent agent(config(), peer);
    const auto cold = agent.handle_query(request(boundary, boundary + 120000));
    check(cold.at("status") == "HIT" && peer->calls == 3, "small pack cold read must use three GETs");
    check(cold.at("metrics").at("http").at("pack").at("requests") == 1, "pack request metrics missing");
    const auto warm = agent.handle_query(request(boundary, boundary + 120000));
    check(warm.at("data") == cold.at("data") && peer->calls == 3 &&
          warm.at("metrics").at("pack_hits") == 1 && warm.at("metrics").at("http").empty(),
          "warm pack read must be identical and network-free");
    const auto slice = agent.handle_query(request(boundary + 60000, boundary + 120000));
    check(slice.at("rows") == 1 && peer->calls == 3, "subrange did not reuse verified pack");
    const auto miss = agent.handle_query(request(boundary, boundary + 180000));
    check(miss.at("status") == "MISS" && peer->calls == 3, "cached bytes invented coverage");

    auto options = config(); options.max_cached_pack_bytes = 0;
    hc::Agent uncached(options, peer);
    check(uncached.handle_query(request()).at("status") == "HIT", "uncached full read failed");
    const auto before = peer->calls;
    const auto again = uncached.handle_query(request());
    check(again.at("status") == "HIT" && peer->calls == before + 1 &&
          again.at("metrics").at("resident_pack_bytes") == 0, "disabled pack cache retained data");

    options = config(); options.max_cached_packs = 1;
    publish(*peer, "202609", {boundary - 60000, boundary});
    hc::Agent eviction(options, peer);
    check(eviction.handle_query(request()).at("status") == "HIT", "eviction first load failed");
    check(eviction.handle_query(request(boundary - 60000, boundary)).at("status") == "HIT", "eviction second load failed");
    const auto evicted = eviction.handle_query(request());
    check(evicted.at("metrics").at("resident_packs") == 1 &&
          evicted.at("metrics").at("pack_hits") == 0 && evicted.at("metrics").at("http").at("pack").at("requests") == 1,
          "pack LRU count bound failed");
    options = config(); options.max_cached_pack_bytes = 1;
    hc::Agent too_small(options, peer);
    check(too_small.handle_query(request()).at("metrics").at("resident_pack_bytes") == 0,
          "oversize object exceeded byte bound");

    options = config(); options.manifest_ttl_seconds = 0;
    hc::Agent refreshing(options, peer);
    const auto original = refreshing.handle_query(request());
    publish(*peer, "202610", {boundary, boundary + 60000}, 2, false, "000001.SZ",
            {{boundary, 20, 21, 19, 20, 100}});
    const auto revision = refreshing.handle_query(request());
    check(revision.at("status") == "HIT" && revision.at("data") != original.at("data") &&
          revision.at("metrics").at("pack_hits") == 0, "new catalog served old pack");
    publish(*peer, "202610", {boundary, boundary + 60000}, 3, true);
    check(refreshing.handle_query(request()).at("rows") == 0, "empty revision served old bytes");
    peer->failure = hc::HttpFailure::dns;
    check(refreshing.handle_query(request()).at("status") == "ERROR", "expired catalog used stale pack on error");
    peer->failure = hc::HttpFailure::none;

    auto corrupt = std::make_shared<Peer>();
    publish(*corrupt, "202610", {boundary, boundary + 60000});
    std::string pack_key;
    hc::Bytes saved;
    for (auto& item : corrupt->objects) if (item.first.find("/data/") != std::string::npos) {
        pack_key = item.first; saved = item.second; item.second.back() ^= 1;
    }
    check(!pack_key.empty(), "fixture pack missing");
    hc::Agent broken(config(), corrupt);
    check(broken.handle_query(request()).at("status") == "ERROR", "corrupt pack admitted");
    corrupt->objects[pack_key] = saved;
    check(broken.handle_query(request()).at("status") == "HIT", "corrupt bytes poisoned cache");

    options = config(); options.full_pack_read_bytes = 0;
    hc::Agent range_only(options, corrupt);
    const auto ranged = range_only.handle_query(request());
    check(ranged.at("status") == "HIT" && ranged.at("metrics").at("http").count("index") == 1 &&
          ranged.at("metrics").at("http").count("block") == 1 &&
          ranged.at("metrics").at("resident_packs") == 0, "large-pack Range fallback changed");
    auto large = std::make_shared<Peer>();
    std::vector<hc::Row> many;
    for (int i = 0; i < 5001; ++i) many.push_back({boundary + i * 60000LL, 10, 11, 9, 10, 100});
    publish(*large, "202610", {boundary, boundary + 5001 * 60000LL}, 1, false, "000001.SZ", many);
    hc::Agent compressed_large(config(), large);
    const auto bounded = compressed_large.handle_query(request());
    check(bounded.at("status") == "HIT" && bounded.at("rows") == 1 &&
          bounded.at("metrics").at("http").count("pack") == 0,
          "compressible large history bypassed Range row bound");
    std::cout << "PASS agent_cache cold3 warm0 subrange LRU bytes revision corruption range_fallback\n";
}

int64_t ms(int year, int month, int day, int hour, int minute = 0) {
    std::tm value{};
    value.tm_year = year - 1900; value.tm_mon = month - 1; value.tm_mday = day;
    value.tm_hour = hour; value.tm_min = minute;
    return static_cast<int64_t>(timegm(&value)) * 1000;
}

Json market_tests() {
    hc::NewYorkClock clock;
    Json vectors = Json::array();
    for (const int year : {2006, 2025}) {
        for (const int month : {7, 12}) {
            const int offset = month == 7 ? 4 : 5;
            const auto wall = ms(year, month, 1, 9, 30);
            check(clock.to_utc(wall + 123) == wall + offset * 3600000 + 123, "US offset or milliseconds lost");
            check(clock.to_wall(wall + offset * 3600000 + 123) == wall + 123, "US UTC roundtrip failed");
        }
    }
    // Historical rules differ from post-2007 rules; trust installed IANA transitions.
    check(clock.to_utc(ms(2006, 3, 20, 9, 30)) == ms(2006, 3, 20, 14, 30), "pre-2007 DST changed");
    for (const auto wall : {ms(2025, 3, 9, 2, 30), ms(2025, 11, 2, 1, 30)})
        test::rejects(hc::ErrorCode::invalid, [&] { clock.to_utc(wall); });
    for (const auto utc : {ms(2025, 11, 2, 5, 30), ms(2025, 11, 2, 6, 30), ms(3000, 1, 1, 0)})
        test::rejects(hc::ErrorCode::invalid, [&] { clock.to_wall(utc); });
    for (int scenario = 0; scenario < 4; ++scenario) {
        const bool winter = scenario == 1;
        const auto month = winter ? "202512" : "202507";
        const auto symbol = scenario == 3 ? "BRK.B" : "AAPL";
        const int count = scenario == 2 ? 960 : 390;
        const auto first = ms(2025, winter ? 12 : 7, 1, scenario == 2 ? 4 : 9, scenario == 2 ? 0 : 30);
        std::vector<hc::Row> rows;
        hc::Bytes expected;
        for (int i = 0; i < count; ++i) {
            hc::Row row{first + i * 60000, 10, 11, 9, 10, 9007199254740993LL + i};
            rows.push_back(row);
            row.timestamp_ms += winter ? 18000000 : 14400000;
            const auto bytes = hc::canonical_row(row);
            expected.insert(expected.end(), bytes.begin(), bytes.end());
        }
        auto peer = std::make_shared<Peer>();
        publish(*peer, month, {first, first + count * 60000}, 1, false, symbol, rows);
        hc::Agent agent(config(), peer);
        auto input = request(clock.to_utc(first), clock.to_utc(first + count * 60000));
        input["symbol"] = symbol; input["max_rows"] = 5000;
        const auto result = agent.handle_query(input);
        check(result.at("status") == "HIT" && result.at("rows") == count, "US session not HIT");
        check(hc::unhex(result.at("data")) == expected && result.at("rows_sha256") == hc::hex(hc::sha256(expected)),
              "US rows/hash are not UTC canonical bytes");
        vectors.push_back({{"request", input}, {"response", result}});
        input["max_rows"] = 3;
        const auto limited = agent.handle_query(input);
        check(limited.at("rows") == 3 && hc::unhex(limited.at("data")) == hc::Bytes(expected.begin(), expected.begin() + 96),
              "US earliest max_rows changed");
        vectors.push_back({{"request", input}, {"response", limited}});
        input["end_ms"] = input.at("end_ms").get<int64_t>() + 60000;
        const auto missing = agent.handle_query(input);
        check(missing.at("status") == "MISS" && missing.at("uncovered")[0][0] == clock.to_utc(first + count * 60000),
              "MISS coverage leaked wall-clock milliseconds");
        vectors.push_back({{"request", input}, {"response", missing}});
    }
    auto peer = std::make_shared<Peer>();
    const auto boundary_us = ms(2025, 7, 1, 0);
    publish(*peer, "202506", {boundary_us - 60000, boundary_us}, 1, false, "AAPL");
    publish(*peer, "202507", {boundary_us, boundary_us + 60000}, 1, false, "AAPL");
    hc::Agent agent(config(), peer);
    auto input = request(clock.to_utc(boundary_us - 60000), clock.to_utc(boundary_us + 60000));
    input["symbol"] = "AAPL";
    const auto result = agent.handle_query(input);
    check(result.at("rows") == 2 && result.at("sources").size() == 2, "US local month routing failed");
    vectors.push_back({{"request", input}, {"response", result}});
    auto empty = std::make_shared<Peer>();
    publish(*empty, "202507", {boundary_us, boundary_us + 60000}, 1, true, "AAPL");
    hc::Agent empty_agent(config(), empty);
    input["start_ms"] = clock.to_utc(boundary_us);
    const auto empty_result = empty_agent.handle_query(input);
    check(empty_result.at("status") == "HIT" && empty_result.at("rows") == 0, "US empty HIT changed");
    vectors.push_back({{"request", input}, {"response", empty_result}});
    std::cout << "PASS agent_market UTC regular390 extended960 max_rows empty month DST hash\n";
    return vectors;
}

void lifetime_tests() {
    auto peer = std::make_shared<Peer>();
    publish(*peer, "202610", {boundary, boundary + 60000});
    auto options = config(); options.manifest_ttl_seconds = 1;
    hc::Agent agent(options, peer);
    for (int i = 0; i < 100; ++i) check(agent.handle_query(request()).at("status") == "HIT", "per-query budget leaked");
    const auto old_deadline = peer->deadlines.back();
    std::this_thread::sleep_for(std::chrono::seconds(61));
    check(agent.handle_query(request()).at("status") == "HIT", "store deadline survived beyond its query");
    check(peer->deadlines.back() > old_deadline + std::chrono::seconds(60), "deadline did not renew");
    std::cout << "PASS agent_lifetime queries=101 idle_seconds=61 network=0\n";
}

void maintenance_tests() {
    auto peer = std::make_shared<Peer>();
    publish(*peer, "202610", {boundary, boundary + 60000});
    auto options = config(); options.foreground_network = false;
    hc::Agent agent(options, peer);
    auto input = request();
    input["op"] = "query"; input["period_seconds"] = 60; input["adjust"] = "none";
    const auto plan = Json::array({input});
    hc::Maintenance scheduler(agent, plan);
    check(!scheduler.step() && peer->calls == 0, "missing idle lease started network work");
    check(agent.handle_query(input).at("reason") == "cache_not_ready" && peer->calls == 0,
          "cold foreground issued HTTP");
    scheduler.lease(true);
    check(scheduler.step() && peer->calls == 3 && scheduler.status().at("hits") == 1,
          "bounded warm failed");
    check(!scheduler.step() && peer->calls == 3, "warm loop exceeded cadence");
    for (auto rate : peer->rates) check(rate == 128 * 1024, "background download not rate limited");
    const auto hot = agent.handle_query(input);
    check(hot.at("status") == "HIT" && hot.at("metrics").at("http").empty(), "warm not reusable");
    hc::Maintenance unchanged(agent, plan);
    unchanged.lease(true);
    check(unchanged.step() && peer->calls == 4, "unchanged pointer re-downloaded manifest/pack");
    std::atomic_bool good{true};
    std::vector<std::thread> readers;
    for (int i = 0; i < 2; ++i) readers.emplace_back([&] {
        for (int n = 0; n < 1000; ++n) {
            const auto result = agent.handle_query(input);
            if (result.at("data") != hot.at("data") || !result.at("metrics").at("http").empty()) good = false;
        }
    });
    for (auto& thread : readers) thread.join();
    check(good && peer->calls == 4, "2000 hot queries used network or changed bytes");
    publish(*peer, "202610", {boundary, boundary + 60000}, 2, true);
    hc::Maintenance revision(agent, plan);
    revision.lease(true);
    check(revision.step() && agent.handle_query(input).at("rows") == 0, "refresh ignored new revision");
    options.manifest_ttl_seconds = 0;
    hc::Agent expired(options, peer);
    auto cancel = std::make_shared<std::atomic_bool>(false);
    check(expired.warm(input, cancel).at("status") == "HIT", "expired warm setup failed");
    const auto before = peer->calls;
    check(expired.handle_query(input).at("reason") == "cache_not_ready" && peer->calls == before,
          "expired foreground performed HTTP or served stale coverage");
    cancel->store(true);
    check(expired.warm(input, cancel).at("reason") == "background_paused" && peer->calls == before,
          "cancelled warm performed HTTP");

    class WaitingPeer final : public hc::HttpTransport {
    public:
        std::atomic_bool entered{false};
        hc::HttpResult perform(const hc::HttpRequest& value) override {
            entered = true;
            while (!value.cancelled->load() && hc::SteadyClock::now() < value.deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            return {hc::HttpDelivery::not_sent, {}, hc::HttpFailure::cancelled};
        }
    };
    auto waiting = std::make_shared<WaitingPeer>();
    hc::Agent busy(options, waiting);
    hc::Maintenance leased(busy, plan);
    leased.lease(true);
    std::thread worker([&] { leased.step(); });
    while (!waiting->entered.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const auto busy_result = busy.warm(input, std::make_shared<std::atomic_bool>(false));
    check(busy_result.at("reason") == "background_busy", "parallel warm admitted");
    check(!leased.step(), "parallel scheduler step admitted");
    check(busy.handle_query(input).at("reason") == "cache_not_ready", "foreground blocked on warm");
    std::this_thread::sleep_for(std::chrono::milliseconds(2050));
    leased.poll();
    worker.join();
    check(!leased.status().at("permitted").get<bool>() && leased.status().at("cancellations") == 1,
          "expired idle lease did not cancel in-flight work");
    leased.stop();
    leased.lease(true);
    check(!leased.step(), "stopped scheduler restarted");
    hc::Maintenance denied(agent, plan);
    denied.lease(true); denied.lease(false);
    check(!denied.step(), "busy signal failed to revoke lease");
    for (auto invalid : {Json::object(), Json::array({input, input}), Json::array({Json::object()})}) {
        bool rejected = false;
        try { hc::Maintenance invalid_scheduler(agent, invalid); } catch (...) { rejected = true; }
        check(rejected, "invalid warm plan accepted");
    }
    auto complete_plan = input;
    complete_plan["row_encoding"] = "le-kline48-v1";
    hc::Maintenance complete_scheduler(agent, Json::array({complete_plan}));
    complete_plan["row_encoding"] = "unknown";
    bool invalid_encoding = false;
    try { hc::Maintenance invalid_scheduler(agent, Json::array({complete_plan})); }
    catch (...) { invalid_encoding = true; }
    check(invalid_encoding, "invalid warm row encoding accepted");
    input["end_ms"] = boundary + 86400001;
    bool rejected = false;
    try { hc::Maintenance invalid_scheduler(agent, Json::array({input})); } catch (...) { rejected = true; }
    check(rejected, "unbounded warm range accepted");
    std::cout << "PASS agent_maintenance cache_only warm refresh rate cadence lease_cancel singleflight hot2000\n";
}

void demand_tests() {
    auto peer = std::make_shared<Peer>();
    publish(*peer, "202610", {boundary, boundary + 60000});
    auto options = config();
    check(hc::AgentConfig{}.foreground_network, "default must support R2 cold reads");
    hc::Agent agent(options, peer);
    std::atomic_bool start{false}, good{true};
    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) readers.emplace_back([&] {
        while (!start.load()) std::this_thread::yield();
        if (agent.handle_query(request()).at("status") != "HIT") good = false;
    });
    start = true;
    for (auto& reader : readers) reader.join();
    check(good && peer->calls == 3, "cold waiters duplicated download or failed");
    for (auto rate : peer->rates) check(rate == 512 * 1024, "demand receive pacing missing");
    options.manifest_ttl_seconds = 0;
    hc::Agent expired(options, peer);
    const auto first = expired.handle_query(request());
    const auto before = peer->calls;
    const auto refreshed = expired.handle_query(request());
    check(refreshed.at("status") == "HIT" && first.at("data") == refreshed.at("data") &&
          peer->calls == before + 1, "expiry must revalidate R2 current and reuse pack");
    publish(*peer, "202610", {boundary, boundary + 60000}, 2, true);
    check(expired.handle_query(request()).at("rows") == 0, "demand refresh ignored revision");
    std::cout << "PASS agent_demand cold_default concurrent8_get3 expired_current_only revision pacing\n";
}

Json complete_tests() {
    Json vectors = Json::array();
    for (bool us : {false, true}) {
        auto peer = std::make_shared<Peer>();
        const std::string symbol = us ? "AAPL" : "000001.SZ";
        const auto first = boundary + 86400000;
        std::vector<hc::Row> rows;
        hc::Bytes expected;
        hc::NewYorkClock clock;
        for (int i = 0; i < 1100; ++i) {
            hc::Row row{first + i * 60000, 10, 11, 9, 10.5F, 9007199254740993LL + i,
                        12345.125 + i, 9007199254741001LL + i};
            rows.push_back(row);
            if (us) row.timestamp_ms = clock.to_utc(row.timestamp_ms);
            const auto bytes = hc::canonical_kline(row);
            expected.insert(expected.end(), bytes.begin(), bytes.end());
        }
        publish(*peer, "202610", {first, first + 1100 * 60000LL}, 1, false, symbol, rows, true);
        hc::Agent agent(config(), peer);
        auto input = request(us ? clock.to_utc(first) : first,
                             us ? clock.to_utc(first + 1100 * 60000LL) : first + 1100 * 60000LL);
        input["symbol"] = symbol; input["max_rows"] = 5000; input["row_encoding"] = "le-kline48-v1";
        const auto cold = agent.handle_query(input);
        check(cold.at("status") == "HIT" && cold.at("rows") == 1100 &&
              cold.at("row_encoding") == "le-kline48-v1" && hc::unhex(cold.at("data")) == expected,
              "complete kline pack fields or timezone changed");
        check(peer->calls == 3 && agent.handle_query(input).at("metrics").at("http").empty(),
              "complete pack cache not reused");
        vectors.push_back({{"request", input}, {"response", cold}});
        auto legacy = input; legacy.erase("row_encoding");
        check(agent.handle_query(legacy).at("status") == "MISS", "full rows leaked into legacy namespace");
        auto legacy_peer = std::make_shared<Peer>();
        auto legacy_rows = rows;
        for (auto& row : legacy_rows) { row.turnover = 0; row.open_interest = 0; }
        publish(*legacy_peer, "202610", {first, first + 1100 * 60000LL}, 1, false, symbol, legacy_rows);
        hc::Agent legacy_agent(config(), legacy_peer);
        check(legacy_agent.handle_query(input).at("status") == "MISS", "missing complete fields fabricated");
        auto range_options = config(); range_options.full_pack_read_bytes = 0;
        hc::Agent ranged(range_options, peer);
        check(ranged.handle_query(input).at("data") == cold.at("data"), "schema2 Range decoder differs");
        auto limited = input; limited["max_rows"] = 3;
        check(hc::unhex(agent.handle_query(limited).at("data")) == hc::Bytes(expected.begin(), expected.begin() + 144),
              "schema2 truncation differs");
        auto invalid = rows.front(); invalid.open_interest = -1;
        bool rejected = false;
        try { (void)hc::canonical_kline(invalid); } catch (const hc::Error&) { rejected = true; }
        check(rejected, "negative open interest accepted");
    }
    std::cout << "PASS agent_complete kline48 exact_fields cold3 warm0 range legacy_miss UTC\n";
    return vectors;
}
}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 7 && std::string(argv[1]) == "candidate") {
            const std::filesystem::path directory(argv[2]);
            const auto manifest_bytes = hc::read_file(directory / "candidate.json", 1024 * 1024);
            const auto manifest = hc::parse_manifest({manifest_bytes.begin(), manifest_bytes.end()});
            const std::string prefix = std::string(argv[3]) + '/';
            auto peer = std::make_shared<Peer>();
            const auto key = test::key(manifest_bytes);
            peer->objects[prefix + key] = manifest_bytes;
            peer->objects[prefix + "current.json"] = test::bytes(hc::serialize_pointer(
                {manifest.dataset_epoch, 1, key, hc::sha256(manifest_bytes)}));
            for (const auto& entry : manifest.entries) if (entry.pack)
                peer->objects[prefix + entry.pack->key] = hc::read_file(
                    directory / std::filesystem::path(entry.pack->key).filename(), hc::kMaxObjectBytes);
            const auto input = hc::read_file(argv[4], 4096);
            hc::Agent agent(config(), peer);
            const auto request_json = Json::parse(input);
            const auto cold = agent.handle_query(request_json);
            const auto expected = hc::read_file(argv[5], 5000 * 64);
            check(cold.at("status") == "HIT" && hc::unhex(cold.at("data")) == expected,
                  "exported candidate differs at agent boundary");
            const auto warm = agent.handle_query(request_json);
            check(warm.at("data") == cold.at("data"), "exported candidate warm read differs");
            const bool cacheable = std::all_of(manifest.entries.begin(), manifest.entries.end(), [](const auto& entry) {
                return entry.row_count <= 5000 && (!entry.pack || entry.pack->bytes <= config().full_pack_read_bytes);
            });
            if (cacheable) check(warm.at("metrics").at("http").empty(), "cacheable candidate issued warm HTTP");
            auto range_config = config(); range_config.full_pack_read_bytes = 0;
            hc::Agent range_agent(range_config, peer);
            check(range_agent.handle_query(request_json).at("data") == cold.at("data"), "native Range path differs");
            hc::write_new_file(argv[6], Json::array({{{"request", request_json}, {"response", cold}}}).dump());
            std::cout << "PASS exported_candidate_agent cold_then_warm fields8 UTC cacheable=" << cacheable << '\n';
            return 0;
        }
        if (argc == 2 && std::string(argv[1]) == "lifetime") lifetime_tests();
        else {
            const auto intraday = intraday_tests();
            archive_handoff_tests();
            service_tests();
            cache_tests();
            maintenance_tests();
            demand_tests();
            adjustment_service_tests();
            versioned_factor_tests();
            auto vectors = market_tests();
            for (const auto& vector : intraday) vectors.push_back(vector);
            const auto complete = complete_tests();
            for (const auto& vector : complete) vectors.push_back(vector);
            if (argc == 3 && std::string(argv[1]) == "vectors") hc::write_new_file(argv[2], vectors.dump());
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
