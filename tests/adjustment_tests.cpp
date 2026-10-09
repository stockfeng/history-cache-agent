#include "history_cache/adjustment.h"
#include "history_cache/factor_snapshot.h"
#include "history_cache/market_clock.h"

#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <nlohmann/json.hpp>

using namespace history_cache;
void check(bool condition) { if (!condition) throw std::runtime_error("adjustment assertion failed"); }
template<class F> void rejects(F function) {
    try { function(); } catch (const Error&) { return; }
    throw std::runtime_error("expected adjustment rejection");
}
Row row(int64_t time, double price) {
    Row value;
    value.timestamp_ms = time;
    value.volume = 9007199254740993LL;
    value.native = Row::NativeFields{{price, price, price, price}, 9007199254740995LL, 9007199254740997LL};
    return value;
}
int main() {
    using Json = nlohmann::json;
    Json document = {{"schema_version", 1}, {"kind", "ddb-adjustment-snapshot-v1"},
        {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "000001.SZ"}, {"market", "SZ"},
        {"model", "cumulative"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
        {"allow_empty", true}, {"observed_at_ms", 1000}, {"valid_until_ms", 2000},
        {"source_epoch", "fixture"}, {"source_proof_sha256", hex(sha256("proof"))},
        {"date_encoding", "exchange-civil-days-since-1970"},
        {"rows", Json::array({{{"code", "000001.SZ"}, {"ex_date", 20}, {"ex_factor", 2.0},
                              {"cum_factor", 2.0}, {"update_time", 1}}})}};
    auto parse = [&](const Json& input) {
        const auto text = input.dump(); const Bytes bytes(text.begin(), text.end());
        return parse_factor_snapshot(bytes, sha256(bytes), "000001.SZ", "SZ", 1500);
    };
    auto parsed = parse(document);
    {
        const auto text = document.dump(); const Bytes bytes(text.begin(), text.end());
        const auto saved = parse_factor_snapshot(bytes, sha256(bytes), "000001.SZ", "SZ", 2000,
            FactorReadPolicy::published_version);
        check(saved.factors.factor_set_hash == parsed.factors.factor_set_hash);
        check(saved.valid_until_ms == 2000 && saved.observed_at_ms == 1000);
        check(parse_factor_snapshot(bytes, sha256(bytes), "000001.SZ", "SZ", 365LL * 86400000,
            FactorReadPolicy::published_version).factors.events[0].cumulative == 2.0);
        rejects([&] { parse_factor_snapshot(bytes, sha256(bytes), "000001.SZ", "SZ", 999,
            FactorReadPolicy::published_version); });
        rejects([&] { parse_factor_snapshot(bytes, sha256(bytes), "000001.SZ", "SZ", 2000); });
    }
    {
        constexpr int64_t day = 20728;
        auto scheduled = document;
        const auto observed = day * 86400000 + 1000;
        const auto until = (day + 3) * 86400000;
        Json contract = {{"policy", "market-source-check-v1"}, {"exchange", "XSHE"},
            {"calendar_sha256", hex(sha256("calendar"))}, {"trading_day", day}, {"next_trading_day", day + 3},
            {"check_minute", 480}, {"source_revision", 1}, {"source_completed_at_ms", observed},
            {"next_check_ms", until}, {"config_sha256", hex(sha256("config"))},
            {"workload_sha256", hex(sha256("workload"))}, {"source_receipt_sha256", hex(sha256("receipt"))}};
        scheduled.update(Json{{"schema_version", 3}, {"kind", "ddb-adjustment-snapshot-v3"},
            {"observed_at_ms", observed}, {"valid_until_ms", until}, {"verification", contract}});
        const auto read = [&](const Json& value, int64_t now) {
            const auto text = value.dump(); const Bytes bytes(text.begin(), text.end());
            return parse_factor_snapshot(bytes, sha256(bytes), value.at("symbol"), value.at("market"), now);
        };
        check(read(scheduled, observed + 2 * 86400000).source_revision == 1);
        rejects([&] { read(scheduled, until); });
        const auto scheduled_text = scheduled.dump();
        const Bytes scheduled_bytes(scheduled_text.begin(), scheduled_text.end());
        check(parse_factor_snapshot(scheduled_bytes, sha256(scheduled_bytes), "000001.SZ", "SZ", until,
            FactorReadPolicy::published_version).source_revision == 1);
        auto bad = scheduled; bad["verification"]["next_check_ms"] = until + 1;
        rejects([&] { read(bad, observed); });
        bad = scheduled; bad["verification"]["exchange"] = "XNYS";
        rejects([&] { read(bad, observed); });
        bad = scheduled; bad["schema_version"] = 1; bad["kind"] = "ddb-adjustment-snapshot-v1";
        bad.erase("verification");
        rejects([&] { read(bad, observed); });
        auto data = document;
        for (const auto* key : {"observed_at_ms", "valid_until_ms", "source_proof_sha256"}) data.erase(key);
        data["schema_version"] = 2; data["kind"] = "ddb-adjustment-data-v2";
        const auto text = data.dump(); const Bytes content(text.begin(), text.end());
        scheduled["kind"] = "ddb-adjustment-reference-v3"; scheduled.erase("rows");
        scheduled["factor_data_sha256"] = hex(sha256(content)); scheduled["factor_data_bytes"] = content.size();
        auto resolve = [&](const Json& value) {
            const auto encoded = value.dump(); const Bytes ref(encoded.begin(), encoded.end());
            return resolve_factor_snapshot(ref, sha256(ref), content, "000001.SZ", "SZ", observed + 1000);
        };
        check(resolve(scheduled).factors.factor_set_hash == sha256(content));
        bad = scheduled; bad["verification"]["source_revision"] = 2;
        bad["source_proof_sha256"] = hex(sha256("new-proof"));
        check(resolve(bad).factors.factor_set_hash == resolve(scheduled).factors.factor_set_hash);
        // New York's autumn transition spans 73 hours from Friday to Monday.
        NewYorkClock clock;
        constexpr int64_t friday = 20756;
        const auto start = clock.to_utc(friday * 86400000 + 180 * 60000);
        const auto end = clock.to_utc((friday + 3) * 86400000 + 180 * 60000);
        check(end - start == 73 * 3600000);
        auto us = document;
        us.update(Json{{"schema_version", 3}, {"kind", "ddb-adjustment-snapshot-v3"}, {"symbol", "AAPL"},
            {"market", "US"}, {"model", "futu_ab"}, {"rows", Json::array()},
            {"observed_at_ms", start + 1000}, {"valid_until_ms", end}});
        contract.update(Json{{"exchange", "XNYS"}, {"trading_day", friday}, {"next_trading_day", friday + 3},
            {"check_minute", 180}, {"source_completed_at_ms", start}, {"next_check_ms", end}});
        us["verification"] = contract;
        check(read(us, end - 1).valid_until_ms == end);
        us["valid_until_ms"] = end - 3600000;
        us["verification"]["next_check_ms"] = end - 3600000;
        rejects([&] { read(us, start + 1000); });
        for (const bool american : {false, true}) {
            auto eod = american ? us : document;
            const auto complete = day * 86400000 + (american ? 86400000 : 15 * 3600000) + 60000;
            const auto deadline = (day + 4) * 86400000 + (american ? 2 : -7) * 3600000;
            auto v = contract;
            v.update(Json{{"policy", "market-eod-check-v1"}, {"exchange", american ? "XNYS" : "XSHE"},
                {"trading_day", day}, {"next_trading_day", day + 3}, {"check_minute", american ? 600 : 60},
                {"source_completed_at_ms", complete}, {"next_check_ms", deadline}});
            eod.update(Json{{"schema_version", 3}, {"kind", "ddb-adjustment-snapshot-v3"},
                {"observed_at_ms", complete}, {"valid_until_ms", deadline}, {"verification", v}});
            check(read(eod, deadline - 1).valid_until_ms == deadline);
            rejects([&] { read(eod, deadline); });
            eod["verification"]["check_minute"] = 180;
            rejects([&] { read(eod, complete); });
            eod["verification"] = v;
            eod["verification"]["source_completed_at_ms"] = complete - 60001;
            rejects([&] { read(eod, complete); });
        }
    }
    {
        auto data = document;
        for (const auto* field : {"observed_at_ms", "valid_until_ms", "source_proof_sha256"}) data.erase(field);
        data["schema_version"] = 2; data["kind"] = "ddb-adjustment-data-v2";
        const auto encoded = data.dump(); const Bytes payload(encoded.begin(), encoded.end());
        auto reference = document;
        reference.erase("rows"); reference["schema_version"] = 2; reference["kind"] = "ddb-adjustment-reference-v2";
        reference["factor_data_sha256"] = hex(sha256(payload)); reference["factor_data_bytes"] = payload.size();
        auto resolve = [&](const Json& value, const Bytes& content) {
            const auto encoded_ref = value.dump(); const Bytes ref(encoded_ref.begin(), encoded_ref.end());
            return resolve_factor_snapshot(ref, sha256(ref), content, "000001.SZ", "SZ", 1500);
        };
        check(resolve(reference, payload).factors.events[0].cumulative == parsed.factors.events[0].cumulative);
        rejects([&] { select_factor_window(parse(reference), 1, 30); });
        auto bad = payload; bad[0] ^= 1;
        rejects([&] { resolve(reference, bad); });
        auto expired_ref = reference; expired_ref["valid_until_ms"] = 1400;
        rejects([&] { resolve(expired_ref, payload); });
        const auto old_text = expired_ref.dump(); const Bytes old_ref(old_text.begin(), old_text.end());
        const auto old = resolve_factor_snapshot(old_ref, sha256(old_ref), payload, "000001.SZ", "SZ", 1500,
            FactorReadPolicy::published_version);
        check(old.factors.factor_set_hash == sha256(payload) && old.valid_until_ms == 1400);
        auto mismatch = reference; mismatch["first_day"] = 0;
        rejects([&] { resolve(mismatch, payload); });
        auto wrong_size = reference; wrong_size["factor_data_bytes"] = payload.size() + 1;
        rejects([&] { resolve(wrong_size, payload); });
        const auto repeated = std::string("{\"schema_version\":2,") + encoded.substr(1);
        const Bytes dup(repeated.begin(), repeated.end());
        reference["factor_data_sha256"] = hex(sha256(dup)); reference["factor_data_bytes"] = dup.size();
        rejects([&] { resolve(reference, dup); });
    }
    const auto duplicate = std::string("{\"schema_version\":1,") + document.dump().substr(1);
    const Bytes duplicate_bytes(duplicate.begin(), duplicate.end());
    rejects([&] { parse_factor_snapshot(duplicate_bytes, sha256(duplicate_bytes), "000001.SZ", "SZ", 1500); });
    rejects([&] { select_factor_window(parsed, 0, 10); });
    rejects([&] { select_factor_window(parsed, 1, 101); });
    check(select_factor_window(parsed, 1, 10).events.empty());
    check(select_factor_window(parsed, 21, 30).events[0].day == 20);
    document["rows"].push_back(document["rows"][0]);
    rejects([&] { parse(document); });
    document["rows"][1]["update_time"] = 2;
    document["rows"][1]["cum_factor"] = 3.0;
    check(parse(document).factors.events[0].cumulative == 3.0);
    document["valid_until_ms"] = 1400;
    rejects([&] { parse(document); });
    document["valid_until_ms"] = 2000;
    document["coverage_complete"] = false;
    rejects([&] { parse(document); });
    rejects([&] { select_factor_window(parsed, 0, 20); });
    AdjustmentSnapshot snapshot;
    snapshot.symbol = "000001.SZ";
    snapshot.factor_set_hash = sha256("fixture-version");
    snapshot.first_day = 1; snapshot.end_day = 100; snapshot.complete = true;
    snapshot.events = {{10, 2.0}, {20, 4.0}};
    std::vector<Row> rows{row(1, 10.125), row(2, 10.125)};
    auto apply = [&](const std::vector<Row>& input, const std::vector<int64_t>& days, AdjustmentMode mode) {
        return adjust_native_rows(snapshot.symbol, input, days, snapshot, mode);
    };
    auto forward = apply(rows, {10, 20}, AdjustmentMode::forward);
    check(forward[0].native->prices[0] == 5.06 && forward[1].native->prices[0] == 10.13);
    auto truncated = apply({rows[0]}, {10}, AdjustmentMode::forward);
    check(truncated[0].native->prices[0] == 10.13);
    auto backward = apply(rows, {9, 20}, AdjustmentMode::backward);
    check(backward[0].native->prices[0] == 10.13 && backward[1].native->prices[0] == 40.5);
    auto nullable = rows;
    nullable[0].native->prices[0] = kDdbNullPrice;
    nullable[0].native->prices[3] = kDdbNullPrice;
    for (auto mode : {AdjustmentMode::forward, AdjustmentMode::backward}) {
        const auto adjusted = apply(nullable, {10, 20}, mode);
        check(adjusted[0].native->prices[0] == kDdbNullPrice && adjusted[0].native->prices[3] == kDdbNullPrice);
        check(adjusted[0].native->prices[1] == (mode == AdjustmentMode::forward ? 5.06 : 20.25));
        (void)canonical_native(adjusted[0]);
    }
    check(forward[0].volume == rows[0].volume && forward[0].native->open_oi == rows[0].native->open_oi &&
          forward[0].native->close_oi == rows[0].native->close_oi && rows[0].native->prices[0] == 10.125);
    check(apply({}, {}, AdjustmentMode::forward).empty());
    rejects([&] { apply(rows, {0, 20}, AdjustmentMode::forward); });
    rejects([&] { apply(rows, {20, 10}, AdjustmentMode::forward); });
    rejects([&] { apply(rows, {10}, AdjustmentMode::forward); });
    snapshot.complete = false;
    rejects([&] { apply(rows, {10, 20}, AdjustmentMode::forward); });
    snapshot.complete = true;
    snapshot.events.push_back({20, 5.0});
    rejects([&] { apply(rows, {10, 20}, AdjustmentMode::forward); });
    snapshot.events.clear();
    rejects([&] { apply(rows, {10, 20}, AdjustmentMode::forward); });
    snapshot.allow_empty = true;
    check(apply(rows, {10, 20}, AdjustmentMode::forward)[0].native->prices[0] == 10.125);
    snapshot.model = AdjustmentModel::futu_ab;
    snapshot.events = {{10, 1, 2, 3, 0.5, -1.5}, {20, 1, 4, 5, 0.25, -1.25}};
    rows = {row(1, 10), row(2, 10), row(3, 10)};
    forward = apply(rows, {9, 10, 20}, AdjustmentMode::forward);
    check(forward[0].native->prices[0] == 97 && forward[1].native->prices[0] == 45 && forward[2].native->prices[0] == 10);
    backward = apply(rows, {9, 10, 20}, AdjustmentMode::backward);
    check(backward[0].native->prices[0] == 10 && backward[1].native->prices[0] == 3.5 && backward[2].native->prices[0] == -0.875);
    snapshot.events[0].forward_a = std::numeric_limits<double>::infinity();
    rejects([&] { apply(rows, {9, 10, 20}, AdjustmentMode::forward); });
    snapshot.events.clear();
    snapshot.carry_factor = 2;
    rejects([&] { apply(rows, {9, 10, 20}, AdjustmentMode::forward); });
    snapshot.carry_factor = 1;
    rows[0].volume = -1;
    rejects([&] { apply(rows, {9, 10, 20}, AdjustmentMode::forward); });
    rows[0].volume = 1;
    rows[0].native.reset();
    rejects([&] { apply(rows, {9, 10, 20}, AdjustmentMode::forward); });
    std::cout << "PASS adjustment cumulative anchor_truncation rounding affine_order effective_day native64 bounds\n";
}
