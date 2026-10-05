#include "history_cache/factor_snapshot.h"
#include "history_cache/market_clock.h"

#include <algorithm>
#include <cmath>
#include <set>
#include <nlohmann/json.hpp>

namespace history_cache {
namespace {
using Json = nlohmann::json;
void require(bool value, const char* text) {
    if (!value) throw Error(ErrorCode::corrupt, text);
}
void fields(const Json& value, const std::set<std::string>& expected) {
    require(value.is_object() && value.size() == expected.size(), "factor schema differs");
    for (const auto& name : expected) require(value.contains(name), "missing factor field");
}
int64_t integer(const Json& value) {
    require(value.is_number_integer() && (!value.is_number_unsigned() ||
        value.get<uint64_t>() <= static_cast<uint64_t>(INT64_MAX)), "factor integer required");
    return value.get<int64_t>();
}
double number(const Json& value, bool positive = false) {
    require(value.is_number(), "factor number required");
    const auto result = value.get<double>();
    require(std::isfinite(result) && (!positive || result > 0), "invalid factor number");
    return result;
}
void verification(const Json& value, const std::string& market, int64_t observed, int64_t until) {
    fields(value, {"policy", "exchange", "calendar_sha256", "trading_day", "next_trading_day",
        "check_minute", "source_revision", "source_completed_at_ms", "next_check_ms",
        "config_sha256", "workload_sha256", "source_receipt_sha256"});
    const std::string exchange = market == "SH" ? "XSHG" : market == "SZ" ? "XSHE" : market == "HK" ? "XHKG" : "XNYS";
    const bool eod = value.at("policy") == "market-eod-check-v1";
    require((eod || value.at("policy") == "market-source-check-v1") && value.at("exchange") == exchange,
            "factor verification exchange differs");
    for (const auto* key : {"calendar_sha256", "config_sha256", "workload_sha256", "source_receipt_sha256"})
        require(parse_digest(value.at(key).get<std::string>()) != Digest{}, "factor verification digest missing");
    const auto day = integer(value.at("trading_day")), next = integer(value.at("next_trading_day"));
    const auto minute = integer(value.at("check_minute")), completed = integer(value.at("source_completed_at_ms"));
    require(day >= 0 && day < next && next <= 47847 && next - day <= 16 &&
            minute >= 0 && (eod ? minute == (market == "US" ? 600 : 60) : minute < (market == "US" ? 240 : 555)) &&
            integer(value.at("source_revision")) > 0,
            "factor verification day/minute invalid");
    const auto to_utc = [&](int64_t civil_day, int64_t check_minute) {
        const auto wall = civil_day * 86400000 + check_minute * 60000;
        if (market == "US") { static const NewYorkClock clock; return clock.to_utc(wall); }
        return wall - 28800000;
    };
    const auto boundary = eod ? (next + 1) * 86400000 + minute * 60000 - 28800000 : to_utc(next, minute);
    const auto earliest = eod ? day * 86400000 + (market == "US" ? 86400000 : 15 * 3600000) : to_utc(day, 0);
    require(integer(value.at("next_check_ms")) == until && boundary == until &&
            earliest <= completed && completed <= observed && (eod || to_utc(day, minute) <= observed) && observed < until &&
            until - observed <= 16LL * 86400000, "factor verification expired or clock differs");
}
}  // namespace

FactorSnapshot parse_factor_snapshot(const Bytes& bytes, const Digest& expected_hash,
    const std::string& symbol, const std::string& market, int64_t now_ms, FactorReadPolicy policy) {
    require(bytes.size() <= kMaxFactorBytes && sha256(bytes) == expected_hash, "factor digest/size mismatch");
    size_t events = 0;
    std::vector<std::set<std::string>> object_keys;
    const auto json = Json::parse(bytes, [&](int depth, Json::parse_event_t event, Json& value) {
        require(depth <= 8 && ++events <= 100000, "factor JSON complexity bound");
        if (event == Json::parse_event_t::object_start) object_keys.emplace_back();
        else if (event == Json::parse_event_t::key) {
            require(!object_keys.empty() && object_keys.back().insert(value.get<std::string>()).second,
                    "duplicate factor JSON key");
        } else if (event == Json::parse_event_t::object_end) object_keys.pop_back();
        return true;
    });
    const auto kind = json.value("kind", std::string{});
    const bool scheduled = kind == "ddb-adjustment-reference-v3" || kind == "ddb-adjustment-snapshot-v3";
    const bool reference = kind == "ddb-adjustment-reference-v2" || kind == "ddb-adjustment-reference-v3";
    std::set<std::string> expected = {"schema_version", "kind", "algorithm", "symbol", "market", "model",
        "first_day", "end_day", "coverage_complete", "allow_empty", "observed_at_ms",
        "valid_until_ms", "source_epoch", "source_proof_sha256", "date_encoding"};
    if (reference) { expected.insert("factor_data_sha256"); expected.insert("factor_data_bytes"); }
    else expected.insert("rows");
    if (scheduled) expected.insert("verification");
    fields(json, expected);
    require(integer(json.at("schema_version")) == (scheduled ? 3 : reference ? 2 : 1) &&
        json.at("kind") == (scheduled ? (reference ? "ddb-adjustment-reference-v3" : "ddb-adjustment-snapshot-v3") :
                            (reference ? "ddb-adjustment-reference-v2" : "ddb-adjustment-snapshot-v1")) &&
        json.at("algorithm") == "upcloud-adjustment-v1" && json.at("symbol") == symbol && json.at("market") == market &&
        json.at("date_encoding") == "exchange-civil-days-since-1970", "factor identity/algorithm mismatch");
    require(json.at("coverage_complete").is_boolean() && json.at("coverage_complete").get<bool>() &&
        json.at("allow_empty").is_boolean(), "factor completeness contract missing");
    const bool cumulative = json.at("model") == "cumulative";
    require((cumulative && (market == "SH" || market == "SZ" || market == "HK")) ||
        (json.at("model") == "futu_ab" && market == "US"), "factor market/model mismatch");
    FactorSnapshot result;
    result.observed_at_ms = integer(json.at("observed_at_ms"));
    result.valid_until_ms = integer(json.at("valid_until_ms"));
    require(result.observed_at_ms > 0 && result.observed_at_ms < result.valid_until_ms &&
        (policy == FactorReadPolicy::structural_only || result.observed_at_ms <= now_ms) &&
        (policy != FactorReadPolicy::fresh_publication || now_ms < result.valid_until_ms) &&
        result.valid_until_ms - result.observed_at_ms <= (scheduled ? 16LL * 86400000 : 86400000),
        "factor snapshot expired or future-dated");
    if (scheduled) {
        verification(json.at("verification"), market, result.observed_at_ms, result.valid_until_ms);
        result.source_revision = static_cast<uint64_t>(integer(json.at("verification").at("source_revision")));
        result.source_receipt_hash = parse_digest(json.at("verification").at("source_receipt_sha256").get<std::string>());
    }
    result.source_epoch = json.at("source_epoch").get<std::string>();
    require(!result.source_epoch.empty() && result.source_epoch.size() <= 128, "invalid factor source epoch");
    require(parse_digest(json.at("source_proof_sha256").get<std::string>()) != Digest{}, "missing factor provenance reference");
    auto& factors = result.factors;
    factors.symbol = symbol;
    factors.factor_set_hash = expected_hash;
    factors.model = cumulative ? AdjustmentModel::cumulative : AdjustmentModel::futu_ab;
    factors.first_day = integer(json.at("first_day"));
    factors.end_day = integer(json.at("end_day"));
    require(factors.first_day >= -25567 && factors.first_day < factors.end_day && factors.end_day <= 47847,
            "invalid factor date coverage");
    factors.complete = true;
    factors.allow_empty = json.at("allow_empty").get<bool>();
    if (reference) {
        result.data_hash = parse_digest(json.at("factor_data_sha256").get<std::string>());
        const auto size = integer(json.at("factor_data_bytes"));
        require(result.data_hash != Digest{} && size > 0 && size <= static_cast<int64_t>(kMaxFactorBytes),
                "invalid factor data reference");
        result.data_bytes = static_cast<uint64_t>(size);
        factors.complete = false;
        return result;
    }
    const auto& rows = json.at("rows");
    require(rows.is_array() && rows.size() <= 20000, "factor row count bound");
    int64_t previous_day = INT64_MIN, previous_update = -1;
    for (const auto& row : rows) {
        fields(row, cumulative ? std::set<std::string>{"code", "ex_date", "ex_factor", "cum_factor", "update_time"}
            : std::set<std::string>{"code", "ex_div_date", "forward_adj_factorA", "forward_adj_factorB",
                                  "backward_adj_factorA", "backward_adj_factorB", "update_time"});
        require(row.at("code") == symbol, "factor row symbol mismatch");
        AdjustmentEvent event;
        event.day = integer(row.at(cumulative ? "ex_date" : "ex_div_date"));
        const auto update = integer(row.at("update_time"));
        require(event.day >= -25567 && event.day < 47847 && update >= 0 &&
            (event.day > previous_day || (event.day == previous_day && update > previous_update)),
            "unordered factors or ambiguous same-version revision");
        if (cumulative) {
            (void)number(row.at("ex_factor"), true);
            event.cumulative = number(row.at("cum_factor"), true);
        } else {
            event.forward_a = number(row.at("forward_adj_factorA"), true);
            event.backward_a = number(row.at("backward_adj_factorA"), true);
            event.forward_b = row.at("forward_adj_factorB").is_null() ? 0 : number(row.at("forward_adj_factorB"));
            event.backward_b = row.at("backward_adj_factorB").is_null() ? 0 : number(row.at("backward_adj_factorB"));
        }
        if (event.day == previous_day) factors.events.back() = event;
        else factors.events.push_back(event);
        previous_day = event.day;
        previous_update = update;
    }
    // Cumulative packages carry the last valid event before their first day.
    // Futu packages instead require the publisher's complete all-event scan.
    if (cumulative) {
        require(std::count_if(factors.events.begin(), factors.events.end(), [&](const auto& event) {
            return event.day < factors.first_day;
        }) <= 1, "multiple carry-in days");
        require(factors.events.empty() || factors.events.back().day < factors.end_day, "factor outside declared window");
    }
    require(factors.allow_empty || !factors.events.empty(), "empty factor snapshot not allowed");
    return result;
}

FactorSnapshot resolve_factor_snapshot(const Bytes& reference, const Digest& expected_hash,
    const Bytes& data, const std::string& symbol, const std::string& market, int64_t now_ms, FactorReadPolicy policy) {
    const auto header = parse_factor_snapshot(reference, expected_hash, symbol, market, now_ms, policy);
    require(header.data_hash != Digest{} && data.size() == header.data_bytes && sha256(data) == header.data_hash,
            "factor data digest/length differs");
    // Validate duplicate keys/depth before using Json::parse's object projection.
    size_t events = 0;
    std::vector<std::set<std::string>> keys;
    const auto content = Json::parse(data, [&](int depth, Json::parse_event_t event, Json& value) {
        require(depth <= 8 && ++events <= 100000, "factor data complexity bound");
        if (event == Json::parse_event_t::object_start) keys.emplace_back();
        else if (event == Json::parse_event_t::key)
            require(!keys.empty() && keys.back().insert(value.get<std::string>()).second, "duplicate factor data key");
        else if (event == Json::parse_event_t::object_end) keys.pop_back();
        return true;
    });
    auto joined = Json::parse(reference);
    auto expected = joined;
    for (const auto* key : {"observed_at_ms", "valid_until_ms", "source_proof_sha256",
                            "factor_data_sha256", "factor_data_bytes", "verification"}) expected.erase(key);
    expected["schema_version"] = 2;
    expected["kind"] = "ddb-adjustment-data-v2";
    expected["rows"] = content.at("rows");
    require(content == expected, "factor data identity/coverage differs");
    joined.erase("factor_data_sha256"); joined.erase("factor_data_bytes");
    joined["schema_version"] = joined.contains("verification") ? 3 : 1;
    joined["kind"] = joined.contains("verification") ? "ddb-adjustment-snapshot-v3" : "ddb-adjustment-snapshot-v1";
    joined["rows"] = content.at("rows");
    const auto encoded = joined.dump();
    const Bytes bytes(encoded.begin(), encoded.end());
    auto result = parse_factor_snapshot(bytes, sha256(bytes), symbol, market, now_ms, policy);
    result.factors.factor_set_hash = joined.contains("verification") || policy == FactorReadPolicy::published_version
        ? header.data_hash : expected_hash;
    result.data_hash = header.data_hash; result.data_bytes = header.data_bytes;
    return result;
}

AdjustmentSnapshot select_factor_window(const FactorSnapshot& snapshot, int64_t first_day, int64_t end_day) {
    const auto& source = snapshot.factors;
    require(source.complete, "unresolved factor data reference");
    require(source.first_day <= first_day && first_day < end_day && end_day <= source.end_day,
            "requested factor window not covered");
    auto selected = source;
    selected.first_day = first_day;
    selected.end_day = end_day;
    if (source.model == AdjustmentModel::cumulative) {
        auto first = std::lower_bound(source.events.begin(), source.events.end(), first_day,
            [](const auto& event, int64_t day) { return event.day < day; });
        if (first != source.events.begin()) --first;
        const auto end = std::lower_bound(first, source.events.end(), end_day,
            [](const auto& event, int64_t day) { return event.day < day; });
        selected.events.assign(first, end);
    }
    require(selected.allow_empty || !selected.events.empty(), "no factors for requested window");
    return selected;
}
}  // namespace history_cache
