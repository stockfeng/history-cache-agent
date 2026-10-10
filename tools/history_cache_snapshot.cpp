#include "history_cache/journal.h"
#include "history_cache/local_store.h"
#include "history_cache/reader.h"
#include "history_cache/s3_store.h"
#include "sample_transfer.h"
#include "snapshot_diagnostics.h"
#include "verified_object_store.h"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#ifdef HC_HAS_CURL
#include <curl/curl.h>
#endif

namespace hc = history_cache;
namespace fs = std::filesystem;
using Json = nlohmann::json;

namespace {

constexpr uint64_t kRows = 5000;
constexpr uint64_t kBytes = 1024 * 1024;
constexpr uint64_t kRequests = 48;
constexpr uint64_t kMonthRows = 31 * kRows;
constexpr uint64_t kMonthBytes = 14 * kBytes;

void require(bool condition, const char* message) {
    if (!condition) throw hc::Error(hc::ErrorCode::invalid, message);
}

class Arguments {
public:
    Arguments(int argc, char** argv, std::initializer_list<const char*> allowed) {
        const std::set<std::string> names(allowed.begin(), allowed.end());
        for (int i = 2; i < argc; i += 2) {
            require(i + 1 < argc && names.count(argv[i]), "unknown or missing argument");
            require(values_.emplace(argv[i], argv[i + 1]).second, "duplicate argument");
        }
    }
    std::string get(const std::string& key) const {
        const auto found = values_.find(key);
        require(found != values_.end() && !found->second.empty(), "required argument missing");
        return found->second;
    }
    std::string optional(const std::string& key, const std::string& fallback) const {
        const auto found = values_.find(key);
        return found == values_.end() ? fallback : found->second;
    }
private:
    std::map<std::string, std::string> values_;
};

std::string canonical(const Json& value) { return value.dump() + '\n'; }

Json read_json(const fs::path& path) {
    const auto bytes = hc::read_file(path, kBytes);
    const std::string text(bytes.begin(), bytes.end());
    size_t events = 0;
    auto result = Json::parse(text, [&](int depth, Json::parse_event_t, Json&) {
        require(depth <= 14 && ++events <= 100000, "source JSON exceeds parsing budget");
        return true;
    });
    require(canonical(result) == text, "source JSON must be canonical");
    return result;
}

uint64_t integer(const Json& value, uint64_t maximum = UINT64_MAX) {
    require(value.is_number_unsigned(), "unsigned JSON integer required");
    const auto number = value.get<uint64_t>();
    require(number <= maximum, "integer exceeds limit");
    return number;
}

uint64_t number(const std::string& text, uint64_t maximum) {
    uint64_t result = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    require(!text.empty() && parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && result <= maximum,
            "invalid integer argument");
    return result;
}

uint64_t little(const uint8_t* value, size_t count) {
    uint64_t result = 0;
    for (size_t i = 0; i < count; ++i) result |= uint64_t(value[i]) << (8U * i);
    return result;
}

hc::Bytes bytes_of(const Json& value) {
    const auto text = canonical(value);
    return {text.begin(), text.end()};
}

size_t row_bytes(const hc::SeriesIdentity& identity) {
    return identity.dataset == "ddb-history-native64" ? 64 : identity.dataset == "ddb-history-kline48" ? 48 : 32;
}

void append_row(hc::Bytes& bytes, const hc::Row& row, const hc::SeriesIdentity& identity) {
    if (row_bytes(identity) == 64) {
        const auto encoded = hc::canonical_native(row);
        bytes.insert(bytes.end(), encoded.begin(), encoded.end());
    } else if (row_bytes(identity) == 48) {
        const auto encoded = hc::canonical_kline(row);
        bytes.insert(bytes.end(), encoded.begin(), encoded.end());
    } else {
        const auto encoded = hc::canonical_row(row);
        bytes.insert(bytes.end(), encoded.begin(), encoded.end());
    }
}

std::vector<hc::Row> parse_rows(const hc::Bytes& bytes, const hc::CatalogEntry& entry) {
    const auto stride = row_bytes(entry.identity);
    require(bytes.size() % stride == 0 && bytes.size() / stride == entry.row_count, "row payload length differs");
    std::vector<hc::Row> result;
    int64_t previous = -1;
    for (size_t offset = 0; offset < bytes.size(); offset += stride) {
        const auto* input = bytes.data() + offset;
        if (stride == 64) {
            hc::Row row;
            const auto timestamp = little(input, 8), volume = little(input + 40, 8);
            const auto open_oi = little(input + 48, 8), close_oi = little(input + 56, 8);
            require(timestamp <= INT64_MAX && volume <= INT64_MAX && open_oi <= INT64_MAX && close_oi <= INT64_MAX,
                    "invalid native integer fields");
            row.timestamp_ms = static_cast<int64_t>(timestamp);
            row.volume = static_cast<int64_t>(volume);
            row.native.emplace();
            for (size_t i = 0; i < 4; ++i) {
                const auto bits = little(input + 8 + i * 8, 8);
                std::memcpy(&row.native->prices[i], &bits, sizeof(bits));
            }
            row.native->open_oi = static_cast<int64_t>(open_oi);
            row.native->close_oi = static_cast<int64_t>(close_oi);
            (void)hc::canonical_native(row);
            require(entry.nullable_prices || !hc::has_null_price(row), "NULL requires explicit price encoding");
            require(row.timestamp_ms > previous && row.timestamp_ms >= entry.coverage.start_ms &&
                    row.timestamp_ms < entry.coverage.end_ms, "unordered or out-of-coverage row");
            previous = row.timestamp_ms;
            result.push_back(row);
            continue;
        }
        const auto timestamp = little(input, 8), volume = little(input + 24, 8);
        require(timestamp <= INT64_MAX && volume <= INT64_MAX, "invalid timestamp or volume");
        float prices[4]{};
        for (size_t i = 0; i < 4; ++i) {
            const auto bits = static_cast<uint32_t>(little(input + 8 + 4 * i, 4));
            std::memcpy(&prices[i], &bits, sizeof(bits));
        }
        hc::Row row{static_cast<int64_t>(timestamp), prices[0], prices[1], prices[2], prices[3],
                    static_cast<int64_t>(volume)};
        if (stride == 48) {
            const auto bits = little(input + 32, 8), interest = little(input + 40, 8);
            require(interest <= INT64_MAX, "invalid open interest");
            std::memcpy(&row.turnover, &bits, sizeof(bits));
            row.open_interest = static_cast<int64_t>(interest);
            (void)hc::canonical_kline(row);
        }
        hc::validate_row(row);
        require(row.timestamp_ms > previous && row.timestamp_ms >= entry.coverage.start_ms &&
                row.timestamp_ms < entry.coverage.end_ms, "unordered or out-of-coverage row");
        previous = row.timestamp_ms;
        result.push_back(row);
    }
    return result;
}

hc::Manifest daily_source_manifest(const Json& source) {
    const bool complete = source.at("identity").at("dataset") == "ddb-history-kline48";
    const bool native = source.at("identity").at("dataset") == "ddb-history-native64";
    if (native) {
        require(source.at("row_encoding") == "le-ddb-native64-v1" && source.at("row_bytes") == 64 &&
                source.at("source").at("field_mapping") == Json{{"open_oi", "open_oi"}, {"close_oi", "close_oi"}},
                "unsupported native field mapping");
    } else if (complete) {
        require(source.at("row_encoding") == "le-kline48-v1" && source.at("row_bytes") == 48 &&
                source.at("source").at("field_mapping") == Json{{"turnover", "amount"}, {"open_interest", "open_oi"}},
                "unsupported complete field mapping");
    } else {
        require(!source.contains("row_encoding") && !source.contains("row_bytes") &&
                !source.at("source").contains("field_mapping"), "legacy source contains complete row contract");
    }
    require(source.at("schema_version") == 1 && source.at("kind") == "ddb-single-chunk-snapshot" &&
            source.at("contract") == "ddb-single-chunk-guard-v1" &&
            source.at("coverage_scope") == "ddb-visible-single-partition-at-native-version" &&
            source.at("coverage_verified") == true && source.at("source_snapshot_proven") == true &&
            source.at("production_eligible") == false && source.at("upstream_finality_proven") == false &&
            source.at("freshness_policy") == "explicit-revalidation-required", "unsupported DDB source proof");
    const auto& before = source.at("guard").at("before");
    require(before == source.at("guard").at("after"), "DDB source changed during export");
    const auto& chunk = before.at("chunk");
    const auto& tablet = before.at("tablet");
    const auto version = integer(chunk.at("version"));
    require(version > 0 && version == integer(tablet.at("version")) &&
            version == integer(source.at("source_version")) && chunk.at("type") == 1 &&
            chunk.at("flag") == 0 && chunk.at("state") == 0 && chunk.at("resolved") == false &&
            integer(tablet.at("rowNum")) > 0, "native chunk is not finalized or versions differ");
    const auto& mapping_source = source.at("source");
    require(chunk.at("chunkId") == tablet.at("chunkId") && chunk.at("dfsPath") == before.at("partition") &&
            tablet.at("dfsPath") == before.at("partition") && chunk.at("site") == before.at("node") &&
            tablet.at("tableName") == mapping_source.at("table"), "native source identity differs");
    const auto path = before.at("partition").get<std::string>();
    const auto database = mapping_source.at("database").get<std::string>();
    require(database.rfind("dfs://", 0) == 0 && path.rfind("/" + database.substr(6) + "/", 0) == 0 &&
            path.find("..") == std::string::npos && path.size() <= 512, "partition outside configured database");
    require(before.at("runtime").get<std::string>().rfind("3.00.5 ", 0) == 0 &&
            before.at("schema").at("engine_type") == "TSDB" &&
            (mapping_source.at("timestamp_offset_ms") == 28800000 ||
             mapping_source.at("timestamp_offset_ms") == 0) &&
            source.at("query_semantics") == (native ? "ddb-none-1m-native64-offset-v1" : complete ? "upcloud-none-1m-kline48-offset-v1" :
                "upcloud-none-1m-double-to-float32-offset-v1"), "unsupported source mapping");
    if (complete || native) {
        std::map<std::string, std::string> types;
        const auto& schema = before.at("schema");
        for (const auto& column : schema.at("columns"))
            require(types.emplace(column.at("name").get<std::string>(),
                                  column.at("typeString").get<std::string>()).second,
                    "duplicate schema column");
        require((native ? types["close_oi"] == "LONG" : types["amount"] == "DOUBLE") && types["open_oi"] == "LONG" && types["volume"] == "LONG" &&
                types[mapping_source.at("time_column").get<std::string>()] == "TIMESTAMP",
                "missing or incompatible complete source fields");
        for (const auto* name : {"open", "high", "low", "close"})
            require(types[name] == "DOUBLE" || (!native && types[name] == "FLOAT"), "invalid price source type");
    }
    Json mapping;
    for (const auto* key : {"host", "port", "database", "table", "time_column", "code_column", "timestamp_offset_ms"})
        mapping[key] = mapping_source.at(key);
    if (complete || native) mapping["field_mapping"] = mapping_source.at("field_mapping");
    const Json origin{{"contract", source.at("contract")}, {"mapping", mapping},
                      {"runtime", before.at("runtime")}, {"node", before.at("node")},
                      {"partition", before.at("partition")}, {"chunk_id", chunk.at("chunkId")},
                      {"physical_table", tablet.at("latestPhysicalDir")}};
    const Json semantics{{"query_semantics", source.at("query_semantics")}, {"mapping", mapping},
                         {"schema", before.at("schema")}};
    const auto epoch = "ddb-chunk-" + hc::hex(hc::sha256(canonical(origin)));
    const auto data_version = hc::sha256(canonical(semantics));
    require(source.at("dataset_epoch") == epoch && source.at("data_version") == hc::hex(data_version) &&
            mapping_source.at("schema_sha256") == hc::hex(hc::sha256(canonical(before.at("schema")))),
            "native proof binding mismatch");
    for (const auto* key : {"config_sha256", "query_sha256"})
        (void)hc::parse_digest(mapping_source.at(key).get<std::string>());
    (void)hc::parse_digest(source.at("raw_rows_sha256").get<std::string>());
    (void)hc::parse_digest(source.at("rows_sha256").get<std::string>());
    hc::CatalogEntry entry;
    if (source.contains("price_null_encoding")) {
        require(native && source.at("price_null_encoding") == "ddb-double-null-v1", "unknown price null encoding");
        entry.nullable_prices = true;
    }
    entry.identity = hc::identity_from_json(source.at("identity"));
    require(entry.identity.dataset == (native ? "ddb-history-native64" : complete ? "ddb-history-kline48" : "ddb-history-snapshot") && entry.identity.period_seconds == 60 &&
            entry.identity.adjust == "none" &&
            (entry.identity.market == "SH" || entry.identity.market == "SZ" ||
             entry.identity.market == "HK" || entry.identity.market == "SHF" ||
             entry.identity.market == "CFE" || entry.identity.market == "CZC" ||
             entry.identity.market == "DCE" || entry.identity.market == "INE" ||
             entry.identity.market == "US"),
            "unsupported market identity");
    const auto dot = entry.identity.symbol.rfind('.');
    if (entry.identity.market == "US") {
        static const std::regex us_symbol("[A-Za-z][A-Za-z0-9.\\-]{0,15}");
        require(std::regex_match(entry.identity.symbol, us_symbol), "invalid US symbol");
    } else {
        require(dot != std::string::npos &&
                entry.identity.symbol.substr(dot + 1) == entry.identity.market,
                "symbol must end with .market");
        const auto prefix = entry.identity.symbol.substr(0, dot);
        const auto is_stock = entry.identity.market == "SH" || entry.identity.market == "SZ" ||
                              entry.identity.market == "HK";
        if (is_stock) {
            require(prefix.size() >= 5 && prefix.size() <= 6 &&
                std::all_of(prefix.begin(), prefix.end(),
                            [](char c) { return c >= '0' && c <= '9'; }),
                "stock symbol prefix must be digits");
        } else {
            static const std::regex derivative("[A-Za-z]{1,3}[0-9]{3,4}((C|P|-C-|-P-)[0-9]{1,8})?");
            require(std::regex_match(prefix, derivative), "invalid derivative symbol prefix");
        }
    }
    entry.data_version = data_version;
    entry.source_version = version;
    entry.coverage = {static_cast<int64_t>(integer(source.at("requested_start_ms"), INT64_MAX - 28800000)),
                      static_cast<int64_t>(integer(source.at("requested_end_ms"), INT64_MAX - 28800000))};
    hc::validate_coverage(entry.coverage);
    require(entry.coverage.end_ms - entry.coverage.start_ms <= 7LL * 86400000, "snapshot exceeds interval limit");
    entry.row_count = integer(source.at("row_count"), kRows);
    const auto limit = integer(source.at("max_rows"), kRows);
    require(limit > 0 && entry.row_count <= limit && source.at("query_row_limit") == limit + 1 &&
            source.at("max_partitions") == 1, "incomplete or oversized source query");
    const auto time_column = mapping_source.at("time_column").get<std::string>();
    const auto symbol_literal = entry.identity.symbol.find('-') == std::string::npos
        ? "`" + entry.identity.symbol : "\"" + entry.identity.symbol + "\"";
    const auto query = "select top " + std::to_string(limit + 1) + " long(" + time_column +
        ") as ddb_timestamp_ms, open, high, low, close, volume" +
        (native ? ", open_oi, close_oi" : complete ? ", amount as turnover, open_oi as open_interest" : "") + " from loadTable(\"" + database +
        "\", \"" + mapping_source.at("table").get<std::string>() + "\") where " +
        mapping_source.at("code_column").get<std::string>() + "=" + symbol_literal +
        " and duration=60 and " + time_column + ">=timestamp(" +
        std::to_string(entry.coverage.start_ms + mapping_source.at("timestamp_offset_ms").get<int64_t>()) +
        ") and " + time_column + "<timestamp(" +
        std::to_string(entry.coverage.end_ms + mapping_source.at("timestamp_offset_ms").get<int64_t>()) +
        ") order by ddb_timestamp_ms asc, open asc, high asc, low asc, close asc, volume asc" +
        (native ? ", open_oi asc, close_oi asc" : complete ? ", turnover asc, open_interest asc" : "");
    require(mapping_source.at("query_sha256") == hc::hex(hc::sha256(query)),
            "symbol, coverage or row limit differs from the original source query");
    return {epoch, {entry}};
}

bool compact_proof(const Json& source) { return source.value("kind", "") == "ddb-monthly-compaction"; }

hc::Manifest source_manifest(const Json& source) {
    if (!compact_proof(source)) return daily_source_manifest(source);
    const auto& sources = source.at("sources");
    require(sources.is_array() && !sources.empty() && sources.size() <= 31,
            "invalid compact proof count");
    const auto& first = sources.front();
    auto manifest = daily_source_manifest(first);
    auto& entry = manifest.entries.front();
    require(entry.identity.dataset == "ddb-history-native64", "compaction requires native64");
    const auto offset = first.at("source").at("timestamp_offset_ms").get<int64_t>();
    const auto first_start = entry.coverage.start_ms;
    auto previous = first_start;
    uint64_t rows = 0;
    auto month = [](int64_t ms) {
        const auto seconds = static_cast<time_t>(ms / 1000);
        std::tm date{};
        require(gmtime_r(&seconds, &date) != nullptr, "invalid compact date");
        return std::make_pair(date.tm_year, date.tm_mon);
    };
    for (const auto& proof : sources) {
        const auto daily = daily_source_manifest(proof);
        const auto& item = daily.entries.front();
        require(daily.dataset_epoch == manifest.dataset_epoch &&
                hc::canonical_identity(item.identity) == hc::canonical_identity(entry.identity) &&
                item.data_version == entry.data_version && item.source_version == entry.source_version &&
                proof.at("guard") == first.at("guard") &&
                item.coverage.start_ms == previous && item.coverage.end_ms - item.coverage.start_ms == 86400000 &&
                (item.coverage.start_ms + offset) % 86400000 == 0 &&
                month(item.coverage.start_ms + offset) == month(first_start + offset) &&
                (!proof.contains("trading_day") || proof.at("trading_day").is_null()) && !proof.contains("window_contract"),
                "compaction requires contiguous physical days at one native guard/version");
        rows += item.row_count;
        entry.nullable_prices = entry.nullable_prices || item.nullable_prices;
        previous = item.coverage.end_ms;
    }
    require(rows <= kMonthRows, "compact rows exceed limit");
    (void)hc::parse_digest(source.at("rows_sha256").get<std::string>());
    const Json expected{{"schema_version", 1}, {"kind", "ddb-monthly-compaction"},
        {"contract", "ddb-native64-daily-proof-bundle-v1"}, {"sources", sources},
        {"identity", first.at("identity")}, {"dataset_epoch", manifest.dataset_epoch},
        {"source_version", entry.source_version}, {"data_version", hc::hex(entry.data_version)},
        {"requested_start_ms", first_start}, {"requested_end_ms", previous}, {"row_count", rows},
        {"start_date", first.at("start_date")}, {"end_date", sources.back().at("end_date")},
        {"row_encoding", "le-ddb-native64-v1"}, {"row_bytes", 64}, {"rows_sha256", source.at("rows_sha256")}};
    require(source == expected, "compact proof summary differs from original daily proofs");
    entry.coverage.end_ms = previous;
    entry.row_count = rows;
    return manifest;
}

void verify_source_rows(const Json& source, const hc::Bytes& rows) {
    require(hc::hex(hc::sha256(rows)) == source.at("rows_sha256"), "pack rows differ from source proof digest");
    if (!compact_proof(source)) return;
    size_t offset = 0;
    for (const auto& proof : source.at("sources")) {
        const auto entry = daily_source_manifest(proof).entries.front();
        const auto length = entry.row_count * 64;
        require(offset <= rows.size() && length <= rows.size() - offset, "compact row slice exceeds payload");
        const hc::Bytes slice(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                              rows.begin() + static_cast<std::ptrdiff_t>(offset + length));
        require(hc::hex(hc::sha256(slice)) == proof.at("rows_sha256"), "compact daily row digest differs");
        (void)parse_rows(slice, entry);
        offset += length;
    }
    require(offset == rows.size(), "compact row slices incomplete");
}

void encode(const Arguments& args) {
    const auto source = read_json(args.get("--source"));
    auto manifest = daily_source_manifest(source);
    auto& entry = manifest.entries.front();
    const auto bytes = hc::read_file(args.get("--rows"), kRows * row_bytes(entry.identity));
    require(hc::hex(hc::sha256(bytes)) == source.at("rows_sha256"), "source row hash mismatch");
    const auto rows = parse_rows(bytes, entry);
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    if (!rows.empty()) {
        const auto path = output / "snapshot.r2b";
        entry.pack = hc::write_pack(path, hc::pack_metadata(entry), [&](uint64_t offset, uint32_t count) {
            return std::vector<hc::Row>(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                                       rows.begin() + static_cast<std::ptrdiff_t>(offset + count));
        });
        hc::verify_pack(path, *entry.pack, hc::pack_metadata(entry));
        fs::rename(path, output / fs::path(entry.pack->key).filename());
    }
    hc::write_new_file(output / "candidate.json", hc::serialize_manifest(manifest));
    hc::write_new_file(output / "source.json", canonical(source));
    hc::write_new_file(output / "rows.bin", bytes);
    std::cout << canonical({{"status", "PASS_DDB_SNAPSHOT_ENCODE"}, {"rows", rows.size()},
                            {"source_version", entry.source_version}, {"rows_sha256", source.at("rows_sha256")},
                            {"coverage_verified", true}, {"production_eligible", false},
                            {"r2_connected", false}, {"current_pointer_written", false}});
}

struct Candidate {
    hc::Manifest manifest;
    std::vector<Json> sources;
    std::map<std::string, hc::Bytes> packs;
    fs::path directory;
};

std::vector<hc::Row> decode_entry_rows(const Candidate& candidate, size_t index) {
    const auto& entry = candidate.manifest.entries.at(index);
    std::vector<hc::Row> result;
    if (!entry.pack) return result;
    const auto found = candidate.packs.find(entry.pack->key);
    require(found != candidate.packs.end(), "candidate pack is unavailable");
    const auto& payload = found->second;
    const hc::RangeReader read = [&payload](uint64_t offset, uint64_t size) {
        require(offset <= payload.size() && size <= payload.size() - offset, "pack range out of bounds");
        return hc::Bytes(payload.begin() + static_cast<std::ptrdiff_t>(offset),
                         payload.begin() + static_cast<std::ptrdiff_t>(offset + size));
    };
    const auto pack = hc::read_pack_index(read, *entry.pack, hc::pack_metadata(entry));
    int64_t previous = -1;
    for (const auto& block : pack.blocks) {
        for (const auto& row : hc::read_pack_block(read, block)) {
            require(row.timestamp_ms > previous && row.timestamp_ms >= entry.coverage.start_ms &&
                    row.timestamp_ms < entry.coverage.end_ms, "unordered or out-of-coverage pack row");
            previous = row.timestamp_ms;
            result.push_back(row);
        }
    }
    require(result.size() == entry.row_count, "pack rows differ from catalog count");
    return result;
}

// One directory holds one manifest and one native source proof per entry. The
// proofs are content-addressed, so the entry they describe is recovered by
// rebuilding it from the proof and matching the catalog.
Candidate load_candidate(const fs::path& directory) {
    Candidate candidate;
    candidate.directory = directory;
    const auto manifest_bytes = hc::read_file(directory / "candidate.json", kBytes);
    candidate.manifest = hc::parse_manifest({manifest_bytes.begin(), manifest_bytes.end()});
    std::vector<Json> proofs;
    for (const auto& item : fs::directory_iterator(directory)) {
        const auto name = item.path().filename().string();
        if (item.path().extension() != ".json" || name == "candidate.json" || name == "shard.json") continue;
        auto source = read_json(item.path());
        if (!source.contains("kind") || (source.at("kind") != "ddb-single-chunk-snapshot" && !compact_proof(source))) continue;
        require(name == "source.json" || hc::hex(hc::sha256(bytes_of(source))) + ".json" == name,
                "source proof file name differs");
        proofs.push_back(std::move(source));
    }
    require(proofs.size() == candidate.manifest.entries.size(),
            "exactly one native source proof per catalog entry is required");
    std::vector<hc::Manifest> expected_proofs;
    for (const auto& proof : proofs) expected_proofs.push_back(source_manifest(proof));
    std::vector<bool> used(proofs.size(), false);
    for (const auto& entry : candidate.manifest.entries) {
        size_t matched = proofs.size();
        for (size_t i = 0; i < proofs.size(); ++i) {
            if (used[i]) continue;
            auto expected = expected_proofs[i];
            // An unrelated nonempty proof cannot borrow an empty entry's null
            // pack (or vice versa) merely to test whether the entries match.
            if (expected.entries.front().row_count != entry.row_count) continue;
            expected.entries.front().pack = entry.pack;
            if (hc::serialize_manifest(expected) == hc::serialize_manifest(hc::Manifest{
                    candidate.manifest.dataset_epoch, {entry}})) {
                matched = i;
                break;
            }
        }
        require(matched != proofs.size(), "catalog entry has no matching source proof");
        used[matched] = true;
        candidate.sources.push_back(proofs[matched]);
    }
    for (size_t i = 0; i < candidate.manifest.entries.size(); ++i) {
        const auto& entry = candidate.manifest.entries.at(i);
        if (!entry.pack) continue;
        const auto path = directory / fs::path(entry.pack->key).filename();
        auto pack = hc::read_file(path, compact_proof(candidate.sources[i]) ? kMonthBytes : kBytes);
        hc::verify_pack(path, *entry.pack, hc::pack_metadata(entry));
        candidate.packs.emplace(entry.pack->key, std::move(pack));
    }
    const auto rows_path = directory / "rows.bin";
    for (size_t i = 0; i < candidate.manifest.entries.size(); ++i) {
        hc::Bytes decoded;
        for (const auto& row : decode_entry_rows(candidate, i))
            append_row(decoded, row, candidate.manifest.entries[i].identity);
        verify_source_rows(candidate.sources[i], decoded);
    }
    if (candidate.manifest.entries.size() == 1 && fs::is_regular_file(rows_path)) {
        const auto& entry = candidate.manifest.entries.front();
        const auto rows = hc::read_file(rows_path,
            (compact_proof(candidate.sources.front()) ? kMonthRows : kRows) * row_bytes(entry.identity));
        require(candidate.sources.front().at("rows_sha256") == hc::hex(hc::sha256(rows)),
                "candidate row hash differs");
        (void)parse_rows(rows, entry);
        hc::Bytes decoded;
        for (const auto& row : decode_entry_rows(candidate, 0)) {
            append_row(decoded, row, entry.identity);
        }
        require(decoded == rows, "pack rows differ from exported source");
    }
    return candidate;
}

void compact_rows(const Json& sources, const hc::Bytes& bytes, const fs::path& output) {
    require(sources.is_array() && !sources.empty() && sources.size() <= 31, "invalid month entry count");
    const auto& first = sources.front();
    Json proof{{"schema_version", 1}, {"kind", "ddb-monthly-compaction"},
        {"contract", "ddb-native64-daily-proof-bundle-v1"}, {"sources", sources},
        {"identity", first.at("identity")}, {"dataset_epoch", first.at("dataset_epoch")},
        {"source_version", first.at("source_version")}, {"data_version", first.at("data_version")},
        {"requested_start_ms", first.at("requested_start_ms")},
        {"requested_end_ms", sources.back().at("requested_end_ms")}, {"row_count", bytes.size() / 64},
        {"start_date", first.at("start_date")}, {"end_date", sources.back().at("end_date")},
        {"row_encoding", "le-ddb-native64-v1"}, {"row_bytes", 64}, {"rows_sha256", hc::hex(hc::sha256(bytes))}};
    require(canonical(proof).size() <= kBytes, "compact proof exceeds metadata budget");
    auto manifest = source_manifest(proof);
    verify_source_rows(proof, bytes);
    auto& entry = manifest.entries.front();
    const auto rows = parse_rows(bytes, entry);
    hc::create_new_directory(output);
    if (!rows.empty()) {
        const auto path = output / "snapshot.r2b";
        entry.pack = hc::write_pack(path, hc::pack_metadata(entry), [&](uint64_t start, uint32_t count) {
            return std::vector<hc::Row>(rows.begin() + static_cast<std::ptrdiff_t>(start),
                                      rows.begin() + static_cast<std::ptrdiff_t>(start + count));
        });
        require(entry.pack->bytes <= kMonthBytes, "compact pack exceeds transfer budget");
        fs::rename(path, output / fs::path(entry.pack->key).filename());
    }
    hc::write_new_file(output / "candidate.json", hc::serialize_manifest(manifest));
    hc::write_new_file(output / "source.json", canonical(proof));
    (void)load_candidate(output);
    std::cout << canonical({{"status", "PASS_MONTH_COMPACTION"}, {"days", sources.size()},
        {"rows", rows.size()}, {"data_objects", rows.empty() ? 0 : 1}, {"proof_objects", 1},
        {"rows_sha256", proof.at("rows_sha256")}, {"ddb_queries", 0}, {"r2_requests", 0}});
}

void compact(const Arguments& args) {
    const auto input = load_candidate(args.get("--candidate"));
    require(input.sources.size() <= 31 && !input.sources.empty(), "invalid month entry count");
    Json sources = Json::array();
    hc::Bytes bytes;
    for (size_t i = 0; i < input.sources.size(); ++i) {
        require(!compact_proof(input.sources[i]), "candidate is already compacted");
        sources.push_back(input.sources[i]);
        for (const auto& row : decode_entry_rows(input, i)) append_row(bytes, row, input.manifest.entries[i].identity);
    }
    compact_rows(sources, bytes, args.get("--output"));
}

void encode_month(const Arguments& args) {
    const auto sources = read_json(args.get("--sources"));
    const auto bytes = hc::read_file(args.get("--rows"), kMonthRows * 64);
    // Same per-day guards, row digests and canonical pack as compact(); no daily
    // intermediate packs or relaxed durability on the retained monthly output.
    compact_rows(sources, bytes, args.get("--output"));
}

hc::Pointer epoch_replacement(const Candidate& before, const Candidate& after, uint64_t seq) {
    for (const auto* candidate : {&before, &after})
        for (const auto& proof : candidate->sources)
            require(!compact_proof(proof), "compact epoch migration requires separate coverage review");
    hc::validate_epoch_replacement(before.manifest, after.manifest);
    auto lineage = [](const Json& proof) {
        auto mapping = proof.at("source");
        mapping.erase("query_sha256");
        const auto& guard = proof.at("guard").at("before");
        return Json{{"mapping", mapping}, {"runtime", guard.at("runtime")}, {"node", guard.at("node")},
            {"partition", guard.at("partition")}, {"chunk_id", guard.at("chunk").at("chunkId")},
            {"contract", proof.at("contract")}, {"query_semantics", proof.at("query_semantics")}};
    };
    const auto origin = lineage(before.sources.front());
    const auto old_physical = before.sources.front().at("guard").at("before").at("tablet").at("latestPhysicalDir");
    for (const auto* candidate : {&before, &after}) {
        for (const auto& source : candidate->sources) {
            require(source.at("identity").at("dataset") == "ddb-history-native64" && lineage(source) == origin,
                    "epoch migration requires the same native64 source lineage");
            if (candidate == &after)
                require(source.at("guard").at("before").at("tablet").at("latestPhysicalDir") != old_physical,
                        "epoch migration did not replace physical table");
        }
    }
    for (size_t i = 0; i < before.manifest.entries.size(); ++i) {
        const auto start = before.manifest.entries[i].coverage.start_ms;
        const auto found = std::find_if(after.manifest.entries.begin(), after.manifest.entries.end(),
            [&](const auto& entry) { return entry.coverage.start_ms == start; });
        require(found != after.manifest.entries.end(), "epoch migration lost interval");
        const auto old_rows = decode_entry_rows(before, i);
        const auto new_rows = decode_entry_rows(after, static_cast<size_t>(found - after.manifest.entries.begin()));
        size_t next = 0;
        for (const auto& row : old_rows) {
            while (next < new_rows.size() && new_rows[next].timestamp_ms < row.timestamp_ms) ++next;
            require(next < new_rows.size() && new_rows[next].timestamp_ms == row.timestamp_ms,
                    "epoch migration lost a published timestamp");
        }
    }
    const auto hash = hc::sha256(hc::serialize_manifest(before.manifest));
    hc::Pointer pointer{before.manifest.dataset_epoch, seq, "manifests/v1/" + hc::hex(hash) + ".json", hash};
    (void)hc::serialize_pointer(pointer);
    return pointer;
}

std::optional<hc::Pointer> migration_from(const Arguments& args, const Candidate& candidate, uint64_t seq) {
    const auto path = args.optional("--epoch-base", "");
    if (path.empty()) return std::nullopt;
    return epoch_replacement(load_candidate(path), candidate, seq);
}

hc::Query query_from(const Candidate& candidate, const Arguments& args) {
    const auto& entries = candidate.manifest.entries;
    const auto first = std::min_element(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
        return a.coverage.start_ms < b.coverage.start_ms; });
    const auto last = std::max_element(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
        return a.coverage.end_ms < b.coverage.end_ms; });
    return {first->identity, first->data_version,
            {static_cast<int64_t>(number(args.optional("--start", std::to_string(first->coverage.start_ms)), INT64_MAX)),
             static_cast<int64_t>(number(args.optional("--end", std::to_string(last->coverage.end_ms)), INT64_MAX))},
            number(args.optional("--max-count", std::to_string(kRows)), kRows)};
}

Json compare_query(const hc::ObjectReader& store, std::shared_ptr<const hc::Snapshot> snapshot,
                   const Candidate& candidate, const hc::Query& query, const fs::path& output) {
    const auto plan = hc::plan_query(snapshot, query, true, true);
    require(plan.result != hc::PlanResult::error, "invalid snapshot query");
    Json result{{"result", hc::plan_result_name(plan.result)}, {"reason", plan.reason},
                {"publication_seq", snapshot->pointer.publication_seq}, {"production_eligible", false},
                {"freshness", "pinned-snapshot-only"}, {"ddb_connected", false}};
    if (plan.result != hc::PlanResult::hit) return result;
    hc::Bytes rows;
    const auto summary = hc::read_plan(store, plan, [&](const std::vector<hc::Row>& values) {
        for (const auto& row : values) {
            append_row(rows, row, query.identity);
        }
    });
    hc::Bytes expected;
    std::vector<size_t> order(candidate.manifest.entries.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return candidate.manifest.entries[a].coverage.start_ms < candidate.manifest.entries[b].coverage.start_ms; });
    for (size_t index : order) {
        for (const auto& row : decode_entry_rows(candidate, index)) {
            if (row.timestamp_ms < query.range.start_ms || row.timestamp_ms >= query.range.end_ms) continue;
            if (expected.size() / row_bytes(query.identity) == query.max_count) break;
            append_row(expected, row, query.identity);
        }
        if (expected.size() / row_bytes(query.identity) == query.max_count) break;
    }
    require(rows == expected, "reader rows differ from pinned DDB source");
    hc::write_new_file(output / "rows.bin", rows);
    result.update({{"rows", summary.rows}, {"first_ms", summary.first_ms}, {"last_ms", summary.last_ms},
                   {"rows_sha256", hc::hex(summary.rows_sha256)}, {"source_rows_exact", true}});
    return result;
}

hc::S3Config config_from(const Arguments& args) {
    const auto path = args.optional("--storage-config", "");
    if (!path.empty()) {
        auto config = hc::load_storage_profile(path, "publisher");
        require(config.account_id == args.get("--account") && config.bucket == args.get("--bucket"),
                "storage profile conflicts with command scope");
        config.key_prefix += args.get("--run-id") + '/';
        return config;
    }
    return {args.get("--account"), args.get("--bucket"), "r2-history-staging/" + args.get("--run-id") + '/', "default"};
}

hc::TransferLimits limits(const Candidate* candidate = nullptr) {
    hc::TransferLimits result;
    result.max_requests = kRequests;
    result.max_upload_bytes = 2 * kBytes;
    result.max_download_bytes = 8 * kBytes;
    if (candidate && (candidate->manifest.entries.size() > 1 || compact_proof(candidate->sources.front()))) {
        require(candidate->manifest.entries.size() <= 64, "snapshot transfer entry budget exceeded");
        uint64_t bytes = hc::serialize_manifest(candidate->manifest).size();
        for (const auto& [key, pack] : candidate->packs) bytes += pack.size();
        for (const auto& source : candidate->sources) bytes += canonical(source).size();
        require(bytes <= 14 * kBytes, "snapshot transfer byte budget exceeded");
        // Include immutable GET/create/readback plus bounded publication and
        // reader checks. These limits belong to the data-side publisher only.
        result.max_requests = std::max(kRequests, 32 + 10 * candidate->manifest.entries.size());
        result.max_upload_bytes = std::max(2 * kBytes, bytes + 2 * kBytes);
        result.max_download_bytes = std::max(8 * kBytes, bytes * 4 + 8 * kBytes);
    }
    result.deadline = hc::SteadyClock::now() + std::chrono::seconds(120);
    return result;
}

Json transfer_plan(const Candidate& candidate, const hc::S3Config& config, uint64_t expected_seq = 0) {
    const bool complete_namespace = config.key_prefix.find("-kline48-") != std::string::npos;
    const bool native_namespace = config.key_prefix.find("-native64-") != std::string::npos;
    for (const auto& entry : candidate.manifest.entries)
        require((row_bytes(entry.identity) == 48) == complete_namespace && (row_bytes(entry.identity) == 64) == native_namespace,
                "complete rows require an isolated -kline48- namespace; legacy rows cannot publish there");
    require(config.environment != "production" || native_namespace, "production publication requires native64");
    auto transport = std::make_shared<hc::CurlHttpTransport>();
    auto dummy = std::make_shared<hc::S3Credentials>("snapshotOfflineId", "snapshotOfflineSecret");
    const auto budget = limits(&candidate);
    hc::S3Store validate(config, transport, dummy, budget);
    const auto manifest = hc::serialize_manifest(candidate.manifest);
    const hc::Pointer target{candidate.manifest.dataset_epoch, expected_seq + 1,
                            "manifests/v1/" + hc::hex(hc::sha256(manifest)) + ".json", hc::sha256(manifest)};
    Json objects = Json::array();
    uint64_t rows = 0;
    for (const auto& entry : candidate.manifest.entries) {
        rows += entry.row_count;
        if (entry.pack)
            objects.push_back({{"key", config.key_prefix + entry.pack->key}, {"bytes", entry.pack->bytes}});
    }
    for (const auto& source : candidate.sources) {
        const auto proof = bytes_of(source);
        objects.push_back({{"key", config.key_prefix + "manifests/v1/" + hc::hex(hc::sha256(proof)) + ".json"},
                           {"bytes", proof.size()}});
    }
    objects.push_back({{"key", config.key_prefix + target.manifest_key}, {"bytes", manifest.size()}});
    objects.push_back({{"key", config.key_prefix + "current.json"}, {"bytes", hc::serialize_pointer(target).size()}});
    return {{"status", "DDB_SNAPSHOT_STAGING_PLAN"}, {"bucket", config.bucket}, {"prefix", config.key_prefix},
            {"scope_id", hc::hex(validate.scope_id())}, {"rows", rows},
            {"entries", candidate.manifest.entries.size()}, {"objects", objects}, {"max_requests", budget.max_requests},
            {"max_upload_bytes", budget.max_upload_bytes}, {"max_download_bytes", budget.max_download_bytes}, {"max_seconds", 120},
            {"expected_seq", expected_seq}, {"target", Json::parse(hc::serialize_pointer(target))},
            {"production_eligible", false}, {"storage_environment", config.environment},
            {"freshness", "pinned-snapshot-only"}, {"delete_requests", 0}};
}

void local(const Arguments& args) {
    const auto candidate = load_candidate(args.get("--candidate"));
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    hc::LocalStore::initialize(output / "store");
    hc::LocalStore store(output / "store");
    for (const auto& [key, bytes] : candidate.packs) store.put(key, bytes);
    (void)store.publish(candidate.manifest, 0);
    auto snapshot = std::make_shared<const hc::Snapshot>(store.load_snapshot());
    auto report = compare_query(store, snapshot, candidate, query_from(candidate, args), output);
    report["r2_connected"] = false;
    hc::write_new_file(output / "report.json", canonical(report));
    std::cout << canonical(report);
}

// Probe the staging namespace and print its current pointer without writing.
bool current(const Arguments& args, const hc::S3Config& config,
             std::shared_ptr<hc::HttpTransport> shared = {}) {
#ifndef HC_HAS_CURL
    (void)args;
    (void)config;
    (void)shared;
    throw hc::Error(hc::ErrorCode::invalid, "snapshot network commands require a curl-enabled build");
#else
    require(args.get("--retain") == "yes", "explicit retained staging scope required");
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    const char* id = std::getenv(config.environment == "production" ? "R2_PRODUCTION_ACCESS_KEY_ID" : "R2_STAGING_ACCESS_KEY_ID");
    const char* secret = std::getenv(config.environment == "production" ? "R2_PRODUCTION_SECRET_ACCESS_KEY" : "R2_STAGING_SECRET_ACCESS_KEY");
    require(id && *id && secret && *secret, "authorized R2 credentials are unavailable");
    auto transport = std::make_shared<hc::sample::SnapshotDiagnostics>(
        shared ? shared : std::make_shared<hc::CurlHttpTransport>(true, hc::CurlHttpTransport::Reuse::publication));
    auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
    hc::S3Store store(config, transport, credentials, limits());
    Json result{{"status", "PASS_DDB_SNAPSHOT_STAGING_CURRENT"}, {"bucket", config.bucket},
                {"prefix", config.key_prefix}, {"scope_id", hc::hex(store.scope_id())},
                {"production_eligible", false}, {"current_pointer_written", false},
                {"delete_requests", 0}, {"storage_environment", config.environment}};
    bool success = false;
    try {
        const auto probe = store.read_current();
        if (!probe) {
            result.update({{"present", false}, {"publication_seq", 0}});
        } else {
            const auto pointer = hc::parse_pointer(std::string(probe->bytes.begin(), probe->bytes.end()));
            const auto etag = hc::hex(hc::sha256(probe->etag));
            result.update({{"present", true}, {"publication_seq", pointer.publication_seq},
                           {"dataset_epoch", pointer.dataset_epoch}, {"manifest_key", pointer.manifest_key},
                           {"manifest_sha256", hc::hex(pointer.manifest_sha256)}, {"etag_sha256", etag}});
        }
        success = true;
    } catch (const hc::Error& error) {
        result["error_code"] = hc::sample::error_name(error.code());
    } catch (const std::exception&) {
        result["error_code"] = "unexpected";
    }
    if (!success) {
        result["status"] = "FAIL_DDB_SNAPSHOT_STAGING_CURRENT";
        result["failure_stage"] = transport->stage;
    }
    const auto usage = store.usage();
    result.update({{"requests", usage.requests}, {"download_reserved_bytes", usage.download_reserved}});
    result["http_summary"] = transport->summary();
    hc::write_new_file(output / "report.json", canonical(result));
    std::cout << canonical(result);
    return success;
#endif
}

bool network(const Arguments& args, bool publish, std::shared_ptr<hc::HttpTransport> shared = {}) {
    const auto reuse = args.optional("--reuse-verified-objects", "no");
    require(reuse == "no" || (publish && reuse == "yes"), "invalid publication read reuse option");
    const auto candidate = load_candidate(args.get("--candidate"));
    const auto config = config_from(args);
    const auto expected_seq = number(args.optional("--expected-seq", "0"), UINT64_MAX);
    const auto epoch_base = migration_from(args, candidate, expected_seq);
    auto plan = transfer_plan(candidate, config, expected_seq);
    if (epoch_base) plan["epoch_base"] = Json::parse(hc::serialize_pointer(*epoch_base));
#ifndef HC_HAS_CURL
    (void)publish;
    (void)plan;
    (void)shared;
    throw hc::Error(hc::ErrorCode::invalid, "snapshot network commands require a curl-enabled build");
#else
    require(args.get("--retain") == "yes", "explicit retained staging scope required");
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    hc::write_new_file(output / "plan.json", canonical(plan));
    Json report{{"status", "FAIL_DDB_SNAPSHOT_R2"}, {"plan", plan}, {"production_eligible", false},
                {"ddb_connected", false}, {"r2_connected", false}, {"current_pointer_written", false},
                {"delete_requests", 0}, {"automatic_retry", false}, {"objects", Json::array()}};
    const char* id = std::getenv(config.environment == "production" ? "R2_PRODUCTION_ACCESS_KEY_ID" : "R2_STAGING_ACCESS_KEY_ID");
    const char* secret = std::getenv(config.environment == "production" ? "R2_PRODUCTION_SECRET_ACCESS_KEY" : "R2_STAGING_SECRET_ACCESS_KEY");
    require(id && *id && secret && *secret, "authorized R2 credentials are unavailable");
    auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
    auto transport = std::make_shared<hc::sample::SnapshotDiagnostics>(
        shared ? shared : std::make_shared<hc::CurlHttpTransport>(true, hc::CurlHttpTransport::Reuse::publication));
    hc::S3Store store(config, transport, credentials, limits(&candidate));
    hc::sample::VerifiedObjectStore verified(store, 16 * kBytes);
    hc::ObjectStore& publication = reuse == "yes" ? static_cast<hc::ObjectStore&>(verified) : store;
    bool success = false;
    try {
        if (publish) {
            const auto resume = args.optional("--resume-journal", "");
            const fs::path journal_path = resume.empty() ? output / "journal" : fs::path(resume);
            transport->stage = resume.empty() ? "journal_prepare" : "journal_open";
            if (resume.empty())
                hc::JournaledPublisher::prepare(journal_path, store.scope_id(), candidate.manifest, expected_seq, {}, epoch_base);
            hc::JournaledPublisher journal(journal_path, publication);
            require(bool(journal.epoch_base()) == bool(epoch_base) && (!epoch_base ||
                    hc::serialize_pointer(*journal.epoch_base()) == hc::serialize_pointer(*epoch_base)),
                    "resume journal epoch authorization differs");
            require(Json::parse(hc::serialize_pointer(journal.target())) == plan.at("target"),
                    "resume journal target differs from candidate or expected sequence");
            report["resumed_existing_journal"] = !resume.empty();
            std::vector<std::pair<std::string, hc::Bytes>> objects;
            for (const auto& [key, bytes] : candidate.packs) objects.emplace_back(key, bytes);
            for (const auto& source : candidate.sources) {
                auto proof = bytes_of(source);
                objects.emplace_back("manifests/v1/" + hc::hex(hc::sha256(proof)) + ".json", std::move(proof));
            }
            transport->stage = "immutable_objects";
            for (const auto& [key, bytes] : objects) {
                const auto receipt = hc::sample::ensure_object(publication, key, bytes);
                report["objects"].push_back({{"key", config.key_prefix + key}, {"bytes", bytes.size()},
                    {"sha256", hc::hex(hc::sha256(bytes))}, {"already_present", receipt.already_present},
                    {"create_ack", receipt.write == hc::WriteOutcome::applied}, {"readback_verified", true}});
                hc::write_new_file(output / fs::path(key).filename(), receipt.readback);
            }
            transport->stage = "journal_resume";
            const auto result = journal.resume();
            report["publish_outcome"] = result.outcome == hc::PublishOutcome::committed ? "committed" :
                result.outcome == hc::PublishOutcome::conflict ? "conflict" :
                result.outcome == hc::PublishOutcome::not_applied ? "not_applied" : "indeterminate";
            report["current_pointer_written"] = result.outcome == hc::PublishOutcome::committed;
            if (result.outcome != hc::PublishOutcome::committed)
                throw hc::Error(result.outcome == hc::PublishOutcome::conflict ? hc::ErrorCode::conflict :
                                hc::ErrorCode::io, "publication not resolved; retain journal for recovery");
        }
        transport->stage = "snapshot_readback";
        auto snapshot = std::make_shared<const hc::Snapshot>(hc::load_snapshot(publication, 1));
        require(snapshot->pointer.publication_seq == expected_seq + 1 &&
                hc::serialize_manifest(snapshot->manifest) == hc::serialize_manifest(candidate.manifest),
                "remote snapshot differs from pinned candidate");
        hc::write_new_file(output / "current.json", hc::serialize_pointer(snapshot->pointer));
        hc::write_new_file(output / "candidate.json", hc::serialize_manifest(snapshot->manifest));
        transport->stage = "query_readback";
        report["query"] = compare_query(publication, snapshot, candidate, query_from(candidate, args), output);
        report["status"] = publish ? "PASS_DDB_SNAPSHOT_STAGING_PUBLICATION" : "PASS_DDB_SNAPSHOT_STAGING_READ";
        success = true;
    } catch (const hc::Error& error) {
        report["error_code"] = hc::sample::error_name(error.code());
    } catch (const std::exception&) {
        report["error_code"] = "unexpected";
    }
    if (!success) report["failure_stage"] = transport->stage;
    report["http_summary"] = transport->summary();
    report["verified_object_reuse"] = {{"enabled", reuse == "yes"}, {"hits", verified.hits()},
        {"bytes", verified.bytes()}, {"lifetime", "single-publication-invocation"}, {"current_cached", false}};
    report["r2_connected"] = report["http_summary"].at("total").get<uint64_t>() != 0;
    const auto usage = store.usage();
    report["requests"] = usage.requests;
    report["upload_reserved_bytes"] = usage.upload_reserved;
    report["download_reserved_bytes"] = usage.download_reserved;
    hc::write_new_file(output / "report.json", canonical(report));
    std::cout << canonical(report);
    return success;
#endif
}

// One bounded pipe worker owns its connections, never cached pointers or journals.
int pipe_worker(const Arguments& options) {
    const auto maximum = number(options.get("--max-commands"), 4096);
    require(maximum > 0, "positive worker command bound required");
    const auto mode = options.get("--mode");
    require(mode == "encoder" || mode == "publisher", "invalid worker mode");
    auto transport = std::make_shared<hc::CurlHttpTransport>(true, hc::CurlHttpTransport::Reuse::publication);
    std::string bound_scope;
    std::cout << canonical({{"status", "SNAPSHOT_WORKER_READY"}, {"mode", mode}}) << std::flush;
    for (uint64_t count = 0; count < maximum; ++count) {
        std::string line;
        char ch;
        while (std::cin.get(ch) && ch != '\n') {
            require(line.size() < 16384, "worker command exceeds byte limit");
            line.push_back(ch);
        }
        if (line.empty() && !std::cin) return 0;
        std::ostringstream output;
        auto* previous = std::cout.rdbuf(output.rdbuf());
        bool success = false;
        try {
            const auto request = Json::parse(line);
            require(request.is_array() && request.size() >= 1 && request.size() <= 40, "invalid worker request");
            std::vector<std::string> words{"history-cache-snapshot"};
            for (const auto& item : request) {
                require(item.is_string(), "worker arguments must be strings");
                words.push_back(item.get<std::string>());
                require(words.back().find('\0') == std::string::npos, "NUL worker argument");
            }
            std::vector<char*> argv;
            for (auto& word : words) argv.push_back(word.data());
            const int argc = static_cast<int>(argv.size());
            const auto& command = words[1];
            if (mode == "encoder") {
                require(command == "encode-month", "encoder worker only accepts encode-month");
                encode_month(Arguments(argc, argv.data(), {"--sources", "--rows", "--output"}));
                success = true;
            } else {
                require(command == "current" || command == "publish", "publisher worker command not allowed");
                const Arguments args(argc, argv.data(), {"--candidate", "--account", "--bucket", "--run-id",
                    "--retain", "--output", "--expected-seq", "--resume-journal", "--storage-config",
                    "--reuse-verified-objects", "--epoch-base"});
                const auto config = config_from(args);
                const auto scope = config.account_id + "/" + config.bucket + "/" + config.environment + "/" + config.jurisdiction;
                require(bound_scope.empty() || bound_scope == scope, "worker storage scope changed");
                bound_scope = scope;
                success = command == "current" ? current(args, config, transport) : network(args, true, transport);
            }
            std::cout.rdbuf(previous);
            const auto result = Json::parse(output.str());
            std::cout << canonical({{"ok", success}, {"result", result}}) << std::flush;
        } catch (const std::exception&) {
            std::cout.rdbuf(previous);
            // Detailed network failures remain in the per-command report, not in credentials or arguments.
            std::cout << canonical({{"ok", false}, {"result", {{"status", "ERROR_SNAPSHOT_WORKER"}}}}) << std::flush;
        }
    }
    return 0;
}

void usage() {
    std::cout << "history-cache-snapshot: bounded native-version DDB snapshots, staging by default\n"
              << "  capabilities (offline build/runtime check; no network requests)\n"
              << "  worker --mode encoder|publisher --max-commands N (bounded JSON-lines stdin/stdout)\n"
              << "  explicit production scope: --storage-config PROFILE on current/plan/publish/read\n"
              << "  encode --source SOURCE_JSON --rows ROWS_BIN --output NEW_DIRECTORY\n"
              << "  encode-month --sources DAILY_PROOFS_JSON --rows NATIVE_ROWS_BIN --output NEW_DIRECTORY\n"
              << "  local --candidate DIR --output NEW_DIRECTORY [--start MS --end MS --max-count N]\n"
              << "  compact --candidate DAILY_MONTH_DIR --output NEW_DIRECTORY\n"
              << "  current --account ID --bucket STAGING --run-id ID --retain yes --output NEW_DIRECTORY\n"
              << "  plan --candidate DIR --account ID --bucket STAGING --run-id ID [--expected-seq N]\n"
              << "  publish --candidate DIR --account ID --bucket STAGING --run-id ID --retain yes"
                 " --output NEW_DIRECTORY [--expected-seq N] [--resume-journal EXISTING_DIRECTORY]"
                 " [--reuse-verified-objects yes] [--epoch-base VERIFIED_OLD_CANDIDATE]\n"
              << "  read --candidate DIR --account ID --bucket STAGING --run-id ID --retain yes --output NEW_DIRECTORY"
                 " [--start MS --end MS --max-count N] [--expected-seq N]\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2 || std::string(argv[1]) == "--help") { usage(); return argc < 2 ? 2 : 0; }
        const std::string command = argv[1];
        if (command == "capabilities") {
            require(argc == 2, "capabilities takes no arguments");
            bool ready = false;
#ifdef HC_HAS_CURL
            const auto* info = curl_version_info(CURLVERSION_NOW);
            ready = info && info->version_num == LIBCURL_VERSION_NUM &&
                    (info->features & CURL_VERSION_SSL) && (info->features & CURL_VERSION_ASYNCHDNS);
#endif
            std::cout << canonical({{"status", "SNAPSHOT_CAPABILITIES"}, {"https_runtime_ready", ready},
                                    {"network_requests", 0}});
        } else if (command == "worker") {
            return pipe_worker(Arguments(argc, argv, {"--mode", "--max-commands"}));
        } else if (command == "encode") {
            encode(Arguments(argc, argv, {"--source", "--rows", "--output"}));
        } else if (command == "encode-month") {
            encode_month(Arguments(argc, argv, {"--sources", "--rows", "--output"}));
        } else if (command == "local") {
            local(Arguments(argc, argv, {"--candidate", "--output", "--start", "--end", "--max-count"}));
        } else if (command == "compact") {
            compact(Arguments(argc, argv, {"--candidate", "--output"}));
        } else if (command == "current") {
            const Arguments args(argc, argv, {"--account", "--bucket", "--run-id", "--retain", "--output", "--storage-config"});
            return current(args, config_from(args)) ? 0 : 2;
        } else if (command == "plan") {
            const Arguments args(argc, argv, {"--candidate", "--account", "--bucket", "--run-id", "--expected-seq", "--storage-config", "--epoch-base"});
            const auto candidate = load_candidate(args.get("--candidate"));
            const auto seq = number(args.optional("--expected-seq", "0"), UINT64_MAX);
            auto plan = transfer_plan(candidate, config_from(args), seq);
            if (const auto base = migration_from(args, candidate, seq))
                plan["epoch_base"] = Json::parse(hc::serialize_pointer(*base));
            std::cout << canonical(plan);
        } else if (command == "publish") {
            return network(Arguments(argc, argv, {"--candidate", "--account", "--bucket", "--run-id", "--retain",
                                                  "--output", "--expected-seq", "--resume-journal", "--storage-config",
                                                  "--reuse-verified-objects", "--epoch-base"}), true) ? 0 : 2;
        } else if (command == "read") {
            return network(Arguments(argc, argv, {"--candidate", "--account", "--bucket", "--run-id", "--retain",
                                                  "--output", "--start", "--end", "--max-count", "--expected-seq", "--storage-config"}),
                           false) ? 0 : 2;
        } else {
            throw hc::Error(hc::ErrorCode::invalid, "unknown snapshot command");
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << canonical({{"status", "ERROR_DDB_SNAPSHOT"}, {"error", error.what()}});
        return 2;
    }
}
