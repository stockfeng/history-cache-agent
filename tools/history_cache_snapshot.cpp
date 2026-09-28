#include "history_cache/journal.h"
#include "history_cache/local_store.h"
#include "history_cache/reader.h"
#include "history_cache/s3_store.h"
#include "sample_transfer.h"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <optional>
#include <set>

namespace hc = history_cache;
namespace fs = std::filesystem;
using Json = nlohmann::json;

namespace {

constexpr uint64_t kRows = 5000;
constexpr uint64_t kBytes = 1024 * 1024;
constexpr uint64_t kRequests = 48;

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
        require(depth <= 12 && ++events <= 10000, "source JSON exceeds parsing budget");
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

std::vector<hc::Row> parse_rows(const hc::Bytes& bytes, const hc::CatalogEntry& entry) {
    require(bytes.size() % 32 == 0 && bytes.size() / 32 == entry.row_count, "row payload length differs");
    std::vector<hc::Row> result;
    int64_t previous = -1;
    for (size_t offset = 0; offset < bytes.size(); offset += 32) {
        const auto* input = bytes.data() + offset;
        const auto timestamp = little(input, 8), volume = little(input + 24, 8);
        require(timestamp <= INT64_MAX && volume <= INT64_MAX, "invalid timestamp or volume");
        float prices[4]{};
        for (size_t i = 0; i < 4; ++i) {
            const auto bits = static_cast<uint32_t>(little(input + 8 + 4 * i, 4));
            std::memcpy(&prices[i], &bits, sizeof(bits));
        }
        hc::Row row{static_cast<int64_t>(timestamp), prices[0], prices[1], prices[2], prices[3],
                    static_cast<int64_t>(volume)};
        hc::validate_row(row);
        require(row.timestamp_ms > previous && row.timestamp_ms >= entry.coverage.start_ms &&
                row.timestamp_ms < entry.coverage.end_ms, "unordered or out-of-coverage row");
        previous = row.timestamp_ms;
        result.push_back(row);
    }
    return result;
}

hc::Manifest source_manifest(const Json& source) {
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
            source.at("query_semantics") == "upcloud-none-1m-double-to-float32-offset-v1", "unsupported source mapping");
    Json mapping;
    for (const auto* key : {"host", "port", "database", "table", "time_column", "code_column", "timestamp_offset_ms"})
        mapping[key] = mapping_source.at(key);
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
    entry.identity = hc::identity_from_json(source.at("identity"));
    require(entry.identity.dataset == "ddb-history-snapshot" && entry.identity.period_seconds == 60 &&
            entry.identity.adjust == "none" &&
            (entry.identity.market == "SH" || entry.identity.market == "SZ" ||
             entry.identity.market == "HK" || entry.identity.market == "SHF" ||
             entry.identity.market == "CFE" || entry.identity.market == "CZC" ||
             entry.identity.market == "DCE" || entry.identity.market == "INE" ||
             entry.identity.market == "US"),
            "unsupported market identity");
    const auto dot = entry.identity.symbol.rfind('.');
    if (entry.identity.market == "US") {
        require(dot == std::string::npos &&
                std::all_of(entry.identity.symbol.begin(), entry.identity.symbol.end(),
                            [](char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                                        (c >= '0' && c <= '9'); }),
                "US symbol must be bare alphanumeric");
    } else {
        require(dot != std::string::npos && dot >= 4 && dot <= 8 &&
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
            require(std::all_of(prefix.begin(), prefix.end(),
                                [](char c) { return (c >= '0' && c <= '9') ||
                                             (c >= 'a' && c <= 'z') ||
                                             (c >= 'A' && c <= 'Z'); }),
                    "futures symbol prefix must be alphanumeric");
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
    const auto query = "select top " + std::to_string(limit + 1) + " long(" + time_column +
        ") as ddb_timestamp_ms, open, high, low, close, volume from loadTable(\"" + database +
        "\", \"" + mapping_source.at("table").get<std::string>() + "\") where " +
        mapping_source.at("code_column").get<std::string>() + "=`" + entry.identity.symbol +
        " and duration=60 and " + time_column + ">=timestamp(" +
        std::to_string(entry.coverage.start_ms + mapping_source.at("timestamp_offset_ms").get<int64_t>()) +
        ") and " + time_column + "<timestamp(" +
        std::to_string(entry.coverage.end_ms + mapping_source.at("timestamp_offset_ms").get<int64_t>()) +
        ") order by ddb_timestamp_ms asc, open asc, high asc, low asc, close asc, volume asc";
    require(mapping_source.at("query_sha256") == hc::hex(hc::sha256(query)),
            "symbol, coverage or row limit differs from the original source query");
    return {epoch, {entry}};
}

void encode(const Arguments& args) {
    const auto source = read_json(args.get("--source"));
    auto manifest = source_manifest(source);
    auto& entry = manifest.entries.front();
    const auto bytes = hc::read_file(args.get("--rows"), kRows * 32);
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
        if (!source.contains("kind") || source.at("kind") != "ddb-single-chunk-snapshot") continue;
        require(name == "source.json" || hc::hex(hc::sha256(bytes_of(source))) + ".json" == name,
                "source proof file name differs");
        proofs.push_back(std::move(source));
    }
    require(proofs.size() == candidate.manifest.entries.size(),
            "exactly one native source proof per catalog entry is required");
    std::vector<bool> used(proofs.size(), false);
    for (const auto& entry : candidate.manifest.entries) {
        size_t matched = proofs.size();
        for (size_t i = 0; i < proofs.size(); ++i) {
            if (used[i]) continue;
            auto expected = source_manifest(proofs[i]);
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
        auto pack = hc::read_file(path, kBytes);
        hc::verify_pack(path, *entry.pack, hc::pack_metadata(entry));
        candidate.packs.emplace(entry.pack->key, std::move(pack));
    }
    const auto rows_path = directory / "rows.bin";
    if (candidate.manifest.entries.size() == 1 && fs::is_regular_file(rows_path)) {
        const auto rows = hc::read_file(rows_path, kRows * 32);
        const auto& entry = candidate.manifest.entries.front();
        require(candidate.sources.front().at("rows_sha256") == hc::hex(hc::sha256(rows)),
                "candidate row hash differs");
        (void)parse_rows(rows, entry);
        hc::Bytes decoded;
        for (const auto& row : decode_entry_rows(candidate, 0)) {
            const auto bytes = hc::canonical_row(row);
            decoded.insert(decoded.end(), bytes.begin(), bytes.end());
        }
        require(decoded == rows, "pack rows differ from exported source");
    }
    return candidate;
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
            const auto bytes = hc::canonical_row(row);
            rows.insert(rows.end(), bytes.begin(), bytes.end());
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
            if (expected.size() / 32 == query.max_count) break;
            const auto bytes = hc::canonical_row(row);
            expected.insert(expected.end(), bytes.begin(), bytes.end());
        }
        if (expected.size() / 32 == query.max_count) break;
    }
    require(rows == expected, "reader rows differ from pinned DDB source");
    hc::write_new_file(output / "rows.bin", rows);
    result.update({{"rows", summary.rows}, {"first_ms", summary.first_ms}, {"last_ms", summary.last_ms},
                   {"rows_sha256", hc::hex(summary.rows_sha256)}, {"source_rows_exact", true}});
    return result;
}

hc::S3Config config_from(const Arguments& args) {
    return {args.get("--account"), args.get("--bucket"), "r2-history-staging/" + args.get("--run-id") + '/', "default"};
}

hc::TransferLimits limits() {
    hc::TransferLimits result;
    result.max_requests = kRequests;
    result.max_upload_bytes = 2 * kBytes;
    result.max_download_bytes = 8 * kBytes;
    result.deadline = hc::SteadyClock::now() + std::chrono::seconds(120);
    return result;
}

Json transfer_plan(const Candidate& candidate, const hc::S3Config& config, uint64_t expected_seq = 0) {
    auto transport = std::make_shared<hc::CurlHttpTransport>();
    auto dummy = std::make_shared<hc::S3Credentials>("snapshotOfflineId", "snapshotOfflineSecret");
    hc::S3Store validate(config, transport, dummy, limits());
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
            {"entries", candidate.manifest.entries.size()}, {"objects", objects}, {"max_requests", kRequests},
            {"max_upload_bytes", 2 * kBytes}, {"max_download_bytes", 8 * kBytes}, {"max_seconds", 120},
            {"expected_seq", expected_seq}, {"target", Json::parse(hc::serialize_pointer(target))},
            {"production_eligible", false}, {"freshness", "pinned-snapshot-only"}, {"delete_requests", 0}};
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
void current(const Arguments& args, const hc::S3Config& config) {
#ifndef HC_HAS_CURL
    (void)args;
    (void)config;
    throw hc::Error(hc::ErrorCode::invalid, "snapshot network commands require a curl-enabled build");
#else
    require(args.get("--retain") == "yes", "explicit retained staging scope required");
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    const char* id = std::getenv("R2_STAGING_ACCESS_KEY_ID");
    const char* secret = std::getenv("R2_STAGING_SECRET_ACCESS_KEY");
    require(id && *id && secret && *secret, "authorized R2 credentials are unavailable");
    auto transport = std::make_shared<hc::CurlHttpTransport>(true);
    auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
    hc::S3Store store(config, transport, credentials, limits());
    Json result{{"status", "PASS_DDB_SNAPSHOT_STAGING_CURRENT"}, {"bucket", config.bucket},
                {"prefix", config.key_prefix}, {"scope_id", hc::hex(store.scope_id())},
                {"production_eligible", false}, {"current_pointer_written", false},
                {"delete_requests", 0}};
    const auto probe = store.read_current();
    if (!probe) {
        result.update({{"present", false}, {"publication_seq", 0}});
        hc::write_new_file(output / "report.json", canonical(result));
        std::cout << canonical(result);
        return;
    }
    const auto pointer = hc::parse_pointer(std::string(probe->bytes.begin(), probe->bytes.end()));
    const auto etag = hc::hex(hc::sha256(probe->etag));
    result.update({{"present", true}, {"publication_seq", pointer.publication_seq},
                   {"dataset_epoch", pointer.dataset_epoch}, {"manifest_key", pointer.manifest_key},
                   {"manifest_sha256", hc::hex(pointer.manifest_sha256)}, {"etag_sha256", etag}});
    const auto usage = store.usage();
    result.update({{"requests", usage.requests}, {"download_reserved_bytes", usage.download_reserved}});
    hc::write_new_file(output / "report.json", canonical(result));
    std::cout << canonical(result);
#endif
}

bool network(const Arguments& args, bool publish) {
    const auto candidate = load_candidate(args.get("--candidate"));
    const auto config = config_from(args);
    const auto expected_seq = number(args.optional("--expected-seq", "0"), UINT64_MAX);
    const auto plan = transfer_plan(candidate, config, expected_seq);
#ifndef HC_HAS_CURL
    (void)publish;
    (void)plan;
    throw hc::Error(hc::ErrorCode::invalid, "snapshot network commands require a curl-enabled build");
#else
    require(args.get("--retain") == "yes", "explicit retained staging scope required");
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    hc::write_new_file(output / "plan.json", canonical(plan));
    Json report{{"status", "FAIL_DDB_SNAPSHOT_R2"}, {"plan", plan}, {"production_eligible", false},
                {"ddb_connected", false}, {"r2_connected", false}, {"current_pointer_written", false},
                {"delete_requests", 0}, {"automatic_retry", false}, {"objects", Json::array()}};
    const char* id = std::getenv("R2_STAGING_ACCESS_KEY_ID");
    const char* secret = std::getenv("R2_STAGING_SECRET_ACCESS_KEY");
    require(id && *id && secret && *secret, "authorized R2 credentials are unavailable");
    auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
    auto transport = std::make_shared<hc::CurlHttpTransport>(true);
    hc::S3Store store(config, transport, credentials, limits());
    bool success = false;
    try {
        report["r2_connected"] = true;
        if (publish) {
            hc::JournaledPublisher::prepare(output / "journal", store.scope_id(), candidate.manifest, expected_seq);
            hc::JournaledPublisher journal(output / "journal", store);
            std::vector<std::pair<std::string, hc::Bytes>> objects;
            for (const auto& [key, bytes] : candidate.packs) objects.emplace_back(key, bytes);
            for (const auto& source : candidate.sources) {
                auto proof = bytes_of(source);
                objects.emplace_back("manifests/v1/" + hc::hex(hc::sha256(proof)) + ".json", std::move(proof));
            }
            for (const auto& [key, bytes] : objects) {
                const auto receipt = hc::sample::ensure_object(store, key, bytes);
                report["objects"].push_back({{"key", config.key_prefix + key}, {"bytes", bytes.size()},
                    {"sha256", hc::hex(hc::sha256(bytes))}, {"already_present", receipt.already_present},
                    {"create_ack", receipt.write == hc::WriteOutcome::applied}, {"readback_verified", true}});
                hc::write_new_file(output / fs::path(key).filename(), receipt.readback);
            }
            const auto result = journal.resume();
            report["publish_outcome"] = result.outcome == hc::PublishOutcome::committed ? "committed" :
                result.outcome == hc::PublishOutcome::conflict ? "conflict" :
                result.outcome == hc::PublishOutcome::not_applied ? "not_applied" : "indeterminate";
            report["current_pointer_written"] = result.outcome == hc::PublishOutcome::committed;
            require(result.outcome == hc::PublishOutcome::committed, "publication not resolved; retain journal for recovery");
        }
        auto snapshot = std::make_shared<const hc::Snapshot>(hc::load_snapshot(store, 1));
        require(snapshot->pointer.publication_seq == expected_seq + 1 &&
                hc::serialize_manifest(snapshot->manifest) == hc::serialize_manifest(candidate.manifest),
                "remote snapshot differs from pinned candidate");
        hc::write_new_file(output / "current.json", hc::serialize_pointer(snapshot->pointer));
        hc::write_new_file(output / "candidate.json", hc::serialize_manifest(snapshot->manifest));
        report["query"] = compare_query(store, snapshot, candidate, query_from(candidate, args), output);
        report["status"] = publish ? "PASS_DDB_SNAPSHOT_STAGING_PUBLICATION" : "PASS_DDB_SNAPSHOT_STAGING_READ";
        success = true;
    } catch (const std::exception& error) {
        report["error"] = error.what();
    }
    const auto usage = store.usage();
    report["requests"] = usage.requests;
    report["upload_reserved_bytes"] = usage.upload_reserved;
    report["download_reserved_bytes"] = usage.download_reserved;
    hc::write_new_file(output / "report.json", canonical(report));
    std::cout << canonical(report);
    return success;
#endif
}

void usage() {
    std::cout << "history-cache-snapshot: bounded native-version DDB snapshots, staging only\n"
              << "  encode --source SOURCE_JSON --rows ROWS_BIN --output NEW_DIRECTORY\n"
              << "  local --candidate DIR --output NEW_DIRECTORY [--start MS --end MS --max-count N]\n"
              << "  current --account ID --bucket STAGING --run-id ID --retain yes --output NEW_DIRECTORY\n"
              << "  plan --candidate DIR --account ID --bucket STAGING --run-id ID [--expected-seq N]\n"
              << "  publish --candidate DIR --account ID --bucket STAGING --run-id ID --retain yes"
                 " --output NEW_DIRECTORY [--expected-seq N]\n"
              << "  read --candidate DIR --account ID --bucket STAGING --run-id ID --retain yes --output NEW_DIRECTORY"
                 " [--start MS --end MS --max-count N] [--expected-seq N]\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2 || std::string(argv[1]) == "--help") { usage(); return argc < 2 ? 2 : 0; }
        const std::string command = argv[1];
        if (command == "encode") {
            encode(Arguments(argc, argv, {"--source", "--rows", "--output"}));
        } else if (command == "local") {
            local(Arguments(argc, argv, {"--candidate", "--output", "--start", "--end", "--max-count"}));
        } else if (command == "current") {
            const Arguments args(argc, argv, {"--account", "--bucket", "--run-id", "--retain", "--output"});
            current(args, config_from(args));
        } else if (command == "plan") {
            const Arguments args(argc, argv, {"--candidate", "--account", "--bucket", "--run-id", "--expected-seq"});
            std::cout << canonical(transfer_plan(load_candidate(args.get("--candidate")), config_from(args),
                                                 number(args.optional("--expected-seq", "0"), UINT64_MAX)));
        } else if (command == "publish") {
            return network(Arguments(argc, argv, {"--candidate", "--account", "--bucket", "--run-id", "--retain",
                                                  "--output", "--expected-seq"}), true) ? 0 : 2;
        } else if (command == "read") {
            return network(Arguments(argc, argv, {"--candidate", "--account", "--bucket", "--run-id", "--retain",
                                                  "--output", "--start", "--end", "--max-count", "--expected-seq"}),
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
