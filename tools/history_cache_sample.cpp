#include "history_cache/columns.h"
#include "history_cache/s3_store.h"
#include "sample_transfer.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <set>

namespace hc = history_cache;
namespace fs = std::filesystem;
using Json = nlohmann::json;

namespace {

constexpr uint64_t kSampleRows = 5000;
constexpr uint64_t kSampleBytes = 1024 * 1024;
constexpr uint64_t kRequestLimit = 8;

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
private:
    std::map<std::string, std::string> values_;
};

std::string json_bytes(const Json& value) { return value.dump() + '\n'; }

Json read_json(const fs::path& path) {
    const auto bytes = hc::read_file(path, kSampleBytes);
    return Json::parse(bytes.begin(), bytes.end());
}

hc::Bytes bytes_of(const Json& value) {
    const auto text = json_bytes(value);
    return {text.begin(), text.end()};
}

uint64_t little(const uint8_t* value, size_t size) {
    uint64_t result = 0;
    for (size_t i = 0; i < size; ++i) result |= uint64_t(value[i]) << (i * 8U);
    return result;
}

std::vector<hc::Row> rows_from(const hc::Bytes& bytes) {
    require(!bytes.empty() && bytes.size() % 32 == 0 && bytes.size() / 32 <= kSampleRows,
            "invalid sample row payload");
    std::vector<hc::Row> rows;
    int64_t previous = -1;
    for (size_t offset = 0; offset < bytes.size(); offset += 32) {
        const auto* input = bytes.data() + offset;
        const auto timestamp = little(input, 8);
        const auto volume = little(input + 24, 8);
        require(timestamp <= INT64_MAX && volume <= INT64_MAX, "invalid timestamp or volume");
        float prices[4]{};
        for (size_t i = 0; i < 4; ++i) {
            const auto bits = static_cast<uint32_t>(little(input + 8 + 4 * i, 4));
            std::memcpy(&prices[i], &bits, sizeof(bits));
        }
        hc::Row row{static_cast<int64_t>(timestamp), prices[0], prices[1], prices[2], prices[3],
                    static_cast<int64_t>(volume)};
        hc::validate_row(row);
        require(row.timestamp_ms > previous, "sample rows must be strictly increasing");
        previous = row.timestamp_ms;
        rows.push_back(row);
    }
    return rows;
}

void validate_source(const Json& source, const hc::Bytes& bytes, const std::vector<hc::Row>& rows) {
    require(source.at("schema_version") == 1 && source.at("kind") == "ddb-query-observation" &&
            source.at("coverage_verified") == false && source.at("source_version").is_null(),
            "only uncommitted DDB observations are accepted");
    const auto& identity = source.at("identity");
    const hc::SeriesIdentity series{identity.at("dataset").get<std::string>(), identity.at("market").get<std::string>(),
        identity.at("symbol").get<std::string>(), identity.at("period_seconds").get<uint32_t>(),
        identity.at("adjust").get<std::string>()};
    hc::validate_identity(series);
    require(series.dataset == "ddb-query-observation" && series.period_seconds == 60 && series.adjust == "none",
            "M1 only accepts none/1m observations");
    const auto start = source.at("requested_start_ms").get<int64_t>();
    const auto end = source.at("requested_end_ms").get<int64_t>();
    require(start >= 0 && end > start && end - start <= 7LL * 86400000 &&
            rows.front().timestamp_ms >= start && rows.back().timestamp_ms < end,
            "rows outside bounded requested interval");
    require(source.at("row_count") == rows.size() && source.at("rows_sha256") == hc::hex(hc::sha256(bytes)),
            "sample source/row hash mismatch");
    const auto& descriptor = source.at("source");
    for (const auto* key : {"config_sha256", "schema_sha256", "query_sha256"})
        (void)hc::parse_digest(descriptor.at(key).get<std::string>());
}

Json encode_sample(const Json& source, const hc::Bytes& row_bytes, const fs::path& output) {
    const auto rows = rows_from(row_bytes);
    validate_source(source, row_bytes, rows);
    hc::Bytes object;
    Json blocks = Json::array();
    for (size_t offset = 0; offset < rows.size(); offset += hc::kBlockRows) {
        const auto count = std::min<size_t>(hc::kBlockRows, rows.size() - offset);
        const std::vector<hc::Row> block(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                                       rows.begin() + static_cast<std::ptrdiff_t>(offset + count));
        const auto raw = hc::encode_columns(block);
        const auto frame = hc::compress_block(raw);
        blocks.push_back({{"offset", object.size()}, {"bytes", frame.size()}, {"raw_bytes", raw.size()},
                          {"rows", count}, {"sha256", hc::hex(hc::sha256(frame))}});
        object.insert(object.end(), frame.begin(), frame.end());
    }
    require(object.size() <= kSampleBytes, "compressed sample exceeds byte limit");
    const auto hash = hc::hex(hc::sha256(object));
    // Reuse the immutable blob transport, but never describe this as an R2H1 pack.
    const std::string key = "data/v1/" + hash + ".r2b";
    const Json descriptor{{"schema_version", 1}, {"kind", "ddb-query-observation-columns"},
        {"codec", "HISTORY_COLUMNS_V1+independent-zstd"}, {"pack_format", nullptr},
        {"coverage_verified", false}, {"production_eligible", false}, {"source_version", nullptr},
        {"source", source}, {"row_count", rows.size()}, {"rows_sha256", hc::hex(hc::sha256(row_bytes))},
        {"object", {{"key", key}, {"sha256", hash}, {"bytes", object.size()}}}, {"blocks", blocks}};
    hc::create_new_directory(output);
    hc::write_new_file(output / "sample.r2b", object);
    hc::write_new_file(output / "observation.json", json_bytes(descriptor));
    return descriptor;
}

hc::Bytes decode_sample(const Json& descriptor, const hc::Bytes& object) {
    require(descriptor.at("schema_version") == 1 && descriptor.at("kind") == "ddb-query-observation-columns" &&
            descriptor.at("codec") == "HISTORY_COLUMNS_V1+independent-zstd" && descriptor.at("pack_format").is_null() &&
            descriptor.at("coverage_verified") == false && descriptor.at("production_eligible") == false &&
            descriptor.at("source_version").is_null(), "invalid observation descriptor");
    const auto hash = hc::hex(hc::sha256(object));
    require(!object.empty() && object.size() <= kSampleBytes && descriptor.at("object").at("bytes") == object.size() &&
            descriptor.at("object").at("sha256") == hash &&
            descriptor.at("object").at("key") == "data/v1/" + hash + ".r2b", "sample content hash mismatch");
    const auto& blocks = descriptor.at("blocks");
    require(blocks.is_array() && !blocks.empty() && blocks.size() <= 5, "invalid sample block count");
    hc::Bytes result;
    uint64_t cursor = 0;
    int64_t previous = -1;
    for (const auto& block : blocks) {
        const auto size = block.at("bytes").get<uint64_t>();
        const auto raw_size = block.at("raw_bytes").get<uint32_t>();
        const auto count = block.at("rows").get<uint32_t>();
        require(block.at("offset") == cursor && size > 0 && size <= hc::kMaxBlockBytes && cursor <= object.size() &&
                size <= object.size() - cursor && count > 0 && count <= hc::kBlockRows &&
                raw_size > 0 && raw_size <= hc::kMaxBlockBytes, "invalid observation block bounds");
        const hc::Bytes frame(object.begin() + static_cast<std::ptrdiff_t>(cursor),
                              object.begin() + static_cast<std::ptrdiff_t>(cursor + size));
        require(block.at("sha256") == hc::hex(hc::sha256(frame)), "sample block checksum mismatch");
        for (const auto& row : hc::decode_columns(hc::decompress_block(frame, raw_size), count)) {
            require(row.timestamp_ms > previous && row.volume >= 0, "invalid cross-block row order");
            previous = row.timestamp_ms;
            const auto bytes = hc::canonical_row(row);
            result.insert(result.end(), bytes.begin(), bytes.end());
        }
        cursor += size;
    }
    require(cursor == object.size() && result.size() / 32 <= kSampleRows &&
            descriptor.at("row_count") == result.size() / 32 &&
            descriptor.at("rows_sha256") == hc::hex(hc::sha256(result)), "sample row count/hash mismatch");
    validate_source(descriptor.at("source"), result, rows_from(result));
    return result;
}

struct Sample {
    Json descriptor;
    hc::Bytes object;
    hc::Bytes rows;
};

Sample load_sample(const fs::path& directory) {
    Sample sample{read_json(directory / "observation.json"), hc::read_file(directory / "sample.r2b", kSampleBytes), {}};
    sample.rows = decode_sample(sample.descriptor, sample.object);
    return sample;
}

hc::S3Config s3_config(const Arguments& args) {
    return {args.get("--account"), args.get("--bucket"), "r2-history-staging/" + args.get("--run-id") + '/', "default"};
}

hc::TransferLimits limits() {
    hc::TransferLimits result;
    result.max_requests = kRequestLimit;
    result.max_upload_bytes = 2 * kSampleBytes;
    result.max_download_bytes = 2 * kSampleBytes;
    result.deadline = hc::SteadyClock::now() + std::chrono::seconds(90);
    return result;
}

Json upload_plan(const Sample& sample, const hc::S3Config& config) {
    auto transport = std::make_shared<hc::CurlHttpTransport>();
    auto dummy = std::make_shared<hc::S3Credentials>("sampleOfflineId", "sampleOfflineSecret");
    hc::S3Store validate(config, transport, dummy, limits());
    const auto description = bytes_of(sample.descriptor);
    require(description.size() <= kSampleBytes, "descriptor exceeds size limit");
    const auto description_key = "manifests/v1/" + hc::hex(hc::sha256(description)) + ".json";
    return {{"status", "DDB_SAMPLE_UPLOAD_PLAN_ONLY"}, {"bucket", config.bucket}, {"prefix", config.key_prefix},
        {"scope_id", hc::hex(validate.scope_id())}, {"row_count", sample.rows.size() / 32},
        {"rows_sha256", hc::hex(hc::sha256(sample.rows))}, {"max_requests", kRequestLimit},
        {"max_upload_bytes", 2 * kSampleBytes}, {"max_download_bytes", 2 * kSampleBytes},
        {"max_seconds", 90}, {"retained_objects", 2}, {"retained_bytes", sample.object.size() + description.size()},
        {"delete_requests", 0}, {"current_pointer_written", false}, {"coverage_verified", false},
        {"objects", Json::array({{{"key", config.key_prefix + sample.descriptor.at("object").at("key").get<std::string>()},
                                  {"bytes", sample.object.size()}},
                                 {{"key", config.key_prefix + description_key}, {"bytes", description.size()}}})}};
}

bool upload(const Arguments& args) {
    const auto sample = load_sample(args.get("--sample"));
    const auto config = s3_config(args);
    const auto plan = upload_plan(sample, config);
    require(args.get("--retain") == "yes", "explicit retention is required");
#ifndef HC_HAS_CURL
    throw hc::Error(hc::ErrorCode::invalid, "sample upload requires a curl-enabled build");
#else
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    hc::write_new_file(output / "plan.json", json_bytes(plan));
    const char* id = std::getenv("R2_STAGING_ACCESS_KEY_ID");
    const char* secret = std::getenv("R2_STAGING_SECRET_ACCESS_KEY");
    require(id && *id && secret && *secret, "authorized R2 credentials are unavailable");
    auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
    auto transport = std::make_shared<hc::CurlHttpTransport>(true);
    hc::S3Store store(config, transport, credentials, limits());
    Json report{{"status", "FAIL_DDB_SAMPLE_R2"}, {"plan", plan}, {"objects", Json::array()},
                {"current_pointer_written", false}, {"delete_requests", 0}, {"coverage_verified", false},
                {"automatic_retry", false}};
    bool passed = false;
    try {
        require(!store.read_current(), "sample namespace already contains a current pointer");
        const auto descriptor_bytes = bytes_of(sample.descriptor);
        const std::vector<std::pair<std::string, hc::Bytes>> objects{
            {sample.descriptor.at("object").at("key").get<std::string>(), sample.object},
            {"manifests/v1/" + hc::hex(hc::sha256(descriptor_bytes)) + ".json", descriptor_bytes}};
        for (size_t index = 0; index < objects.size(); ++index) {
            const auto& [key, bytes] = objects[index];
            const auto result = store.create(key, bytes);
            Json state{{"key", config.key_prefix + key}, {"bytes", bytes.size()},
                       {"sha256", hc::hex(hc::sha256(bytes))}, {"create_ack", result == hc::WriteOutcome::applied},
                       {"write_indeterminate", result == hc::WriteOutcome::indeterminate}};
            report["objects"].push_back(state);
            hc::write_new_file(output / ("object-" + std::to_string(index) + "-write.json"), json_bytes(state));
            require(result == hc::WriteOutcome::applied, "create-only write not acknowledged; retained state requires review");
            auto readback = store.get(key, bytes.size());
            require(readback == bytes, "sample remote readback differs");
            hc::write_new_file(output / (index == 0 ? "sample.r2b" : "observation.json"), readback);
            report["objects"].back()["readback_verified"] = true;
        }
        const auto remote = load_sample(output);
        require(remote.rows == sample.rows, "decoded remote sample rows differ");
        hc::write_new_file(output / "rows.bin", remote.rows);
        report["status"] = "PASS_DDB_SAMPLE_R2";
        report["rows"] = remote.rows.size() / 32;
        report["rows_sha256"] = hc::hex(hc::sha256(remote.rows));
        report["first_ms"] = static_cast<int64_t>(little(remote.rows.data(), 8));
        report["last_ms"] = static_cast<int64_t>(little(remote.rows.data() + remote.rows.size() - 32, 8));
        passed = true;
    } catch (const std::exception& error) {
        report["error"] = error.what();
    }
    const auto usage = store.usage();
    report["requests"] = usage.requests;
    report["upload_reserved_bytes"] = usage.upload_reserved;
    report["download_reserved_bytes"] = usage.download_reserved;
    hc::write_new_file(output / "report.json", json_bytes(report));
    std::cout << json_bytes(report);
    return passed;
#endif
}

bool sync_sample(const Arguments& args) {
    const auto sample = load_sample(args.get("--sample"));
    const auto config = s3_config(args);
    auto plan = upload_plan(sample, config);
    plan["status"] = "DDB_SAMPLE_SYNC_PLAN";
    plan["read_before_create"] = true;
    require(args.get("--retain") == "yes", "explicit retention is required");
#ifndef HC_HAS_CURL
    throw hc::Error(hc::ErrorCode::invalid, "sample sync requires a curl-enabled build");
#else
    const fs::path output(args.get("--output"));
    hc::create_new_directory(output);
    hc::write_new_file(output / "plan.json", json_bytes(plan));
    const char* id = std::getenv("R2_STAGING_ACCESS_KEY_ID");
    const char* secret = std::getenv("R2_STAGING_SECRET_ACCESS_KEY");
    require(id && *id && secret && *secret, "authorized R2 credentials are unavailable");
    auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
    auto transport = std::make_shared<hc::CurlHttpTransport>(true);
    hc::S3Store store(config, transport, credentials, limits());
    Json report{{"status", "FAIL_DDB_SAMPLE_SYNC"}, {"plan", plan}, {"objects", Json::array()},
                {"current_pointer_written", false}, {"delete_requests", 0}, {"coverage_verified", false},
                {"automatic_retry", false}, {"read_before_create", true}};
    bool passed = false;
    try {
        require(!store.read_current(), "observation namespace contains a current pointer");
        const auto descriptor_bytes = bytes_of(sample.descriptor);
        const std::vector<std::pair<std::string, hc::Bytes>> objects{
            {sample.descriptor.at("object").at("key").get<std::string>(), sample.object},
            {"manifests/v1/" + hc::hex(hc::sha256(descriptor_bytes)) + ".json", descriptor_bytes}};
        for (size_t index = 0; index < objects.size(); ++index) {
            const auto& [key, bytes] = objects[index];
            const auto receipt = hc::sample::ensure_object(store, key, bytes);
            const bool ack = receipt.write == hc::WriteOutcome::applied;
            const Json state{{"key", config.key_prefix + key}, {"bytes", bytes.size()},
                {"sha256", hc::hex(hc::sha256(bytes))}, {"already_present", receipt.already_present},
                {"create_attempted", receipt.write.has_value()}, {"create_ack", ack},
                {"write_indeterminate", receipt.write == hc::WriteOutcome::indeterminate},
                {"ownership", ack ? "create_ack_this_attempt" : "not_proven"}, {"readback_verified", true}};
            hc::write_new_file(output / ("object-" + std::to_string(index) + "-receipt.json"), json_bytes(state));
            hc::write_new_file(output / (index == 0 ? "sample.r2b" : "observation.json"), receipt.readback);
            report["objects"].push_back(state);
        }
        const auto remote = load_sample(output);
        require(remote.rows == sample.rows, "decoded remote sample rows differ");
        hc::write_new_file(output / "rows.bin", remote.rows);
        report["status"] = "PASS_DDB_SAMPLE_SYNC";
        report["rows"] = remote.rows.size() / 32;
        report["rows_sha256"] = hc::hex(hc::sha256(remote.rows));
        report["first_ms"] = static_cast<int64_t>(little(remote.rows.data(), 8));
        report["last_ms"] = static_cast<int64_t>(little(remote.rows.data() + remote.rows.size() - 32, 8));
        passed = true;
    } catch (const std::exception& error) {
        report["error"] = error.what();
    }
    const auto usage = store.usage();
    report["requests"] = usage.requests;
    report["upload_reserved_bytes"] = usage.upload_reserved;
    report["download_reserved_bytes"] = usage.download_reserved;
    hc::write_new_file(output / "report.json", json_bytes(report));
    std::cout << json_bytes(report);
    return passed;
#endif
}

void usage() {
    std::cout << "history-cache-sample: bounded query observations, never a production catalog\n"
              << "  encode --source SOURCE_JSON --rows ROWS_BIN --output NEW_DIRECTORY\n"
              << "  decode --sample DIRECTORY --output NEW_ROWS_BIN\n"
              << "  plan --sample DIRECTORY --account ID --bucket STAGING --run-id ID\n"
              << "  upload --sample DIRECTORY --account ID --bucket STAGING --run-id ID --retain yes --output NEW_DIRECTORY\n"
              << "  sync --sample DIRECTORY --account ID --bucket STAGING --run-id ID --retain yes --output NEW_DIRECTORY\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2 || std::string(argv[1]) == "--help") { usage(); return argc < 2 ? 2 : 0; }
        const std::string command = argv[1];
        if (command == "encode") {
            const Arguments args(argc, argv, {"--source", "--rows", "--output"});
            std::cout << json_bytes(encode_sample(read_json(args.get("--source")),
                hc::read_file(args.get("--rows"), kSampleRows * 32), args.get("--output")));
        } else if (command == "decode") {
            const Arguments args(argc, argv, {"--sample", "--output"});
            const auto sample = load_sample(args.get("--sample"));
            hc::write_new_file(args.get("--output"), sample.rows);
            std::cout << Json{{"status", "PASS_SAMPLE_DECODE"}, {"rows", sample.rows.size() / 32},
                              {"rows_sha256", hc::hex(hc::sha256(sample.rows))}}.dump() << '\n';
        } else if (command == "plan") {
            const Arguments args(argc, argv, {"--sample", "--account", "--bucket", "--run-id"});
            std::cout << json_bytes(upload_plan(load_sample(args.get("--sample")), s3_config(args)));
        } else if (command == "upload") {
            return upload(Arguments(argc, argv, {"--sample", "--account", "--bucket", "--run-id", "--retain", "--output"})) ? 0 : 2;
        } else if (command == "sync") {
            return sync_sample(Arguments(argc, argv, {"--sample", "--account", "--bucket", "--run-id", "--retain", "--output"})) ? 0 : 2;
        } else {
            throw hc::Error(hc::ErrorCode::invalid, "unknown sample command");
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << Json{{"status", "ERROR_SAMPLE"}, {"error", error.what()}}.dump() << '\n';
        return 2;
    }
}
