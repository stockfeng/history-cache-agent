#include "history_cache/reader.h"
#include "history_cache/local_store.h"

#include <algorithm>
#include <charconv>
#include <iostream>
#include <map>
#include <set>

namespace hc = history_cache;
using Json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

constexpr int64_t kFixtureStart = 1704067200000;

class Arguments {
public:
    Arguments(int argc, char** argv, std::initializer_list<const char*> allowed) {
        std::set<std::string> names;
        for (const auto* name : allowed) names.insert(name);
        for (int i = 2; i < argc; i += 2) {
            const std::string name = argv[i];
            if (!names.count(name) || i + 1 >= argc || !values_.emplace(name, argv[i + 1]).second)
                throw hc::Error(hc::ErrorCode::invalid, "unknown, duplicate, or missing argument: " + name);
        }
    }
    std::string required(const std::string& name) const {
        const auto found = values_.find(name);
        if (found == values_.end() || found->second.empty())
            throw hc::Error(hc::ErrorCode::invalid, "missing argument: " + name);
        return found->second;
    }
    std::string optional(const std::string& name, const std::string& fallback) const {
        const auto found = values_.find(name);
        return found == values_.end() ? fallback : found->second;
    }
private:
    std::map<std::string, std::string> values_;
};

uint64_t number(const std::string& text, uint64_t maximum = UINT64_MAX) {
    uint64_t value = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || value > maximum)
        throw hc::Error(hc::ErrorCode::invalid, "invalid unsigned integer: " + text);
    return value;
}

std::string read_text(const fs::path& path) {
    const auto bytes = hc::read_file(path, hc::kMaxMetadataBytes);
    return std::string(bytes.begin(), bytes.end());
}

hc::Row fixture_row(uint64_t index) {
    const auto price = 100.0F + static_cast<float>(index % 1000) * 0.125F;
    return {kFixtureStart + static_cast<int64_t>(index) * 60000,
            price, price + 1.0F, price - 1.0F, price + 0.5F, static_cast<int64_t>(index * 17 + 1)};
}

void fixture(const Arguments& args) {
    const fs::path output(args.required("--output"));
    const auto count = number(args.optional("--rows", "240"), hc::kMaxRows);
    hc::create_new_directory(output);
    hc::CatalogEntry entry;
    entry.identity = {"synthetic", "TEST", "FIXTURE", 60, "none"};
    entry.data_version = hc::sha256(std::string("synthetic-none-1m-v1\n"));
    entry.source_version = 1;
    entry.coverage = {kFixtureStart, kFixtureStart + static_cast<int64_t>(std::max<uint64_t>(count, 1)) * 60000};
    entry.row_count = count;
    hc::Sha256 row_hash;
    if (count) {
        const auto path = output / "fixture.r2b";
        entry.pack = hc::write_pack(path, hc::pack_metadata(entry), [&row_hash](uint64_t start, uint32_t rows) {
            std::vector<hc::Row> result;
            result.reserve(rows);
            for (uint64_t i = start; i < start + rows; ++i) {
                result.push_back(fixture_row(i));
                row_hash.update(hc::canonical_row(result.back()));
            }
            return result;
        });
        fs::rename(path, output / fs::path(entry.pack->key).filename());
    }
    const hc::Manifest manifest{"fixture-epoch-1", {entry}};
    hc::write_new_file(output / "candidate.json", hc::serialize_manifest(manifest));
    const Json summary{{"source", "synthetic-only"}, {"rows", count},
                       {"rows_sha256", hc::hex(row_hash.finish())},
                       {"series_id", hc::hex(hc::series_id(entry.identity))},
                       {"data_version", hc::hex(entry.data_version)},
                       {"coverage_start_ms", entry.coverage.start_ms}, {"coverage_end_ms", entry.coverage.end_ms},
                       {"ddb_connected", false}, {"r2_connected", false}};
    hc::write_new_file(output / "fixture.json", summary.dump() + "\n");
    std::cout << summary.dump() << '\n';
}

hc::FailPoint fail_point(const std::string& name) {
    if (name == "none") return hc::FailPoint::none;
    if (name == "after-objects") return hc::FailPoint::after_objects;
    if (name == "after-manifest") return hc::FailPoint::after_manifest;
    if (name == "after-pointer") return hc::FailPoint::after_pointer;
    throw hc::Error(hc::ErrorCode::invalid, "unknown fail point");
}

void publish(const Arguments& args) {
    hc::LocalStore store(args.required("--root"));
    const auto manifest = hc::parse_manifest(read_text(args.required("--candidate")));
    const fs::path pack_dir(args.required("--pack-dir"));
    for (const auto& entry : manifest.entries) {
        if (entry.pack) store.put_file(entry.pack->key, pack_dir / fs::path(entry.pack->key).filename(), entry.pack->sha256);
    }
    const auto pointer = store.publish(manifest, number(args.required("--expected-seq")),
                                       fail_point(args.optional("--fail", "none")));
    std::cout << hc::serialize_pointer(pointer);
}

int read(const Arguments& args) {
    hc::LocalStore store(args.required("--root"));
    auto snapshot = std::make_shared<const hc::Snapshot>(store.load_snapshot(
        number(args.optional("--min-seq", "0"))));
    const auto requested_series = hc::parse_series_id(args.required("--series-id"));
    const auto found = std::find_if(snapshot->manifest.entries.begin(), snapshot->manifest.entries.end(),
        [&requested_series](const hc::CatalogEntry& entry) { return hc::series_id(entry.identity) == requested_series; });
    if (found == snapshot->manifest.entries.end()) {
        std::cout << Json{{"result", "MISS"}, {"reason", "unknown_series"}}.dump() << '\n';
        return 3;
    }
    const hc::Query query{found->identity, hc::parse_digest(args.required("--data-version")),
                          {static_cast<int64_t>(number(args.required("--start"), INT64_MAX)),
                           static_cast<int64_t>(number(args.required("--end"), INT64_MAX))},
                          number(args.optional("--max-count", "500000"), hc::kMaxRows)};
    const auto plan = hc::plan_query(snapshot, query, true, true);
    if (plan.result != hc::PlanResult::hit) {
        std::cout << Json{{"result", hc::plan_result_name(plan.result)}, {"reason", plan.reason}}.dump() << '\n';
        return plan.result == hc::PlanResult::error ? 2 : 3;
    }
    const auto summary = hc::read_plan(store, plan);
    std::cout << Json{{"result", "HIT"}, {"publication_seq", snapshot->pointer.publication_seq},
                      {"rows", summary.rows}, {"first_ms", summary.first_ms}, {"last_ms", summary.last_ms},
                      {"rows_sha256", hc::hex(summary.rows_sha256)}}.dump() << '\n';
    return 0;
}

void usage() {
    std::cout << "history-cache-tool: offline fixture utility (no network or DDB support)\n"
              << "  init --root NEW_DIRECTORY\n"
              << "  fixture --output NEW_DIRECTORY [--rows 0..500000]\n"
              << "  publish --root STORE --candidate FILE --pack-dir DIR --expected-seq N [--fail STAGE]\n"
              << "  read --root STORE --series-id HEX32 --data-version HEX64 --start MS --end MS"
                 " [--max-count N] [--min-seq N]\n"
              << "  rollback --root STORE --manifest-key KEY --manifest-sha256 HEX64 --expected-seq N\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2 || std::string(argv[1]) == "--help") { usage(); return argc < 2 ? 2 : 0; }
        const std::string command = argv[1];
        if (command == "init") {
            const Arguments args(argc, argv, {"--root"});
            hc::LocalStore::initialize(args.required("--root"));
            std::cout << "{\"initialized\":true,\"network_enabled\":false}\n";
        } else if (command == "fixture") {
            fixture(Arguments(argc, argv, {"--output", "--rows"}));
        } else if (command == "publish") {
            publish(Arguments(argc, argv, {"--root", "--candidate", "--pack-dir", "--expected-seq", "--fail"}));
        } else if (command == "read") {
            return read(Arguments(argc, argv, {"--root", "--series-id", "--data-version", "--start", "--end",
                                              "--max-count", "--min-seq"}));
        } else if (command == "rollback") {
            const Arguments args(argc, argv, {"--root", "--manifest-key", "--manifest-sha256", "--expected-seq"});
            hc::LocalStore store(args.required("--root"));
            std::cout << hc::serialize_pointer(store.rollback(args.required("--manifest-key"),
                hc::parse_digest(args.required("--manifest-sha256")), number(args.required("--expected-seq"))));
        } else {
            throw hc::Error(hc::ErrorCode::invalid, "unknown command");
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << Json{{"result", "ERROR"}, {"message", error.what()}}.dump() << '\n';
        return 2;
    }
}
