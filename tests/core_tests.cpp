#include "history_cache/columns.h"
#include "history_cache/local_store.h"
#include "history_cache/reader.h"

#include "binary.h"

#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <sys/file.h>

namespace hc = history_cache;
namespace fs = std::filesystem;
using Json = nlohmann::json;

namespace {

void check(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

template <class Callable> void rejects(Callable call, const std::string& label) {
    try { call(); }
    catch (const hc::Error&) { return; }
    throw std::runtime_error("expected rejection: " + label);
}

class Workspace {
public:
    Workspace() {
        auto pattern = (fs::temp_directory_path() / "history-cache-test-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        auto* path = ::mkdtemp(buffer.data());
        if (!path) throw std::runtime_error("mkdtemp failed");
        root = path;
    }
    ~Workspace() { std::error_code ignored; fs::remove_all(root, ignored); }
    fs::path root;
};

hc::SeriesIdentity identity() { return {"synthetic", "TEST", "FIXTURE", 60, "none"}; }
hc::Digest version() { return hc::sha256(std::string("test-query-semantics-v1\n")); }

std::vector<hc::Row> make_rows(uint64_t count, int64_t start = 1000) {
    std::vector<hc::Row> rows;
    for (uint64_t i = 0; i < count; ++i) {
        const auto value = static_cast<float>(i % 100) * 0.125F;
        rows.push_back({start + static_cast<int64_t>(i) * 60000, value, value + 2, value - 1, value + 1,
                        static_cast<int64_t>(i)});
    }
    return rows;
}

hc::CatalogEntry empty_entry(int64_t start, int64_t end, uint64_t source = 1) {
    return {identity(), version(), source, {start, end}, 0, std::nullopt};
}

hc::CatalogEntry write_entry(const fs::path& file, const std::vector<hc::Row>& rows,
                             hc::Coverage coverage, uint64_t source = 1) {
    hc::CatalogEntry entry{identity(), version(), source, coverage, rows.size(), std::nullopt};
    if (!rows.empty()) {
        entry.pack = hc::write_pack(file, hc::pack_metadata(entry), [&rows](uint64_t offset, uint32_t count) {
            return std::vector<hc::Row>(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                                        rows.begin() + static_cast<std::ptrdiff_t>(offset + count));
        });
    }
    return entry;
}

hc::Manifest manifest(hc::CatalogEntry entry) { return {"fixture-epoch-1", {std::move(entry)}}; }

void compare(const std::vector<hc::Row>& lhs, const std::vector<hc::Row>& rhs) {
    check(lhs.size() == rhs.size(), "row count differs");
    for (size_t i = 0; i < lhs.size(); ++i) check(hc::same_row(lhs[i], rhs[i]), "row bit pattern differs");
}

void columns() {
    const auto bytes = hc::read_file(fs::path(HC_SOURCE_DIR) / "tests/golden/columns-v1.json", hc::kMaxMetadataBytes);
    const auto golden = Json::parse(bytes);
    for (const auto& item : golden.at("cases")) {
        std::vector<hc::Row> rows;
        for (const auto& row : item.at("rows")) {
            rows.push_back({row.at(0).get<int64_t>(), row.at(1).get<float>(), row.at(2).get<float>(),
                            row.at(3).get<float>(), row.at(4).get<float>(), row.at(5).get<int64_t>()});
        }
        const auto raw = hc::encode_columns(rows);
        check(hc::hex(raw.data(), raw.size()) == item.at("hex"), "fixed golden column bytes differ");
        compare(hc::decode_columns(hc::unhex(item.at("hex").get<std::string>()), static_cast<uint32_t>(rows.size())), rows);
    }
    auto rows = make_rows(hc::kBlockRows);
    rows[0].open = -0.0F;
    rows[0].volume = INT64_MIN;
    rows[1].volume = INT64_MAX;
    auto raw = hc::encode_columns(rows);
    auto compressed = hc::compress_block(raw);
    compare(hc::decode_columns(hc::decompress_block(compressed, static_cast<uint32_t>(raw.size())), hc::kBlockRows), rows);
    check(hc::compress_block(raw) == compressed, "compression is nondeterministic within one toolchain");
    rejects([&] { hc::decode_columns(raw, hc::kBlockRows - 1); }, "row count mismatch");
    rejects([&] { hc::decompress_block(compressed, static_cast<uint32_t>(raw.size() + 1)); }, "content size mismatch");
    auto bad = compressed;
    bad.back() ^= 1;
    rejects([&] { hc::decompress_block(bad, static_cast<uint32_t>(raw.size())); }, "checksum mismatch");
    bad = compressed; bad[4] &= static_cast<uint8_t>(~4U);
    rejects([&] { hc::decompress_block(bad, static_cast<uint32_t>(raw.size())); }, "checksum flag absent");
    bad = compressed; bad.insert(bad.end(), compressed.begin(), compressed.end());
    rejects([&] { hc::decompress_block(bad, static_cast<uint32_t>(raw.size())); }, "concatenated frames");
    bad = compressed; bad.pop_back();
    rejects([&] { hc::decompress_block(bad, static_cast<uint32_t>(raw.size())); }, "truncated frame");
    bad = raw; bad[12] = 1;
    rejects([&] { hc::decode_columns(bad, hc::kBlockRows); }, "reserved bit");
    bad = raw; bad.back() = 0; bad.pop_back();
    rejects([&] { hc::decode_columns(bad, hc::kBlockRows); }, "truncated columns");
    auto small = make_rows(2);
    small[1].timestamp_ms = small[0].timestamp_ms;
    rejects([&] { hc::encode_columns(small); }, "duplicate timestamps");
    small = make_rows(2); small[0].open = std::numeric_limits<float>::infinity();
    rejects([&] { hc::encode_columns(small); }, "nonfinite price");
    rejects([&] { hc::encode_columns({}); }, "empty columns");
    rejects([&] { hc::encode_columns(make_rows(hc::kBlockRows + 1)); }, "oversized columns");
    small = make_rows(2); small[0].timestamp_ms = INT64_MAX - 2; small[1].timestamp_ms = INT64_MAX;
    compare(hc::decode_columns(hc::encode_columns(small), 2), small);
    bad = hc::encode_columns(make_rows(2));
    hc::detail::put_be(bad, 16, static_cast<uint64_t>(INT64_MAX - 1), 8);
    rejects([&] { hc::decode_columns(bad, 2); }, "timestamp addition overflow");
    bad = hc::encode_columns(make_rows(3)); bad[27] = 0x01;
    auto decreasing = hc::decode_columns(bad, 3);
    check(decreasing[2].timestamp_ms - decreasing[1].timestamp_ms == 59999, "negative delta-of-delta");
}

void pack() {
    Workspace workspace;
    const auto golden_bytes = hc::read_file(fs::path(HC_SOURCE_DIR) / "tests/golden/pack-v1.json", hc::kMaxMetadataBytes);
    const auto golden = Json::parse(golden_bytes);
    const auto golden_manifest = hc::parse_manifest(golden.at("manifest").dump() + "\n");
    const auto golden_path = workspace.root / "golden.r2b";
    hc::write_new_file(golden_path, hc::unhex(golden.at("pack_hex").get<std::string>()));
    const auto& golden_entry = golden_manifest.entries.front();
    hc::verify_pack(golden_path, *golden_entry.pack, hc::pack_metadata(golden_entry));
    const auto rows = make_rows(2050);
    const auto file = workspace.root / "pack.r2b";
    auto entry = write_entry(file, rows, {0, rows.back().timestamp_ms + 1});
    hc::verify_pack(file, *entry.pack, hc::pack_metadata(entry));
    auto same = write_entry(workspace.root / "same.r2b", rows, entry.coverage);
    check(same.pack->sha256 == entry.pack->sha256, "identical pack output differs");
    auto bytes = hc::read_file(file, hc::kMaxObjectBytes);
    const auto range = [&bytes](uint64_t offset, uint64_t size) {
        check(offset <= bytes.size() && size <= bytes.size() - offset, "test range outside bytes");
        return hc::Bytes(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                         bytes.begin() + static_cast<std::ptrdiff_t>(offset + size));
    };
    auto index = hc::read_pack_index(range, *entry.pack, hc::pack_metadata(entry));
    check(index.blocks.size() == 3, "pack block count");
    std::vector<hc::Row> decoded;
    for (const auto& block : index.blocks) {
        auto values = hc::read_pack_block(range, block);
        decoded.insert(decoded.end(), values.begin(), values.end());
    }
    compare(decoded, rows);
    auto wrong = hc::pack_metadata(entry); wrong.source_version++;
    rejects([&] { hc::read_pack_index(range, *entry.pack, wrong); }, "catalog metadata mismatch");
    bytes[160 + 48] ^= 1;
    rejects([&] { hc::read_pack_index(range, *entry.pack, hc::pack_metadata(entry)); }, "index digest");
    bytes[160 + 48] ^= 1;
    const auto payload = static_cast<size_t>(index.blocks[0].offset);
    bytes[payload + 10] ^= 1;
    rejects([&] { hc::read_pack_block(range, index.blocks[0]); }, "block digest");
    bytes[payload + 10] ^= 1;
    auto descriptor = *entry.pack;
    hc::detail::put_be(bytes, 160 + 32, UINT64_MAX, 8);
    const auto index_bytes = hc::kPackHeaderBytes + index.blocks.size() * hc::kTocEntryBytes;
    descriptor.index_sha256 = hc::sha256(hc::Bytes(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(index_bytes)));
    rejects([&] { hc::read_pack_index(range, descriptor, hc::pack_metadata(entry)); }, "authenticated bad offset");
    const auto short_read = [&range](uint64_t offset, uint64_t size) {
        auto result = range(offset, size); result.pop_back(); return result;
    };
    rejects([&] { hc::read_pack_index(short_read, *entry.pack, hc::pack_metadata(entry)); }, "short Range GET");
    rejects([&] { write_entry(workspace.root / "outside", rows, {1001, rows.back().timestamp_ms + 1}); }, "outside coverage");
}

void catalog() {
    const auto id = hc::series_id(identity());
    auto entry = empty_entry(1000, 2000);
    auto original = manifest(entry);
    const auto text = hc::serialize_manifest(original);
    check(hc::serialize_manifest(hc::parse_manifest(text)) == text, "manifest roundtrip");
    entry.source_version++;
    entry.data_version = hc::sha256(std::string("changed semantics"));
    check(hc::series_id(entry.identity) == id, "series id depends on version");
    auto changed_identity = identity(); changed_identity.market = "OTHER";
    check(hc::series_id(changed_identity) != id, "series namespace collision");
    auto bad = original;
    bad.entries.push_back(empty_entry(1500, 3000));
    rejects([&] { hc::serialize_manifest(bad); }, "overlap");
    bad = original; bad.entries[0].identity.adjust = "forward";
    rejects([&] { hc::serialize_manifest(bad); }, "unsupported adjust");
    bad = original; bad.entries[0].row_count = 1;
    rejects([&] { hc::serialize_manifest(bad); }, "rows without object");
    auto object = Json::parse(text);
    object["entries"][0]["coverage_complete"] = false;
    rejects([&] { hc::parse_manifest(object.dump() + "\n"); }, "incomplete interval");
    object = Json::parse(text); object["extra"] = true;
    rejects([&] { hc::parse_manifest(object.dump() + "\n"); }, "unknown field");
    object = Json::parse(text); object["entries"][0]["rows"] = -1;
    rejects([&] { hc::parse_manifest(object.dump() + "\n"); }, "negative unsigned field");
    rejects([&] { hc::parse_manifest(text + "\n"); }, "noncanonical metadata");
    rejects([&] { hc::parse_manifest("{\"a\":1,\"a\":2}\n"); }, "duplicate JSON key");
    rejects([&] { hc::parse_manifest(std::string(hc::kMaxMetadataBytes + 1, ' ')); }, "JSON size budget");
    rejects([&] { hc::parse_manifest(std::string(50, '[') + std::string(50, ']') + "\n"); }, "JSON depth budget");
    const auto hash = hc::sha256(text);
    hc::Pointer pointer{"fixture-epoch-1", UINT64_MAX, "manifests/v1/" + hc::hex(hash) + ".json", hash};
    check(hc::parse_pointer(hc::serialize_pointer(pointer)).publication_seq == UINT64_MAX, "uint64 JSON precision");
    pointer.manifest_key = "../../bad";
    rejects([&] { hc::serialize_pointer(pointer); }, "unsafe pointer key");
    auto unsafe = identity(); unsafe.symbol = "../SECRET";
    rejects([&] { hc::series_id(unsafe); }, "unsafe identity");
}

void store() {
    Workspace workspace;
    const auto root = workspace.root / "store";
    hc::LocalStore::initialize(root);
    hc::LocalStore local(root);
    auto first = manifest(empty_entry(0, 10000, 1));
    rejects([&] { local.publish(first, 0, hc::FailPoint::after_objects); }, "crash after objects");
    rejects([&] { local.load_snapshot(); }, "pointer missing before first commit");
    rejects([&] { local.publish(first, 0, hc::FailPoint::after_manifest); }, "crash before pointer");
    check(!fs::exists(root / "current.json"), "uncommitted manifest became visible");
    rejects([&] { local.publish(first, 0, hc::FailPoint::after_pointer); }, "lost pointer acknowledgement");
    check(local.load_snapshot().pointer.publication_seq == 1, "pointer was not committed");
    check(!fs::exists(root / "checkpoint.json"), "checkpoint unexpectedly committed");
    const auto pointer1 = local.publish(first, 0);
    check(pointer1.publication_seq == 1 && fs::exists(root / "checkpoint.json"), "lost ACK recovery duplicated commit");
    hc::detail::File checkpoint(root / "checkpoint.json", O_WRONLY | O_TRUNC);
    checkpoint.write(0, hc::Bytes{'b', 'a', 'd'}); checkpoint.sync();
    check(local.publish(first, 0).publication_seq == 1, "checkpoint corruption prevented recovery");
    auto second = manifest(empty_entry(0, 10000, 2));
    check(local.publish(second, 1).publication_seq == 2, "second publish");
    rejects([&] { local.publish(first, 2); }, "stale source snapshot");
    rejects([&] { local.publish(first, 0); }, "stale CAS");
    auto changed = second; changed.entries[0].data_version = hc::sha256(std::string("new semantics"));
    rejects([&] { local.publish(changed, 2); }, "same source version changed content");
    const auto rolled = local.rollback(pointer1.manifest_key, pointer1.manifest_sha256, 2);
    check(rolled.publication_seq == 3 && rolled.manifest_sha256 == pointer1.manifest_sha256,
          "rollback did not advance publication sequence");
    rejects([&] { local.load_snapshot(4); }, "reader monotonic sequence floor");
    {
        hc::detail::File lock(root / "writer.lock", O_RDWR);
        check(::flock(lock.fd(), LOCK_EX | LOCK_NB) == 0, "test writer lock");
        rejects([&] { local.publish(second, 3); }, "concurrent writer");
    }
    const auto pack_file = workspace.root / "pack.r2b";
    const auto rows = make_rows(4);
    auto entry = write_entry(pack_file, rows, {0, 200000}, 4);
    rejects([&] { local.publish(manifest(entry), 3); }, "missing pack cannot commit");
    local.put_file(entry.pack->key, pack_file, entry.pack->sha256);
    local.put_file(entry.pack->key, pack_file, entry.pack->sha256);
    check(local.publish(manifest(entry), 3).publication_seq == 4, "pack publish");
    {
        hc::detail::File corrupt(local.object_path(entry.pack->key), O_WRONLY);
        corrupt.write(0, hc::Bytes{0}); corrupt.sync();
    }
    rejects([&] { local.put_file(entry.pack->key, pack_file, entry.pack->sha256); }, "immutable overwrite");
    rejects([&] { local.object_path("../secret"); }, "unsafe object key");
    const auto link_root = workspace.root / "symlink-store";
    fs::create_directory_symlink(root, link_root);
    rejects([&] { hc::LocalStore unsafe(link_root); }, "symlink store");
    rejects([&] { hc::LocalStore::initialize(root); }, "initialize existing directory");
    fs::create_directory(workspace.root / "outside");
    fs::create_directory_symlink(workspace.root / "outside", workspace.root / "link");
    rejects([&] { hc::create_new_directory(workspace.root / "link" / "new"); }, "symlink output parent");
    check(!fs::exists(workspace.root / "outside" / "new"), "symlink check wrote outside output");
}

void reader() {
    Workspace workspace;
    const auto root = workspace.root / "store";
    hc::LocalStore::initialize(root);
    hc::LocalStore local(root);
    auto rows = make_rows(2050);
    const auto file = workspace.root / "pack.r2b";
    auto entry = write_entry(file, rows, {0, rows.back().timestamp_ms + 60000});
    local.put_file(entry.pack->key, file, entry.pack->sha256);
    auto catalog = manifest(entry);
    const auto end = entry.coverage.end_ms;
    catalog.entries.push_back(empty_entry(end, end + 60000));
    const auto pointer1 = local.publish(catalog, 0);
    auto snapshot = std::make_shared<const hc::Snapshot>(local.load_snapshot());
    hc::Query query{identity(), version(), {rows[1000].timestamp_ms, rows[1100].timestamp_ms}, 500000};
    check(hc::plan_query(snapshot, query).result == hc::PlanResult::bypass, "default must bypass");
    check(hc::plan_query(snapshot, query, true, false).result == hc::PlanResult::bypass, "unhealthy must bypass");
    const auto plan = hc::plan_query(snapshot, query, true);
    check(plan.result == hc::PlanResult::hit, "complete request should hit");
    std::vector<hc::Row> received;
    const auto result = hc::read_plan(local, plan, [&received](const std::vector<hc::Row>& batch) {
        received.insert(received.end(), batch.begin(), batch.end());
    });
    compare(received, std::vector<hc::Row>(rows.begin() + 1000, rows.begin() + 1100));
    check(result.rows == 100 && result.last_ms == rows[1099].timestamp_ms, "end must be exclusive");
    query.max_count = 3;
    check(hc::read_plan(local, hc::plan_query(snapshot, query, true)).last_ms == rows[1002].timestamp_ms,
          "max_count must select earliest rows");
    query.range = {end, end + 60000};
    const auto empty_plan = hc::plan_query(snapshot, query, true);
    check(empty_plan.result == hc::PlanResult::hit && hc::read_plan(local, empty_plan).rows == 0,
          "confirmed empty range must hit");
    query.range.end_ms++;
    check(hc::plan_query(snapshot, query, true).result == hc::PlanResult::miss, "uncovered empty is MISS");
    query.range = {0, end}; query.data_version = hc::sha256(std::string("wrong version"));
    check(hc::plan_query(snapshot, query, true).result == hc::PlanResult::miss, "version mismatch");
    query.data_version = version(); query.identity.period_seconds = 300;
    check(hc::plan_query(snapshot, query, true).result == hc::PlanResult::miss, "higher period bypasses cache");
    query.identity = identity(); query.max_count = 0;
    check(hc::plan_query(snapshot, query, true).result == hc::PlanResult::error, "invalid query");
    const auto newer = manifest(empty_entry(0, end + 60000, 2));
    local.publish(newer, 1);
    check(hc::read_plan(local, plan).rows == 100 && plan.snapshot->pointer.publication_seq == pointer1.publication_seq,
          "in-flight request changed generation");
    query = {identity(), version(), {0, end + 60000}, 500000};
    auto gap = std::make_shared<hc::Snapshot>(*snapshot);
    gap->manifest.entries[1].coverage.start_ms += 1;
    check(hc::plan_query(gap, query, true).result == hc::PlanResult::miss, "internal coverage gap");
    {
        const auto range = hc::file_range_reader(local.object_path(entry.pack->key), entry.pack->bytes);
        const auto index = hc::read_pack_index(range, *entry.pack, hc::pack_metadata(entry));
        hc::detail::File corrupt(local.object_path(entry.pack->key), O_WRONLY);
        corrupt.write(index.blocks[1].offset, hc::Bytes{0}); corrupt.sync();
    }
    query = {identity(), version(), {0, end}, 500000};
    size_t delivered = 0;
    rejects([&] { hc::read_plan(local, hc::plan_query(snapshot, query, true),
        [&delivered](const std::vector<hc::Row>& batch) { delivered += batch.size(); }); }, "midstream corruption");
    check(delivered == hc::kBlockRows, "valid prefix should be provisional before stream failure");
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::runtime_error("one suite argument required");
        const std::string suite = argv[1];
        if (suite == "columns") columns();
        else if (suite == "pack") pack();
        else if (suite == "catalog") catalog();
        else if (suite == "store") store();
        else if (suite == "reader") reader();
        else throw std::runtime_error("unknown suite");
        std::cout << "PASS " << suite << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
