#include "history_cache/catalog.h"

#include "binary.h"

#include <set>
#include <tuple>

namespace history_cache {
namespace {

using Json = nlohmann::json;

void fields(const Json& value, std::initializer_list<const char*> names) {
    detail::require(value.is_object() && value.size() == names.size(), "JSON object fields mismatch");
    for (auto name : names) detail::require(value.contains(name), std::string("missing JSON field: ") + name);
}

uint64_t integer(const Json& value, const char* name, uint64_t max = UINT64_MAX) {
    const auto& item = value.at(name);
    detail::require(item.is_number_unsigned(), std::string("unsigned integer required: ") + name);
    const auto result = item.get<uint64_t>();
    detail::require(result <= max, std::string("JSON integer out of bounds: ") + name);
    return result;
}

std::string string(const Json& value, const char* name, size_t max = 256) {
    const auto& item = value.at(name);
    detail::require(item.is_string(), std::string("JSON string required: ") + name);
    auto text = item.get<std::string>();
    detail::require(text.size() <= max, std::string("JSON string exceeds budget: ") + name);
    return text;
}

Json parse_json(const std::string& text) {
    detail::require(!text.empty() && text.size() <= kMaxMetadataBytes, "JSON exceeds metadata budget");
    size_t values = 0;
    const auto callback = [&values](int depth, Json::parse_event_t, Json&) {
        detail::require(depth <= 12 && ++values <= 100000, "JSON nesting or item budget exceeded");
        return true;
    };
    try {
        auto result = Json::parse(text, callback);
        detail::require(result.dump() + "\n" == text, "JSON must be canonical (no duplicate or unknown fields)");
        return result;
    } catch (const Json::exception& error) {
        throw Error(ErrorCode::corrupt, std::string("invalid JSON: ") + error.what());
    }
}

void validate_epoch(const std::string& epoch) {
    SeriesIdentity identity{epoch, "TEST", "FIXTURE", 60, "none"};
    validate_identity(identity);
}

void validate_pointer(const Pointer& pointer) {
    validate_epoch(pointer.dataset_epoch);
    detail::require(pointer.publication_seq > 0 && pointer.manifest_sha256 != Digest{} &&
                    pointer.manifest_key == "manifests/v1/" + hex(pointer.manifest_sha256) + ".json",
                    "invalid pointer reference", ErrorCode::invalid);
}

Json entry_json(const CatalogEntry& entry) {
    Json object = nullptr;
    if (entry.pack) {
        object = {{"bytes", entry.pack->bytes}, {"index_sha256", hex(entry.pack->index_sha256)},
                  {"key", entry.pack->key}, {"sha256", hex(entry.pack->sha256)}};
    }
    return {{"identity", identity_json(entry.identity)}, {"series_id", hex(series_id(entry.identity))},
            {"data_version", hex(entry.data_version)}, {"source_version", entry.source_version},
            {"coverage_start_ms", entry.coverage.start_ms}, {"coverage_end_ms", entry.coverage.end_ms},
            {"coverage_complete", true}, {"rows", entry.row_count}, {"object", object}};
}

}  // namespace

nlohmann::json identity_json(const SeriesIdentity& identity) {
    validate_identity(identity);
    return {{"dataset", identity.dataset}, {"market", identity.market}, {"symbol", identity.symbol},
            {"period_seconds", identity.period_seconds}, {"adjust", identity.adjust}};
}

SeriesIdentity identity_from_json(const nlohmann::json& value) {
    fields(value, {"dataset", "market", "symbol", "period_seconds", "adjust"});
    SeriesIdentity identity{string(value, "dataset", 128), string(value, "market", 128),
                            string(value, "symbol", 128),
                            static_cast<uint32_t>(integer(value, "period_seconds", UINT32_MAX)),
                            string(value, "adjust", 16)};
    validate_identity(identity);
    return identity;
}

PackMetadata pack_metadata(const CatalogEntry& entry) {
    return {series_id(entry.identity), entry.data_version, entry.source_version, entry.coverage, entry.row_count};
}

void validate_manifest(const Manifest& manifest) {
    validate_epoch(manifest.dataset_epoch);
    detail::require(!manifest.entries.empty() && manifest.entries.size() <= kMaxCatalogEntries,
                    "invalid catalog entry count", ErrorCode::resource_limit);
    std::tuple<std::string, int64_t> previous_key;
    const CatalogEntry* previous = nullptr;
    for (const auto& entry : manifest.entries) {
        validate_identity(entry.identity);
        validate_coverage(entry.coverage);
        detail::require(entry.identity.period_seconds == 60 && entry.identity.adjust == "none",
                        "only none/1m catalog entries are supported", ErrorCode::invalid);
        detail::require(entry.source_version > 0 && entry.data_version != Digest{} && entry.row_count <= kMaxRows,
                        "invalid catalog version or row limit", ErrorCode::invalid);
        const auto id = hex(series_id(entry.identity));
        const auto key = std::make_tuple(id, entry.coverage.start_ms);
        if (previous) {
            detail::require(previous_key < key, "catalog must be sorted by series_id and coverage start",
                            ErrorCode::invalid);
            if (std::get<0>(previous_key) == id) {
                detail::require(canonical_identity(previous->identity) == canonical_identity(entry.identity) &&
                                previous->data_version == entry.data_version &&
                                previous->coverage.end_ms <= entry.coverage.start_ms,
                                "series identity collision, version conflict or overlapping coverage", ErrorCode::invalid);
            }
        }
        detail::require(bool(entry.pack) == (entry.row_count != 0),
                        "empty coverage must have zero rows and no object", ErrorCode::invalid);
        if (entry.pack) {
            const auto& pack = *entry.pack;
            detail::require(pack.bytes > kPackHeaderBytes && pack.bytes <= kMaxObjectBytes &&
                            pack.sha256 != Digest{} && pack.index_sha256 != Digest{} &&
                            pack.key == "data/v1/" + hex(pack.sha256) + ".r2b",
                            "invalid content-addressed object reference", ErrorCode::invalid);
        }
        previous = &entry;
        previous_key = key;
    }
}

void validate_publication_transition(const Manifest& before, const Manifest& after) {
    validate_manifest(before);
    validate_manifest(after);
    detail::require(before.dataset_epoch == after.dataset_epoch,
                    "epoch migration requires a separate store", ErrorCode::conflict);
    std::vector<SeriesId> old_ids;
    old_ids.reserve(before.entries.size());
    for (const auto& entry : before.entries) old_ids.push_back(series_id(entry.identity));
    for (const auto& entry : after.entries) {
        const auto id = series_id(entry.identity);
        for (size_t i = 0; i < before.entries.size(); ++i) {
            const auto& old = before.entries[i];
            if (id != old_ids[i] || entry.coverage.start_ms >= old.coverage.end_ms ||
                old.coverage.start_ms >= entry.coverage.end_ms) continue;
            detail::require(canonical_identity(entry.identity) == canonical_identity(old.identity),
                            "series identity collision", ErrorCode::conflict);
            detail::require(entry.source_version >= old.source_version,
                            "stale source version cannot replace newer coverage", ErrorCode::conflict);
            if (entry.source_version == old.source_version) {
                const bool same_object = (!entry.pack && !old.pack) ||
                    (entry.pack && old.pack && entry.pack->sha256 == old.pack->sha256);
                detail::require(entry.data_version == old.data_version &&
                                entry.coverage.start_ms == old.coverage.start_ms &&
                                entry.coverage.end_ms == old.coverage.end_ms &&
                                entry.row_count == old.row_count && same_object,
                                "same source version cannot change an interval snapshot", ErrorCode::conflict);
            }
        }
    }
}

std::string serialize_manifest(const Manifest& manifest) {
    validate_manifest(manifest);
    Json entries = Json::array();
    for (const auto& entry : manifest.entries) entries.push_back(entry_json(entry));
    Json value{{"schema_version", 1}, {"kind", "complete-interval-snapshot"},
               {"dataset_epoch", manifest.dataset_epoch}, {"entries", entries}};
    auto text = value.dump() + "\n";
    detail::require(text.size() <= kMaxMetadataBytes, "manifest exceeds metadata budget", ErrorCode::resource_limit);
    return text;
}

Manifest parse_manifest(const std::string& text) {
    const auto value = parse_json(text);
    fields(value, {"schema_version", "kind", "dataset_epoch", "entries"});
    detail::require(integer(value, "schema_version") == 1 &&
                    string(value, "kind") == "complete-interval-snapshot", "unsupported manifest schema");
    Manifest result{string(value, "dataset_epoch", 128), {}};
    const auto& entries = value.at("entries");
    detail::require(entries.is_array() && entries.size() <= kMaxCatalogEntries, "invalid manifest entry list");
    for (const auto& item : entries) {
        fields(item, {"identity", "series_id", "data_version", "source_version", "coverage_start_ms",
                      "coverage_end_ms", "coverage_complete", "rows", "object"});
        CatalogEntry entry;
        entry.identity = identity_from_json(item.at("identity"));
        detail::require(string(item, "series_id") == hex(series_id(entry.identity)), "series_id mismatch");
        detail::require(item.at("coverage_complete").is_boolean() && item.at("coverage_complete").get<bool>(),
                        "partial coverage is not publishable");
        entry.data_version = parse_digest(string(item, "data_version"));
        entry.source_version = integer(item, "source_version");
        entry.coverage = {static_cast<int64_t>(integer(item, "coverage_start_ms", INT64_MAX)),
                          static_cast<int64_t>(integer(item, "coverage_end_ms", INT64_MAX))};
        entry.row_count = integer(item, "rows", kMaxRows);
        const auto& object = item.at("object");
        if (!object.is_null()) {
            fields(object, {"bytes", "index_sha256", "key", "sha256"});
            entry.pack = PackDescriptor{string(object, "key"), integer(object, "bytes", kMaxObjectBytes),
                                       parse_digest(string(object, "sha256")),
                                       parse_digest(string(object, "index_sha256"))};
        }
        result.entries.push_back(entry);
    }
    validate_manifest(result);
    detail::require(serialize_manifest(result) == text, "noncanonical manifest");
    return result;
}

std::string serialize_pointer(const Pointer& pointer) {
    validate_pointer(pointer);
    Json value{{"schema_version", 1}, {"dataset_epoch", pointer.dataset_epoch},
               {"publication_seq", pointer.publication_seq}, {"manifest_key", pointer.manifest_key},
               {"manifest_sha256", hex(pointer.manifest_sha256)}};
    return value.dump() + "\n";
}

Pointer parse_pointer(const std::string& text) {
    const auto value = parse_json(text);
    fields(value, {"schema_version", "dataset_epoch", "publication_seq", "manifest_key", "manifest_sha256"});
    detail::require(integer(value, "schema_version") == 1, "unsupported pointer schema");
    Pointer result{string(value, "dataset_epoch", 128), integer(value, "publication_seq"),
                   string(value, "manifest_key"), parse_digest(string(value, "manifest_sha256"))};
    validate_pointer(result);
    detail::require(serialize_pointer(result) == text, "noncanonical pointer");
    return result;
}

}  // namespace history_cache
