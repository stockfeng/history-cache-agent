#pragma once

#include "history_cache/pack.h"

#include <nlohmann/json.hpp>
#include <optional>

namespace history_cache {

struct CatalogEntry {
    SeriesIdentity identity;
    Digest data_version{};
    uint64_t source_version = 0;
    Coverage coverage;
    uint64_t row_count = 0;
    std::optional<PackDescriptor> pack;
    bool nullable_prices = false;
};

struct Manifest {
    std::string dataset_epoch;
    std::vector<CatalogEntry> entries;
};

struct Pointer {
    std::string dataset_epoch;
    uint64_t publication_seq = 0;
    std::string manifest_key;
    Digest manifest_sha256{};
};

struct Snapshot {
    Pointer pointer;
    Manifest manifest;
};

void validate_manifest(const Manifest& manifest);
void validate_publication_transition(const Manifest& before, const Manifest& after);
// Explicit whole-coverage replacement only; ordinary publication still rejects epochs.
void validate_epoch_replacement(const Manifest& before, const Manifest& after);
PackMetadata pack_metadata(const CatalogEntry& entry);
nlohmann::json identity_json(const SeriesIdentity& identity);
SeriesIdentity identity_from_json(const nlohmann::json& value);
std::string serialize_manifest(const Manifest& manifest);
Manifest parse_manifest(const std::string& text);
std::string serialize_pointer(const Pointer& pointer);
Pointer parse_pointer(const std::string& text);

}  // namespace history_cache
