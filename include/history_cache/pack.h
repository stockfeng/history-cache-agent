#pragma once

#include "history_cache/common.h"

#include <functional>

namespace history_cache {

constexpr uint32_t kPackHeaderBytes = 160;
constexpr uint32_t kTocEntryBytes = 80;

struct PackDescriptor {
    std::string key;
    uint64_t bytes = 0;
    Digest sha256{};
    Digest index_sha256{};
};

struct PackMetadata {
    SeriesId series{};
    Digest data_version{};
    uint64_t source_version = 0;
    Coverage coverage;
    uint64_t row_count = 0;
    uint16_t row_schema = 1;
};

struct BlockIndex {
    int64_t first_ms = 0;
    int64_t last_ms = 0;
    uint32_t rows = 0;
    uint32_t raw_bytes = 0;
    uint32_t compressed_bytes = 0;
    uint64_t offset = 0;
    Digest sha256{};
    uint16_t row_schema = 1;
};

struct PackIndex {
    PackMetadata metadata;
    std::vector<BlockIndex> blocks;
};

using RowProvider = std::function<std::vector<Row>(uint64_t offset, uint32_t count)>;
using RangeReader = std::function<Bytes(uint64_t offset, uint64_t size)>;

PackDescriptor write_pack(const std::filesystem::path& path,
                          const PackMetadata& metadata, const RowProvider& provider);
PackIndex read_pack_index(const RangeReader& read, const PackDescriptor& descriptor,
                          const PackMetadata& expected);
std::vector<Row> read_pack_block(const RangeReader& read, const BlockIndex& block);
RangeReader file_range_reader(const std::filesystem::path& path, uint64_t expected_size);
void verify_pack(const RangeReader& read, const PackDescriptor& descriptor,
                 const PackMetadata& metadata);
void verify_pack(const std::filesystem::path& path, const PackDescriptor& descriptor,
                 const PackMetadata& metadata);

}  // namespace history_cache
