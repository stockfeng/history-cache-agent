#include "history_cache/pack.h"

#include "binary.h"
#include "history_cache/columns.h"

#include <memory>

namespace history_cache {
namespace {

void validate_metadata(const PackMetadata& metadata) {
    detail::require(metadata.row_schema >= 1 && metadata.row_schema <= 3, "unsupported row schema");
    validate_coverage(metadata.coverage);
    detail::require(metadata.source_version > 0 && metadata.data_version != Digest{} &&
                    metadata.series != SeriesId{}, "pack identity/version is missing", ErrorCode::invalid);
    detail::require(metadata.row_count > 0 && metadata.row_count <= kMaxRows,
                    "pack row count exceeds limits", ErrorCode::resource_limit);
}

Bytes exact_read(const RangeReader& read, uint64_t offset, uint64_t size) {
    auto bytes = read(offset, size);
    detail::require(bytes.size() == size, "range response length mismatch");
    return bytes;
}

}  // namespace

PackDescriptor write_pack(const std::filesystem::path& path,
                          const PackMetadata& metadata, const RowProvider& provider) {
    validate_metadata(metadata);
    detail::require(bool(provider), "row provider is missing", ErrorCode::invalid);
    const uint32_t block_count = static_cast<uint32_t>((metadata.row_count + kBlockRows - 1) / kBlockRows);
    const uint64_t data_offset = kPackHeaderBytes + uint64_t(block_count) * kTocEntryBytes;
    Bytes index(static_cast<size_t>(data_offset), 0);
    detail::File file(path, O_RDWR | O_CREAT | O_EXCL);
    file.write(0, index);
    uint64_t offset = data_offset;
    uint64_t emitted = 0;
    int64_t first_ms = 0;
    int64_t last_ms = -1;
    for (uint32_t seq = 0; seq < block_count; ++seq) {
        const auto count = static_cast<uint32_t>(std::min<uint64_t>(kBlockRows, metadata.row_count - emitted));
        auto rows = provider(emitted, count);
        if (metadata.row_schema == 1) for (const auto& row : rows)
            detail::require(row.turnover == 0 && row.open_interest == 0,
                            "legacy pack cannot discard complete kline fields", ErrorCode::invalid);
        detail::require(rows.size() == count, "row provider returned an incomplete block", ErrorCode::invalid);
        detail::require(rows.front().timestamp_ms >= metadata.coverage.start_ms &&
                        rows.front().timestamp_ms > last_ms && rows.back().timestamp_ms < metadata.coverage.end_ms,
                        "pack rows are outside coverage or not increasing", ErrorCode::invalid);
        auto raw = encode_columns(rows, metadata.row_schema);
        auto compressed = compress_block(raw);
        detail::require(compressed.size() <= kMaxObjectBytes - offset,
                        "pack exceeds object budget", ErrorCode::resource_limit);
        const auto block_hash = sha256(compressed);
        const size_t toc = kPackHeaderBytes + size_t(seq) * kTocEntryBytes;
        detail::put_be(index, toc, seq, 4);
        detail::put_be(index, toc + 4, count, 4);
        detail::put_be(index, toc + 8, static_cast<uint64_t>(rows.front().timestamp_ms), 8);
        detail::put_be(index, toc + 16, static_cast<uint64_t>(rows.back().timestamp_ms), 8);
        detail::put_be(index, toc + 24, raw.size(), 4);
        detail::put_be(index, toc + 28, compressed.size(), 4);
        detail::put_be(index, toc + 32, offset, 8);
        std::copy(block_hash.begin(), block_hash.end(), index.begin() + static_cast<std::ptrdiff_t>(toc + 48));
        file.write(offset, compressed);
        offset += compressed.size();
        emitted += count;
        if (seq == 0) first_ms = rows.front().timestamp_ms;
        last_ms = rows.back().timestamp_ms;
    }
    index[0] = 'R'; index[1] = '2'; index[2] = 'H'; index[3] = '1';
    detail::put_be(index, 4, 1, 2);
    detail::put_be(index, 6, kPackHeaderBytes, 2);
    detail::put_be(index, 8, metadata.row_schema, 2);
    detail::put_be(index, 10, 1, 2);
    detail::put_be(index, 12, 60, 4);
    std::copy(metadata.series.begin(), metadata.series.end(), index.begin() + 16);
    std::copy(metadata.data_version.begin(), metadata.data_version.end(), index.begin() + 32);
    detail::put_be(index, 64, metadata.source_version, 8);
    detail::put_be(index, 72, static_cast<uint64_t>(metadata.coverage.start_ms), 8);
    detail::put_be(index, 80, static_cast<uint64_t>(metadata.coverage.end_ms), 8);
    detail::put_be(index, 88, static_cast<uint64_t>(first_ms), 8);
    detail::put_be(index, 96, static_cast<uint64_t>(last_ms), 8);
    detail::put_be(index, 104, metadata.row_count, 8);
    detail::put_be(index, 112, block_count, 4);
    detail::put_be(index, 116, kTocEntryBytes, 4);
    detail::put_be(index, 120, kPackHeaderBytes, 8);
    detail::put_be(index, 128, data_offset, 8);
    detail::put_be(index, 136, offset - data_offset, 8);
    file.write(0, index);
    file.sync();
    detail::sync_directory(path.parent_path().empty() ? "." : path.parent_path());
    PackDescriptor descriptor;
    descriptor.bytes = offset;
    descriptor.index_sha256 = sha256(index);
    descriptor.sha256 = file_sha256(path, kMaxObjectBytes);
    descriptor.key = "data/v1/" + hex(descriptor.sha256) + ".r2b";
    return descriptor;
}

PackIndex read_pack_index(const RangeReader& read, const PackDescriptor& descriptor,
                          const PackMetadata& expected) {
    validate_metadata(expected);
    detail::require(descriptor.bytes >= kPackHeaderBytes && descriptor.bytes <= kMaxObjectBytes,
                    "invalid object size");
    const auto header = exact_read(read, 0, kPackHeaderBytes);
    detail::require(detail::be(header, 0, 4) == 0x52324831 && detail::be(header, 4, 2) == 1 &&
                    detail::be(header, 6, 2) == kPackHeaderBytes && detail::be(header, 8, 2) == expected.row_schema &&
                    detail::be(header, 10, 2) == 1 && detail::be(header, 12, 4) == 60,
                    "unsupported pack header");
    detail::require(detail::fixed<16>(header, 16) == expected.series &&
                    detail::fixed<32>(header, 32) == expected.data_version &&
                    detail::be(header, 64, 8) == expected.source_version &&
                    detail::be(header, 72, 8) == static_cast<uint64_t>(expected.coverage.start_ms) &&
                    detail::be(header, 80, 8) == static_cast<uint64_t>(expected.coverage.end_ms) &&
                    detail::be(header, 104, 8) == expected.row_count, "pack metadata differs from catalog");
    const uint64_t count = detail::be(header, 112, 4);
    detail::require(count == (expected.row_count + kBlockRows - 1) / kBlockRows &&
                    detail::be(header, 116, 4) == kTocEntryBytes &&
                    detail::be(header, 120, 8) == kPackHeaderBytes &&
                    detail::be(header, 144, 8) == 0 && detail::be(header, 152, 8) == 0,
                    "invalid pack index layout or reserved fields");
    const uint64_t data_offset = kPackHeaderBytes + count * kTocEntryBytes;
    detail::require(data_offset < descriptor.bytes && detail::be(header, 128, 8) == data_offset &&
                    detail::be(header, 136, 8) == descriptor.bytes - data_offset,
                    "invalid pack data offsets");
    const auto toc = exact_read(read, kPackHeaderBytes, count * kTocEntryBytes);
    Sha256 index_hash;
    index_hash.update(header);
    index_hash.update(toc);
    detail::require(index_hash.finish() == descriptor.index_sha256, "pack index SHA-256 mismatch");
    PackIndex result{expected, {}};
    uint64_t row_sum = 0;
    uint64_t offset = data_offset;
    int64_t last_ms = -1;
    for (size_t i = 0; i < count; ++i) {
        const size_t pos = i * kTocEntryBytes;
        const uint64_t first = detail::be(toc, pos + 8, 8);
        const uint64_t last = detail::be(toc, pos + 16, 8);
        detail::require(first <= INT64_MAX && last <= INT64_MAX, "TOC timestamp overflow");
        BlockIndex block;
        block.row_schema = expected.row_schema;
        block.rows = static_cast<uint32_t>(detail::be(toc, pos + 4, 4));
        block.first_ms = static_cast<int64_t>(first);
        block.last_ms = static_cast<int64_t>(last);
        block.raw_bytes = static_cast<uint32_t>(detail::be(toc, pos + 24, 4));
        block.compressed_bytes = static_cast<uint32_t>(detail::be(toc, pos + 28, 4));
        block.offset = detail::be(toc, pos + 32, 8);
        block.sha256 = detail::fixed<32>(toc, pos + 48);
        const auto expected_rows = std::min<uint64_t>(kBlockRows, expected.row_count - row_sum);
        detail::require(detail::be(toc, pos, 4) == i && block.rows == expected_rows && block.rows > 0 &&
                        block.first_ms >= expected.coverage.start_ms && block.first_ms > last_ms &&
                        block.last_ms >= block.first_ms && block.last_ms < expected.coverage.end_ms &&
                        (block.rows != 1 || block.first_ms == block.last_ms) &&
                        block.raw_bytes >= 24 + uint64_t(block.rows) * (expected.row_schema == 3 ? 56U : expected.row_schema == 2 ? 40U : 24U) && block.raw_bytes <= kMaxBlockBytes &&
                        block.compressed_bytes > 0 && block.compressed_bytes <= kMaxBlockBytes &&
                        block.offset == offset && block.compressed_bytes <= descriptor.bytes - offset &&
                        detail::be(toc, pos + 40, 8) == 0, "invalid TOC entry");
        row_sum += block.rows;
        offset += block.compressed_bytes;
        last_ms = block.last_ms;
        result.blocks.push_back(block);
    }
    detail::require(row_sum == expected.row_count && offset == descriptor.bytes &&
                    detail::be(header, 88, 8) == static_cast<uint64_t>(result.blocks.front().first_ms) &&
                    detail::be(header, 96, 8) == static_cast<uint64_t>(result.blocks.back().last_ms),
                    "pack summary differs from index");
    return result;
}

std::vector<Row> read_pack_block(const RangeReader& read, const BlockIndex& block) {
    detail::require(block.compressed_bytes > 0 && block.compressed_bytes <= kMaxBlockBytes,
                    "block exceeds download budget");
    const auto compressed = exact_read(read, block.offset, block.compressed_bytes);
    detail::require(sha256(compressed) == block.sha256, "block SHA-256 mismatch");
    const auto raw = decompress_block(compressed, block.raw_bytes);
    auto rows = decode_columns(raw, block.rows, block.row_schema);
    detail::require(rows.front().timestamp_ms == block.first_ms && rows.back().timestamp_ms == block.last_ms,
                    "decoded timestamps differ from TOC");
    return rows;
}

RangeReader file_range_reader(const std::filesystem::path& path, uint64_t expected_size) {
    detail::require(expected_size > 0 && expected_size <= kMaxObjectBytes, "invalid range reader size");
    auto file = std::make_shared<detail::File>(path, O_RDONLY);
    detail::require(file->size() == expected_size, "object length differs from catalog");
    return [file](uint64_t offset, uint64_t size) { return file->read(offset, size); };
}

void verify_pack(const RangeReader& read, const PackDescriptor& descriptor,
                 const PackMetadata& metadata) {
    Sha256 hash;
    uint64_t consumed = 0;
    const RangeReader checked = [&](uint64_t offset, uint64_t size) {
        detail::require(offset == consumed && offset <= descriptor.bytes && size <= descriptor.bytes - offset,
                        "pack verification must cover consecutive bytes");
        auto bytes = exact_read(read, offset, size);
        hash.update(bytes);
        consumed += size;
        return bytes;
    };
    const auto index = read_pack_index(checked, descriptor, metadata);
    for (const auto& block : index.blocks) (void)read_pack_block(checked, block);
    detail::require(consumed == descriptor.bytes && hash.finish() == descriptor.sha256, "object SHA-256 mismatch");
}

void verify_pack(const std::filesystem::path& path, const PackDescriptor& descriptor,
                 const PackMetadata& metadata) {
    verify_pack(file_range_reader(path, descriptor.bytes), descriptor, metadata);
}

}  // namespace history_cache
