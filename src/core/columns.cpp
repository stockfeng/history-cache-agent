#include "history_cache/columns.h"

#include "binary.h"

#include <limits>
#include <memory>
#include <zstd.h>

namespace history_cache {
namespace {

uint64_t zigzag(int64_t value) {
    return (detail::bits<uint64_t>(value) << 1U) ^ (value < 0 ? UINT64_MAX : 0);
}

int64_t unzigzag(uint64_t value) {
    return detail::bits<int64_t>((value >> 1U) ^ (uint64_t(0) - (value & 1U)));
}

void varint(Bytes& bytes, uint64_t value) {
    while (value >= 128) {
        bytes.push_back(static_cast<uint8_t>(value) | 128U);
        value >>= 7U;
    }
    bytes.push_back(static_cast<uint8_t>(value));
}

uint64_t read_varint(const Bytes& bytes, size_t& offset, size_t end) {
    uint64_t result = 0;
    for (size_t i = 0; i < 10; ++i) {
        detail::require(offset < end, "timestamp varint is truncated");
        const uint8_t byte = bytes[offset++];
        detail::require(i != 9 || (byte & 0xfeU) == 0, "timestamp varint overflows");
        result |= uint64_t(byte & 127U) << (7U * i);
        if (!(byte & 128U)) {
            detail::require(i == 0 || byte != 0, "timestamp varint is not canonical");
            return result;
        }
    }
    throw Error(ErrorCode::corrupt, "timestamp varint is unterminated");
}

void zstd_ok(size_t result) {
    if (ZSTD_isError(result))
        throw Error(ErrorCode::corrupt, std::string("Zstd: ") + ZSTD_getErrorName(result));
}

}  // namespace

Bytes encode_columns(const std::vector<Row>& rows, uint16_t schema) {
    detail::require(schema >= 1 && schema <= 4, "unsupported row schema");
    detail::require(!rows.empty() && rows.size() <= kBlockRows, "invalid block row count",
                    ErrorCode::resource_limit);
    for (const auto& row : rows) {
        detail::require((schema >= 3) == row.native.has_value(), "native schema mismatch");
        detail::require(schema == 4 || !has_null_price(row), "NULL requires nullable schema");
        validate_row(row);
        if (schema == 1)
            detail::require(row.turnover == 0 && row.open_interest == 0,
                            "legacy columns cannot discard complete kline fields");
    }
    Bytes timestamps(8);
    detail::put_be(timestamps, 0, static_cast<uint64_t>(rows.front().timestamp_ms), 8);
    int64_t previous_delta = 0;
    for (size_t i = 1; i < rows.size(); ++i) {
        detail::require(rows[i].timestamp_ms > rows[i - 1].timestamp_ms,
                        "timestamps must be strictly increasing", ErrorCode::invalid);
        const int64_t delta = rows[i].timestamp_ms - rows[i - 1].timestamp_ms;
        varint(timestamps, zigzag(i == 1 ? delta : delta - previous_delta));
        previous_delta = delta;
    }
    Bytes bytes(16, 0);
    bytes[0] = schema == 4 ? 3 : schema == 3 ? 2 : 1;
    bytes[1] = 2;
    detail::put_be(bytes, 2, schema >= 2 ? 0x00ff : 0x003f, 2);
    detail::put_be(bytes, 4, rows.size(), 4);
    detail::put_be(bytes, 8, timestamps.size(), 4);
    bytes.insert(bytes.end(), timestamps.begin(), timestamps.end());
    if (schema >= 3) {
        for (size_t i = 0; i < 4; ++i)
            for (const auto& row : rows) detail::append_le(bytes, detail::bits<uint64_t>(row.native->prices[i]), 8);
    } else {
        for (auto member : {&Row::open, &Row::high, &Row::low, &Row::close})
            for (const auto& row : rows) detail::append_le(bytes, detail::bits<uint32_t>(row.*member), 4);
    }
    for (const auto& row : rows) detail::append_le(bytes, detail::bits<uint64_t>(row.volume), 8);
    if (schema == 2) {
        for (const auto& row : rows) {
            (void)canonical_kline(row);
            detail::append_le(bytes, detail::bits<uint64_t>(row.turnover), 8);
        }
        for (const auto& row : rows) detail::append_le(bytes, static_cast<uint64_t>(row.open_interest), 8);
    } else if (schema >= 3) {
        for (const auto& row : rows) {
            (void)canonical_native(row);
            detail::append_le(bytes, static_cast<uint64_t>(row.native->open_oi), 8);
        }
        for (const auto& row : rows) detail::append_le(bytes, static_cast<uint64_t>(row.native->close_oi), 8);
    }
    detail::require(bytes.size() <= kMaxBlockBytes, "column block exceeds budget", ErrorCode::resource_limit);
    return bytes;
}

std::vector<Row> decode_columns(const Bytes& bytes, uint32_t expected_rows, uint16_t schema) {
    detail::require(schema >= 1 && schema <= 4, "unsupported row schema");
    detail::require(expected_rows > 0 && expected_rows <= kBlockRows &&
                    bytes.size() >= 24 && bytes.size() <= kMaxBlockBytes, "invalid column block limits");
    detail::require(bytes[0] == (schema == 4 ? 3 : schema == 3 ? 2 : 1) && bytes[1] == 2 && detail::be(bytes, 2, 2) == (schema >= 2 ? 0x00ffU : 0x003fU) &&
                    detail::be(bytes, 4, 4) == expected_rows && detail::be(bytes, 12, 4) == 0,
                    "column header schema mismatch");
    const uint64_t timestamp_bytes = detail::be(bytes, 8, 4);
    detail::require(timestamp_bytes >= 8 && 16 + timestamp_bytes + uint64_t(expected_rows) * (schema >= 3 ? 56U : schema == 2 ? 40U : 24U) == bytes.size(),
                    "column lengths mismatch");
    const size_t timestamp_end = static_cast<size_t>(16 + timestamp_bytes);
    const uint64_t first = detail::be(bytes, 16, 8);
    detail::require(first <= INT64_MAX, "timestamp is outside nonnegative int64");
    std::vector<Row> rows(expected_rows);
    rows[0].timestamp_ms = static_cast<int64_t>(first);
    int64_t delta = 0;
    size_t offset = 24;
    for (size_t i = 1; i < rows.size(); ++i) {
        const auto difference = unzigzag(read_varint(bytes, offset, timestamp_end));
        if (i == 1) delta = difference;
        else {
            if (difference > 0)
                detail::require(delta <= INT64_MAX - difference, "timestamp delta overflows");
            else
                detail::require(difference > -delta, "timestamp delta is not positive");
            delta += difference;
        }
        detail::require(delta > 0 && rows[i - 1].timestamp_ms <= INT64_MAX - delta,
                        "timestamp is not increasing or overflows");
        rows[i].timestamp_ms = rows[i - 1].timestamp_ms + delta;
    }
    detail::require(offset == timestamp_end, "timestamp column has trailing bytes");
    if (schema >= 3) {
        for (auto& row : rows) row.native.emplace();
        for (size_t i = 0; i < 4; ++i) for (auto& row : rows) {
            row.native->prices[i] = detail::bits<double>(detail::le(bytes, offset, 8)); offset += 8;
        }
    } else for (auto member : {&Row::open, &Row::high, &Row::low, &Row::close}) {
        for (auto& row : rows) {
            row.*member = detail::bits<float>(static_cast<uint32_t>(detail::le(bytes, offset, 4)));
            offset += 4;
        }
    }
    for (auto& row : rows) {
        row.volume = detail::bits<int64_t>(detail::le(bytes, offset, 8));
        offset += 8;
        validate_row(row);
    }
    if (schema == 2) {
        for (auto& row : rows) { row.turnover = detail::bits<double>(detail::le(bytes, offset, 8)); offset += 8; }
        for (auto& row : rows) {
            row.open_interest = detail::bits<int64_t>(detail::le(bytes, offset, 8)); offset += 8;
            (void)canonical_kline(row);
        }
    } else if (schema >= 3) {
        for (auto& row : rows) { row.native->open_oi = detail::bits<int64_t>(detail::le(bytes, offset, 8)); offset += 8; }
        for (auto& row : rows) {
            row.native->close_oi = detail::bits<int64_t>(detail::le(bytes, offset, 8)); offset += 8;
            (void)canonical_native(row);
            detail::require(schema == 4 || !has_null_price(row), "NULL requires nullable schema");
        }
    }
    return rows;
}

Bytes compress_block(const Bytes& bytes) {
    detail::require(!bytes.empty() && bytes.size() <= kMaxBlockBytes,
                    "invalid compression input size", ErrorCode::resource_limit);
    std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context(ZSTD_createCCtx(), ZSTD_freeCCtx);
    detail::require(bool(context), "cannot allocate Zstd compressor", ErrorCode::resource_limit);
    zstd_ok(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_compressionLevel, 1));
    zstd_ok(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, 1));
    zstd_ok(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, 1));
    zstd_ok(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_nbWorkers, 0));
    Bytes result(ZSTD_compressBound(bytes.size()));
    const size_t size = ZSTD_compress2(context.get(), result.data(), result.size(), bytes.data(), bytes.size());
    zstd_ok(size);
    detail::require(size <= kMaxBlockBytes, "compressed block exceeds budget", ErrorCode::resource_limit);
    result.resize(size);
    return result;
}

Bytes decompress_block(const Bytes& bytes, uint32_t expected_bytes) {
    detail::require(expected_bytes > 0 && expected_bytes <= kMaxBlockBytes &&
                    bytes.size() >= 6 && bytes.size() <= kMaxBlockBytes, "invalid decompression limits");
    detail::require(detail::le(bytes, 0, 4) == 0xfd2fb528U &&
                    (bytes[4] & 0x04U) != 0 && (bytes[4] & 0x18U) == 0 &&
                    (bytes[4] & 0x03U) == 0, "Zstd frame requires checksum and no dictionary");
    const auto content_size = ZSTD_getFrameContentSize(bytes.data(), bytes.size());
    detail::require(content_size == expected_bytes, "Zstd content size mismatch or unknown");
    const auto frame_size = ZSTD_findFrameCompressedSize(bytes.data(), bytes.size());
    zstd_ok(frame_size);
    detail::require(frame_size == bytes.size(), "Zstd trailing or concatenated frame");
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> context(ZSTD_createDCtx(), ZSTD_freeDCtx);
    detail::require(bool(context), "cannot allocate Zstd decompressor", ErrorCode::resource_limit);
    zstd_ok(ZSTD_DCtx_setParameter(context.get(), ZSTD_d_windowLogMax, 16));
    Bytes result(expected_bytes);
    ZSTD_inBuffer input{bytes.data(), bytes.size(), 0};
    ZSTD_outBuffer output{result.data(), result.size(), 0};
    while (true) {
        const size_t before_in = input.pos;
        const size_t before_out = output.pos;
        const size_t remaining = ZSTD_decompressStream(context.get(), &output, &input);
        zstd_ok(remaining);
        if (remaining == 0) break;
        detail::require((input.pos != before_in || output.pos != before_out) &&
                        output.pos < output.size && input.pos < input.size, "incomplete Zstd frame");
    }
    detail::require(output.pos == expected_bytes && input.pos == input.size, "Zstd length mismatch");
    return result;
}

}  // namespace history_cache
