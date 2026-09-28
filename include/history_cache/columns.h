#pragma once

#include "history_cache/common.h"

namespace history_cache {

Bytes encode_columns(const std::vector<Row>& rows);
std::vector<Row> decode_columns(const Bytes& bytes, uint32_t expected_rows);
Bytes compress_block(const Bytes& bytes);
Bytes decompress_block(const Bytes& bytes, uint32_t expected_bytes);

}  // namespace history_cache

