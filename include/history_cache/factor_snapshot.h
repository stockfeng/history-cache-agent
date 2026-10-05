#pragma once

#include "history_cache/adjustment.h"

namespace history_cache {

constexpr uint64_t kMaxFactorBytes = 512 * 1024;
constexpr size_t kMaxFactorSnapshots = 16;

struct FactorSnapshot {
    AdjustmentSnapshot factors;
    int64_t observed_at_ms = 0;
    int64_t valid_until_ms = 0;
    std::string source_epoch;
    Digest data_hash{};
    uint64_t data_bytes = 0;
    uint64_t source_revision = 0;
    Digest source_receipt_hash{};
};

// JSON rows retain DDB field names and native numeric types. The source proof
// hash is an audit reference; this parser is not a DDB snapshot verifier.
FactorSnapshot parse_factor_snapshot(const Bytes& bytes, const Digest& expected_hash,
    const std::string& symbol, const std::string& market, int64_t now_ms, bool require_fresh = true);
AdjustmentSnapshot select_factor_window(const FactorSnapshot& snapshot,
    int64_t first_day, int64_t end_day);

// Reference snapshots are metadata-only until their immutable data is bound.
FactorSnapshot resolve_factor_snapshot(const Bytes& reference, const Digest& expected_hash,
    const Bytes& data, const std::string& symbol, const std::string& market, int64_t now_ms,
    bool require_fresh = true);

}  // namespace history_cache
