#pragma once

#include "history_cache/common.h"

namespace history_cache {

enum class AdjustmentMode { forward, backward };
enum class AdjustmentModel { cumulative, futu_ab };

// Days are exchange-local civil days since 1970-01-01, not UTC dates.
struct AdjustmentEvent {
    int64_t day = 0;
    double cumulative = 1.0;
    double forward_a = 1.0;
    double forward_b = 0.0;
    double backward_a = 1.0;
    double backward_b = 0.0;
};

struct AdjustmentSnapshot {
    std::string symbol;
    Digest factor_set_hash{};
    AdjustmentModel model = AdjustmentModel::cumulative;
    int64_t first_day = 0;
    int64_t end_day = 0;
    bool complete = false;
    bool allow_empty = false;
    double carry_factor = 1.0;
    std::vector<AdjustmentEvent> events;
};

// Input must be the final selected/merged sequence, after max_count truncation.
// Snapshot provenance and freshness are the caller's responsibility.
std::vector<Row> adjust_native_rows(const std::string& symbol,
    const std::vector<Row>& rows, const std::vector<int64_t>& local_days,
    const AdjustmentSnapshot& factors, AdjustmentMode mode);

}  // namespace history_cache
