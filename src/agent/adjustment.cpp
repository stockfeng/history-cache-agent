#include "history_cache/adjustment.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace history_cache {
namespace {
void require(bool condition, const char* message) {
    if (!condition) throw Error(ErrorCode::invalid, message);
}
void positive(double value) {
    require(std::isfinite(value) && value > 0, "invalid adjustment multiplier");
}
double rounded(double value) {
    require(std::isfinite(value) && std::isfinite(value * 100.0), "adjusted price overflow");
    return std::round(value * 100.0) / 100.0;
}
}  // namespace

std::vector<Row> adjust_native_rows(const std::string& symbol,
    const std::vector<Row>& rows, const std::vector<int64_t>& local_days,
    const AdjustmentSnapshot& factors, AdjustmentMode mode) {
    require(mode == AdjustmentMode::forward || mode == AdjustmentMode::backward, "unknown adjustment mode");
    require(factors.model == AdjustmentModel::cumulative || factors.model == AdjustmentModel::futu_ab,
            "unknown adjustment model");
    require(rows.size() <= 5000 && rows.size() == local_days.size(), "adjustment row bound");
    require(factors.events.size() <= 20000, "adjustment event bound");
    require(!symbol.empty() && symbol == factors.symbol && factors.factor_set_hash != Digest{},
            "adjustment identity/version missing");
    require(factors.complete && factors.first_day < factors.end_day, "incomplete factor coverage");
    require(factors.allow_empty || !factors.events.empty(), "empty factor set not permitted");
    positive(factors.carry_factor);
    require(!factors.events.empty() || factors.carry_factor == 1.0,
            "carry-only history must retain its source event");
    std::vector<int64_t> days;
    for (const auto& event : factors.events) {
        require(days.empty() || days.back() < event.day, "duplicate or unordered factor day");
        if (factors.model == AdjustmentModel::cumulative) positive(event.cumulative);
        else {
            positive(event.forward_a);
            positive(event.backward_a);
            require(std::isfinite(event.forward_b) && std::isfinite(event.backward_b), "invalid affine offset");
        }
        days.push_back(event.day);
    }
    for (size_t i = 0; i < rows.size(); ++i) {
        require(rows[i].native.has_value(), "adjustment requires original native64 prices");
        (void)canonical_native(rows[i]);
        require(local_days[i] >= factors.first_day && local_days[i] < factors.end_day,
                "factor coverage missing for row");
        require(i == 0 || (rows[i - 1].timestamp_ms < rows[i].timestamp_ms && local_days[i - 1] <= local_days[i]),
                "unordered adjustment input");
    }
    if (rows.empty()) return {};
    const auto count = days.size();
    auto index_for = [&](int64_t day) {
        return static_cast<size_t>(std::upper_bound(days.begin(), days.end(), day) - days.begin());
    };
    auto cumulative = [&](int64_t day) {
        const auto index = index_for(day);
        return index == 0 ? factors.carry_factor : factors.events[index - 1].cumulative;
    };
    std::vector<double> suffix_a(count + 1, 1.0), suffix_b(count + 1, 0.0);
    std::vector<double> prefix_a(count + 1, 1.0), prefix_b(count + 1, 0.0);
    if (factors.model == AdjustmentModel::futu_ab) {
        for (size_t i = count; i > 0; --i) {
            const auto& event = factors.events[i - 1];
            suffix_a[i - 1] = event.forward_a * suffix_a[i];
            suffix_b[i - 1] = event.forward_b * suffix_a[i] + suffix_b[i];
        }
        for (size_t i = 0; i < count; ++i) {
            const auto& event = factors.events[i];
            prefix_a[i + 1] = event.backward_a * prefix_a[i];
            prefix_b[i + 1] = event.backward_b * prefix_a[i] + prefix_b[i];
        }
    }
    const double anchor = cumulative(local_days.back());
    auto result = rows;
    for (size_t i = 0; i < result.size(); ++i) {
        auto& prices = result[i].native->prices;
        const auto index = index_for(local_days[i]);
        for (auto& price : prices) {
            if (price == kDdbNullPrice) continue;
            if (factors.model == AdjustmentModel::cumulative) {
                // The upstream allows an explicitly empty factor set without rounding.
                if (!factors.events.empty()) {
                    const double factor = cumulative(local_days[i]);
                    price = rounded(price * (mode == AdjustmentMode::forward ? factor / anchor : factor));
                }
            } else if (!factors.events.empty()) {
                price = mode == AdjustmentMode::forward ? price * suffix_a[index] + suffix_b[index]
                                                       : price * prefix_a[index] + prefix_b[index];
            }
            require(std::isfinite(price) && std::abs(price) <= std::numeric_limits<float>::max(),
                    "adjusted price cannot be encoded by client protocol");
        }
        const auto project = [](double price) {
            return price == kDdbNullPrice ? std::numeric_limits<float>::quiet_NaN() : static_cast<float>(price);
        };
        result[i].open = project(prices[0]);
        result[i].high = project(prices[1]);
        result[i].low = project(prices[2]);
        result[i].close = project(prices[3]);
        (void)canonical_native(result[i]);
    }
    return result;
}
}  // namespace history_cache
