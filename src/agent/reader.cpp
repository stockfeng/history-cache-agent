#include "history_cache/reader.h"

#include "binary.h"

namespace history_cache {

const char* plan_result_name(PlanResult result) {
    switch (result) {
        case PlanResult::hit: return "HIT";
        case PlanResult::miss: return "MISS";
        case PlanResult::bypass: return "BYPASS";
        case PlanResult::error: return "ERROR";
    }
    return "ERROR";
}

Plan plan_query(std::shared_ptr<const Snapshot> snapshot, const Query& query, bool enabled, bool healthy) {
    Plan plan;
    plan.query = query;
    plan.snapshot = std::move(snapshot);
    try {
        validate_identity(query.identity);
        validate_coverage(query.range);
        detail::require(query.max_count > 0 && query.max_count <= kMaxRows && query.data_version != Digest{},
                        "invalid query count or data version", ErrorCode::invalid);
    } catch (const Error& error) {
        plan.result = PlanResult::error;
        plan.reason = error.what();
        return plan;
    }
    if (!enabled || !healthy) {
        plan.result = PlanResult::bypass;
        plan.reason = !enabled ? "disabled" : "unhealthy";
        return plan;
    }
    if (!plan.snapshot) { plan.reason = "no_snapshot"; return plan; }
    if (query.identity.period_seconds != 60 || query.identity.adjust != "none") {
        plan.reason = "unsupported_series";
        return plan;
    }
    try {
        validate_manifest(plan.snapshot->manifest);
    } catch (const Error&) {
        plan.result = PlanResult::bypass;
        plan.reason = "invalid_catalog";
        return plan;
    }
    int64_t cursor = query.range.start_ms;
    const auto id = series_id(query.identity);
    for (size_t i = 0; i < plan.snapshot->manifest.entries.size(); ++i) {
        const auto& entry = plan.snapshot->manifest.entries[i];
        if (series_id(entry.identity) != id || canonical_identity(entry.identity) != canonical_identity(query.identity) ||
            entry.data_version != query.data_version || entry.coverage.end_ms <= cursor) continue;
        if (entry.coverage.start_ms > cursor) break;
        plan.entry_indices.push_back(i);
        cursor = std::min(query.range.end_ms, entry.coverage.end_ms);
        if (cursor == query.range.end_ms) {
            plan.result = PlanResult::hit;
            plan.reason = "complete_coverage";
            return plan;
        }
    }
    plan.entry_indices.clear();
    plan.reason = "uncovered_range_or_version";
    return plan;
}

ReadSummary read_plan(const ObjectReader& store, const Plan& plan,
                     const std::function<void(const std::vector<Row>&)>& on_rows) {
    detail::require(plan.result == PlanResult::hit && bool(plan.snapshot),
                    "only a complete HIT plan is readable", ErrorCode::invalid);
    const auto checked = plan_query(plan.snapshot, plan.query, true, true);
    detail::require(checked.result == PlanResult::hit && checked.entry_indices == plan.entry_indices,
                    "plan coverage is invalid", ErrorCode::invalid);
    ReadSummary summary;
    Sha256 hash;
    int64_t previous = -1;
    for (size_t index : plan.entry_indices) {
        if (summary.rows == plan.query.max_count) break;
        const auto& entry = plan.snapshot->manifest.entries.at(index);
        if (!entry.pack) continue;
        const auto read = store.open_range(entry.pack->key, entry.pack->bytes);
        const auto pack = read_pack_index(read, *entry.pack, pack_metadata(entry));
        for (const auto& block : pack.blocks) {
            if (summary.rows == plan.query.max_count) break;
            if (block.last_ms < plan.query.range.start_ms || block.first_ms >= plan.query.range.end_ms) continue;
            auto rows = read_pack_block(read, block);
            std::vector<Row> selected;
            selected.reserve(rows.size());
            for (const auto& row : rows) {
                if (row.timestamp_ms < plan.query.range.start_ms || row.timestamp_ms >= plan.query.range.end_ms) continue;
                if (summary.rows == plan.query.max_count) break;
                detail::require(row.timestamp_ms > previous, "read result is not strictly increasing");
                previous = row.timestamp_ms;
                if (summary.rows == 0) summary.first_ms = row.timestamp_ms;
                summary.last_ms = row.timestamp_ms;
                if (plan.query.identity.dataset == "ddb-history-native64") hash.update(canonical_native(row));
                else if (plan.query.identity.dataset == "ddb-history-kline48") hash.update(canonical_kline(row));
                else hash.update(canonical_row(row));
                ++summary.rows;
                selected.push_back(row);
            }
            if (on_rows && !selected.empty()) on_rows(selected);
        }
    }
    summary.rows_sha256 = hash.finish();
    return summary;
}

}  // namespace history_cache
