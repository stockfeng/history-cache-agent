#pragma once

#include "history_cache/catalog.h"
#include "history_cache/object_store.h"

#include <memory>

namespace history_cache {

enum class PlanResult { hit, miss, bypass, error };

struct Query {
    SeriesIdentity identity;
    Digest data_version{};
    Coverage range;
    uint64_t max_count = kMaxRows;
};

struct Plan {
    PlanResult result = PlanResult::miss;
    std::string reason;
    Query query;
    std::shared_ptr<const Snapshot> snapshot;
    std::vector<size_t> entry_indices;
};

struct ReadSummary {
    uint64_t rows = 0;
    int64_t first_ms = 0;
    int64_t last_ms = 0;
    Digest rows_sha256{};
};

Plan plan_query(std::shared_ptr<const Snapshot> snapshot, const Query& query,
                bool enabled = false, bool healthy = true);
ReadSummary read_plan(const ObjectReader& store, const Plan& plan,
                     const std::function<void(const std::vector<Row>&)>& on_rows = {});
const char* plan_result_name(PlanResult result);

}  // namespace history_cache
