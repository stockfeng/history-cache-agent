#pragma once

#include "history_cache/factor_snapshot.h"
#include "history_cache/publisher.h"

namespace history_cache {

// Persist bytes, expected_base and store.scope_id() before dispatch; retries
// must retain the same intent. This primitive does not supply a crash journal.
[[nodiscard]] PublishResult publish_factors(ObjectStore& store, const Bytes& bytes,
    const std::string& symbol, const std::string& market,
    const std::optional<Pointer>& expected_base, const std::function<int64_t()>& now_ms,
    bool recover_only = false);

}  // namespace history_cache
