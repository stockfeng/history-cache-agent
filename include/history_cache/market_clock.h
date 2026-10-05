#pragma once

#include "history_cache/common.h"

namespace history_cache {

// Read-only IANA TZif transitions; never modifies the process-global TZ.
class NewYorkClock {
public:
    NewYorkClock();
    int64_t to_wall(int64_t utc_ms) const;
    int64_t to_utc(int64_t wall_ms) const;
private:
    struct Interval { int64_t start; int64_t end; int32_t offset; };
    std::vector<Interval> intervals_;
};

} // namespace history_cache
