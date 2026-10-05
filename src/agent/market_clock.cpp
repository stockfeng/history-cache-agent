#include "history_cache/market_clock.h"

#include <algorithm>
#include <limits>
#include <optional>

namespace history_cache {
namespace {
uint32_t be32(const Bytes& bytes, size_t pos) {
    if (pos > bytes.size() || bytes.size() - pos < 4) throw Error(ErrorCode::corrupt, "truncated TZif");
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) value = (value << 8) | bytes[pos + i];
    return value;
}
struct Header { uint32_t gmt, standard, leaps, times, types, chars; };
Header header(const Bytes& bytes, size_t pos) {
    if (pos > bytes.size() || bytes.size() - pos < 44 ||
        std::string(bytes.begin() + static_cast<ptrdiff_t>(pos), bytes.begin() + static_cast<ptrdiff_t>(pos + 4)) != "TZif")
        throw Error(ErrorCode::corrupt, "invalid TZif header");
    Header h{be32(bytes, pos + 20), be32(bytes, pos + 24), be32(bytes, pos + 28),
             be32(bytes, pos + 32), be32(bytes, pos + 36), be32(bytes, pos + 40)};
    if (!h.types || h.types > 256 || h.times > 4096 || h.chars > 4096 || h.leaps ||
        h.gmt > h.types || h.standard > h.types)
        throw Error(ErrorCode::corrupt, "unsupported TZif counts");
    return h;
}
}

NewYorkClock::NewYorkClock() {
    const auto bytes = read_file("/usr/share/zoneinfo/America/New_York", 128 * 1024);
    const auto old = header(bytes, 0);
    if (bytes[4] != '2' && bytes[4] != '3' && bytes[4] != '4')
        throw Error(ErrorCode::corrupt, "64-bit TZif required");
    const size_t second = 44 + size_t{old.times} * 5 + size_t{old.types} * 6 + old.chars + old.gmt + old.standard;
    const auto h = header(bytes, second);
    const size_t transitions = second + 44, indices = transitions + size_t{h.times} * 8;
    const size_t types = indices + h.times;
    if (h.times < 2 || types + size_t{h.types} * 6 + h.chars + h.gmt + h.standard > bytes.size())
        throw Error(ErrorCode::corrupt, "truncated TZif transitions");
    std::vector<int64_t> times;
    for (size_t i = 0; i < h.times; ++i) {
        const uint64_t raw = (uint64_t{be32(bytes, transitions + i * 8)} << 32) | be32(bytes, transitions + i * 8 + 4);
        const int64_t seconds = raw <= uint64_t{INT64_MAX} ? static_cast<int64_t>(raw)
            : -1 - static_cast<int64_t>(~raw);
        if (seconds < INT64_MIN / 1000 || seconds > INT64_MAX / 1000 ||
            (!times.empty() && seconds * 1000 <= times.back()))
            throw Error(ErrorCode::corrupt, "invalid TZif transition order");
        times.push_back(seconds * 1000);
    }
    // Do not extrapolate the POSIX footer. Outside explicit transitions fail closed.
    for (size_t i = 0; i + 1 < times.size(); ++i) {
        const auto type = bytes[indices + i];
        if (type >= h.types) throw Error(ErrorCode::corrupt, "invalid TZif type");
        const uint32_t raw = be32(bytes, types + size_t{type} * 6);
        const int64_t offset = raw <= uint32_t{INT32_MAX} ? static_cast<int64_t>(raw) : int64_t{raw} - 4294967296LL;
        if (offset < -86400 || offset > 86400) throw Error(ErrorCode::corrupt, "invalid TZif offset");
        const int64_t start = std::max<int64_t>(times[i], 0);
        if (start < times[i + 1]) intervals_.push_back({start, times[i + 1], static_cast<int32_t>(offset * 1000)});
    }
    if (intervals_.empty()) throw Error(ErrorCode::corrupt, "no usable New York transitions");
}

int64_t NewYorkClock::to_utc(int64_t wall_ms) const {
    std::optional<int64_t> result;
    if (wall_ms <= 0 || wall_ms > 32503680000000LL) throw Error(ErrorCode::invalid, "unsupported US wall time");
    for (const auto& interval : intervals_) {
        const auto candidate = wall_ms - interval.offset;
        if (candidate >= interval.start && candidate < interval.end) {
            if (result) throw Error(ErrorCode::invalid, "ambiguous US wall time");
            result = candidate;
        }
    }
    if (!result) throw Error(ErrorCode::invalid, "nonexistent or unsupported US wall time");
    return *result;
}

int64_t NewYorkClock::to_wall(int64_t utc_ms) const {
    for (const auto& interval : intervals_) {
        if (utc_ms >= interval.start && utc_ms < interval.end) {
            const auto wall = utc_ms + interval.offset;
            if (to_utc(wall) != utc_ms) throw Error(ErrorCode::invalid, "ambiguous US instant");
            return wall;
        }
    }
    throw Error(ErrorCode::invalid, "US instant outside explicit timezone transitions");
}
} // namespace history_cache
