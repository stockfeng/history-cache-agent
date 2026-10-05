#include "history_cache/maintenance.h"

#include <algorithm>
#include <set>

namespace history_cache {
using Json = nlohmann::json;

Maintenance::Maintenance(Agent& agent, const Json& plan) : agent_(agent) {
    if (!plan.is_array() || plan.size() > 8) throw Error(ErrorCode::invalid, "invalid warm plan");
    std::set<std::string> unique;
    for (const auto& request : plan) {
        const bool complete = request.is_object() && request.contains("row_encoding");
        if (!request.is_object() || request.size() != (complete ? 11U : 10U) || request.value("op", "") != "query" ||
            (complete && request.at("row_encoding") != "le-kline48-v1" && request.at("row_encoding") != "le-ddb-native64-v1") ||
            request.value("protocol_version", 0) != 1 ||
            request.value("timestamp_semantics", "") != "utc-instant-ms" ||
            request.value("range_semantics", "") != "half-open" ||
            request.value("period_seconds", 0) != 60 || request.value("adjust", "") != "none")
            throw Error(ErrorCode::invalid, "invalid warm plan request");
        for (const auto* field : {"start_ms", "end_ms", "max_rows", "period_seconds", "protocol_version"})
            if (!request.at(field).is_number_integer()) throw Error(ErrorCode::invalid, "invalid warm integer");
        const auto start = request.at("start_ms").get<int64_t>();
        const auto end = request.at("end_ms").get<int64_t>();
        const auto rows = request.at("max_rows").get<int64_t>();
        const auto symbol = request.at("symbol").get<std::string>();
        if (symbol.empty() || symbol.size() > 32 || start <= 0 || end <= start ||
            end > 32503680000000LL || end - start > 86400000 || rows < 1 || rows > 5000 ||
            !unique.insert(request.dump()).second)
            throw Error(ErrorCode::invalid, "invalid warm bounds");
        items_.push_back({request, SteadyClock::now(), 0});
    }
}

bool Maintenance::allowed(SteadyClock::time_point now) const {
    return !stopped_ && now < lease_until_;
}

Json Maintenance::lease(bool idle, SteadyClock::time_point expires) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = SteadyClock::now();
    idle = idle && expires > now && expires <= now + std::chrono::seconds(2);
    lease_until_ = idle && !stopped_ ? expires : SteadyClock::time_point{};
    if (!idle && cancelled_) cancelled_->store(true);
    const auto remaining = idle && !stopped_ ?
        std::chrono::duration_cast<std::chrono::milliseconds>(lease_until_ - now).count() : 0;
    return {{"status", "OK"}, {"lease_ms", remaining}};
}

void Maintenance::poll() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!allowed(SteadyClock::now()) && cancelled_) cancelled_->store(true);
}

void Maintenance::stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
    if (cancelled_) cancelled_->store(true);
}

Json Maintenance::status() {
    std::lock_guard<std::mutex> lock(mutex_);
    return {{"status", "OK"}, {"targets", items_.size()}, {"active", bool(cancelled_)},
            {"permitted", allowed(SteadyClock::now())}, {"attempts", attempts_},
            {"hits", hits_}, {"failures", failures_}, {"cancellations", cancellations_},
            {"http_requests", http_requests_}, {"http_bytes", http_bytes_}};
}

bool Maintenance::step() {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto now = SteadyClock::now();
    if (!allowed(now) || cancelled_ || now < next_start_) return false;
    size_t selected = items_.size();
    for (size_t n = 0; n < items_.size(); ++n) {
        const auto index = (cursor_ + n) % items_.size();
        if (items_[index].due <= now) { selected = index; break; }
    }
    if (selected == items_.size()) return false;
    cursor_ = (selected + 1) % items_.size();
    // No startup burst, catch-up loop or unbounded request-derived warm queue.
    next_start_ = now + std::chrono::seconds(30);
    cancelled_ = std::make_shared<std::atomic_bool>(false);
    const auto cancelled = cancelled_;
    const auto request = items_[selected].request;
    ++attempts_;
    lock.unlock();
    Json result;
    try { result = agent_.warm(request, cancelled); }
    catch (...) { result = {{"status", "ERROR"}}; }
    lock.lock();
    auto& item = items_[selected];
    if (result.contains("metrics")) for (const auto& stage : result.at("metrics").at("http")) {
        http_requests_ += stage.value("requests", uint64_t{0});
        http_bytes_ += stage.value("bytes", uint64_t{0});
    }
    const bool hit = !cancelled->load() && result.value("status", "") == "HIT";
    if (cancelled->load()) ++cancellations_;
    if (hit) { ++hits_; item.failures = 0; }
    else { ++failures_; item.failures = std::min(5U, item.failures + 1); }
    // Stable per-target staggering, with bounded exponential failure backoff.
    const auto delay = hit ? 240U + static_cast<unsigned>(selected) * 3U : 30U << item.failures;
    item.due = SteadyClock::now() + std::chrono::seconds(delay);
    cancelled_.reset();
    return true;
}

} // namespace history_cache
