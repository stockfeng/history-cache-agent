#pragma once

#include "history_cache/agent.h"

namespace history_cache {

// One scheduler per daemon. The event loop calls poll() every <=200 ms so
// expired leases also cancel a transfer that is already in flight.
class Maintenance {
public:
    Maintenance(Agent& agent, const nlohmann::json& plan);
    nlohmann::json lease(bool idle, SteadyClock::time_point expires = SteadyClock::now() + std::chrono::seconds(2));
    nlohmann::json status();
    void poll();
    bool step();
    void stop();

private:
    struct Item {
        nlohmann::json request;
        SteadyClock::time_point due;
        unsigned failures = 0;
    };
    bool allowed(SteadyClock::time_point now) const;
    Agent& agent_;
    std::mutex mutex_;
    std::vector<Item> items_;
    SteadyClock::time_point lease_until_{};
    SteadyClock::time_point next_start_{};
    std::shared_ptr<std::atomic_bool> cancelled_;
    bool stopped_ = false;
    size_t cursor_ = 0;
    uint64_t attempts_ = 0, hits_ = 0, failures_ = 0, cancellations_ = 0;
    uint64_t http_requests_ = 0, http_bytes_ = 0;
};

} // namespace history_cache
