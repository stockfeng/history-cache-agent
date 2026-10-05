#pragma once

#include "history_cache/catalog.h"
#include "history_cache/s3_store.h"
#include "history_cache/factor_snapshot.h"

#include <mutex>
#include <unordered_map>

namespace history_cache {

struct AgentConfig {
    std::string socket_path = "/run/history-cache/agent.sock";
    std::string account_id;
    std::string bucket = "history-cache-staging";
    std::string key_prefix = "r2-history-staging/";
    std::string access_key_id;
    std::string secret_access_key;
    uint64_t manifest_ttl_seconds = 300;
    uint64_t max_rows = 5000;
    uint64_t max_cached_snapshots = 256;
    uint64_t max_requests = 128;
    uint64_t max_cached_pack_bytes = 32 * 1024 * 1024;
    uint64_t max_cached_packs = 256;
    uint64_t full_pack_read_bytes = 1024 * 1024;
    std::chrono::milliseconds query_timeout{5000};
    bool foreground_network = true;
    bool enable_adjustment = false;
    std::string storage_environment = "staging";
    std::string jurisdiction = "default";
};

class Agent {
public:
    explicit Agent(AgentConfig config, std::shared_ptr<HttpTransport> transport);
    nlohmann::json handle_query(const nlohmann::json& request);
    nlohmann::json warm(const nlohmann::json& request, std::shared_ptr<std::atomic_bool> cancelled);

private:
    struct CachedSnapshot {
        std::shared_ptr<const Snapshot> snapshot;
        SteadyClock::time_point fetched_at;
    };
    struct QueryContext;
    nlohmann::json query(const nlohmann::json& request, QueryContext& query);
    nlohmann::json execute(const nlohmann::json& request, QueryContext& context);
    std::shared_ptr<const Snapshot> get_snapshot(const std::string& name, QueryContext& query);
    std::shared_ptr<const FactorSnapshot> get_factors(const std::string& name, const std::string& symbol,
                                                    const std::string& market, QueryContext& query);
    struct CachedFactors {
        Pointer pointer;
        std::shared_ptr<const FactorSnapshot> snapshot;
        SteadyClock::time_point fetched_at;
    };
    std::shared_ptr<const Bytes> get_pack(const std::shared_ptr<S3Store>& store,
                                        const CatalogEntry& entry, QueryContext& query);
    struct CachedPack {
        std::shared_ptr<const Bytes> bytes;
        uint64_t used;
    };

    AgentConfig config_;
    std::shared_ptr<const S3Credentials> credentials_;
    std::shared_ptr<HttpTransport> transport_;
    std::mutex cache_mutex_;
    std::unordered_map<std::string, CachedSnapshot> cache_;
    std::unordered_map<std::string, CachedPack> packs_;
    std::unordered_map<std::string, CachedFactors> factors_;
    uint64_t pack_bytes_ = 0;
    uint64_t pack_clock_ = 0;
    std::mutex warm_mutex_;
    std::timed_mutex cold_mutex_;
    std::mutex priority_mutex_;
    std::weak_ptr<std::atomic_bool> background_cancel_;
    std::atomic<unsigned> foreground_pending_{0};
};

}  // namespace history_cache
