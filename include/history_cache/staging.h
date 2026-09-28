#pragma once

#include "history_cache/catalog.h"
#include "history_cache/s3_store.h"

namespace history_cache::staging {

class Plan {
public:
    // Configuration may be unconfirmed for planning, never for execution.
    static Plan prepare(const std::string& config, const std::string& manifest,
                        const Bytes& pack, const std::string& run_id);
    nlohmann::json intent() const;
    Digest intent_hash() const;
    Digest config_hash() const;
private:
    struct State;
    std::shared_ptr<const State> state_;
    explicit Plan(std::shared_ptr<const State> state) : state_(std::move(state)) {}
    friend class Executor;
};

struct Approval {
    bool execute = false;
    bool exclusive_namespace = false;
    Digest config_sha256{};
    Digest intent_sha256{};
};

enum class LedgerStep { before_intent_sync, before_request_sync, before_dispatch_sync, before_result_sync, before_finish_sync,
                        before_start_sync };
using LedgerHook = std::function<void(LedgerStep)>;
using CredentialsLoader = std::function<std::shared_ptr<const S3Credentials>()>;
using TransportFactory = std::function<std::shared_ptr<HttpTransport>()>;

class Executor {
public:
    // A new evidence leaf and a persistent local namespace registry. Both parents
    // must already exist. Reopening a used namespace never enables another run.
    static nlohmann::json run(const Plan& plan, const Approval& approval,
                              const std::filesystem::path& evidence, const std::filesystem::path& registry,
                              const CredentialsLoader& credentials, const TransportFactory& transport,
                              const LedgerHook& hook = {});
};

// Strict, offline-only inspection. Does not restart requests or replenish budgets.
nlohmann::json inspect_run(const std::filesystem::path& evidence);
// Read inputs without following symlinks/FIFOs or unbounded allocations.
Bytes read_input(const std::filesystem::path& input, uint64_t max_bytes);

}  // namespace history_cache::staging
