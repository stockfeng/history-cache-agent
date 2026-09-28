#pragma once

#include "history_cache/staging.h"
#include "binary.h"

#include <set>

namespace history_cache::staging {
using Json = nlohmann::json;
using detail::require;

inline Bytes bytes(const std::string& text) { return {text.begin(), text.end()}; }
inline std::string text(const Bytes& value) { return {value.begin(), value.end()}; }
inline std::string canonical(const Json& value) { return value.dump() + '\n'; }
inline const char* method_name(HttpMethod value) {
    switch (value) { case HttpMethod::get: return "GET"; case HttpMethod::put: return "PUT"; case HttpMethod::erase: return "DELETE"; }
    throw Error(ErrorCode::invalid, "invalid staging method");
}

Json parse(const std::string& raw, uint64_t limit);
void fields(const Json& value, std::initializer_list<const char*> names);
uint64_t number(const Json& value, uint64_t cap);
inline bool retains_objects(const Json& value) { return value.at("profile") == "cpp-staging-2050-retain-v2"; }

struct Step {
    std::string id, phase, key;
    HttpMethod method = HttpMethod::get;
    Bytes body;
    uint64_t limit = 0;
    unsigned status = 0;
    HttpHeaders headers;
    std::optional<Digest> response_hash;
    std::string save_etag, content_range;
    bool suppress_ack = false;
    bool suppress_dispatch = false;
    bool recovery = false;
    Json json() const;
};

struct Scenario {
    struct Object { std::string key, role, final_sha256; uint64_t max_bytes = 0; };
    std::string name;
    S3Config config;
    std::vector<Object> objects;
    bool retain_objects = false;
    std::vector<Step> business;
    std::map<std::string, Step> recovery;
    std::vector<Step> cleanup;
    uint64_t final_seq = 1;
    TransferUsage worst;
};

struct Plan::State {
    Json config, intent;
    Digest config_hash{};
    Manifest candidate;
    Bytes pack, manifest;
    std::vector<Scenario> scenarios;
};

struct Owned {
    std::string key, etag, sha256;
    bool owned = false, deleted = false, absent = false, uncertain = false;
    uint64_t body_bytes = 0;
    bool touched = false, identity_changed = false;
};

class Ledger {
public:
    Ledger(const std::filesystem::path& root, const Json& intent, const LedgerHook& hook);
    explicit Ledger(const std::filesystem::path& root);
    ~Ledger();
    void request(const Json& event);
    void result(const Json& event);
    void finish(const Json& event);
    Json report() const;
    bool poisoned() const;
    const TransferUsage& usage() const;
private:
    struct State;
    std::unique_ptr<State> state_;
};

void register_namespaces(const std::filesystem::path& registry, const Json& intent);
void write_report(const std::filesystem::path& root, const Json& report);
bool no_such_key(const HttpResponse& response);

}  // namespace history_cache::staging
