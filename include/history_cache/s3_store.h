#pragma once

#include "history_cache/http.h"
#include "history_cache/object_store.h"

#include <functional>

namespace history_cache {

class S3Credentials {
public:
    S3Credentials(std::string access_key_id, std::string secret_access_key);
    ~S3Credentials();
    S3Credentials(const S3Credentials&) = delete;
    S3Credentials& operator=(const S3Credentials&) = delete;
private:
    std::string id_;
    std::string secret_;
    friend void sign_s3_request(HttpRequest&, const S3Credentials&, const std::string&,
                                const std::string&, const std::string&);
};

void sign_s3_request(HttpRequest& request, const S3Credentials& credentials,
                     const std::string& host, const std::string& path, const std::string& amz_date);

struct S3Config {
    std::string account_id;
    std::string bucket;
    std::string key_prefix;
    std::string jurisdiction = "default";
    std::string environment = "staging";
    bool read_only = false;
};

// Non-secret, explicit scope. A profile cannot grant cloud-side permissions.
S3Config load_storage_profile(const std::string& path, const std::string& role);

struct TransferLimits {
    SteadyClock::time_point deadline = SteadyClock::now() + std::chrono::seconds(30);
    std::chrono::milliseconds request_timeout{10000};
    std::chrono::milliseconds connect_timeout{3000};
    uint64_t max_requests = 32;
    uint64_t max_upload_bytes = 4 * 1024 * 1024;
    uint64_t max_download_bytes = 16 * 1024 * 1024;
    std::shared_ptr<std::atomic_bool> cancelled = std::make_shared<std::atomic_bool>(false);
};

struct TransferUsage {
    uint64_t requests = 0;
    uint64_t upload_reserved = 0;
    uint64_t download_reserved = 0;
};

// Bound to an explicit namespace and to one finite operation budget.
class S3Store final : public ObjectStore {
public:
    S3Store(S3Config config, std::shared_ptr<HttpTransport> transport,
            std::shared_ptr<const S3Credentials> credentials, TransferLimits limits = {},
            std::function<std::string()> signing_time = {});
    Digest scope_id() const override;
    Bytes get(const std::string& key, uint64_t max_bytes) const override;
    RangeReader open_range(const std::string& key, uint64_t expected_size) const override;
    std::optional<VersionedObject> read_current() const override;
    WriteOutcome create(const std::string& key, const Bytes& bytes) override;
    WriteOutcome write_current(const Bytes& bytes, const std::optional<std::string>& expected_etag) override;
    // Staging cleanup only. The caller must prove ownership and absence of writers.
    WriteOutcome erase_staging_object(const std::string& key, const std::string& expected_etag);
    TransferUsage usage() const;
private:
    struct State;
    std::shared_ptr<State> state_;
};

}  // namespace history_cache
