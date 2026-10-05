#pragma once

#include "history_cache/pack.h"

#include <optional>

namespace history_cache {

constexpr uint64_t kMaxPointerBytes = 4096;

struct ImmutableKey {
    Digest sha256{};
    uint64_t max_bytes = 0;
};

ImmutableKey parse_immutable_key(const std::string& key);
void validate_etag(const std::string& etag);

class ObjectReader {
public:
    virtual ~ObjectReader() = default;
    // Only immutable keys; enforce max_bytes before allocating/receiving the body.
    virtual Bytes get(const std::string& key, uint64_t max_bytes) const = 0;
    // The reader pins an immutable object, checks its total size, and returns exact ranges.
    // A network implementation must validate status, Content-Range and object identity.
    virtual RangeReader open_range(const std::string& key, uint64_t expected_size) const = 0;
};

struct VersionedObject {
    Bytes bytes;
    std::string etag;
};

// not_applied is proof about THIS request, not about earlier uncertain requests.
enum class WriteOutcome { applied, precondition_failed, not_applied, indeterminate };

class ObjectStore : public ObjectReader {
public:
    // Stable, credential-independent identity of endpoint/bucket/namespace.
    virtual Digest scope_id() const = 0;
    // One authoritative GET must supply both body and strong, opaque ETag. Only a
    // definite missing key returns nullopt; permission/network errors must not.
    // Receive at most kMaxPointerBytes, with no intermediary cache or stale fallback.
    virtual std::optional<VersionedObject> read_current() const = 0;
    // Exactly one create-only request (If-None-Match: *), with no implicit retries.
    virtual WriteOutcome create(const std::string& key, const Bytes& bytes) = 0;
    // nullopt means create-only; otherwise use the exact ETag in If-Match.
    // All failures after possible dispatch are indeterminate unless non-application
    // is proven. Implementations must bound time/body and must not blindly retry.
    virtual WriteOutcome write_current(const Bytes& bytes,
                                       const std::optional<std::string>& expected_etag) = 0;
};

// A successful create or an existing identical object must pass a full read-back.
// Observing identical bytes does not establish ownership; this API never deletes.
[[nodiscard]] WriteOutcome put_immutable(ObjectStore& store, const std::string& key, const Bytes& bytes);
// Skip PUT only after a full byte match. Only authoritative missing permits a write.
[[nodiscard]] WriteOutcome reuse_or_put_immutable(ObjectStore& store, const std::string& key, const Bytes& bytes);

}  // namespace history_cache
