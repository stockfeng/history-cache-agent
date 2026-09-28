#include "history_cache/object_store.h"

#include "binary.h"

namespace history_cache {

ImmutableKey parse_immutable_key(const std::string& key) {
    const std::string data_prefix = "data/v1/";
    const std::string manifest_prefix = "manifests/v1/";
    if (key.size() == data_prefix.size() + 64 + 4 && key.rfind(data_prefix, 0) == 0 &&
        key.substr(key.size() - 4) == ".r2b")
        return {parse_digest(key.substr(data_prefix.size(), 64)), kMaxObjectBytes};
    if (key.size() == manifest_prefix.size() + 64 + 5 && key.rfind(manifest_prefix, 0) == 0 &&
        key.substr(key.size() - 5) == ".json")
        return {parse_digest(key.substr(manifest_prefix.size(), 64)), kMaxMetadataBytes};
    throw Error(ErrorCode::invalid, "unsupported or unsafe immutable object key");
}

void validate_etag(const std::string& etag) {
    detail::require(etag.size() >= 2 && etag.size() <= 1024 && etag.front() == '"' && etag.back() == '"',
                    "a bounded strong ETag is required", ErrorCode::corrupt);
    for (size_t i = 1; i + 1 < etag.size(); ++i) {
        const auto ch = static_cast<unsigned char>(etag[i]);
        detail::require(ch == 0x21 || (ch >= 0x23 && ch <= 0x7e),
                        "invalid or multiple ETag values", ErrorCode::corrupt);
    }
}

WriteOutcome put_immutable(ObjectStore& store, const std::string& key, const Bytes& bytes) {
    const auto reference = parse_immutable_key(key);
    detail::require(!bytes.empty() && bytes.size() <= reference.max_bytes,
                    "immutable object exceeds budget", ErrorCode::resource_limit);
    detail::require(sha256(bytes) == reference.sha256, "object key does not match content hash", ErrorCode::invalid);
    WriteOutcome outcome;
    try {
        outcome = store.create(key, bytes);
    } catch (const Error& error) {
        if (error.code() != ErrorCode::io) throw;
        outcome = WriteOutcome::indeterminate;
    }
    if (outcome == WriteOutcome::not_applied) return outcome;
    try {
        const auto observed = store.get(key, reference.max_bytes);
        detail::require(observed == bytes, "immutable key contains different bytes", ErrorCode::conflict);
        return WriteOutcome::applied;
    } catch (const Error& error) {
        if (error.code() != ErrorCode::missing && error.code() != ErrorCode::io) throw;
        return WriteOutcome::indeterminate;
    }
}

}  // namespace history_cache
