#pragma once

#include "history_cache/object_store.h"
#include <map>
#include <memory>

namespace history_cache::sample {

// One publication invocation only. Never seed from local candidates or reuse a
// mutable pointer; only complete, hash-verified remote reads enter this cache.
class VerifiedObjectStore final : public ObjectStore {
public:
    explicit VerifiedObjectStore(ObjectStore& remote, uint64_t capacity)
        : remote_(remote), capacity_(capacity) {}

    Digest scope_id() const override { return remote_.scope_id(); }
    std::optional<VersionedObject> read_current() const override { return remote_.read_current(); }
    WriteOutcome write_current(const Bytes& bytes, const std::optional<std::string>& etag) override {
        return remote_.write_current(bytes, etag);
    }
    WriteOutcome create(const std::string& key, const Bytes& bytes) override {
        const auto found = objects_.find(key);
        if (found != objects_.end()) {
            used_ -= found->second->size();
            objects_.erase(found);
        }
        return remote_.create(key, bytes);
    }
    Bytes get(const std::string& key, uint64_t max_bytes) const override {
        const auto reference = parse_immutable_key(key);
        const auto found = objects_.find(key);
        if (found != objects_.end()) {
            if (found->second->size() > max_bytes)
                throw Error(ErrorCode::resource_limit, "cached object exceeds requested bound");
            ++hits_;
            return *found->second;
        }
        auto bytes = remote_.get(key, max_bytes);
        if (bytes.empty() || bytes.size() > max_bytes || bytes.size() > reference.max_bytes ||
            sha256(bytes) != reference.sha256)
            throw Error(ErrorCode::corrupt, "remote immutable read failed verification");
        if (bytes.size() <= capacity_ - used_) {
            used_ += bytes.size();
            objects_.emplace(key, std::make_shared<const Bytes>(bytes));
        }
        return bytes;
    }
    RangeReader open_range(const std::string& key, uint64_t expected_size) const override {
        (void)parse_immutable_key(key);
        const auto found = objects_.find(key);
        if (found == objects_.end()) return remote_.open_range(key, expected_size);
        auto bytes = found->second;
        if (bytes->size() != expected_size)
            throw Error(ErrorCode::corrupt, "verified object size differs");
        ++hits_;
        return [bytes](uint64_t offset, uint64_t size) {
            if (offset > bytes->size() || size > bytes->size() - offset)
                throw Error(ErrorCode::invalid, "verified object range exceeds bounds");
            return Bytes(bytes->begin() + static_cast<ptrdiff_t>(offset),
                         bytes->begin() + static_cast<ptrdiff_t>(offset + size));
        };
    }
    uint64_t bytes() const { return used_; }
    uint64_t hits() const { return hits_; }

private:
    ObjectStore& remote_;
    uint64_t capacity_;
    mutable uint64_t used_ = 0, hits_ = 0;
    mutable std::map<std::string, std::shared_ptr<const Bytes>> objects_;
};

}  // namespace history_cache::sample
