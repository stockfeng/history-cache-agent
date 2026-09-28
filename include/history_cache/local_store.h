#pragma once

#include "history_cache/catalog.h"
#include "history_cache/object_store.h"

namespace history_cache {

enum class FailPoint { none, after_objects, after_manifest, after_pointer };

class LocalStore : public ObjectReader {
public:
    static void initialize(const std::filesystem::path& root);
    explicit LocalStore(std::filesystem::path root);

    std::filesystem::path object_path(const std::string& key) const;
    Bytes get(const std::string& key, uint64_t max_bytes) const override;
    RangeReader open_range(const std::string& key, uint64_t expected_size) const override;
    void put(const std::string& key, const Bytes& bytes);
    void put_file(const std::string& key, const std::filesystem::path& source,
                  const Digest& expected_hash);
    Snapshot load_snapshot(uint64_t min_publication_seq = 0) const;
    Pointer publish(const Manifest& manifest, uint64_t expected_seq,
                    FailPoint fail_point = FailPoint::none);
    Pointer rollback(const std::string& manifest_key, const Digest& manifest_hash,
                     uint64_t expected_seq);

private:
    Pointer commit(const Manifest& manifest, uint64_t expected_seq,
                   FailPoint fail_point, bool rollback);
    std::filesystem::path root_;
};

}  // namespace history_cache
