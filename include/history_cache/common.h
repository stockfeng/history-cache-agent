#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace history_cache {

using Bytes = std::vector<uint8_t>;
using Digest = std::array<uint8_t, 32>;
using SeriesId = std::array<uint8_t, 16>;
using RowBytes = std::array<uint8_t, 32>;

constexpr uint64_t kMaxObjectBytes = 32ULL * 1024 * 1024;
constexpr uint32_t kMaxBlockBytes = 64U * 1024;
constexpr uint32_t kBlockRows = 1024;
constexpr uint64_t kMaxRows = 500000;
constexpr uint64_t kMaxMetadataBytes = 1024 * 1024;
constexpr size_t kMaxCatalogEntries = 1024;

enum class ErrorCode { invalid, corrupt, missing, conflict, resource_limit, io };

class Error : public std::runtime_error {
public:
    Error(ErrorCode code, const std::string& message) : std::runtime_error(message), code_(code) {}
    ErrorCode code() const noexcept { return code_; }
private:
    ErrorCode code_;
};

struct Row {
    int64_t timestamp_ms = 0;
    float open = 0;
    float high = 0;
    float low = 0;
    float close = 0;
    int64_t volume = 0;
    double turnover = 0;
    int64_t open_interest = 0;
    struct NativeFields {
        std::array<double, 4> prices{};
        int64_t open_oi = 0;
        int64_t close_oi = 0;
    };
    std::optional<NativeFields> native = std::nullopt;
};

struct Coverage {
    int64_t start_ms = 0;
    int64_t end_ms = 0;
};

struct SeriesIdentity {
    std::string dataset;
    std::string market;
    std::string symbol;
    uint32_t period_seconds = 60;
    std::string adjust = "none";
};

class Sha256 {
public:
    Sha256();
    ~Sha256();
    Sha256(const Sha256&) = delete;
    Sha256& operator=(const Sha256&) = delete;
    void update(const void* data, size_t size);
    void update(const Bytes& bytes) { update(bytes.data(), bytes.size()); }
    template <size_t N> void update(const std::array<uint8_t, N>& bytes) { update(bytes.data(), bytes.size()); }
    Digest finish();
private:
    void* context_ = nullptr;
    bool finished_ = false;
};

Digest sha256(const Bytes& bytes);
Digest sha256(const std::string& text);
Digest file_sha256(const std::filesystem::path& path, uint64_t max_bytes);
std::string hex(const uint8_t* data, size_t size);
template <size_t N> std::string hex(const std::array<uint8_t, N>& data) {
    return hex(data.data(), data.size());
}
Bytes unhex(const std::string& text);
Digest parse_digest(const std::string& text);
SeriesId parse_series_id(const std::string& text);
void validate_identity(const SeriesIdentity& identity);
void validate_coverage(const Coverage& coverage);
std::string canonical_identity(const SeriesIdentity& identity);
SeriesId series_id(const SeriesIdentity& identity);
void validate_row(const Row& row);
RowBytes canonical_row(const Row& row);
std::array<uint8_t, 48> canonical_kline(const Row& row);
std::array<uint8_t, 64> canonical_native(const Row& row);
bool same_row(const Row& lhs, const Row& rhs);
Bytes read_file(const std::filesystem::path& path, uint64_t max_bytes);
void create_new_directory(const std::filesystem::path& path);
void write_new_file(const std::filesystem::path& path, const Bytes& bytes);
void write_new_file(const std::filesystem::path& path, const std::string& text);

}  // namespace history_cache
