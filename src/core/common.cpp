#include "history_cache/common.h"

#include "binary.h"

#include <cerrno>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <openssl/evp.h>

namespace history_cache {
namespace {

void crypto_ok(int result) {
    if (result != 1) throw Error(ErrorCode::io, "OpenSSL SHA-256 operation failed");
}

bool safe_token(const std::string& text, bool uppercase_only) {
    if (text.empty() || text.size() > 128 || text == "." || text == "..") return false;
    for (unsigned char ch : text) {
        if ((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
            ch == '.' || ch == '_' || ch == '-') continue;
        if (!uppercase_only && ch >= 'a' && ch <= 'z') continue;
        return false;
    }
    return true;
}

}  // namespace

Sha256::Sha256() {
    auto* context = EVP_MD_CTX_new();
    if (!context) throw Error(ErrorCode::resource_limit, "cannot allocate SHA-256 context");
    context_ = context;
    if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(context);
        context_ = nullptr;
        throw Error(ErrorCode::io, "cannot initialize SHA-256");
    }
}

Sha256::~Sha256() { EVP_MD_CTX_free(static_cast<EVP_MD_CTX*>(context_)); }

void Sha256::update(const void* data, size_t size) {
    detail::require(!finished_, "SHA-256 context is already finalized", ErrorCode::invalid);
    if (size) crypto_ok(EVP_DigestUpdate(static_cast<EVP_MD_CTX*>(context_), data, size));
}

Digest Sha256::finish() {
    detail::require(!finished_, "SHA-256 context is already finalized", ErrorCode::invalid);
    Digest result{};
    unsigned int size = 0;
    crypto_ok(EVP_DigestFinal_ex(static_cast<EVP_MD_CTX*>(context_), result.data(), &size));
    finished_ = true;
    detail::require(size == result.size(), "invalid SHA-256 output", ErrorCode::io);
    return result;
}

Digest sha256(const Bytes& bytes) {
    Sha256 hash;
    hash.update(bytes);
    return hash.finish();
}

Digest sha256(const std::string& text) {
    Sha256 hash;
    hash.update(text.data(), text.size());
    return hash.finish();
}

Digest file_sha256(const std::filesystem::path& path, uint64_t max_bytes) {
    detail::File file(path, O_RDONLY);
    const uint64_t size = file.size();
    detail::require(size <= max_bytes, "file exceeds hash budget", ErrorCode::resource_limit);
    Sha256 hash;
    for (uint64_t offset = 0; offset < size;) {
        auto bytes = file.read(offset, std::min<uint64_t>(65536, size - offset));
        hash.update(bytes);
        offset += bytes.size();
    }
    return hash.finish();
}

std::string hex(const uint8_t* data, size_t size) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(size * 2);
    for (size_t i = 0; i < size; ++i) {
        result.push_back(digits[data[i] >> 4U]);
        result.push_back(digits[data[i] & 15U]);
    }
    return result;
}

Bytes unhex(const std::string& text) {
    detail::require(text.size() % 2 == 0 && text.size() <= kMaxMetadataBytes * 2,
                    "invalid hex length", ErrorCode::invalid);
    const auto digit = [](char ch) -> uint8_t {
        if (ch >= '0' && ch <= '9') return static_cast<uint8_t>(ch - '0');
        if (ch >= 'a' && ch <= 'f') return static_cast<uint8_t>(ch - 'a' + 10);
        throw Error(ErrorCode::invalid, "hex must be lowercase ASCII");
    };
    Bytes result;
    result.reserve(text.size() / 2);
    for (size_t i = 0; i < text.size(); i += 2)
        result.push_back(static_cast<uint8_t>((digit(text[i]) << 4U) | digit(text[i + 1])));
    return result;
}

Digest parse_digest(const std::string& text) {
    detail::require(text.size() == 64, "SHA-256 must have 64 hex characters", ErrorCode::invalid);
    return detail::fixed<32>(unhex(text), 0);
}

SeriesId parse_series_id(const std::string& text) {
    detail::require(text.size() == 32, "series_id must have 32 hex characters", ErrorCode::invalid);
    return detail::fixed<16>(unhex(text), 0);
}

void validate_identity(const SeriesIdentity& identity) {
    detail::require(safe_token(identity.dataset, false) && safe_token(identity.market, true) &&
                    safe_token(identity.symbol, false),
                    "identity requires normalized ASCII dataset/market/symbol", ErrorCode::invalid);
    detail::require(identity.period_seconds > 0 && identity.period_seconds <= 31536000,
                    "invalid period", ErrorCode::invalid);
    detail::require(identity.adjust == "none" || identity.adjust == "forward" ||
                    identity.adjust == "backward", "invalid adjust", ErrorCode::invalid);
}

void validate_coverage(const Coverage& coverage) {
    detail::require(coverage.start_ms >= 0 && coverage.end_ms > coverage.start_ms,
                    "coverage must be a nonnegative increasing [start,end) range", ErrorCode::invalid);
}

std::string canonical_identity(const SeriesIdentity& identity) {
    validate_identity(identity);
    std::string result = "history-series-v1\n";
    for (const auto& field : {identity.dataset, identity.market, identity.symbol,
                              std::to_string(identity.period_seconds), identity.adjust}) {
        result += std::to_string(field.size()) + ":" + field + "\n";
    }
    return result;
}

SeriesId series_id(const SeriesIdentity& identity) {
    const auto hash = sha256(canonical_identity(identity));
    SeriesId result{};
    std::copy_n(hash.begin(), result.size(), result.begin());
    return result;
}

void validate_row(const Row& row) {
    detail::require(row.timestamp_ms >= 0 && std::isfinite(row.open) &&
                    std::isfinite(row.high) && std::isfinite(row.low) && std::isfinite(row.close),
                    "row has negative timestamp or nonfinite price", ErrorCode::invalid);
}

RowBytes canonical_row(const Row& row) {
    validate_row(row);
    static_assert(sizeof(float) == 4 && std::numeric_limits<float>::is_iec559);
    RowBytes result{};
    size_t offset = 0;
    const auto append = [&result, &offset](uint64_t value, size_t width) {
        for (size_t i = 0; i < width; ++i) result[offset++] = static_cast<uint8_t>(value >> (i * 8U));
    };
    append(detail::bits<uint64_t>(row.timestamp_ms), 8);
    for (float price : {row.open, row.high, row.low, row.close})
        append(detail::bits<uint32_t>(price), 4);
    append(detail::bits<uint64_t>(row.volume), 8);
    return result;
}

bool same_row(const Row& lhs, const Row& rhs) { return canonical_row(lhs) == canonical_row(rhs); }

Bytes read_file(const std::filesystem::path& path, uint64_t max_bytes) {
    detail::File file(path, O_RDONLY);
    const auto size = file.size();
    detail::require(size <= max_bytes, "file exceeds read budget", ErrorCode::resource_limit);
    return file.read(0, size);
}

void create_new_directory(const std::filesystem::path& path) {
    detail::reject_symlinks(path);
    detail::require(!path.filename().empty() && path.filename() != "." && path.filename() != "..",
                    "a new leaf directory is required", ErrorCode::invalid);
    if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path());
    detail::require(std::filesystem::create_directory(path), "directory already exists", ErrorCode::conflict);
    std::filesystem::permissions(path, std::filesystem::perms::owner_all);
    detail::sync_directory(path);
    detail::sync_directory(path.parent_path().empty() ? "." : path.parent_path());
}

void write_new_file(const std::filesystem::path& path, const Bytes& bytes) {
    detail::File file(path, O_WRONLY | O_CREAT | O_EXCL);
    file.write(0, bytes);
    file.sync();
    detail::sync_directory(path.parent_path().empty() ? "." : path.parent_path());
}

void write_new_file(const std::filesystem::path& path, const std::string& text) {
    write_new_file(path, Bytes(text.begin(), text.end()));
}

namespace detail {

void reject_symlinks(const std::filesystem::path& path) {
    auto current = std::filesystem::path();
    for (const auto& part : std::filesystem::absolute(path)) {
        require(part != "..", "parent traversal is forbidden", ErrorCode::invalid);
        current /= part;
        std::error_code error;
        const auto status = std::filesystem::symlink_status(current, error);
        require(!std::filesystem::is_symlink(status), "symlink paths are forbidden", ErrorCode::invalid);
        if (error && error != std::errc::no_such_file_or_directory)
            throw Error(ErrorCode::io, "cannot inspect path: " + error.message());
    }
}

File::File(const std::filesystem::path& path, int flags, mode_t mode) {
    reject_symlinks(path);
    fd_ = ::open(path.c_str(), flags | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK, mode);
    if (fd_ < 0) {
        const auto code = errno == ENOENT ? ErrorCode::missing :
                          errno == EEXIST ? ErrorCode::conflict : ErrorCode::io;
        throw Error(code, "cannot open file: " + std::string(std::strerror(errno)));
    }
    struct stat status{};
    if (::fstat(fd_, &status) != 0 || !S_ISREG(status.st_mode)) {
        ::close(fd_);
        fd_ = -1;
        throw Error(ErrorCode::invalid, "only regular files are accepted");
    }
}

uint64_t File::size() const {
    struct stat status{};
    require(::fstat(fd_, &status) == 0 && status.st_size >= 0, "cannot stat file", ErrorCode::io);
    return static_cast<uint64_t>(status.st_size);
}

Bytes File::read(uint64_t offset, uint64_t count) const {
    const auto file_size = size();
    require(count <= kMaxObjectBytes, "range exceeds read budget", ErrorCode::resource_limit);
    require(offset <= file_size && count <= file_size - offset, "range exceeds file size");
    Bytes result(static_cast<size_t>(count));
    size_t done = 0;
    while (done < result.size()) {
        const auto got = ::pread(fd_, result.data() + done, result.size() - done,
                                 static_cast<off_t>(offset + done));
        if (got < 0 && errno == EINTR) continue;
        require(got > 0, "short read or read failure", ErrorCode::io);
        done += static_cast<size_t>(got);
    }
    return result;
}

void File::write(uint64_t offset, const Bytes& bytes) {
    require(offset <= kMaxObjectBytes && bytes.size() <= kMaxObjectBytes - offset,
            "write exceeds file budget", ErrorCode::resource_limit);
    size_t done = 0;
    while (done < bytes.size()) {
        const auto written = ::pwrite(fd_, bytes.data() + done, bytes.size() - done,
                                      static_cast<off_t>(offset + done));
        if (written < 0 && errno == EINTR) continue;
        require(written > 0, "write failure", ErrorCode::io);
        done += static_cast<size_t>(written);
    }
}

void File::sync() { require(::fsync(fd_) == 0, "file fsync failed", ErrorCode::io); }

void sync_directory(const std::filesystem::path& path) {
    reject_symlinks(path);
    const int fd = ::open(path.c_str(), O_DIRECTORY | O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    require(fd >= 0, "cannot open directory for fsync", ErrorCode::io);
    const int result = ::fsync(fd);
    ::close(fd);
    require(result == 0, "directory fsync failed", ErrorCode::io);
}

}  // namespace detail
}  // namespace history_cache
