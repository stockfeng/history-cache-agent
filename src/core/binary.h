#pragma once

#include "history_cache/common.h"

#include <algorithm>
#include <cstring>
#include <string_view>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace history_cache::detail {

inline void require(bool condition, std::string_view message,
                    ErrorCode code = ErrorCode::corrupt) {
    if (!condition) throw Error(code, std::string(message));
}

inline uint64_t be(const Bytes& bytes, size_t offset, size_t width) {
    require(width <= 8 && offset <= bytes.size() && width <= bytes.size() - offset,
            "binary field is truncated");
    uint64_t value = 0;
    for (size_t i = 0; i < width; ++i) value = (value << 8U) | bytes[offset + i];
    return value;
}

inline uint64_t le(const Bytes& bytes, size_t offset, size_t width) {
    require(width <= 8 && offset <= bytes.size() && width <= bytes.size() - offset,
            "binary field is truncated");
    uint64_t value = 0;
    for (size_t i = 0; i < width; ++i) value |= uint64_t(bytes[offset + i]) << (i * 8U);
    return value;
}

inline void put_be(Bytes& bytes, size_t offset, uint64_t value, size_t width) {
    require(width <= 8 && offset <= bytes.size() && width <= bytes.size() - offset,
            "binary write is out of bounds", ErrorCode::invalid);
    for (size_t i = 0; i < width; ++i)
        bytes[offset + i] = static_cast<uint8_t>(value >> ((width - 1 - i) * 8U));
}

inline void append_le(Bytes& bytes, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; ++i) bytes.push_back(static_cast<uint8_t>(value >> (i * 8U)));
}

template <class To, class From> To bits(From value) {
    static_assert(sizeof(To) == sizeof(From));
    To result;
    std::memcpy(&result, &value, sizeof(result));
    return result;
}

template <size_t N> std::array<uint8_t, N> fixed(const Bytes& bytes, size_t offset) {
    require(offset <= bytes.size() && N <= bytes.size() - offset, "binary digest is truncated");
    std::array<uint8_t, N> result{};
    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), N, result.begin());
    return result;
}

class File {
public:
    File(const std::filesystem::path& path, int flags, mode_t mode = 0600);
    ~File() { if (fd_ >= 0) ::close(fd_); }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    int fd() const { return fd_; }
    uint64_t size() const;
    Bytes read(uint64_t offset, uint64_t size) const;
    void write(uint64_t offset, const Bytes& bytes);
    void sync();
private:
    int fd_ = -1;
};

void sync_directory(const std::filesystem::path& path);
void reject_symlinks(const std::filesystem::path& path);

}  // namespace history_cache::detail
