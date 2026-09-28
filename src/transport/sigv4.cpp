#include "history_cache/s3_store.h"

#include "binary.h"

#include <openssl/crypto.h>
#include <openssl/hmac.h>

namespace history_cache {
namespace {

void wipe(std::string& value) { if (!value.empty()) OPENSSL_cleanse(value.data(), value.size()); }

struct SecretDigest {
    Digest value{};
    ~SecretDigest() { OPENSSL_cleanse(value.data(), value.size()); }
};

void hmac(SecretDigest& output, const uint8_t* key, size_t key_size, const std::string& value) {
    unsigned size = 0;
    detail::require(key_size <= 1024 && HMAC(EVP_sha256(), key, static_cast<int>(key_size),
                    reinterpret_cast<const uint8_t*>(value.data()), value.size(), output.value.data(), &size) && size == 32,
                    "S3 request signing failed", ErrorCode::io);
}

}  // namespace

S3Credentials::S3Credentials(std::string access_key_id, std::string secret_access_key)
    : id_(std::move(access_key_id)), secret_(std::move(secret_access_key)) {
    const auto safe = [](const std::string& value) {
        return !value.empty() && value.size() <= 128 && std::all_of(value.begin(), value.end(), [](unsigned char ch) {
            return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                   (ch >= '0' && ch <= '9') || ch == '/' || ch == '+' || ch == '=';
        });
    };
    if (!safe(id_) || !safe(secret_) || id_.find_first_of("/+=") != std::string::npos) {
        wipe(id_); wipe(secret_);
        throw Error(ErrorCode::invalid, "invalid S3 credential shape");
    }
}

S3Credentials::~S3Credentials() { wipe(id_); wipe(secret_); }

void sign_s3_request(HttpRequest& request, const S3Credentials& credentials,
                     const std::string& host, const std::string& path, const std::string& amz_date) {
    detail::require(amz_date.size() == 16 && amz_date[8] == 'T' && amz_date[15] == 'Z',
                    "invalid signing timestamp", ErrorCode::invalid);
    for (size_t i = 0; i < amz_date.size(); ++i)
        detail::require(i == 8 || i == 15 || (amz_date[i] >= '0' && amz_date[i] <= '9'),
                        "invalid signing timestamp", ErrorCode::invalid);
    const auto number = [&](size_t offset, size_t count) { return http_unsigned(amz_date.substr(offset, count)); };
    const auto year = number(0, 4), month = number(4, 2), day = number(6, 2);
    const unsigned days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    detail::require(year >= 2000 && month >= 1 && month <= 12 && day >= 1 &&
                    day <= days[month - 1] + (month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 1U : 0U) &&
                    number(9, 2) <= 23 && number(11, 2) <= 59 && number(13, 2) <= 59,
                    "invalid signing calendar timestamp", ErrorCode::invalid);
    detail::require(!host.empty() && host.size() <= 128 && !path.empty() && path[0] == '/' && path.size() <= 512 &&
                    request.url == "https://" + host + path && request.body.size() <= kMaxObjectBytes,
                    "invalid S3 canonical URL", ErrorCode::invalid);
    for (const unsigned char ch : host)
        detail::require((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == '-',
                        "invalid signing host", ErrorCode::invalid);
    for (const unsigned char ch : path)
        detail::require((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
                        ch == '/' || ch == '.' || ch == '-' || ch == '_', "noncanonical signing path", ErrorCode::invalid);
    detail::require((request.method == HttpMethod::get || request.method == HttpMethod::put || request.method == HttpMethod::erase) &&
                    (request.method == HttpMethod::put || request.body.empty()) &&
                    path.substr(path.find_last_of('/') + 1) != "." && path.substr(path.find_last_of('/') + 1) != ".." &&
                    path.find("//") == std::string::npos && path.find("/../") == std::string::npos &&
                    path.find("/./") == std::string::npos && !request.headers.count("authorization"),
                    "unsafe or already signed request", ErrorCode::invalid);
    request.headers["host"] = host;
    request.headers["x-amz-date"] = amz_date;
    request.headers["x-amz-content-sha256"] = hex(sha256(request.body));
    std::string canonical_headers;
    std::string signed_headers;
    for (const auto& [name, value] : request.headers) {
        detail::require(!name.empty() && name.size() <= 64 && value.size() <= 1024 && !value.empty() &&
                        value.front() != ' ' && value.back() != ' ' && value.find("  ") == std::string::npos,
                        "noncanonical signing header", ErrorCode::invalid);
        for (const unsigned char ch : name)
            detail::require((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-',
                            "invalid signed header name", ErrorCode::invalid);
        for (const unsigned char ch : value)
            detail::require(ch >= 0x20 && ch <= 0x7e, "invalid signed header value", ErrorCode::invalid);
        detail::require(canonical_headers.size() + name.size() + value.size() + 2 <= 8192,
                        "signing headers exceed budget", ErrorCode::resource_limit);
        canonical_headers += name + ':' + value + '\n';
        if (!signed_headers.empty()) signed_headers += ';';
        signed_headers += name;
    }
    detail::require(canonical_headers.size() <= 8192, "signing headers exceed budget", ErrorCode::resource_limit);
    const auto canonical = std::string(request.method == HttpMethod::get ? "GET\n" :
                                       request.method == HttpMethod::put ? "PUT\n" : "DELETE\n") + path + "\n\n" +
                           canonical_headers + '\n' + signed_headers + '\n' + request.headers.at("x-amz-content-sha256");
    const auto scope = amz_date.substr(0, 8) + "/auto/s3/aws4_request";
    const auto to_sign = "AWS4-HMAC-SHA256\n" + amz_date + '\n' + scope + '\n' + hex(sha256(canonical));
    std::string initial_key;
    initial_key.reserve(4 + credentials.secret_.size());
    initial_key.append("AWS4");
    initial_key.append(credentials.secret_);
    SecretDigest date, region, service, signing, signature;
    try { hmac(date, reinterpret_cast<const uint8_t*>(initial_key.data()), initial_key.size(), amz_date.substr(0, 8)); }
    catch (...) { wipe(initial_key); throw; }
    wipe(initial_key);
    hmac(region, date.value.data(), date.value.size(), "auto");
    hmac(service, region.value.data(), region.value.size(), "s3");
    hmac(signing, service.value.data(), service.value.size(), "aws4_request");
    hmac(signature, signing.value.data(), signing.value.size(), to_sign);
    request.headers["authorization"] = "AWS4-HMAC-SHA256 Credential=" + credentials.id_ + '/' + scope +
        ", SignedHeaders=" + signed_headers + ", Signature=" + hex(signature.value);
}

}  // namespace history_cache
