#include "history_cache/http.h"

#include "binary.h"

#include <charconv>

namespace history_cache {
namespace {

bool token(unsigned char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= '0' && ch <= '9') || std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(ch)) != std::string_view::npos;
}

std::string trim(std::string_view value) {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) value.remove_suffix(1);
    return std::string(value);
}

}  // namespace

uint64_t http_unsigned(const std::string& value) {
    uint64_t result = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    detail::require(!value.empty() && parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size(),
                    "invalid HTTP numeric field");
    return result;
}

HttpResponseBuffer::HttpResponseBuffer(uint64_t success_body_limit) : limit_(success_body_limit) {
    detail::require(limit_ <= kMaxObjectBytes, "HTTP body limit exceeds object budget", ErrorCode::resource_limit);
}

void HttpResponseBuffer::header(std::string_view line) {
    detail::require(!finished_, "HTTP response is already finished");
    detail::require(line.size() <= 32768 - header_bytes_, "HTTP headers exceed budget", ErrorCode::resource_limit);
    header_bytes_ += line.size();
    detail::require(line.size() >= 2 && line.substr(line.size() - 2) == "\r\n", "invalid HTTP header framing");
    line.remove_suffix(2);
    for (const unsigned char ch : line)
        detail::require(ch == '\t' || (ch >= 0x20 && ch != 0x7f), "invalid HTTP line character");
    if (line.rfind("HTTP/", 0) == 0) {
        detail::require(!in_headers_ && !final_headers_ && ++status_lines_ <= 4 && line.size() >= 12 &&
                        (line.substr(0, 9) == "HTTP/1.1 " || line.substr(0, 9) == "HTTP/1.0 ") &&
                        (line.size() == 12 || line[12] == ' '), "invalid HTTP status sequence");
        const auto status = http_unsigned(std::string(line.substr(9, 3)));
        detail::require(status >= 100 && status <= 599 && status != 101 &&
                        (status >= 200 || status == 100 || status == 103), "unsupported HTTP status");
        response_ = {};
        response_.status = static_cast<unsigned>(status);
        in_headers_ = true;
        return;
    }
    detail::require(in_headers_ && !final_headers_, "HTTP trailers or missing status are not supported");
    if (line.empty()) {
        in_headers_ = false;
        if (response_.status < 200) {
            detail::require(!response_.headers.count("content-length") && !response_.headers.count("transfer-encoding"),
                            "informational response has a body");
            return;
        }
        final_headers_ = true;
        const auto& fields = response_.headers;
        const auto encoding = fields.find("content-encoding");
        detail::require(encoding == fields.end() || encoding->second == "identity", "encoded HTTP response rejected");
        const auto length = fields.find("content-length");
        const auto transfer = fields.find("transfer-encoding");
        detail::require(transfer == fields.end() || (transfer->second == "chunked" && length == fields.end()),
                        "ambiguous HTTP response framing");
        const auto budget = response_.status >= 300 ? 4096 : limit_;
        if (length != fields.end())
            detail::require(http_unsigned(length->second) <= budget, "HTTP body exceeds budget", ErrorCode::resource_limit);
        return;
    }
    const auto separator = line.find(':');
    detail::require(separator != std::string_view::npos && separator > 0, "invalid HTTP header field");
    auto name = std::string(line.substr(0, separator));
    for (auto& ch : name) {
        detail::require(token(static_cast<unsigned char>(ch)), "invalid HTTP header name");
        if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch - 'A' + 'a');
    }
    for (const auto ch : line.substr(separator + 1))
        detail::require(ch == '\t' || (static_cast<unsigned char>(ch) >= 0x20 && ch != 0x7f), "invalid HTTP header value");
    if (name == "content-length" || name == "content-range" || name == "etag" ||
        name == "content-encoding" || name == "transfer-encoding" || name == "age") {
        detail::require(response_.headers.emplace(name, trim(line.substr(separator + 1))).second,
                        "duplicate HTTP singleton header");
    }
}

void HttpResponseBuffer::body(const uint8_t* data, size_t size) {
    detail::require(!finished_ && final_headers_ && (size == 0 || data != nullptr), "invalid HTTP body state");
    const auto budget = response_.status >= 300 ? 4096 : limit_;
    detail::require(size <= budget - response_.body.size(), "HTTP body exceeds budget", ErrorCode::resource_limit);
    if (size) response_.body.insert(response_.body.end(), data, data + size);
}

HttpResponse HttpResponseBuffer::finish() {
    detail::require(!finished_ && final_headers_ && !in_headers_, "incomplete or finished HTTP response");
    const auto length = response_.headers.find("content-length");
    if (length != response_.headers.end())
        detail::require(http_unsigned(length->second) == response_.body.size(), "short HTTP response body");
    finished_ = true;
    return std::move(response_);
}

}  // namespace history_cache
