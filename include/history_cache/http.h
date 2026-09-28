#pragma once

#include "history_cache/common.h"

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <string_view>

namespace history_cache {

using SteadyClock = std::chrono::steady_clock;
using HttpHeaders = std::map<std::string, std::string>;
enum class HttpMethod { get, put, erase };

struct HttpRequest {
    HttpMethod method = HttpMethod::get;
    std::string url;
    HttpHeaders headers;
    Bytes body;
    uint64_t response_limit = 0;
    SteadyClock::time_point deadline;
    std::chrono::milliseconds connect_timeout{3000};
    std::shared_ptr<std::atomic_bool> cancelled;
};

struct HttpResponse {
    unsigned status = 0;
    HttpHeaders headers;
    Bytes body;
    bool tls_verified = false;
};

enum class HttpDelivery { not_sent, complete, indeterminate };
struct HttpResult {
    HttpDelivery delivery = HttpDelivery::indeterminate;
    HttpResponse response;
};

class HttpTransport {
public:
    virtual ~HttpTransport() = default;
    // One attempt. Never log authorization, raw response bodies or credentials.
    virtual HttpResult perform(const HttpRequest& request) = 0;
};

// Used by the actual curl callbacks and by offline fragmented-response tests.
class HttpResponseBuffer {
public:
    explicit HttpResponseBuffer(uint64_t success_body_limit);
    void header(std::string_view line);
    void body(const uint8_t* data, size_t size);
    HttpResponse finish();
private:
    uint64_t limit_;
    size_t header_bytes_ = 0;
    unsigned status_lines_ = 0;
    bool in_headers_ = false;
    bool final_headers_ = false;
    bool finished_ = false;
    HttpResponse response_;
};

// Built only with HISTORY_CACHE_CURL. Construction does not enable network I/O.
class CurlHttpTransport final : public HttpTransport {
public:
    explicit CurlHttpTransport(bool network_enabled = false) : enabled_(network_enabled) {}
    HttpResult perform(const HttpRequest& request) override;
    static std::string runtime_version();
private:
    bool enabled_;
};

uint64_t http_unsigned(const std::string& value);

}  // namespace history_cache
