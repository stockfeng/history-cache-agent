#include "history_cache/http.h"

#include "binary.h"
#include <mutex>

#ifdef HC_HAS_CURL
#include <curl/curl.h>
#include <limits>
#include <memory>
#endif

namespace history_cache {

#ifdef HC_HAS_CURL
namespace {

struct Callbacks {
    explicit Callbacks(const HttpRequest& value) : request(value), buffer(value.response_limit) {}
    const HttpRequest& request;
    HttpResponseBuffer buffer;
    bool failed = false;
};

bool stopped(const HttpRequest& request) {
    return SteadyClock::now() >= request.deadline || (request.cancelled && request.cancelled->load());
}

size_t receive_header(char* data, size_t size, size_t count, void* context) noexcept {
    auto& callbacks = *static_cast<Callbacks*>(context);
    try {
        detail::require(count == 0 || size <= std::numeric_limits<size_t>::max() / count, "HTTP header size overflow");
        detail::require(!stopped(callbacks.request), "HTTP request stopped", ErrorCode::io);
        callbacks.buffer.header(std::string_view(data, size * count));
        return size * count;
    } catch (...) { callbacks.failed = true; return 0; }
}

size_t receive_body(char* data, size_t size, size_t count, void* context) noexcept {
    auto& callbacks = *static_cast<Callbacks*>(context);
    try {
        detail::require(count == 0 || size <= std::numeric_limits<size_t>::max() / count, "HTTP body size overflow");
        detail::require(!stopped(callbacks.request), "HTTP request stopped", ErrorCode::io);
        callbacks.buffer.body(reinterpret_cast<const uint8_t*>(data), size * count);
        return size * count;
    } catch (...) { callbacks.failed = true; return 0; }
}

int progress(void* context, curl_off_t, curl_off_t, curl_off_t, curl_off_t) noexcept {
    return stopped(static_cast<Callbacks*>(context)->request) ? 1 : 0;
}

template <class Value> void option(CURL* handle, CURLoption name, Value value) {
    detail::require(curl_easy_setopt(handle, name, value) == CURLE_OK, "cannot configure HTTPS transport", ErrorCode::io);
}

struct Headers {
    curl_slist* value = nullptr;
    ~Headers() { curl_slist_free_all(value); }
    void add(const std::string& field) {
        auto* next = curl_slist_append(value, field.c_str());
        detail::require(next != nullptr, "cannot allocate HTTPS headers", ErrorCode::io);
        value = next;
    }
};

}  // namespace
#endif

struct CurlHttpTransport::Pool {
#ifdef HC_HAS_CURL
    std::mutex mutex;
    std::vector<CURL*> idle;
    size_t active = 0;
    Pool() { idle.reserve(4); }
    ~Pool() { for (auto* handle : idle) curl_easy_cleanup(handle); }
#endif
};

CurlHttpTransport::CurlHttpTransport(bool enabled, bool reuse)
    : CurlHttpTransport(enabled, reuse ? Reuse::reads : Reuse::disabled) {}
CurlHttpTransport::CurlHttpTransport(bool enabled, Reuse reuse)
    : enabled_(enabled), reuse_(reuse), pool_(reuse != Reuse::disabled ? std::make_shared<Pool>() : nullptr) {}
CurlHttpTransport::~CurlHttpTransport() = default;

std::string CurlHttpTransport::runtime_version() {
#ifdef HC_HAS_CURL
    const auto* info = curl_version_info(CURLVERSION_NOW);
    return info && info->version ? info->version : "unavailable";
#else
    return "not-built";
#endif
}

HttpResult CurlHttpTransport::perform(const HttpRequest& request) {
    // This gate precedes even curl initialization; offline tests cannot open sockets.
    if (!enabled_) return {HttpDelivery::not_sent, {}};
#ifndef HC_HAS_CURL
    (void)request;
    return {HttpDelivery::not_sent, {}};
#else
    bool dispatched = false;
    try {
        if (stopped(request)) return {HttpDelivery::not_sent, {}};
        detail::require(request.url.rfind("https://", 0) == 0 && request.url.size() <= 1024 &&
                        request.url.find_first_of("\r\n\t @?#") == std::string::npos &&
                        (request.method == HttpMethod::get || request.method == HttpMethod::put || request.method == HttpMethod::erase) &&
                        request.response_limit <= kMaxObjectBytes && request.body.size() <= kMaxObjectBytes &&
                        (request.method == HttpMethod::put || request.body.empty()), "invalid HTTPS request", ErrorCode::invalid);
        static const auto initialized = curl_global_init(CURL_GLOBAL_DEFAULT);
        detail::require(initialized == CURLE_OK, "curl initialization failed", ErrorCode::io);
        const auto* info = curl_version_info(CURLVERSION_NOW);
        detail::require(info && info->version_num == LIBCURL_VERSION_NUM && (info->features & CURL_VERSION_SSL) &&
                        (info->features & CURL_VERSION_ASYNCHDNS), "matching TLS/async-DNS curl runtime required", ErrorCode::io);
        bool reusable = false;
        CURL* pooled = nullptr;
        if (pool_) {
            std::lock_guard<std::mutex> guard(pool_->mutex);
            if (pool_->active >= 4) return {HttpDelivery::not_sent, {}, HttpFailure::resource_limit};
            if (!pool_->idle.empty()) {
                pooled = pool_->idle.back();
                pool_->idle.pop_back();
            }
            if (!pooled) pooled = curl_easy_init();
            detail::require(pooled != nullptr, "cannot allocate HTTPS handle", ErrorCode::io);
            ++pool_->active;
        }
        // Reset clears per-request headers/method/callbacks but retains connections.
        auto release = [&](CURL* value) {
            if (value) curl_easy_reset(value);
            if (pool_) {
                std::lock_guard<std::mutex> guard(pool_->mutex);
                --pool_->active;
                if (value && reusable) { pool_->idle.push_back(value); return; }
            }
            if (value) curl_easy_cleanup(value);
        };
        std::unique_ptr<CURL, decltype(release)> handle(pooled ? pooled : curl_easy_init(), release);
        detail::require(bool(handle), "cannot allocate HTTPS handle", ErrorCode::io);
        Headers headers;
        size_t header_size = 0;
        for (const auto& [name, value] : request.headers) {
            detail::require(!name.empty() && name.size() <= 64 && value.size() <= 2048 &&
                            std::all_of(name.begin(), name.end(), [](char ch) {
                                return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-';
                            }) &&
                            std::all_of(value.begin(), value.end(), [](unsigned char ch) { return ch >= 0x20 && ch <= 0x7e; }),
                            "invalid HTTPS header", ErrorCode::invalid);
            header_size += name.size() + value.size() + 2;
            detail::require(header_size <= 16384, "HTTPS request headers exceed budget", ErrorCode::resource_limit);
            headers.add(name + ": " + value);
        }
        headers.add("Expect:");
        Callbacks callbacks(request);
        auto* curl = handle.get();
        option(curl, CURLOPT_URL, request.url.c_str());
        // CURLOPT_PROTOCOLS_STR requires curl >= 7.85; Rocky 9 has 7.76.
#if LIBCURL_VERSION_NUM >= 0x075500
        option(curl, CURLOPT_PROTOCOLS_STR, "https");
        option(curl, CURLOPT_REDIR_PROTOCOLS_STR, "https");
#else
        option(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTPS);
        option(curl, CURLOPT_REDIR_PROTOCOLS, CURLPROTO_HTTPS);
#endif
        option(curl, CURLOPT_FOLLOWLOCATION, 0L);
        option(curl, CURLOPT_MAXREDIRS, 0L);
        option(curl, CURLOPT_PROXY, "");
        option(curl, CURLOPT_NOPROXY, "*");
        option(curl, CURLOPT_NETRC, static_cast<long>(CURL_NETRC_IGNORED));
        option(curl, CURLOPT_HTTPAUTH, static_cast<long>(CURLAUTH_NONE));
        option(curl, CURLOPT_SSL_VERIFYPEER, 1L);
        option(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        option(curl, CURLOPT_SSLVERSION, static_cast<long>(CURL_SSLVERSION_TLSv1_2));
        option(curl, CURLOPT_HTTP_VERSION, static_cast<long>(CURL_HTTP_VERSION_1_1));
        option(curl, CURLOPT_FRESH_CONNECT, pool_ ? 0L : 1L);
        option(curl, CURLOPT_FORBID_REUSE, pool_ ? 0L : 1L);
        option(curl, CURLOPT_MAXCONNECTS, 2L);
        option(curl, CURLOPT_MAXAGE_CONN, 30L);
        option(curl, CURLOPT_HTTP_CONTENT_DECODING, 0L);
        option(curl, CURLOPT_NOSIGNAL, 1L);
        option(curl, CURLOPT_HTTPHEADER, headers.value);
        option(curl, CURLOPT_HEADERFUNCTION, receive_header);
        option(curl, CURLOPT_HEADERDATA, &callbacks);
        option(curl, CURLOPT_WRITEFUNCTION, receive_body);
        option(curl, CURLOPT_WRITEDATA, &callbacks);
        option(curl, CURLOPT_NOPROGRESS, 0L);
        option(curl, CURLOPT_MAX_RECV_SPEED_LARGE, static_cast<curl_off_t>(request.receive_bytes_per_second));
        option(curl, CURLOPT_XFERINFOFUNCTION, progress);
        option(curl, CURLOPT_XFERINFODATA, &callbacks);
        if (request.method == HttpMethod::put) {
            option(curl, CURLOPT_POSTFIELDS, request.body.empty() ? "" : reinterpret_cast<const char*>(request.body.data()));
            option(curl, CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.body.size()));
            option(curl, CURLOPT_CUSTOMREQUEST, "PUT");
        } else if (request.method == HttpMethod::get) {
            option(curl, CURLOPT_HTTPGET, 1L);
        } else {
            option(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(request.deadline - SteadyClock::now()).count();
        detail::require(remaining > 0 && remaining <= 30000 && request.connect_timeout.count() > 0,
                        "invalid HTTPS request deadline", ErrorCode::invalid);
        option(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(remaining));
        option(curl, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(std::min(request.connect_timeout.count(), remaining)));
        if (stopped(request)) return {HttpDelivery::not_sent, {}};
        dispatched = true;
        const auto code = curl_easy_perform(curl);
        HttpResult::Diagnostics diagnostics;
        diagnostics.curl_code = static_cast<int>(code);
        long tls = -1;
        if (curl_easy_getinfo(curl, CURLINFO_SSL_VERIFYRESULT, &tls) == CURLE_OK)
            diagnostics.tls_verify_result = tls;
        HttpResult::Timings timings;
        const auto duration = [&](CURLINFO field) {
            curl_off_t value = 0;
            curl_easy_getinfo(curl, field, &value);
            return value > 0 ? static_cast<uint64_t>(value) : uint64_t{0};
        };
        timings.dns_us = duration(CURLINFO_NAMELOOKUP_TIME_T);
        timings.connect_us = duration(CURLINFO_CONNECT_TIME_T);
        timings.tls_us = duration(CURLINFO_APPCONNECT_TIME_T);
        timings.first_byte_us = duration(CURLINFO_STARTTRANSFER_TIME_T);
        timings.total_us = duration(CURLINFO_TOTAL_TIME_T);
        long connections = 0;
        curl_easy_getinfo(curl, CURLINFO_NUM_CONNECTS, &connections);
        timings.new_connections = connections > 0 ? static_cast<uint64_t>(connections) : 0;
        if (code != CURLE_OK || callbacks.failed || stopped(request)) {
            auto failure = HttpFailure::transport;
            if (request.cancelled && request.cancelled->load()) failure = HttpFailure::cancelled;
            else if (stopped(request) || code == CURLE_OPERATION_TIMEDOUT) failure = HttpFailure::deadline;
            else if (code == CURLE_COULDNT_RESOLVE_HOST) failure = HttpFailure::dns;
            else if (code == CURLE_COULDNT_CONNECT) failure = HttpFailure::connect;
            else if (code == CURLE_SSL_CONNECT_ERROR || code == CURLE_PEER_FAILED_VERIFICATION ||
                     code == CURLE_SSL_CACERT_BADFILE) failure = HttpFailure::tls;
            return {HttpDelivery::indeterminate, {}, failure, timings, diagnostics};
        }
        auto response = callbacks.buffer.finish();
        long status = 0;
        detail::require(diagnostics.tls_verify_result == 0 &&
                        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status) == CURLE_OK &&
                        status == static_cast<long>(response.status), "unverified HTTPS response", ErrorCode::io);
        response.tls_verified = true;
        reusable = pool_ && request.method == HttpMethod::get && response.status >= 200 && response.status < 300;
        // Missing-object probes and successful conditional writes are normal in a
        // publication. Reuse the connection, not the response or write outcome.
        if (pool_ && reuse_ == Reuse::publication)
            reusable = reusable || (request.method == HttpMethod::get && response.status == 404) ||
                       (request.method == HttpMethod::put && response.status == 200);
        return {HttpDelivery::complete, std::move(response), HttpFailure::none, timings, diagnostics};
    } catch (...) {
        return {dispatched ? HttpDelivery::indeterminate : HttpDelivery::not_sent, {}};
    }
#endif
}

}  // namespace history_cache
