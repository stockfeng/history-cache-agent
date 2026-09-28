#include "history_cache/http.h"

#include "binary.h"

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
        std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> handle(curl_easy_init(), curl_easy_cleanup);
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
        option(curl, CURLOPT_PROTOCOLS_STR, "https");
        option(curl, CURLOPT_REDIR_PROTOCOLS_STR, "https");
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
        option(curl, CURLOPT_FRESH_CONNECT, 1L);
        option(curl, CURLOPT_FORBID_REUSE, 1L);
        option(curl, CURLOPT_HTTP_CONTENT_DECODING, 0L);
        option(curl, CURLOPT_NOSIGNAL, 1L);
        option(curl, CURLOPT_HTTPHEADER, headers.value);
        option(curl, CURLOPT_HEADERFUNCTION, receive_header);
        option(curl, CURLOPT_HEADERDATA, &callbacks);
        option(curl, CURLOPT_WRITEFUNCTION, receive_body);
        option(curl, CURLOPT_WRITEDATA, &callbacks);
        option(curl, CURLOPT_NOPROGRESS, 0L);
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
        if (code != CURLE_OK || callbacks.failed || stopped(request)) return {HttpDelivery::indeterminate, {}};
        auto response = callbacks.buffer.finish();
        long tls = -1, status = 0;
        detail::require(curl_easy_getinfo(curl, CURLINFO_SSL_VERIFYRESULT, &tls) == CURLE_OK && tls == 0 &&
                        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status) == CURLE_OK &&
                        status == static_cast<long>(response.status), "unverified HTTPS response", ErrorCode::io);
        response.tls_verified = true;
        return {HttpDelivery::complete, std::move(response)};
    } catch (...) {
        return {dispatched ? HttpDelivery::indeterminate : HttpDelivery::not_sent, {}};
    }
#endif
}

}  // namespace history_cache
