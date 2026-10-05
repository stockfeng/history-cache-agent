#pragma once

#include "history_cache/http.h"
#include <nlohmann/json.hpp>
#include <deque>

namespace history_cache::sample {

inline const char* error_name(ErrorCode code) {
    switch (code) {
        case ErrorCode::invalid: return "invalid";
        case ErrorCode::corrupt: return "corrupt";
        case ErrorCode::missing: return "missing";
        case ErrorCode::conflict: return "conflict";
        case ErrorCode::resource_limit: return "resource_limit";
        case ErrorCode::io: return "io";
    }
    return "unknown";
}

inline const char* failure_name(HttpFailure failure) {
    switch (failure) {
        case HttpFailure::none: return "none";
        case HttpFailure::deadline: return "deadline";
        case HttpFailure::cancelled: return "cancelled";
        case HttpFailure::resource_limit: return "resource_limit";
        case HttpFailure::dns: return "dns";
        case HttpFailure::connect: return "connect";
        case HttpFailure::tls: return "tls";
        case HttpFailure::transport: return "transport";
    }
    return "unknown";
}

// Data-side CLI only: bounded diagnostics, no URLs, headers, bodies or retries.
class SnapshotDiagnostics final : public HttpTransport {
public:
    explicit SnapshotDiagnostics(std::shared_ptr<HttpTransport> inner) : inner_(std::move(inner)) {}
    const char* stage = "current_probe";
    static constexpr size_t capacity = 32;

    HttpResult perform(const HttpRequest& request) override {
        const auto started = SteadyClock::now();
        HttpResult result;
        try {
            result = inner_->perform(request);
        } catch (...) {
            result.failure = HttpFailure::transport;
            record(request, result, started, true);
            throw;
        }
        record(request, result, started, false);
        return result;
    }

    nlohmann::json summary() const {
        return {{"total", total_}, {"dropped", total_ - recent_.size()}, {"recent", recent_}};
    }

private:
    void record(const HttpRequest& request, const HttpResult& result,
                SteadyClock::time_point started, bool exception) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            SteadyClock::now() - started).count();
        const auto query = request.url.find('?');
        const auto path = request.url.substr(0, query);
        const auto slash = path.rfind('/');
        const auto leaf = path.substr(slash == std::string::npos ? 0 : slash + 1);
        const char* kind = leaf == "current.json" ? "current" :
            path.find("/manifests/") != std::string::npos ? "manifest_or_proof" :
            path.find("/packs/") != std::string::npos ? "pack" : "other";
        if (recent_.size() == capacity) recent_.pop_front();
        recent_.push_back({{"stage", stage}, {"method", request.method == HttpMethod::get ? "GET" :
            request.method == HttpMethod::put ? "PUT" : "DELETE"}, {"object_kind", kind},
            {"delivery", result.delivery == HttpDelivery::complete ? "complete" :
             result.delivery == HttpDelivery::not_sent ? "not_sent" : "indeterminate"},
            {"failure", failure_name(result.failure)}, {"http_status", result.response.status},
            {"tls_verified", result.response.tls_verified}, {"elapsed_us", elapsed},
            {"transport_exception", exception}, {"dns_us", result.timings.dns_us},
            {"connect_us", result.timings.connect_us}, {"tls_us", result.timings.tls_us},
            {"first_byte_us", result.timings.first_byte_us}, {"total_us", result.timings.total_us},
            {"new_connections", result.timings.new_connections},
            {"curl_code", result.diagnostics.curl_code},
            {"tls_verify_result", result.diagnostics.tls_verify_result}});
        ++total_;
    }

    std::shared_ptr<HttpTransport> inner_;
    uint64_t total_ = 0;
    std::deque<nlohmann::json> recent_;
};

}  // namespace history_cache::sample
