#pragma once

#include "history_cache/s3_store.h"
#include "history_cache/catalog.h"

#include <cstdlib>
#include <iostream>
#include <map>
#include <utility>

namespace test {
namespace hc = history_cache;
namespace fs = std::filesystem;

inline size_t cases = 0;
inline std::string case_filter;
inline void check(bool value, const std::string& message) { if (!value) throw std::runtime_error(message); }
template <class Call> void rejects(hc::ErrorCode code, Call call) {
    try { call(); }
    catch (const hc::Error& error) { check(error.code() == code, "unexpected error: " + std::string(error.what())); return; }
    throw std::runtime_error("expected rejection");
}
template <class Call> void run_case(const std::string& name, Call call) {
    if (!case_filter.empty() && case_filter != name) return;
    call(); ++cases; std::cout << "PASS " << name << '\n';
}
inline hc::Bytes bytes(const std::string& text) { return {text.begin(), text.end()}; }
inline std::string text(const hc::Bytes& value) { return {value.begin(), value.end()}; }
inline hc::SeriesIdentity identity() { return {"synthetic", "TEST", "FIXTURE", 60, "none"}; }
inline hc::Digest version() { return hc::sha256(std::string("test-query-semantics-v1\n")); }
inline hc::Manifest manifest(uint64_t source = 1) {
    return {"fixture-epoch-1", {{identity(), version(), source, {0, 10000}, 0, std::nullopt}}};
}
inline std::string key(const hc::Bytes& body) { return "manifests/v1/" + hc::hex(hc::sha256(body)) + ".json"; }
inline hc::Bytes pointer(uint64_t seq = 1) {
    const auto value = bytes(hc::serialize_manifest(manifest(seq)));
    return bytes(hc::serialize_pointer({"fixture-epoch-1", seq, key(value), hc::sha256(value)}));
}
inline hc::S3Config config() { return {std::string(32, 'a'), "history-cache-staging", "r2-history-staging/offline-001/", "default"}; }
inline std::shared_ptr<const hc::S3Credentials> credentials() {
    return std::make_shared<const hc::S3Credentials>("SYNTHETICKEYID", "SYNTHETIC/SECRET+NOTREAL=");
}
inline std::string signing_time() { return "20260921T010203Z"; }
inline hc::TransferLimits limits() {
    hc::TransferLimits result;
    result.max_requests = 128;
    result.max_download_bytes = 64 * 1024 * 1024;
    return result;
}

class Workspace {
public:
    Workspace() {
        auto pattern = (fs::temp_directory_path() / "history-transport-test-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end()); buffer.push_back('\0');
        const auto* path = ::mkdtemp(buffer.data());
        if (!path) throw std::runtime_error("mkdtemp failed");
        root = path;
    }
    ~Workspace() { std::error_code ignored; fs::remove_all(root, ignored); }
    fs::path root;
};

inline hc::HttpResult response(unsigned status, hc::Bytes body = {}, hc::HttpHeaders fields = {}, uint64_t limit = hc::kMaxObjectBytes) {
    hc::HttpResponseBuffer parser(limit);
    parser.header("HTTP/1.1 " + std::to_string(status) + " Synthetic\r\n");
    if (!fields.count("content-length") && !fields.count("transfer-encoding")) fields["content-length"] = std::to_string(body.size());
    for (const auto& [name, value] : fields) parser.header(name + ": " + value + "\r\n");
    parser.header("\r\n");
    for (size_t offset = 0; offset < body.size(); offset += std::min<size_t>(137, body.size() - offset))
        parser.body(body.data() + offset, std::min<size_t>(137, body.size() - offset));
    auto result = parser.finish(); result.tls_verified = true;
    return {hc::HttpDelivery::complete, std::move(result)};
}

class ScriptedHttp : public hc::HttpTransport {
public:
    std::function<hc::HttpResult(const hc::HttpRequest&)> handler;
    std::vector<hc::HttpRequest> calls;
    hc::HttpResult perform(const hc::HttpRequest& request) override {
        calls.push_back(request);
        if (handler) return handler(request);
        return response(500, {}, {}, request.response_limit);
    }
};

enum class PointerFault { none, not_sent, unknown, lost_ack, lost_ack_unreadable };

// In-memory HTTP peer: never opens a socket. Optional backing is synthetic
// object data for fork/exec crash tests, not an S3 filesystem implementation.
class FakeS3 : public hc::HttpTransport {
public:
    struct Object { hc::Bytes body; std::string etag; };
    std::map<std::string, Object> objects;
    std::vector<hc::HttpRequest> calls;
    PointerFault fault = PointerFault::none;
    std::function<void()> after_pointer;
    bool reject_current_read = false;
    uint64_t writes = 0;

    hc::HttpResult perform(const hc::HttpRequest& request) override {
        calls.push_back(request);
        check(request.url.rfind("https://" + config().account_id + ".r2.cloudflarestorage.com/" + config().bucket +
                                '/' + config().key_prefix, 0) == 0, "request escaped synthetic staging namespace");
        check(request.headers.at("cache-control") == "no-cache, no-store" &&
              request.headers.at("accept-encoding") == "identity" && request.headers.count("authorization"), "request not signed or uncached");
        check(request.headers.at("x-amz-content-sha256") == hc::hex(hc::sha256(request.body)), "signed body mismatch");
        const bool current = request.url.substr(request.url.size() - 12) == "current.json";
        if (request.method == hc::HttpMethod::get && current && reject_current_read) {
            reject_current_read = false;
            return {hc::HttpDelivery::indeterminate, {}};
        }
        auto found = objects.find(request.url);
        if (request.method == hc::HttpMethod::put) {
            check((request.headers.count("if-match") + request.headers.count("if-none-match")) == 1, "unconditional PUT");
            const auto mode = current ? std::exchange(fault, PointerFault::none) : PointerFault::none;
            if (mode == PointerFault::not_sent) return {hc::HttpDelivery::not_sent, {}};
            if (mode == PointerFault::unknown) return {hc::HttpDelivery::indeterminate, {}};
            if (request.headers.count("if-none-match")) {
                check(request.headers.at("if-none-match") == "*", "wrong create-only condition");
                if (found != objects.end()) return response(412, {}, {}, request.response_limit);
            } else if (found == objects.end() || found->second.etag != request.headers.at("if-match")) {
                return response(412, {}, {}, request.response_limit);
            }
            const auto etag = "\"opaque/S3-" + std::to_string(++writes) + "\"";
            objects[request.url] = Object{request.body, etag};
            if (current) {
                auto hook = std::exchange(after_pointer, {});
                if (hook) hook();
            }
            if (mode == PointerFault::lost_ack_unreadable) reject_current_read = true;
            if (mode == PointerFault::lost_ack || mode == PointerFault::lost_ack_unreadable)
                return {hc::HttpDelivery::indeterminate, {}};
            return response(200, {}, {{"etag", etag}}, request.response_limit);
        }
        if (found == objects.end())
            return response(404, bytes("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code><Message>Missing</Message></Error>"), {}, request.response_limit);
        const auto& object = found->second;
        if (request.headers.count("if-match") && request.headers.at("if-match") != object.etag)
            return response(412, {}, {}, request.response_limit);
        if (request.headers.count("range")) {
            const auto& range = request.headers.at("range");
            const auto dash = range.find('-');
            check(range.rfind("bytes=", 0) == 0 && dash != std::string::npos, "bad Range request");
            const auto start = hc::http_unsigned(range.substr(6, dash - 6));
            const auto end = hc::http_unsigned(range.substr(dash + 1));
            check(start <= end && end < object.body.size(), "synthetic peer Range out of bounds");
            return response(206, hc::Bytes(object.body.begin() + static_cast<std::ptrdiff_t>(start),
                                          object.body.begin() + static_cast<std::ptrdiff_t>(end + 1)),
                            {{"etag", object.etag}, {"content-range", "bytes " + range.substr(6) + '/' + std::to_string(object.body.size())}},
                            request.response_limit);
        }
        return response(200, object.body, {{"etag", object.etag}}, request.response_limit);
    }
};

class PackFixture {
public:
    PackFixture() {
        for (int64_t i = 0; i < 2050; ++i) {
            const auto price = static_cast<float>(i % 100) * 0.125F;
            rows.push_back({1000 + i * 60000, price, price + 2, price - 1, price + 1, i});
        }
        candidate = manifest();
        auto& entry = candidate.entries[0];
        entry.coverage.end_ms = rows.back().timestamp_ms + 60000;
        entry.row_count = rows.size();
        const auto path = workspace.root / "fixture.r2b";
        entry.pack = hc::write_pack(path, hc::pack_metadata(entry), [&](uint64_t offset, uint32_t count) {
            return std::vector<hc::Row>(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                                      rows.begin() + static_cast<std::ptrdiff_t>(offset + count));
        });
        data = hc::read_file(path, hc::kMaxObjectBytes);
    }
    Workspace workspace;
    std::vector<hc::Row> rows;
    hc::Manifest candidate;
    hc::Bytes data;
};

}  // namespace test
