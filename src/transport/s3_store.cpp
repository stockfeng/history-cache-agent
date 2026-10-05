#include "history_cache/s3_store.h"

#include "history_cache/catalog.h"
#include "binary.h"
#include <nlohmann/json.hpp>

#include <ctime>
#include <mutex>
#include <set>

namespace history_cache {
namespace {

bool lower_alnum(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9');
}

bool isolated_scope(const std::string& value) {
    return value.rfind("isolated-", 0) == 0 && value.size() >= 10 && value.size() <= 49 &&
        lower_alnum(value[9]) && std::all_of(value.begin(), value.end(), [](char ch) {
            return lower_alnum(ch) || ch == '-';
        });
}

std::string endpoint(const S3Config& config) {
    detail::require(config.account_id.size() == 32 &&
                    std::all_of(config.account_id.begin(), config.account_id.end(), [](char ch) {
                        return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
                    }), "invalid R2 account identifier", ErrorCode::invalid);
    const auto& bucket = config.bucket;
    detail::require(bucket.size() >= 3 && bucket.size() <= 63 && lower_alnum(bucket.front()) &&
                    lower_alnum(bucket.back()) && std::all_of(bucket.begin(), bucket.end(), [](char ch) {
                        return lower_alnum(ch) || ch == '-';
                    }), "invalid staging bucket", ErrorCode::invalid);
    const auto segments = '-' + bucket + '-';
    detail::require(config.environment == "staging" || config.environment == "production",
                    "invalid storage environment", ErrorCode::invalid);
    if (config.environment == "staging") {
        detail::require(segments.find("-staging-") != std::string::npos &&
                    segments.find("-prod-") == std::string::npos &&
                    segments.find("-production-") == std::string::npos,
                    "a dedicated staging bucket name is required", ErrorCode::invalid);
    } else {
        detail::require(segments.find("-production-") != std::string::npos &&
                        segments.find("-staging-") == std::string::npos,
                        "a dedicated production bucket name is required", ErrorCode::invalid);
    }
    const std::string prefix = config.environment == "production" ? "r2-history-production/" : "r2-history-staging/";
    detail::require(config.key_prefix.rfind(prefix, 0) == 0 && config.key_prefix.back() == '/',
                    "invalid staging namespace", ErrorCode::invalid);
    auto run = config.key_prefix.substr(prefix.size(), config.key_prefix.size() - prefix.size() - 1);
    const auto slash = run.find('/');
    if (slash != std::string::npos) {
        detail::require(config.environment == "staging" && isolated_scope(run.substr(0, slash)),
                        "invalid isolated staging scope", ErrorCode::invalid);
        run = run.substr(slash + 1);
    }
    detail::require(run.size() >= 3 && run.size() <= 63 && lower_alnum(run.front()) &&
                    std::all_of(run.begin(), run.end(), [](char ch) { return lower_alnum(ch) || ch == '-'; }),
                    "invalid staging run identifier", ErrorCode::invalid);
    detail::require(config.jurisdiction == "default" || config.jurisdiction == "eu",
                    "unsupported R2 jurisdiction", ErrorCode::invalid);
    return config.account_id + (config.jurisdiction == "eu" ? ".eu" : "") + ".r2.cloudflarestorage.com";
}

std::string utc_now() {
    const auto now = std::time(nullptr);
    std::tm utc{};
    char output[17]{};
    detail::require(::gmtime_r(&now, &utc) && std::strftime(output, sizeof(output), "%Y%m%dT%H%M%SZ", &utc) == 16,
                    "cannot obtain signing time", ErrorCode::io);
    return output;
}

const std::string& field(const HttpResponse& response, const char* name) {
    const auto found = response.headers.find(name);
    detail::require(found != response.headers.end(), "required S3 response header is missing");
    return found->second;
}

void verified(const HttpResult& result, uint64_t success_limit) {
    if (result.delivery != HttpDelivery::complete) {
        switch (result.failure) {
            case HttpFailure::resource_limit:
                throw Error(ErrorCode::resource_limit, "S3 operation budget exhausted before dispatch");
            case HttpFailure::deadline: throw Error(ErrorCode::io, "S3 operation deadline exceeded");
            case HttpFailure::cancelled: throw Error(ErrorCode::io, "S3 operation cancelled");
            case HttpFailure::dns: throw Error(ErrorCode::io, "S3 DNS lookup failed");
            case HttpFailure::connect: throw Error(ErrorCode::io, "S3 connection failed");
            case HttpFailure::tls: throw Error(ErrorCode::io, "S3 TLS verification or handshake failed");
            default: throw Error(ErrorCode::io, result.delivery == HttpDelivery::not_sent
                ? "S3 request was not sent" : "S3 transport exchange did not complete");
        }
    }
    detail::require(result.response.tls_verified, "S3 response TLS was not verified", ErrorCode::io);
    const auto& response = result.response;
    detail::require(response.status >= 200 && response.status <= 599, "invalid S3 response status");
    const auto limit = response.status >= 300 ? 4096 : success_limit;
    detail::require(response.body.size() <= limit, "S3 response exceeds receive budget", ErrorCode::resource_limit);
    const auto length = response.headers.find("content-length");
    const auto transfer = response.headers.find("transfer-encoding");
    if (length != response.headers.end())
        detail::require(http_unsigned(length->second) == response.body.size(), "S3 response length mismatch");
    detail::require(transfer == response.headers.end() ||
                    (transfer->second == "chunked" && length == response.headers.end()), "ambiguous S3 framing");
    const auto encoding = response.headers.find("content-encoding");
    detail::require(encoding == response.headers.end() || encoding->second == "identity", "encoded S3 response");
    const auto age = response.headers.find("age");
    detail::require(age == response.headers.end() || http_unsigned(age->second) == 0, "cached S3 response rejected");
}

bool missing_key(const HttpResponse& response) {
    if (response.status != 404) return false;
    std::string body(response.body.begin(), response.body.end());
    const auto trim = [&] {
        const auto first = body.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) { body.clear(); return; }
        body = body.substr(first, body.find_last_not_of(" \t\r\n") - first + 1);
    };
    trim();
    if (body.rfind("<?xml ", 0) == 0) {
        const auto end = body.find("?>");
        if (end == std::string::npos || end > 128 || body.substr(1, end - 1).find('<') != std::string::npos) return false;
        body.erase(0, end + 2);
        trim();
    }
    // Only a well-framed direct NoSuchKey child proves absence; fail closed on
    // nested markup, duplicates, DTDs, bare 404s and NoSuchBucket responses.
    if (body.size() < 15 || body.rfind("<Error>", 0) != 0 || body.substr(body.size() - 8) != "</Error>") return false;
    body = body.substr(7, body.size() - 15);
    std::set<std::string> fields;
    bool missing = false;
    while (true) {
        trim();
        if (body.empty()) return missing;
        const auto end = body.find('>');
        if (body.front() != '<' || end == std::string::npos) return false;
        const auto name = body.substr(1, end - 1);
        if (name != "Code" && name != "Message" && name != "Key" && name != "RequestId" &&
            name != "HostId" && name != "Resource" && name != "BucketName") return false;
        if (!fields.insert(name).second) return false;
        const auto next = body.find('<', end + 1);
        const auto closing = "</" + name + '>';
        if (next == std::string::npos || body.compare(next, closing.size(), closing) != 0) return false;
        if (name == "Code") missing = body.substr(end + 1, next - end - 1) == "NoSuchKey";
        body.erase(0, next + closing.size());
    }
}

void read_status(const HttpResponse& response, unsigned expected) {
    if (missing_key(response)) throw Error(ErrorCode::missing, "S3 object is missing");
    if (response.status == 412) throw Error(ErrorCode::conflict, "S3 object identity changed");
    detail::require(response.status == expected, "unexpected S3 read status", ErrorCode::io);
}

WriteOutcome write_result(const HttpResult& result) {
    if (result.delivery == HttpDelivery::not_sent) return WriteOutcome::not_applied;
    try {
        verified(result, 0);
        if (result.response.status == 412) return WriteOutcome::precondition_failed;
        if (result.response.status == 200 && !result.response.headers.count("content-range")) {
            validate_etag(field(result.response, "etag"));
            return WriteOutcome::applied;
        }
    } catch (...) {}
    return WriteOutcome::indeterminate;
}

}  // namespace

S3Config load_storage_profile(const std::string& path, const std::string& role) {
    const auto bytes = read_file(path, 4096);
    const auto value = nlohmann::json::parse(bytes);
    detail::require(value.is_object() && value.size() == 7 && value.at("version").is_number_integer() &&
                    value.at("version") == 1 &&
                    value.at("role") == role && (role == "publisher" || role == "reader"),
                    "invalid storage profile schema or role", ErrorCode::invalid);
    S3Config config{value.at("account_id").get<std::string>(), value.at("bucket").get<std::string>(),
                    value.at("prefix").get<std::string>(), value.at("jurisdiction").get<std::string>(),
                    value.at("environment").get<std::string>(), role == "reader"};
    const auto root = config.environment == "production" ? "r2-history-production/" : "r2-history-staging/";
    const std::string root_text(root);
    const bool isolated = config.environment == "staging" && config.key_prefix.rfind(root_text, 0) == 0 &&
        config.key_prefix.back() == '/' && isolated_scope(config.key_prefix.substr(root_text.size(),
            config.key_prefix.size() - root_text.size() - 1));
    detail::require(config.key_prefix == root || isolated,
                    "storage profile must name the canonical root or isolated staging scope", ErrorCode::invalid);
    auto probe = config;
    probe.key_prefix += "profile-validation/";
    (void)endpoint(probe);
    return config;
}

struct S3Store::State {
    S3Config config;
    std::string host;
    std::shared_ptr<HttpTransport> transport;
    std::shared_ptr<const S3Credentials> credentials;
    TransferLimits limits;
    std::function<std::string()> signing_time;
    mutable std::mutex mutex;
    TransferUsage used;

    HttpResult request(HttpMethod method, const std::string& key, const Bytes& body,
                       uint64_t response_limit, HttpHeaders headers = {}) {
        HttpRequest request;
        try {
            const auto now = SteadyClock::now();
            if (limits.cancelled->load()) return {HttpDelivery::not_sent, {}, HttpFailure::cancelled};
            if (now >= limits.deadline) return {HttpDelivery::not_sent, {}, HttpFailure::deadline};
            request.method = method;
            const auto path = '/' + config.bucket + '/' + config.key_prefix + key;
            request.url = "https://" + host + path;
            request.body = body;
            request.response_limit = response_limit;
            request.deadline = std::min(limits.deadline, now + limits.request_timeout);
            request.connect_timeout = limits.connect_timeout;
            request.cancelled = limits.cancelled;
            request.headers = std::move(headers);
            request.headers["cache-control"] = "no-cache, no-store";
            request.headers["accept-encoding"] = "identity";
            if (method == HttpMethod::put) request.headers["content-type"] = "application/octet-stream";
            sign_s3_request(request, *credentials, host, path, signing_time());
            const auto reserved = std::max<uint64_t>(response_limit, 4096);
            std::lock_guard<std::mutex> guard(mutex);
            if (limits.cancelled->load()) return {HttpDelivery::not_sent, {}, HttpFailure::cancelled};
            if (SteadyClock::now() >= request.deadline) return {HttpDelivery::not_sent, {}, HttpFailure::deadline};
            if (used.requests >= limits.max_requests || body.size() > limits.max_upload_bytes - used.upload_reserved ||
                reserved > limits.max_download_bytes - used.download_reserved)
                return {HttpDelivery::not_sent, {}, HttpFailure::resource_limit};
            ++used.requests;
            used.upload_reserved += body.size();
            used.download_reserved += reserved;
        } catch (...) {
            return {HttpDelivery::not_sent, {}};
        }
        // Once control reaches a transport, an exception cannot prove non-delivery.
        try {
            auto result = transport->perform(request);
            if (result.delivery != HttpDelivery::not_sent &&
                (limits.cancelled->load() || SteadyClock::now() >= request.deadline))
                return {HttpDelivery::indeterminate, {}, limits.cancelled->load()
                    ? HttpFailure::cancelled : HttpFailure::deadline};
            return result;
        }
        catch (...) { return {HttpDelivery::indeterminate, {}}; }
    }
};

S3Store::S3Store(S3Config config, std::shared_ptr<HttpTransport> transport,
                 std::shared_ptr<const S3Credentials> credentials, TransferLimits limits,
                 std::function<std::string()> signing_time) : state_(std::make_shared<State>()) {
    const auto host = endpoint(config);
    detail::require(transport && credentials && limits.cancelled, "missing S3 dependencies", ErrorCode::invalid);
    detail::require(limits.request_timeout.count() > 0 && limits.request_timeout <= std::chrono::seconds(30) &&
                    limits.connect_timeout.count() > 0 && limits.connect_timeout <= limits.request_timeout &&
                    limits.deadline <= SteadyClock::now() + std::chrono::minutes(5) &&
                    limits.max_requests <= 4096 && limits.max_upload_bytes <= 64 * 1024 * 1024 &&
                    limits.max_download_bytes <= 64 * 1024 * 1024,
                    "invalid finite S3 operation budget", ErrorCode::invalid);
    state_->config = std::move(config);
    state_->host = host;
    state_->transport = std::move(transport);
    state_->credentials = std::move(credentials);
    state_->limits = std::move(limits);
    state_->signing_time = signing_time ? std::move(signing_time) : utc_now;
}

Digest S3Store::scope_id() const {
    return sha256("history-s3-scope-v1\nhttps://" + state_->host + '\n' + state_->config.bucket + '\n' +
                  state_->config.key_prefix + '\n');
}

TransferUsage S3Store::usage() const {
    std::lock_guard<std::mutex> guard(state_->mutex);
    return state_->used;
}

Bytes S3Store::get(const std::string& key, uint64_t max_bytes) const {
    const auto reference = parse_immutable_key(key);
    detail::require(max_bytes > 0 && max_bytes <= reference.max_bytes, "invalid S3 GET budget", ErrorCode::resource_limit);
    auto result = state_->request(HttpMethod::get, key, {}, max_bytes);
    verified(result, max_bytes);
    read_status(result.response, 200);
    detail::require(!result.response.headers.count("content-range"), "unexpected partial S3 GET");
    validate_etag(field(result.response, "etag"));
    detail::require(!result.response.body.empty() && sha256(result.response.body) == reference.sha256,
                    "S3 immutable content hash mismatch");
    return std::move(result.response.body);
}

RangeReader S3Store::open_range(const std::string& key, uint64_t expected_size) const {
    const auto reference = parse_immutable_key(key);
    detail::require(expected_size > 0 && expected_size <= reference.max_bytes,
                    "invalid S3 Range object size", ErrorCode::resource_limit);
    struct Pin { std::mutex mutex; std::optional<std::string> etag; };
    const auto pin = std::make_shared<Pin>();
    return [state = state_, pin, key, expected_size](uint64_t offset, uint64_t size) {
        detail::require(size > 0 && size <= kMaxBlockBytes && offset <= expected_size && size <= expected_size - offset,
                        "invalid S3 Range bounds", ErrorCode::resource_limit);
        std::lock_guard<std::mutex> guard(pin->mutex);
        const auto interval = std::to_string(offset) + '-' + std::to_string(offset + size - 1);
        HttpHeaders headers{{"range", "bytes=" + interval}};
        if (pin->etag) headers["if-match"] = *pin->etag;
        auto result = state->request(HttpMethod::get, key, {}, size, std::move(headers));
        verified(result, size);
        read_status(result.response, 206);
        detail::require(field(result.response, "content-range") == "bytes " + interval + '/' + std::to_string(expected_size) &&
                        result.response.body.size() == size, "S3 Range interval or total length mismatch");
        const auto& etag = field(result.response, "etag");
        validate_etag(etag);
        detail::require(!pin->etag || *pin->etag == etag, "S3 Range ETag changed", ErrorCode::conflict);
        pin->etag = etag;
        return std::move(result.response.body);
    };
}

std::optional<VersionedObject> S3Store::read_current() const {
    auto result = state_->request(HttpMethod::get, "current.json", {}, kMaxPointerBytes);
    verified(result, kMaxPointerBytes);
    if (missing_key(result.response)) return std::nullopt;
    read_status(result.response, 200);
    detail::require(!result.response.headers.count("content-range") && !result.response.body.empty(),
                    "invalid complete pointer response");
    const auto etag = field(result.response, "etag");
    validate_etag(etag);
    (void)parse_pointer(std::string(result.response.body.begin(), result.response.body.end()));
    return VersionedObject{std::move(result.response.body), etag};
}

WriteOutcome S3Store::create(const std::string& key, const Bytes& bytes) {
    detail::require(!state_->config.read_only, "read-only storage scope", ErrorCode::invalid);
    const auto reference = parse_immutable_key(key);
    detail::require(!bytes.empty() && bytes.size() <= reference.max_bytes, "invalid S3 upload size", ErrorCode::resource_limit);
    detail::require(sha256(bytes) == reference.sha256, "S3 key/content mismatch", ErrorCode::invalid);
    return write_result(state_->request(HttpMethod::put, key, bytes, 0, {{"if-none-match", "*"}}));
}

WriteOutcome S3Store::write_current(const Bytes& bytes, const std::optional<std::string>& expected_etag) {
    detail::require(!state_->config.read_only, "read-only storage scope", ErrorCode::invalid);
    detail::require(!bytes.empty() && bytes.size() <= kMaxPointerBytes, "invalid pointer upload size", ErrorCode::resource_limit);
    (void)parse_pointer(std::string(bytes.begin(), bytes.end()));
    if (expected_etag) validate_etag(*expected_etag);
    HttpHeaders headers = expected_etag ? HttpHeaders{{"if-match", *expected_etag}} : HttpHeaders{{"if-none-match", "*"}};
    return write_result(state_->request(HttpMethod::put, "current.json", bytes, 0, std::move(headers)));
}

WriteOutcome S3Store::erase_staging_object(const std::string& key, const std::string& expected_etag) {
    detail::require(state_->config.environment == "staging" && !state_->config.read_only,
                    "deletion forbidden outside writable staging", ErrorCode::invalid);
    if (key != "current.json") (void)parse_immutable_key(key);
    validate_etag(expected_etag);
    const auto result = state_->request(HttpMethod::erase, key, {}, 0, {{"if-match", expected_etag}});
    if (result.delivery == HttpDelivery::not_sent) return WriteOutcome::not_applied;
    try {
        verified(result, 0);
        if (result.response.status == 412) return WriteOutcome::precondition_failed;
        if (result.response.status == 204 && !result.response.headers.count("content-range")) return WriteOutcome::applied;
    } catch (...) {}
    return WriteOutcome::indeterminate;
}

}  // namespace history_cache
