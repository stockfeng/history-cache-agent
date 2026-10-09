#include "history_cache/agent.h"
#include "history_cache/market_clock.h"

#include <algorithm>
#include <ctime>
#include <cstring>
#include <iomanip>
#include <regex>
#include <sstream>
#include <thread>

namespace history_cache {
namespace {
using Json = nlohmann::json;

// Every namespace and retry in one query shares this aggregate receive budget.
class QueryTransport final : public HttpTransport {
public:
    QueryTransport(std::shared_ptr<HttpTransport> transport, TransferLimits limits, Json& metrics,
                   std::string& phase, bool background)
        : transport_(std::move(transport)), limits_(std::move(limits)), metrics_(metrics), phase_(phase),
          background_(background) {}

    HttpResult perform(const HttpRequest& request) override {
        if (SteadyClock::now() >= limits_.deadline)
            return {HttpDelivery::not_sent, {}, HttpFailure::deadline};
        if (limits_.cancelled->load()) return {HttpDelivery::not_sent, {}, HttpFailure::cancelled};
        const auto reserved = std::max<uint64_t>(request.response_limit, 4096);
        if (request.method != HttpMethod::get || requests_ >= limits_.max_requests ||
            reserved > limits_.max_download_bytes - download_)
            return {HttpDelivery::not_sent, {}, HttpFailure::resource_limit};
        ++requests_;
        download_ += reserved;
        const auto started = SteadyClock::now();
        auto bounded = request;
        bounded.receive_bytes_per_second = background_ ? 128 * 1024 : 512 * 1024;
        auto result = transport_->perform(bounded);
        const char* stage = request.headers.count("range") ? phase_.c_str() :
            request.url.find("/current.json") != std::string::npos ? "current" :
            request.url.find("/manifests/") != std::string::npos ? "manifest" : "pack";
        auto& stats = metrics_["http"][stage];
        if (stats.is_null()) stats = Json::object();
        const auto add = [&](const char* name, uint64_t value) { stats[name] = stats.value(name, uint64_t{0}) + value; };
        add("requests", 1);
        add("elapsed_us", static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            SteadyClock::now() - started).count()));
        add("bytes", result.response.body.size());
        add("new_connections", result.timings.new_connections);
        add("dns_us", result.timings.dns_us);
        add("connect_us", result.timings.connect_us);
        add("tls_us", result.timings.tls_us);
        add("first_byte_us", result.timings.first_byte_us);
        add("failures", result.delivery == HttpDelivery::complete ? 0 : 1);
        return result;
    }
private:
    std::shared_ptr<HttpTransport> transport_;
    TransferLimits limits_;
    uint64_t requests_ = 0;
    uint64_t download_ = 0;
    Json& metrics_;
    std::string& phase_;
    bool background_;
};

struct CacheNotReady {};

int64_t wall_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

Json factor_metadata(const FactorSnapshot& snapshot, const AdjustmentSnapshot& factors) {
    const auto now = wall_now_ms();
    if (snapshot.observed_at_ms > now) throw Error(ErrorCode::corrupt, "factor observation is future-dated");
    return {{"algorithm", "upcloud-adjustment-v1"}, {"factor_set_hash", hex(factors.factor_set_hash)},
        {"factor_source_epoch", snapshot.source_epoch},
        {"model", factors.model == AdjustmentModel::cumulative ? "cumulative" : "futu_ab"},
        {"factor_freshness_policy", "versioned-v1"}, {"factor_observed_at_ms", snapshot.observed_at_ms},
        {"factor_evaluated_at_ms", now}, {"factor_next_check_ms", snapshot.valid_until_ms},
        {"factor_check_status", now < snapshot.valid_until_ms ? "current" : "overdue"},
        {"factor_valid_until_ms", snapshot.valid_until_ms}};
}

std::string market_of(const std::string& symbol) {
    static const std::regex stock("[0-9]{6}\\.(SH|SZ)");
    static const std::regex hk("[0-9]{5}\\.HK");
    static const std::regex futures("[A-Za-z]{1,3}[0-9]{3,4}\\.(SHF|CFE|CZC|DCE|INE)");
    static const std::regex us("[A-Za-z][A-Za-z0-9.\\-]{0,15}");
    static const std::regex options("AP[0-9]{3}[CP][0-9]{1,8}\\.CZC");
    if (std::regex_match(symbol, options)) return "CZC";
    if (std::regex_match(symbol, stock) || std::regex_match(symbol, hk) || std::regex_match(symbol, futures))
        return symbol.substr(symbol.rfind('.') + 1);
    const auto suffix = symbol.substr(symbol.rfind('.') + 1);
    if (suffix == "SHF" || suffix == "CFE" || suffix == "CZC" || suffix == "DCE" || suffix == "INE")
        throw Error(ErrorCode::invalid, "unsupported domestic derivative symbol");
    if (std::regex_match(symbol, us)) return "US";
    throw Error(ErrorCode::invalid, "unsupported symbol");
}

std::vector<std::string> months_of(int64_t start_ms, int64_t end_ms, bool us) {
    const int64_t offset = us ? 0 : 28800000;
    auto start = static_cast<time_t>((start_ms + offset) / 1000);
    auto end = static_cast<time_t>((end_ms - 1 + offset) / 1000);
    std::tm first{}, last{};
    if (!gmtime_r(&start, &first) || !gmtime_r(&end, &last))
        throw Error(ErrorCode::invalid, "invalid query dates");
    int year = first.tm_year + 1900, month = first.tm_mon + 1;
    std::vector<std::string> months;
    while (true) {
        std::ostringstream value;
        value << std::setfill('0') << std::setw(4) << year << std::setw(2) << month;
        months.push_back(value.str());
        if (year == last.tm_year + 1900 && month == last.tm_mon + 1) break;
        if (++month == 13) { month = 1; ++year; }
    }
    return months;
}

template<class Call> auto read_with_retry(Call call, SteadyClock::time_point deadline) {
    for (int attempt = 0;; ++attempt) {
        if (SteadyClock::now() >= deadline) throw Error(ErrorCode::io, "S3 operation deadline exceeded");
        try { return call(); }
        catch (const Error& error) {
            const auto delay = std::chrono::milliseconds(100 * (attempt + 1));
            if (error.code() != ErrorCode::io || attempt == 2 || SteadyClock::now() + delay >= deadline) throw;
            std::this_thread::sleep_for(delay);
        }
    }
}
}  // namespace

struct Agent::QueryContext {
    const Agent& agent;
    TransferLimits limits;
    Json metrics = {{"http", Json::object()}, {"snapshot_hits", 0}, {"pack_hits", 0}};
    std::string phase = "range";
    std::shared_ptr<HttpTransport> transport;
    std::unordered_map<std::string, std::shared_ptr<S3Store>> stores;
    bool background;
    bool network;
    bool full = false;
    bool native = false;
    std::string adjust = "none";

    explicit QueryContext(const Agent& owner, bool warming = false,
                          std::shared_ptr<std::atomic_bool> cancelled = {},
                          SteadyClock::time_point external_deadline = SteadyClock::time_point::max())
        : agent(owner), background(warming), network(warming || owner.config_.foreground_network) {
        limits.deadline = std::min(external_deadline, SteadyClock::now() + agent.config_.query_timeout);
        limits.max_requests = agent.config_.max_requests;
        if (warming) {
            limits.max_requests = std::min<uint64_t>(6, limits.max_requests);
            limits.max_download_bytes = 4 * 1024 * 1024;
            limits.deadline = SteadyClock::now() + std::chrono::seconds(5);
        }
        if (cancelled) limits.cancelled = std::move(cancelled);
        transport = std::make_shared<QueryTransport>(agent.transport_, limits, metrics, phase, background);
    }

    std::shared_ptr<S3Store> store(const std::string& name) {
        auto& result = stores[name];
        if (!result) result = std::make_shared<S3Store>(S3Config{agent.config_.account_id,
            agent.config_.bucket, agent.config_.key_prefix + name + "/", agent.config_.jurisdiction,
            agent.config_.storage_environment, true}, transport, agent.credentials_, limits);
        return result;
    }
};

Agent::Agent(AgentConfig config, std::shared_ptr<HttpTransport> transport)
    : config_(std::move(config)),
      credentials_(std::make_shared<S3Credentials>(config_.access_key_id, config_.secret_access_key)),
      transport_(std::move(transport)) {
    validate_suspensions(config_.confirmed_suspensions);
    if (!transport_ || config_.max_rows == 0 || config_.max_rows > 5000 ||
        config_.manifest_ttl_seconds > 86400 ||
        config_.max_requests == 0 || config_.max_requests > 4096 ||
        config_.query_timeout.count() <= 0 || config_.query_timeout > std::chrono::seconds(30) ||
        config_.max_cached_pack_bytes > 256 * 1024 * 1024 || config_.max_cached_packs > 4096 ||
        config_.full_pack_read_bytes > 1024 * 1024 ||
        config_.max_cached_snapshots == 0 || config_.max_cached_snapshots > 4096)
        throw Error(ErrorCode::invalid, "invalid agent limits");
}

std::shared_ptr<const Snapshot> Agent::get_snapshot(const std::string& name, QueryContext& query) {
    std::shared_ptr<const Snapshot> previous;
    {
        std::lock_guard<std::mutex> guard(cache_mutex_);
        const auto found = cache_.find(name);
        if (found != cache_.end()) previous = found->second.snapshot;
        if (!query.background && found != cache_.end() && SteadyClock::now() - found->second.fetched_at <
            std::chrono::seconds(name.rfind("intraday-", 0) == 0 ?
                std::min<uint64_t>(15, config_.manifest_ttl_seconds) : config_.manifest_ttl_seconds)) {
            query.metrics["snapshot_hits"] = query.metrics["snapshot_hits"].get<uint64_t>() + 1;
            return found->second.snapshot;
        }
    }
    if (!query.network) throw CacheNotReady{};
    auto store = query.store(name);
    const auto snapshot = read_with_retry([&]() -> std::shared_ptr<const Snapshot> {
        const auto current = store->read_current();
        if (!current) return nullptr;
        auto pointer = parse_pointer(std::string(current->bytes.begin(), current->bytes.end()));
        if (previous && (pointer.publication_seq < previous->pointer.publication_seq ||
            (pointer.publication_seq == previous->pointer.publication_seq &&
             serialize_pointer(pointer) != serialize_pointer(previous->pointer))))
            throw Error(ErrorCode::conflict, "current pointer regressed or changed at the same sequence");
        if (previous && pointer.dataset_epoch == previous->pointer.dataset_epoch &&
            pointer.publication_seq == previous->pointer.publication_seq &&
            pointer.manifest_key == previous->pointer.manifest_key &&
            pointer.manifest_sha256 == previous->pointer.manifest_sha256) return previous;
        const auto bytes = store->get(pointer.manifest_key, kMaxMetadataBytes);
        auto manifest = parse_manifest(std::string(bytes.begin(), bytes.end()));
        if (pointer.dataset_epoch != manifest.dataset_epoch)
            throw Error(ErrorCode::corrupt, "pointer/manifest epoch mismatch");
        return std::make_shared<const Snapshot>(Snapshot{std::move(pointer), std::move(manifest)});
    }, query.limits.deadline);
    std::lock_guard<std::mutex> guard(cache_mutex_);
    if (!snapshot) {
        // A removed current pointer must not leave previously verified coverage live.
        const auto found = cache_.find(name);
        if (found != cache_.end() && found->second.snapshot == previous) cache_.erase(found);
        return nullptr;
    }
    const auto found = cache_.find(name);
    if (found != cache_.end() && found->second.snapshot->pointer.publication_seq > snapshot->pointer.publication_seq)
        return found->second.snapshot; // A slower refresh cannot return older coverage either.
    if (found != cache_.end() && found->second.snapshot->pointer.publication_seq == snapshot->pointer.publication_seq &&
        serialize_pointer(found->second.snapshot->pointer) != serialize_pointer(snapshot->pointer))
        throw Error(ErrorCode::conflict, "concurrent current pointers disagree");
    if (found == cache_.end() && cache_.size() >= config_.max_cached_snapshots) {
        const auto oldest = std::min_element(cache_.begin(), cache_.end(), [](const auto& a, const auto& b) {
            return a.second.fetched_at < b.second.fetched_at;
        });
        cache_.erase(oldest);
    }
    cache_[name] = {snapshot, SteadyClock::now()};
    return snapshot;
}

Json Agent::handle_query(const Json& request) {
    auto deadline = SteadyClock::time_point::max();
    if (request.contains("deadline_mono_ms")) {
        const auto& value = request.at("deadline_mono_ms");
        if (!value.is_number_integer() || (value.is_number_unsigned() && value.get<uint64_t>() > uint64_t{INT64_MAX}))
            return {{"status", "ERROR"}, {"reason", "invalid_request"}};
        const auto ms = value.get<int64_t>();
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(SteadyClock::now().time_since_epoch()).count();
        if (ms <= now || ms - now > 30000) return {{"status", "ERROR"}, {"reason", "query_budget_exhausted"}};
        deadline = SteadyClock::time_point(std::chrono::milliseconds(ms));
    }
    QueryContext context(*this, false, {}, deadline);
    const bool network = context.network;
    context.network = false;
    auto cached = execute(request, context);
    if (!network || cached.value("reason", "") != "cache_not_ready") return cached;
    struct Pending {
        std::atomic<unsigned>& count;
        explicit Pending(std::atomic<unsigned>& value) : count(value) { ++count; }
        ~Pending() { --count; }
    } pending(foreground_pending_);
    {
        std::lock_guard<std::mutex> lock(priority_mutex_);
        if (auto cancel = background_cancel_.lock()) cancel->store(true);
    }
    // One cold operation at a time. Waiters recheck shared caches after the
    // leader completes; waiting consumes the original query deadline.
    std::unique_lock<std::timed_mutex> cold(cold_mutex_, std::defer_lock);
    if (!cold.try_lock_until(context.limits.deadline)) {
        cached["status"] = "ERROR";
        cached["reason"] = "query_busy";
        return cached;
    }
    context.network = true;
    context.metrics = {{"http", Json::object()}, {"snapshot_hits", 0}, {"pack_hits", 0}};
    return execute(request, context);
}

std::shared_ptr<const FactorSnapshot> Agent::get_factors(const std::string& name,
    const std::string& symbol, const std::string& market, QueryContext& query) {
    std::optional<CachedFactors> previous;
    {
        std::lock_guard<std::mutex> guard(cache_mutex_);
        const auto found = factors_.find(name);
        if (found != factors_.end()) previous = found->second;
        if (previous && wall_now_ms() >= previous->snapshot->observed_at_ms &&
            SteadyClock::now() - previous->fetched_at <
                std::chrono::seconds(std::min<uint64_t>(15, config_.manifest_ttl_seconds))) {
            query.metrics["factor_hits"] = query.metrics.value("factor_hits", uint64_t{0}) + 1;
            return previous->snapshot;
        }
    }
    if (!query.network) throw CacheNotReady{};
    const auto store = query.store(name);
    const auto current = read_with_retry([&] { return store->read_current(); }, query.limits.deadline);
    if (!current) {
        std::lock_guard<std::mutex> guard(cache_mutex_);
        factors_.erase(name);
        throw Error(ErrorCode::missing, "adjustment factors unavailable");
    }
    const auto pointer = parse_pointer(std::string(current->bytes.begin(), current->bytes.end()));
    if (previous && (pointer.publication_seq < previous->pointer.publication_seq ||
        (pointer.publication_seq == previous->pointer.publication_seq &&
         serialize_pointer(pointer) != serialize_pointer(previous->pointer))))
        throw Error(ErrorCode::conflict, "factor current regressed or changed at same sequence");
    std::shared_ptr<const FactorSnapshot> snapshot;
    if (previous && serialize_pointer(pointer) == serialize_pointer(previous->pointer)) {
        if (wall_now_ms() < previous->snapshot->observed_at_ms)
            throw Error(ErrorCode::corrupt, "factor observation is future-dated");
        snapshot = previous->snapshot;
    } else {
        const auto bytes = read_with_retry([&] { return store->get(pointer.manifest_key, kMaxFactorBytes); }, query.limits.deadline);
        auto parsed = parse_factor_snapshot(bytes, pointer.manifest_sha256, symbol, market, wall_now_ms(),
            FactorReadPolicy::published_version);
        if (parsed.data_hash != Digest{}) {
            const auto key = "manifests/v1/" + hex(parsed.data_hash) + ".json";
            const auto data = read_with_retry([&] { return store->get(key, parsed.data_bytes); }, query.limits.deadline);
            parsed = resolve_factor_snapshot(bytes, pointer.manifest_sha256, data, symbol, market, wall_now_ms(),
                FactorReadPolicy::published_version);
        }
        snapshot = std::make_shared<const FactorSnapshot>(std::move(parsed));
        if (snapshot->source_epoch != pointer.dataset_epoch)
            throw Error(ErrorCode::corrupt, "factor epoch mismatch");
        if (previous && previous->snapshot->source_revision &&
            (snapshot->source_revision < previous->snapshot->source_revision ||
             (snapshot->source_revision == previous->snapshot->source_revision &&
              (snapshot->source_receipt_hash != previous->snapshot->source_receipt_hash ||
               snapshot->data_hash != previous->snapshot->data_hash))))
            throw Error(ErrorCode::conflict, "factor source revision regressed or changed");
    }
    std::lock_guard<std::mutex> guard(cache_mutex_);
    if (!factors_.count(name) && factors_.size() >= kMaxFactorSnapshots) {
        const auto oldest = std::min_element(factors_.begin(), factors_.end(), [](const auto& a, const auto& b) {
            return a.second.fetched_at < b.second.fetched_at;
        });
        factors_.erase(oldest);
    }
    factors_[name] = {pointer, snapshot, SteadyClock::now()};
    return snapshot;
}

Json Agent::warm(const Json& request, std::shared_ptr<std::atomic_bool> cancelled) {
    std::unique_lock<std::mutex> lock(warm_mutex_, std::try_to_lock);
    if (!lock.owns_lock()) return {{"status", "ERROR"}, {"reason", "background_busy"}};
    if (!cancelled || cancelled->load()) return {{"status", "ERROR"}, {"reason", "background_paused"}};
    std::unique_lock<std::timed_mutex> cold(cold_mutex_, std::try_to_lock);
    if (!cold.owns_lock()) return {{"status", "ERROR"}, {"reason", "background_busy"}};
    {
        std::lock_guard<std::mutex> priority(priority_mutex_);
        if (foreground_pending_.load()) return {{"status", "ERROR"}, {"reason", "background_busy"}};
        background_cancel_ = cancelled;
    }
    QueryContext context(*this, true, std::move(cancelled));
    return execute(request, context);
}

Json Agent::execute(const Json& request, QueryContext& context) {
    const auto started = SteadyClock::now();
    auto result = query(request, context);
    result["protocol_version"] = 1;
    result["timestamp_semantics"] = "utc-instant-ms";
    result["range_semantics"] = "half-open";
    result["row_encoding"] = context.native ? "le-ddb-native64-v1" : context.full ? "le-kline48-v1" : "le-i64-f32x4-i64-v1";
    if (context.adjust != "none") result["adjust"] = context.adjust;
    context.metrics["total_us"] = std::chrono::duration_cast<std::chrono::microseconds>(SteadyClock::now() - started).count();
    {
        std::lock_guard<std::mutex> guard(cache_mutex_);
        context.metrics["resident_pack_bytes"] = pack_bytes_;
        context.metrics["resident_packs"] = packs_.size();
        context.metrics["resident_factor_snapshots"] = factors_.size();
    }
    result["metrics"] = std::move(context.metrics);
    return result;
}

std::shared_ptr<const Bytes> Agent::get_pack(const std::shared_ptr<S3Store>& store,
                                          const CatalogEntry& entry, QueryContext& query) {
    const auto& descriptor = *entry.pack;
    // Highly compressible large histories must not turn a tiny request into a
    // full-history decompression; retain the Range path for those packs.
    if (descriptor.bytes > config_.full_pack_read_bytes || entry.row_count > 5000) {
        if (!query.network || query.background) throw CacheNotReady{};
        return nullptr;
    }
    const auto key = hex(store->scope_id()) + ":" + descriptor.key + ":" + hex(descriptor.sha256);
    {
        std::lock_guard<std::mutex> guard(cache_mutex_);
        const auto found = packs_.find(key);
        if (found != packs_.end() && found->second.bytes->size() == descriptor.bytes) {
            found->second.used = ++pack_clock_;
            query.metrics["pack_hits"] = query.metrics["pack_hits"].get<uint64_t>() + 1;
            return found->second.bytes;
        }
    }
    if (!query.network) throw CacheNotReady{};
    if (query.background && (config_.max_cached_packs == 0 || descriptor.bytes > config_.max_cached_pack_bytes))
        throw CacheNotReady{};
    auto bytes = std::make_shared<const Bytes>(read_with_retry([&] {
        return store->get(descriptor.key, descriptor.bytes);
    }, query.limits.deadline));
    if (bytes->size() != descriptor.bytes || sha256(*bytes) != descriptor.sha256)
        throw Error(ErrorCode::corrupt, "pack size or digest mismatch");
    // Cache only a fully validated immutable object; coverage still comes from the current snapshot.
    const RangeReader range = [bytes](uint64_t offset, uint64_t size) {
        if (offset > bytes->size() || size > bytes->size() - offset)
            throw Error(ErrorCode::corrupt, "cached pack range invalid");
        return Bytes(bytes->begin() + static_cast<std::ptrdiff_t>(offset),
                     bytes->begin() + static_cast<std::ptrdiff_t>(offset + size));
    };
    verify_pack(range, descriptor, pack_metadata(entry));
    if (config_.max_cached_packs == 0 || bytes->size() > config_.max_cached_pack_bytes) return bytes;
    std::lock_guard<std::mutex> guard(cache_mutex_);
    if (packs_.count(key)) return bytes;
    while (!packs_.empty() && (packs_.size() >= config_.max_cached_packs ||
           bytes->size() > config_.max_cached_pack_bytes - pack_bytes_)) {
        const auto oldest = std::min_element(packs_.begin(), packs_.end(), [](const auto& a, const auto& b) {
            return a.second.used < b.second.used;
        });
        pack_bytes_ -= oldest->second.bytes->size();
        packs_.erase(oldest);
    }
    pack_bytes_ += bytes->size();
    packs_.emplace(key, CachedPack{bytes, ++pack_clock_});
    return bytes;
}

Json Agent::query(const Json& request, QueryContext& query) {
    try {
        const auto encoding = request.value("row_encoding", "le-i64-f32x4-i64-v1");
        query.full = encoding == "le-kline48-v1";
        query.native = encoding == "le-ddb-native64-v1";
        if (!query.native && !query.full && encoding != "le-i64-f32x4-i64-v1")
            throw Error(ErrorCode::invalid, "unsupported row encoding");
        if (!request.contains("protocol_version") || !request.at("protocol_version").is_number_integer() ||
            request.at("protocol_version") != 1 || request.value("timestamp_semantics", "") != "utc-instant-ms" ||
            request.value("range_semantics", "") != "half-open")
            throw Error(ErrorCode::invalid, "explicit UTC query protocol v1 required");
        for (const auto* field : {"start_ms", "end_ms", "max_rows", "period_seconds"}) {
            if (request.contains(field) && !request.at(field).is_number_integer())
                throw Error(ErrorCode::invalid, "integer request field required");
        }
        const auto symbol = request.value("symbol", "");
        const auto start = request.value("start_ms", int64_t{0});
        const auto end = request.value("end_ms", int64_t{0});
        const auto max_rows = request.value("max_rows", config_.max_rows);
        if (start <= 0 || end <= start || end > 32503680000000LL ||
            end - start > 366LL * 86400000 || max_rows == 0 || max_rows > config_.max_rows)
            throw Error(ErrorCode::invalid, "invalid request bounds");
        query.adjust = request.value("adjust", "none");
        const bool adjusted = query.adjust != "none";
        if (request.value("period_seconds", 60) != 60 ||
            (adjusted && (!config_.enable_adjustment || !query.native || query.background ||
                (query.adjust != "forward" && query.adjust != "backward"))))
            return {{"status", "MISS"}, {"reason", "unsupported_series"}};
        const auto market = market_of(symbol);
        if (adjusted && market != "SH" && market != "SZ" && market != "HK" && market != "US")
            return {{"status", "MISS"}, {"reason", "unsupported_adjustment_market"}};
        const NewYorkClock* clock = nullptr;
        if (market == "US") {
            static const NewYorkClock new_york;
            clock = &new_york;
        }
        const auto stored_start = clock ? clock->to_wall(start) : start;
        const auto stored_end = clock ? clock->to_wall(end) : end;
        if (stored_start >= stored_end) throw Error(ErrorCode::invalid, "invalid wall-clock interval");
        std::string slug;
        for (unsigned char c : symbol) if (c != '.') slug.push_back(static_cast<char>(std::tolower(c)));
        if (market == "US") slug += "-us";
        const bool intraday = request.value("include_intraday", false);
        const bool allow_partial = request.value("allow_partial", false);
        const bool composite = request.value("op", "query") == "adjust_rows";
        if (composite) {
            if (!adjusted || !query.native || query.background || intraday || allow_partial ||
                request.value("input_adjust", "") != "none")
                throw Error(ErrorCode::invalid, "raw native composite required");
            const auto& encoded = request.at("data").get_ref<const std::string&>();
            if (encoded.size() > max_rows * 128 || encoded.size() % 128 != 0)
                throw Error(ErrorCode::invalid, "composite row limit exceeded");
            const auto data = unhex(encoded);
            if (parse_digest(request.at("rows_sha256")) != sha256(data))
                throw Error(ErrorCode::invalid, "composite hash mismatch");
            const auto split = request.at("prefix_end_ms").get<int64_t>();
            const auto prefix_rows = request.at("prefix_rows").get<uint64_t>();
            if (!request.at("prefix_end_ms").is_number_integer() ||
                !request.at("prefix_rows").is_number_integer() || split <= start || split > end ||
                prefix_rows > data.size() / 64)
                throw Error(ErrorCode::invalid, "invalid composite boundary");
            std::vector<Row> rows;
            std::vector<int64_t> days;
            int64_t previous = start - 1;
            for (size_t offset = 0; offset < data.size(); offset += 64) {
                const auto word = [&](size_t position) {
                    uint64_t value = 0;
                    for (size_t i = 0; i < 8; ++i) value |= uint64_t{data[offset + position + i]} << (8 * i);
                    return value;
                };
                Row row;
                for (size_t position : {size_t{0}, size_t{40}, size_t{48}, size_t{56}})
                    if (word(position) > uint64_t{INT64_MAX})
                        throw Error(ErrorCode::invalid, "invalid native integer");
                row.timestamp_ms = static_cast<int64_t>(word(0));
                row.volume = static_cast<int64_t>(word(40));
                row.native = Row::NativeFields{};
                row.native->open_oi = static_cast<int64_t>(word(48));
                row.native->close_oi = static_cast<int64_t>(word(56));
                for (size_t i = 0; i < 4; ++i) {
                    const auto bits = word(8 + i * 8);
                    std::memcpy(&row.native->prices[i], &bits, 8);
                }
                (void)canonical_native(row);
                // Prefix and tail must already have applied the same visibility
                // policy before their row limits. Never shrink a composite here.
                if (has_null_price(row)) throw Error(ErrorCode::invalid, "unfiltered NULL in composite");
                if (row.timestamp_ms <= previous || row.timestamp_ms < start || row.timestamp_ms >= end ||
                    (rows.size() < prefix_rows ? row.timestamp_ms >= split : row.timestamp_ms < split))
                    throw Error(ErrorCode::invalid, "unordered or out-of-range composite");
                previous = row.timestamp_ms;
                days.push_back((clock ? clock->to_wall(previous) : previous + 28800000) / 86400000);
                rows.push_back(row);
            }
            // The trusted local gateway supplies raw rows. Only factors are read
            // here: this is a composite result, not an R2 coverage attestation.
            const auto snapshot = get_factors("factors-" + slug + "-v1", symbol, market, query);
            const auto factors = select_factor_window(*snapshot,
                (stored_start + (clock ? 0 : 28800000)) / 86400000,
                (stored_end - 1 + (clock ? 0 : 28800000)) / 86400000 + 1);
            const auto output = adjust_native_rows(symbol, rows, days, factors,
                query.adjust == "forward" ? AdjustmentMode::forward : AdjustmentMode::backward);
            Bytes result;
            for (const auto& row : output) {
                const auto bytes = canonical_native(row);
                result.insert(result.end(), bytes.begin(), bytes.end());
            }
            if (query.limits.cancelled->load() || SteadyClock::now() >= query.limits.deadline)
                throw Error(ErrorCode::resource_limit, "adjustment query deadline reached");
            auto metadata = factor_metadata(*snapshot, factors);
            metadata["anchor_day"] = query.adjust == "forward" && factors.model == AdjustmentModel::cumulative &&
                !days.empty() ? Json(days.back()) : Json(nullptr);
            return {{"status", "HIT"}, {"result_kind", "gateway-raw-composite-v1"},
                {"rows", output.size()}, {"first_ms", rows.empty() ? 0 : rows.front().timestamp_ms},
                {"last_ms", rows.empty() ? 0 : rows.back().timestamp_ms},
                {"rows_sha256", hex(sha256(result))}, {"data", hex(result.data(), result.size())},
                {"input_rows_sha256", hex(sha256(data))}, {"prefix_rows", prefix_rows}, {"prefix_end_ms", split},
                {"adjustment", std::move(metadata)}};
        }
        if ((intraday || allow_partial) && !query.native)
            throw Error(ErrorCode::invalid, "partial and intraday queries require native64");
        struct Match { CatalogEntry entry; std::shared_ptr<S3Store> store; int64_t start; int64_t end; };
        std::vector<Match> matches;
        std::vector<std::pair<CatalogEntry, std::string>> candidates;
        std::vector<std::string> namespaces;
        std::optional<Digest> version;
        int64_t cursor = stored_start;
        for (const auto& month : months_of(stored_start, stored_end, market == "US")) {
            if (query.limits.cancelled->load()) throw Error(ErrorCode::resource_limit, "query cancelled");
            const auto name = "history-" + slug + "-" + month + (query.native ? "-native64-001" : query.full ? "-kline48-001" : "-001");
            const auto snapshot = get_snapshot(name, query);
            if (!snapshot) continue;
            for (const auto& entry : snapshot->manifest.entries) {
                if (entry.identity.symbol != symbol || entry.identity.market != market ||
                    entry.identity.dataset != (query.native ? "ddb-history-native64" : query.full ? "ddb-history-kline48" : "ddb-history-snapshot") || entry.identity.period_seconds != 60 ||
                    entry.identity.adjust != "none") continue;
                if (entry.coverage.end_ms > stored_start && entry.coverage.start_ms < stored_end)
                    candidates.emplace_back(entry, name);
            }
        }
        const auto accept = [&](const CatalogEntry& entry, const std::string& name) {
            if (version && *version != entry.data_version)
                throw Error(ErrorCode::corrupt, "incompatible data versions");
            version = entry.data_version;
            const auto next = std::min(stored_end, entry.coverage.end_ms);
            matches.push_back({entry, query.store(name), cursor, next});
            if (namespaces.empty() || namespaces.back() != name) namespaces.push_back(name);
            cursor = next;
        };
        // Never scan arbitrary daily namespaces for a year-long historical gap.
        // Two physical dates cover the bounded midnight handover published by
        // the intraday batch. All reads still share the original query budget.
        std::vector<std::string> daily_probes;
        while (cursor < stored_end) {
            const auto found = std::find_if(candidates.begin(), candidates.end(), [&](const auto& item) {
                return item.first.coverage.start_ms <= cursor && item.first.coverage.end_ms > cursor;
            });
            if (found != candidates.end()) { accept(found->first, found->second); continue; }
            if (!intraday || daily_probes.size() >= 2) break;
            auto seconds = static_cast<time_t>((cursor + (market == "US" ? 0 : 28800000)) / 1000);
            std::tm date{};
            if (!gmtime_r(&seconds, &date)) throw Error(ErrorCode::invalid, "invalid intraday date");
            std::ostringstream day;
            day << std::put_time(&date, "%Y%m%d");
            const auto name = "intraday-" + slug + "-" + day.str() + "-native64-001";
            if (std::find(daily_probes.begin(), daily_probes.end(), name) != daily_probes.end()) break;
            daily_probes.push_back(name);
            const auto snapshot = get_snapshot(name, query);
            if (!snapshot) break;
            for (const auto& entry : snapshot->manifest.entries) {
                if (entry.identity.symbol == symbol && entry.identity.market == market &&
                    entry.identity.dataset == "ddb-history-native64" && entry.identity.period_seconds == 60 &&
                    entry.identity.adjust == "none" && entry.coverage.start_ms <= cursor &&
                    entry.coverage.end_ms > cursor) {
                    accept(entry, name);
                    break;
                }
            }
        }
        const bool partial = cursor < stored_end;
        if (partial && adjusted)
            return {{"status", "MISS"}, {"reason", "adjustment_requires_complete_range"}};
        if (partial && (!allow_partial || cursor == stored_start)) return {{"status", "MISS"}, {"reason", "uncovered_range"},
                                  {"uncovered", Json::array({{clock ? clock->to_utc(cursor) : cursor, end}})}};
        Sha256 hash;
        Bytes data;
        uint64_t count = 0;
        uint64_t scanned = 0, omitted = 0;
        std::vector<ConfirmedSuspension> suspensions;
        for (const auto& item : config_.confirmed_suspensions)
            if (item.symbol == symbol && item.coverage.start_ms < end && item.coverage.end_ms > start)
                suspensions.push_back(item);
        std::vector<Row> adjustment_rows;
        std::vector<int64_t> adjustment_days;
        int64_t first = 0, last = 0, previous = -1;
        for (const auto& match : matches) {
            if (query.limits.cancelled->load()) throw Error(ErrorCode::resource_limit, "query cancelled");
            const auto& entry = match.entry;
            if (count >= max_rows) break;
            if (!entry.pack) continue;
            // Preserve one Range ETag pin across retries, including block reads.
            const auto cached = get_pack(match.store, entry, query);
            const RangeReader raw_range = cached ? RangeReader([cached](uint64_t offset, uint64_t size) {
                if (offset > cached->size() || size > cached->size() - offset)
                    throw Error(ErrorCode::corrupt, "cached pack range invalid");
                return Bytes(cached->begin() + static_cast<std::ptrdiff_t>(offset),
                             cached->begin() + static_cast<std::ptrdiff_t>(offset + size));
            }) : match.store->open_range(entry.pack->key, entry.pack->bytes);
            const RangeReader range = [&](uint64_t offset, uint64_t size) {
                return read_with_retry([&] { return raw_range(offset, size); }, query.limits.deadline);
            };
            query.phase = "index";
            const auto index = read_pack_index(range, *entry.pack, pack_metadata(entry));
            for (const auto& block : index.blocks) {
                if (count >= max_rows) break;
                if (block.last_ms < match.start || block.first_ms >= match.end) continue;
                query.phase = "block";
                if (query.limits.cancelled->load() || SteadyClock::now() >= query.limits.deadline)
                    throw Error(ErrorCode::resource_limit, "query scan deadline reached");
                for (auto row : read_pack_block(range, block)) {
                    if (row.timestamp_ms < match.start || row.timestamp_ms >= match.end) continue;
                    if (count >= max_rows) break;
                    if (clock) row.timestamp_ms = clock->to_utc(row.timestamp_ms);
                    if (row.timestamp_ms < start || row.timestamp_ms >= end)
                        throw Error(ErrorCode::corrupt, "row outside UTC request");
                    validate_row(row);
                    if (row.timestamp_ms <= previous) throw Error(ErrorCode::corrupt, "unordered rows");
                    previous = row.timestamp_ms;
                    if (++scanned > kMaxRows) throw Error(ErrorCode::resource_limit, "query scan row limit");
                    if (has_null_price(row)) {
                        if (!confirmed_suspension(row, symbol, suspensions))
                            throw Error(ErrorCode::invalid, "unclassified NULL price row");
                        ++omitted;
                        continue;
                    }
                    if (count == 0) first = row.timestamp_ms;
                    last = row.timestamp_ms;
                    if (adjusted) {
                        adjustment_rows.push_back(row);
                        adjustment_days.push_back(((clock ? clock->to_wall(row.timestamp_ms) : row.timestamp_ms + 28800000)) / 86400000);
                    } else if (query.native) {
                        const auto bytes = canonical_native(row);
                        data.insert(data.end(), bytes.begin(), bytes.end());
                        hash.update(bytes);
                    } else if (query.full) {
                        const auto bytes = canonical_kline(row);
                        hash.update(bytes);
                        data.insert(data.end(), bytes.begin(), bytes.end());
                    } else {
                        const auto bytes = canonical_row(row);
                        hash.update(bytes);
                        data.insert(data.end(), bytes.begin(), bytes.end());
                    }
                    ++count;
                }
            }
        }
        Json adjustment = Json::object();
        if (adjusted) {
            const auto factor_snapshot = get_factors("factors-" + slug + "-v1", symbol, market, query);
            // Select on the original requested date window, then anchor on the
            // selected result. This preserves upstream empty-set/rounding rules.
            const auto factors = select_factor_window(*factor_snapshot,
                (stored_start + (clock ? 0 : 28800000)) / 86400000,
                (stored_end - 1 + (clock ? 0 : 28800000)) / 86400000 + 1);
            const auto output = adjust_native_rows(symbol, adjustment_rows, adjustment_days, factors,
                query.adjust == "forward" ? AdjustmentMode::forward : AdjustmentMode::backward);
            if (query.limits.cancelled->load() || SteadyClock::now() >= query.limits.deadline)
                throw Error(ErrorCode::resource_limit, "adjustment query deadline reached");
            for (const auto& row : output) {
                const auto bytes = canonical_native(row);
                data.insert(data.end(), bytes.begin(), bytes.end());
                hash.update(bytes);
            }
            adjustment = factor_metadata(*factor_snapshot, factors);
            adjustment["anchor_day"] = query.adjust == "forward" && factors.model == AdjustmentModel::cumulative &&
                !adjustment_days.empty() ? Json(adjustment_days.back()) : Json(nullptr);
        }
        Json result = {{"status", partial ? "PARTIAL" : "HIT"}, {"rows", count}, {"first_ms", first}, {"last_ms", last},
                {"rows_sha256", hex(hash.finish())}, {"data", hex(data.data(), data.size())}, {"sources", namespaces}};
        result["omitted_suspension_rows"] = omitted;
        if (adjusted) result["adjustment"] = std::move(adjustment);
        if (partial || intraday || allow_partial) {
            result["coverage_start_ms"] = start;
            result["coverage_end_ms"] = clock ? clock->to_utc(cursor) : cursor;
            result["finalized_through_ms"] = nullptr;
            result["row_limit_reached"] = count == max_rows;
            if (partial) result["uncovered"] = Json::array({{result["coverage_end_ms"], end}});
        }
        return result;
    } catch (const CacheNotReady&) {
        return {{"status", "MISS"}, {"reason", "cache_not_ready"}};
    } catch (const Error& error) {
        const auto reason = error.code() == ErrorCode::invalid ? "invalid_request" :
            error.code() == ErrorCode::resource_limit ? "query_budget_exhausted" : "r2_read_failed";
        return {{"status", "ERROR"}, {"reason", reason}, {"detail", error.what()}};
    } catch (const Json::exception&) {
        return {{"status", "ERROR"}, {"reason", "invalid_request"}};
    }
}
}  // namespace history_cache
