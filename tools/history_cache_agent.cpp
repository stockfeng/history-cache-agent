// Cache agent daemon: serves R2 history packs over a Unix domain socket.
// Deployed on the cloud gateway machine alongside cloud_gateway_v2.

#include "history_cache/catalog.h"
#include "history_cache/common.h"
#include "history_cache/pack.h"
#include "history_cache/s3_store.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {

namespace hc = history_cache;
using Json = nlohmann::json;

std::atomic_bool running{true};

void handle_signal(int) { running = false; }

struct AgentConfig {
    std::string socket_path = "/run/history-cache/agent.sock";
    std::string account_id;
    std::string bucket = "history-cache-staging";
    std::string key_prefix = "r2-history-staging/";
    std::string access_key_id;
    std::string secret_access_key;
    uint64_t manifest_ttl_seconds = 300;
    uint64_t max_rows = 5000;
};

struct CachedSnapshot {
    hc::Snapshot snapshot;
    std::chrono::steady_clock::time_point fetched_at;
};

class Agent {
public:
    explicit Agent(const AgentConfig& config)
        : config_(config),
          credentials_(std::make_shared<hc::S3Credentials>(
              config.access_key_id, config.secret_access_key)) {}

    Json handle_query(const Json& request) {
        const auto symbol = request.value("symbol", "");
        const auto start_ms = request.value("start_ms", 0LL);
        const auto end_ms = request.value("end_ms", 0LL);
        const auto max_rows = request.value("max_rows", static_cast<uint64_t>(config_.max_rows));
        if (symbol.empty() || start_ms <= 0 || end_ms <= start_ms || max_rows == 0 || max_rows > hc::kMaxRows) {
            return {{"status", "ERROR"}, {"reason", "invalid_request"}};
        }

        const auto slug = market_slug(symbol);
        const auto months = months_of(start_ms, end_ms);

        // Copy entries and keep snapshots alive; pointers into the snapshot
        // would dangle once the month loop moves to the next iteration.
        std::vector<hc::CatalogEntry> matches;
        std::vector<std::string> namespaces;
        std::vector<std::shared_ptr<const hc::Snapshot>> snapshots;
        int64_t cursor = start_ms;

        for (const auto& month : months) {
            const auto namespace_name = "history-" + slug + "-" + month + "-001";
            const auto snapshot = get_snapshot(namespace_name);
            if (!snapshot) continue;
            namespaces.push_back(namespace_name);
            snapshots.push_back(snapshot);
            for (const auto& entry : snapshot->manifest.entries) {
                if (entry.identity.symbol != symbol) continue;
                if (entry.coverage.end_ms <= cursor || entry.coverage.start_ms >= end_ms) continue;
                if (entry.coverage.start_ms > cursor) break;
                matches.push_back(entry);
                cursor = std::min<int64_t>(end_ms, entry.coverage.end_ms);
                if (cursor >= end_ms) break;
            }
            if (cursor >= end_ms) break;
        }

        if (cursor < end_ms) {
            return {{"status", "MISS"},
                    {"reason", "uncovered_range"},
                    {"uncovered", Json::array({{cursor, end_ms}})}};
        }

        // Read rows from matching entries across namespaces.
        hc::Sha256 hash;
        std::string data;
        uint64_t row_count = 0;
        int64_t first_ms = 0, last_ms = 0;
        int64_t previous = -1;
        for (const auto& entry : matches) {
            if (row_count >= max_rows) break;
            if (!entry.pack) continue;
            const auto store = get_store(namespaces.front());
            if (!store) return {{"status", "ERROR"}, {"reason", "r2_unavailable"}};
            hc::PackIndex index;
            hc::RangeReader range;
            try {
                range = store->open_range(entry.pack->key, entry.pack->bytes);
                index = hc::read_pack_index(range, *entry.pack, hc::pack_metadata(entry));
            } catch (const hc::Error&) {
                // R2 TLS can be transient; retry once before failing.
                try {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                    range = store->open_range(entry.pack->key, entry.pack->bytes);
                    index = hc::read_pack_index(range, *entry.pack, hc::pack_metadata(entry));
                } catch (const hc::Error& retry_error) {
                    return {{"status", "ERROR"},
                            {"reason", "r2_read_failed"},
                            {"detail", retry_error.what()}};
                }
            }
            for (const auto& block : index.blocks) {
                if (row_count >= max_rows) break;
                if (block.last_ms < start_ms || block.first_ms >= end_ms) continue;
                const auto rows = hc::read_pack_block(range, block);
                for (const auto& row : rows) {
                    if (row.timestamp_ms < start_ms || row.timestamp_ms >= end_ms) continue;
                    if (row_count >= max_rows) break;
                    hc::validate_row(row);
                    if (row.timestamp_ms <= previous) {
                        return {{"status", "ERROR"}, {"reason", "unordered_rows"}};
                    }
                    previous = row.timestamp_ms;
                    if (row_count == 0) first_ms = row.timestamp_ms;
                    last_ms = row.timestamp_ms;
                    hash.update(hc::canonical_row(row));
                    data.append(reinterpret_cast<const char*>(hc::canonical_row(row).data()),
                                hc::canonical_row(row).size());
                    ++row_count;
                }
            }
        }

        if (row_count == 0) {
            return {{"status", "MISS"}, {"reason", "no_rows_in_range"}};
        }

        // Hex-encode the row data for JSON transport.
        static const char* hex_chars = "0123456789abcdef";
        std::string hex_data;
        hex_data.reserve(data.size() * 2);
        for (unsigned char byte : data) {
            hex_data.push_back(hex_chars[byte >> 4]);
            hex_data.push_back(hex_chars[byte & 0xf]);
        }

        return {{"status", "HIT"},
                {"rows", row_count},
                {"first_ms", first_ms},
                {"last_ms", last_ms},
                {"rows_sha256", hc::hex(hash.finish())},
                {"data", hex_data},
                {"sources", namespaces}};
    }

private:
    static std::string market_slug(const std::string& symbol) {
        std::string text;
        for (char c : symbol) {
            if (c != '.') text.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
        if (symbol.find('.') == std::string::npos) text += "-us";
        return text;
    }

    static std::vector<std::string> months_of(int64_t start_ms, int64_t end_ms) {
        std::vector<std::string> months;
        auto time = static_cast<time_t>(start_ms / 1000);
        std::tm tm{};
        gmtime_r(&time, &tm);
        int year = tm.tm_year + 1900, month = tm.tm_mon + 1;
        while (true) {
            std::ostringstream stream;
            stream << std::setfill('0') << std::setw(4) << year
                   << std::setw(2) << month;
            months.push_back(stream.str());
            const auto month_end = static_cast<int64_t>(month_end_ms(year, month));
            if (month_end >= end_ms) break;
            ++month;
            if (month > 12) { month = 1; ++year; }
        }
        return months;
    }

    static int64_t month_end_ms(int year, int month) {
        std::tm tm{};
        tm.tm_year = year - 1900;
        tm.tm_mon = month;  // next month (0-based)
        tm.tm_mday = 1;
        return static_cast<int64_t>(timegm(&tm)) * 1000;
    }

    std::shared_ptr<hc::S3Store> get_store(const std::string& namespace_name) {
        std::lock_guard<std::mutex> guard(stores_mutex_);
        auto found = stores_.find(namespace_name);
        if (found != stores_.end()) return found->second;
        hc::S3Config s3_config{config_.account_id, config_.bucket, config_.key_prefix + namespace_name + "/"};
        auto store = std::make_shared<hc::S3Store>(s3_config, transport_, credentials_);
        stores_[namespace_name] = store;
        return store;
    }

    std::shared_ptr<const hc::Snapshot> get_snapshot(const std::string& namespace_name) {
        const auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> guard(cache_mutex_);
            auto found = cache_.find(namespace_name);
            if (found != cache_.end() &&
                std::chrono::duration_cast<std::chrono::seconds>(now - found->second.fetched_at).count()
                    < static_cast<int64_t>(config_.manifest_ttl_seconds)) {
                return std::shared_ptr<const hc::Snapshot>(&found->second.snapshot,
                    [base = &found->second.snapshot](const hc::Snapshot*) {});
            }
        }
        auto store = get_store(namespace_name);
        auto current = store->read_current();
        if (!current) return nullptr;
        const auto pointer = hc::parse_pointer(std::string(current->bytes.begin(), current->bytes.end()));
        auto manifest_bytes = store->get(pointer.manifest_key, hc::kMaxMetadataBytes);
        auto manifest = hc::parse_manifest(std::string(manifest_bytes.begin(), manifest_bytes.end()));
        auto snapshot = std::make_shared<hc::Snapshot>(hc::Snapshot{pointer, std::move(manifest)});
        {
            std::lock_guard<std::mutex> guard(cache_mutex_);
            cache_[namespace_name] = {*snapshot, now};
        }
        return snapshot;
    }

    AgentConfig config_;
    std::shared_ptr<hc::S3Credentials> credentials_;
    std::shared_ptr<hc::HttpTransport> transport_ = std::make_shared<hc::CurlHttpTransport>(true);
    std::mutex cache_mutex_;
    std::unordered_map<std::string, CachedSnapshot> cache_;
    std::mutex stores_mutex_;
    std::unordered_map<std::string, std::shared_ptr<hc::S3Store>> stores_;
};

void serve_connection(int client_fd, Agent& agent) {
    std::string buffer;
    char chunk[65536];
    while (running) {
        const auto received = recv(client_fd, chunk, sizeof(chunk), 0);
        if (received <= 0) break;
        buffer.append(chunk, static_cast<size_t>(received));
        size_t newline;
        while ((newline = buffer.find('\n')) != std::string::npos) {
            const auto line = buffer.substr(0, newline);
            buffer.erase(0, newline + 1);
            if (line.empty()) continue;
            Json response;
            try {
                const auto request = Json::parse(line);
                const auto op = request.value("op", "");
                if (op == "query") {
                    response = agent.handle_query(request);
                } else if (op == "ping") {
                    response = {{"status", "PONG"}};
                } else {
                    response = {{"status", "ERROR"}, {"reason", "unknown_op"}};
                }
            } catch (const std::exception& error) {
                response = {{"status", "ERROR"}, {"reason", error.what()}};
            }
            const auto output = response.dump() + "\n";
            if (send(client_fd, output.data(), output.size(), MSG_NOSIGNAL) < 0) return;
        }
    }
    close(client_fd);
}

}  // namespace

int main(int argc, char* argv[]) {
    AgentConfig config;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&]() -> std::string {
            return i + 1 < argc ? argv[++i] : "";
        };
        if (arg == "--socket") config.socket_path = value();
        else if (arg == "--account") config.account_id = value();
        else if (arg == "--bucket") config.bucket = value();
        else if (arg == "--prefix") config.key_prefix = value();
        else if (arg == "--access-key-id") config.access_key_id = value();
        else if (arg == "--secret-access-key") config.secret_access_key = value();
        else if (arg == "--max-rows") config.max_rows = std::stoull(value());
        else {
            std::cerr << "unknown argument: " << arg << "\n";
            return 2;
        }
    }
    if (config.account_id.empty() || config.access_key_id.empty() || config.secret_access_key.empty()) {
        std::cerr << "required: --account, --access-key-id, --secret-access-key\n";
        return 2;
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGPIPE, SIG_IGN);

    ::unlink(config.socket_path.c_str());
    const auto fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, config.socket_path.c_str(), sizeof(address.sun_path) - 1);
    if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(fd, 16) < 0) { perror("listen"); return 1; }
    std::cout << "listening on " << config.socket_path << "\n";

    Agent agent(config);
    while (running) {
        const auto client = accept(fd, nullptr, nullptr);
        if (client < 0) {
            if (running) perror("accept");
            continue;
        }
        std::thread(serve_connection, client, std::ref(agent)).detach();
    }
    ::unlink(config.socket_path.c_str());
    close(fd);
    return 0;
}
