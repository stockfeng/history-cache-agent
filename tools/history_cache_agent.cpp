#include "history_cache/agent.h"
#include "history_cache/maintenance.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>

namespace {
namespace hc = history_cache;
using Json = nlohmann::json;
static_assert(std::atomic_bool::is_always_lock_free);
std::atomic_bool running{true};
void handle_signal(int) { running.store(false); }

struct SocketLock {
    int fd = -1;
    ~SocketLock() { if (fd >= 0) close(fd); }
};

void load_credentials(const std::string& path, hc::AgentConfig& config) {
    // Open once, then validate/read the same inode. Never include input in errors.
    SocketLock file;
    file.fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    struct stat info{};
    if (file.fd < 0 || fstat(file.fd, &info) < 0 || !S_ISREG(info.st_mode) ||
        info.st_uid != geteuid() || info.st_nlink != 1 ||
        ((info.st_mode & 07777) != 0400 && (info.st_mode & 07777) != 0600))
        throw std::runtime_error("credentials file must be owned by service uid with mode 0400 or 0600");
    std::array<char, 4097> data{};
    size_t size = 0;
    while (size < data.size()) {
        const auto n = read(file.fd, data.data() + size, data.size() - size);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) throw std::runtime_error("credentials file read failed");
        if (n == 0) break;
        size += static_cast<size_t>(n);
    }
    if (size == 0 || size > 4096) throw std::runtime_error("invalid credentials file size");
    try {
        const auto value = Json::parse(data.data(), data.data() + size);
        if (!value.is_object() || value.size() != 3) throw std::runtime_error("schema");
        const auto account = value.at("account_id").get<std::string>();
        const auto key = value.at("access_key_id").get<std::string>();
        const auto secret = value.at("secret_access_key").get<std::string>();
        const auto printable = [](const std::string& text) {
            if (text.empty() || text.size() > 1024) return false;
            for (unsigned char ch : text) if (ch < 33 || ch > 126) return false;
            return true;
        };
        if (account.size() != 32 || account.find_first_not_of("0123456789abcdefABCDEF") != std::string::npos ||
            !printable(key) || !printable(secret)) throw std::runtime_error("schema");
        config.account_id = account;
        config.access_key_id = key;
        config.secret_access_key = secret;
    } catch (const std::exception&) {
        throw std::runtime_error("invalid credentials file schema");
    }
}

void serve_connection(int fd, hc::Agent& agent, hc::Maintenance& maintenance) {
    timeval timeout{2, 0};
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) return;
    std::string buffer;
    size_t request_limit = 8192;
    bool framed_composite = false;
    char chunk[4096];
    // One bounded request per connection, matching the gateway UDS client.
    const auto deadline = hc::SteadyClock::now() + std::chrono::seconds(2);
    while (running && hc::SteadyClock::now() < deadline) {
        const auto n = recv(fd, chunk, sizeof(chunk), 0);
        if (n <= 0) return;
        buffer.append(chunk, static_cast<size_t>(n));
        // An explicit preamble opts into a bounded large local-only operation.
        // Ordinary query/control requests keep their original 8 KiB limit.
        constexpr const char* preamble = "ADJUST64\n";
        if (!framed_composite && buffer.rfind(preamble, 0) == 0) {
            framed_composite = true;
            buffer.erase(0, 9);
            request_limit = 5000 * 128 + 8192;
        }
        if (buffer.size() > request_limit) return;
        const auto newline = buffer.find('\n');
        if (newline == std::string::npos) continue;
        Json response;
        try {
            const auto request = Json::parse(buffer.substr(0, newline));
            const auto op = request.value("op", "");
            if (framed_composite != (op == "adjust_rows"))
                response = {{"status", "ERROR"}, {"reason", "invalid_request"}};
            else if (op == "query" || op == "adjust_rows") response = agent.handle_query(request);
            else if (op == "ping") response = {{"status", "PONG"}};
            else if (op == "maintenance_status") response = maintenance.status();
            else if (op == "maintenance_lease") {
                const auto& until = request.at("valid_until_mono_ms");
                if (!until.is_number_integer()) throw std::runtime_error("invalid lease deadline");
                const auto value = until.get<int64_t>();
                const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    hc::SteadyClock::now().time_since_epoch()).count();
                const auto expiry = value > now && value <= now + 2000 ?
                    hc::SteadyClock::time_point(std::chrono::milliseconds(value)) : hc::SteadyClock::time_point{};
                response = maintenance.lease(request.at("idle").get<bool>(), expiry);
            }
            else response = {{"status", "ERROR"}, {"reason", "unknown_op"}};
        } catch (const std::exception&) {
            response = {{"status", "ERROR"}, {"reason", "invalid_request"}};
        }
        const auto output = response.dump() + "\n";
        size_t offset = 0;
        const auto send_deadline = hc::SteadyClock::now() + std::chrono::seconds(2);
        while (running && offset < output.size() && hc::SteadyClock::now() < send_deadline) {
            const auto sent = send(fd, output.data() + offset, output.size() - offset, MSG_NOSIGNAL);
            if (sent < 0 && errno == EINTR) continue;
            if (sent <= 0) return;
            offset += static_cast<size_t>(sent);
        }
        return;
    }
}
}  // namespace

int main(int argc, char* argv[]) {
    hc::AgentConfig config;
    try {
        std::string credentials_file;
        std::string storage_file;
        bool scope_flags = false;
        std::string warm_plan;
        bool file_seen = false;
        bool inline_credentials = false;
        bool reuse_connections = true;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (i + 1 >= argc) throw std::runtime_error("missing argument value");
            const std::string value = argv[++i];
            if (arg == "--account" || arg == "--access-key-id" || arg == "--secret-access-key")
                inline_credentials = true;
            if (arg == "--account" || arg == "--bucket" || arg == "--prefix") scope_flags = true;
            if (arg == "--socket") config.socket_path = value;
            else if (arg == "--warm-plan") warm_plan = value;
            else if (arg == "--foreground-network") {
                if (value != "yes" && value != "no") throw std::runtime_error("invalid foreground network option");
                config.foreground_network = value == "yes";
            }
            else if (arg == "--adjustment") {
                if (value != "yes" && value != "no") throw std::runtime_error("invalid adjustment option");
                config.enable_adjustment = value == "yes";
            }
            else if (arg == "--credentials-file") {
                if (file_seen || value.empty()) throw std::runtime_error("invalid credentials-file option");
                file_seen = true;
                credentials_file = value;
            }
            else if (arg == "--storage-config") {
                if (!storage_file.empty() || value.empty()) throw std::runtime_error("duplicate storage config");
                storage_file = value;
            }
            else if (arg == "--account") config.account_id = value;
            else if (arg == "--bucket") config.bucket = value;
            else if (arg == "--prefix") config.key_prefix = value;
            else if (arg == "--access-key-id") config.access_key_id = value;
            else if (arg == "--secret-access-key") config.secret_access_key = value;
            else if (arg == "--max-rows") config.max_rows = std::stoull(value);
            else if (arg == "--reuse-connections") {
                if (value != "yes" && value != "no") throw std::runtime_error("invalid reuse option");
                reuse_connections = value == "yes";
            }
            else if (arg == "--pack-cache-bytes" || arg == "--full-pack-read-bytes") {
                if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
                    throw std::runtime_error("invalid pack limit");
                const auto bytes = std::stoull(value);
                if (arg == "--pack-cache-bytes") config.max_cached_pack_bytes = bytes;
                else config.full_pack_read_bytes = bytes;
            }
            else if (arg == "--query-timeout-ms") {
                if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
                    throw std::runtime_error("invalid query timeout");
                const auto timeout = std::stoull(value);
                if (timeout == 0 || timeout > 30000) throw std::runtime_error("invalid query timeout");
                config.query_timeout = std::chrono::milliseconds(timeout);
            }
            else throw std::runtime_error("unknown argument");
        }
        if (file_seen) {
            if (inline_credentials) throw std::runtime_error("credentials-file conflicts with inline credentials");
            load_credentials(credentials_file, config);
        }
        if (!storage_file.empty()) {
            const auto storage = hc::load_storage_profile(storage_file, "reader");
            if (scope_flags || !file_seen || config.account_id != storage.account_id)
                throw std::runtime_error("storage profile conflicts with credentials or command scope");
            config.bucket = storage.bucket;
            config.key_prefix = storage.key_prefix;
            config.jurisdiction = storage.jurisdiction;
            config.storage_environment = storage.environment;
        }
        if (config.account_id.empty() || config.access_key_id.empty() || config.secret_access_key.empty())
            throw std::runtime_error("required: --account, --access-key-id, --secret-access-key");
        sockaddr_un address{};
        if (config.socket_path.empty() || config.socket_path.size() >= sizeof(address.sun_path))
            throw std::runtime_error("invalid socket path");
        hc::Agent agent(config, std::make_shared<hc::CurlHttpTransport>(true, reuse_connections));
        Json plan = Json::array();
        if (!warm_plan.empty()) {
            const auto bytes = hc::read_file(warm_plan, 16384);
            plan = Json::parse(bytes.begin(), bytes.end());
        }
        hc::Maintenance maintenance(agent, plan);
        SocketLock socket_lock;
        socket_lock.fd = open((config.socket_path + ".lock").c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
        struct stat lock_stat{};
        if (socket_lock.fd < 0 || fstat(socket_lock.fd, &lock_stat) < 0 || !S_ISREG(lock_stat.st_mode) ||
            lock_stat.st_uid != geteuid() || lock_stat.st_nlink != 1 ||
            (lock_stat.st_mode & 0077) || flock(socket_lock.fd, LOCK_EX | LOCK_NB) < 0)
            throw std::runtime_error("socket ownership lock unavailable");
        struct stat socket_stat{};
        if (lstat(config.socket_path.c_str(), &socket_stat) == 0) {
            if (!S_ISSOCK(socket_stat.st_mode) || socket_stat.st_uid != geteuid())
                throw std::runtime_error("refusing to remove an unowned socket path");
            // A legacy server may not use the lock. Refuse any live listener.
            const int probe = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            if (probe < 0) throw std::runtime_error("socket probe failed");
            address.sun_family = AF_UNIX;
            std::memcpy(address.sun_path, config.socket_path.c_str(), config.socket_path.size() + 1);
            const int connected = connect(probe, reinterpret_cast<sockaddr*>(&address), sizeof(address));
            const int saved_errno = errno;
            close(probe);
            if (connected == 0 || saved_errno != ECONNREFUSED)
                throw std::runtime_error("socket already has a listener");
            if (unlink(config.socket_path.c_str()) < 0) throw std::runtime_error("stale socket cleanup failed");
        } else if (errno != ENOENT) throw std::runtime_error("socket path inspection failed");
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
        std::signal(SIGPIPE, SIG_IGN);
        const int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
        if (fd < 0) throw std::runtime_error("socket failed");
        address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path, config.socket_path.c_str(), config.socket_path.size() + 1);
        // The ownership lock protects startup, stale recovery and final unlink.
        if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
            close(fd);
            throw std::runtime_error("bind failed; check for an existing socket");
        }
        if (chmod(config.socket_path.c_str(), 0600) < 0 || listen(fd, 16) < 0) {
            close(fd);
            unlink(config.socket_path.c_str());
            throw std::runtime_error("socket setup failed");
        }
        std::mutex mutex;
        std::condition_variable ready;
        std::deque<int> pending;
        bool stopping = false;
        std::array<int, 2> active{-1, -1};
        std::vector<std::thread> workers;
        for (size_t i = 0; i < active.size(); ++i) workers.emplace_back([&, i] {
            for (;;) {
                std::unique_lock<std::mutex> lock(mutex);
                ready.wait(lock, [&] { return stopping || !pending.empty(); });
                if (stopping) return;
                const int client = pending.front();
                pending.pop_front();
                active[i] = client;
                lock.unlock();
                try { serve_connection(client, agent, maintenance); } catch (const std::exception&) {}
                lock.lock();
                active[i] = -1;
                close(client);
            }
        });
        std::thread background;
        if (!plan.empty()) background = std::thread([&] {
                while (running) {
                    maintenance.step();
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                }
            });
        std::cout << "listening on " << config.socket_path << std::endl;
        while (running) {
            maintenance.poll();
            pollfd event{fd, POLLIN, 0};
            if (poll(&event, 1, 200) <= 0) continue;
            const int client = accept4(fd, nullptr, nullptr, SOCK_CLOEXEC);
            if (client < 0) continue;
            std::lock_guard<std::mutex> lock(mutex);
            if (pending.size() >= 4) close(client);
            else { pending.push_back(client); ready.notify_one(); }
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
            for (int client : pending) close(client);
            for (int client : active) if (client >= 0) shutdown(client, SHUT_RDWR);
        }
        ready.notify_all();
        maintenance.stop();
        if (background.joinable()) background.join();
        for (auto& worker : workers) worker.join();
        close(fd);
        unlink(config.socket_path.c_str());
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
