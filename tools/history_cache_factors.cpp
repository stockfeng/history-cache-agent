#include "history_cache/factor_snapshot.h"
#include "history_cache/journal.h"
#include "history_cache/s3_store.h"
#include "snapshot_diagnostics.h"

#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <set>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace hc = history_cache;
namespace fs = std::filesystem;
using Json = nlohmann::json;
namespace {
constexpr uint64_t kAttempts = 3, kRequests = 16, kUpload = 4 * 1024 * 1024, kDownload = 8 * 1024 * 1024;
void require(bool value, const char* message) { if (!value) throw hc::Error(hc::ErrorCode::invalid, message); }
int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
std::string text(const hc::Bytes& bytes) { return {bytes.begin(), bytes.end()}; }
std::string key(const hc::Bytes& bytes) { return "manifests/v1/" + hc::hex(hc::sha256(bytes)) + ".json"; }
int64_t day(const std::string& value) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, "%Y-%m-%d");
    require(!input.fail() && input.eof() && value.size() == 10, "invalid plan date");
    const auto instant = ::timegm(&tm);
    char encoded[11]{};
    require(std::strftime(encoded, sizeof(encoded), "%Y-%m-%d", &tm) == 10 && value == encoded, "normalized plan date differs");
    return instant / 86400;
}
std::string dotted(std::string value) { (void)day(value); std::replace(value.begin(), value.end(), '-', '.'); return value; }
void private_root(const fs::path& root) {
    require(fs::canonical(root) == fs::absolute(root), "unsafe publication root");
    struct stat st{};
    require(::lstat(root.c_str(), &st) == 0 && S_ISDIR(st.st_mode) && st.st_uid == ::geteuid() &&
            (st.st_mode & 07777) == 0700, "private owned publication root required");
}
struct Lock {
    int fd;
    explicit Lock(const fs::path& path) : fd(::open(path.c_str(), O_RDWR | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK)) {
        struct stat st{};
        const bool valid = fd >= 0 && ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_uid == ::geteuid() &&
                st.st_nlink == 1 && (st.st_mode & 07777) == 0600 && ::flock(fd, LOCK_EX | LOCK_NB) == 0;
        if (!valid) { if (fd >= 0) ::close(fd); throw hc::Error(hc::ErrorCode::invalid, "unsafe or locked publication"); }
    }
    ~Lock() { ::close(fd); }
};
struct Bundle {
    hc::Bytes factor, proof, source;
    Json document;
};
struct Compact {
    hc::Bytes manifest;
    std::vector<hc::Bytes> objects;
};
hc::Bytes canonical(const Json& value) {
    const auto text = value.dump() + '\n';
    return {text.begin(), text.end()};
}
Compact compact_bundle(const Bundle& b) {
    auto data = b.document;
    for (const auto* field : {"observed_at_ms", "valid_until_ms", "source_proof_sha256", "verification"}) data.erase(field);
    data["schema_version"] = 2; data["kind"] = "ddb-adjustment-data-v2";
    const auto data_bytes = canonical(data);
    auto proof = Json::parse(b.proof);
    const auto state = canonical(proof.at("guard").at("before"));
    proof["kind"] = "ddb-factor-native-observation-v2";
    proof["guard"].erase("before"); proof["guard"].erase("after");
    proof["guard"]["before_sha256"] = hc::hex(hc::sha256(state));
    proof["guard"]["after_sha256"] = hc::hex(hc::sha256(state));
    const auto proof_bytes = canonical(proof);
    auto reference = b.document;
    reference.erase("rows"); reference["schema_version"] = 2;
    reference["kind"] = "ddb-adjustment-reference-v2";
    if (b.document.contains("verification")) {
        reference["schema_version"] = 3; reference["kind"] = "ddb-adjustment-reference-v3";
    }
    reference["source_proof_sha256"] = hc::hex(hc::sha256(proof_bytes));
    reference["factor_data_sha256"] = hc::hex(hc::sha256(data_bytes));
    reference["factor_data_bytes"] = data_bytes.size();
    Compact result{canonical(reference), {b.source, state, data_bytes, proof_bytes}};
    require(result.manifest.size() <= hc::kMaxFactorBytes && data_bytes.size() <= hc::kMaxFactorBytes,
            "compact factor size limit");
    for (const auto& object : result.objects) require(object.size() <= hc::kMaxMetadataBytes, "compact proof size limit");
    return result;
}
Bundle bundle(const fs::path& root, bool fresh) {
    private_root(root);
    Bundle b{hc::read_file(root / "factor.json", hc::kMaxFactorBytes),
             hc::read_file(root / "observation.json", hc::kMaxMetadataBytes),
             hc::read_file(root / "source-0.json", hc::kMaxMetadataBytes), {}};
    b.document = Json::parse(b.factor);
    const auto& f = b.document;
    (void)hc::parse_factor_snapshot(b.factor, hc::sha256(b.factor), f.at("symbol"), f.at("market"), now_ms(),
        fresh ? hc::FactorReadPolicy::fresh_publication : hc::FactorReadPolicy::structural_only);
    const auto p = Json::parse(b.proof), s = Json::parse(b.source);
    const auto& plan = p.at("plan");
    require(plan.contains("verification") == f.contains("verification"), "verification plan presence differs");
    if (f.contains("verification")) require(plan.at("verification") == f.at("verification") &&
        f.at("verification").at("config_sha256") == plan.at("config_sha256"), "verification plan binding differs");
    require(p.at("plan_sha256") == hc::hex(hc::sha256(plan.dump() + '\n')) &&
            day(plan.at("start")) == f.at("first_day").get<int64_t>() &&
            day(plan.at("end")) == f.at("end_day").get<int64_t>(), "proof plan hash or dates differ");
    Json binding = Json::object();
    for (const auto* field : {"host", "port", "database", "table", "symbol", "market", "model"}) binding[field] = plan.at(field);
    require(f.at("source_epoch") == "ddb-factor-" + hc::hex(hc::sha256(binding.dump() + '\n')), "source epoch differs");
    std::string domain = "code=\"" + f.at("symbol").get<std::string>() + "\"";
    if (f.at("model") == "cumulative") {
        domain += " and ex_date<" + dotted(plan.at("end"));
        if (!plan.at("carry_lower_bound").is_null()) {
            require(day(plan.at("carry_lower_bound")) < f.at("first_day").get<int64_t>(), "invalid carry probe");
            domain += " and ex_date>=" + dotted(plan.at("carry_lower_bound"));
        }
    }
    require(p.at("guard").at("domain") == domain &&
            p.at("guard").at("scope") == "ddb-visible-factor-domain-at-native-version-vector", "factor guard domain differs");
    require(f.at("source_proof_sha256") == hc::hex(hc::sha256(b.proof)) &&
            p.at("kind") == "ddb-factor-native-snapshot-v1" && p.at("snapshot_consistency_proven") == true &&
            p.at("guard").at("contract") == "ddb-factor-native-vector-v1" &&
            p.at("guard").at("before") == p.at("guard").at("after") &&
            p.at("source_sha256") == Json::array({hc::hex(hc::sha256(b.source)), hc::hex(hc::sha256(b.source))}),
            "factor proof binding differs");
    require(p.at("observed_at_ms") == f.at("observed_at_ms") &&
            p.at("completed_at_ms").get<int64_t>() >= f.at("observed_at_ms").get<int64_t>() &&
            p.at("completed_at_ms").get<int64_t>() < f.at("valid_until_ms").get<int64_t>(), "proof timing differs");
    for (const auto* name : {"symbol", "market", "model", "allow_empty"})
        require(p.at("plan").at(name) == f.at(name), "proof identity differs");
    const auto& guard = p.at("guard").at("before");
    const auto date_field = f.at("model") == "cumulative" ? "ex_date" : "ex_div_date";
    require(guard.at("layout").at("partitionColumnName") == Json::array({date_field, "code"}), "factor partition layout differs");
    require(guard.at("runtime").get<std::string>().rfind("3.00.5 ", 0) == 0 &&
            guard.at("layout").at("engineType") == "TSDB" && guard.at("layout").at("colDefs") == s.at("schema") &&
            guard.at("paths").size() <= 512 && guard.at("paths").size() == guard.at("chunks").size() &&
            guard.at("paths").size() == guard.at("tablets").size(), "invalid native factor vector");
    for (size_t i = 0; i < guard.at("paths").size(); ++i) {
        const auto path = guard.at("paths")[i].get<std::string>();
        const auto database = plan.at("database").get<std::string>();
        require(database.rfind("dfs://", 0) == 0 && path.rfind("/" + database.substr(6) + "/", 0) == 0 &&
                path.find("..") == std::string::npos && (i == 0 || guard.at("paths")[i-1].get<std::string>() < path),
                "unsafe, duplicate or unordered partition path");
        const auto& c = guard.at("chunks").at(i);
        const auto& t = guard.at("tablets").at(i);
        require(c.size() == 1 && t.size() <= 1 && c[0].at("version").get<int64_t>() > 0 &&
                c[0].at("dfsPath") == guard.at("paths")[i] && c[0].at("site") == guard.at("node") &&
                c[0].at("type") == 1 && c[0].at("flag") == 0 && c[0].at("state") == 0 && c[0].at("resolved") == false,
                "invalid native chunk state");
        if (!t.empty()) require(t[0].at("version") == c[0].at("version") && t[0].at("chunkId") == c[0].at("chunkId") &&
            t[0].at("dfsPath") == c[0].at("dfsPath") && t[0].at("tableName") == p.at("plan").at("table"), "factor tablet differs");
    }
    std::map<std::string, std::string> types;
    for (const auto& col : s.at("schema")) require(types.emplace(col.at("name"), col.at("typeString")).second, "duplicate source column");
    require(types.at("code") == "SYMBOL" || types.at("code") == "STRING", "source code type differs");
    require(types.at(date_field) == "DATE" && types.at("update_time") == "TIMESTAMP" &&
            s.at("temporal_units").at("DATE") == "D" && s.at("temporal_units").at("TIMESTAMP") == "ms", "source temporal encoding differs");
    size_t offset = 0;
    for (const auto& batch : s.at("batches")) for (const auto& row : batch) {
        require(offset < f.at("rows").size(), "source has extra rows");
        for (auto it = f.at("rows")[offset].begin(); it != f.at("rows")[offset].end(); ++it) {
            if (it.key() != "code" && it.key() != date_field && it.key() != "update_time")
                require(types.at(it.key()) == "DOUBLE", "source factor type differs");
            require(row.at(it.key()) == it.value(), "factor projection differs");
        }
        ++offset;
    }
    require(offset == f.at("rows").size(), "source has missing rows");
    if (f.at("model") == "cumulative") {
        require(s.at("batches").size() == 2, "carry/range batches required");
        for (const auto& row : s.at("batches")[0]) require(row.at(date_field).get<int64_t>() < f.at("first_day").get<int64_t>(), "carry date differs");
        for (const auto& row : s.at("batches")[1]) require(row.at(date_field).get<int64_t>() >= f.at("first_day").get<int64_t>(), "range date differs");
        if (!plan.at("carry_lower_bound").is_null()) require(!s.at("batches")[0].empty() &&
            s.at("batches")[0][0].at(date_field).get<int64_t>() >= day(plan.at("carry_lower_bound")), "bounded carry missing");
    } else require(s.at("batches").size() == 1, "US event batch differs");
    return b;
}
hc::TransferLimits limits(int64_t until, bool compact) {
    hc::TransferLimits value;
    value.max_requests = compact ? 24 : kRequests; value.max_upload_bytes = kUpload;
    value.max_download_bytes = compact ? 12 * 1024 * 1024 : kDownload;
    value.deadline = hc::SteadyClock::now() + std::chrono::milliseconds(std::min<int64_t>(120000, until - now_ms()));
    return value;
}
hc::S3Config config(const fs::path& profile, const Json& factor, bool production) {
    auto c = hc::load_storage_profile(profile.string(), "publisher");
    require(c.environment == "staging" || production, "production factor publication requires explicit opt-in");
    std::string slug;
    for (unsigned char ch : factor.at("symbol").get<std::string>()) if (ch != '.') slug += static_cast<char>(std::tolower(ch));
    if (factor.at("market") == "US") slug += "-us";
    c.key_prefix += "factors-" + slug + "-v1/";
    return c;
}
}  // namespace

int main(int argc, char** argv) {
    std::shared_ptr<hc::sample::SnapshotDiagnostics> diagnostics;
    const char* stage = "validation";
    auto failure_report = [&](const char* code) {
        Json report{{"outcome", "failed"}, {"stage", stage}, {"error_code", code}};
        if (diagnostics) report["diagnostics"] = diagnostics->summary();
        std::cout << report.dump() << '\n';
    };
    try {
        require(argc >= 2, "prepare or run required");
        const std::string command(argv[1]);
        require(command == "prepare" || command == "run" || command == "verify", "unsupported command");
        const std::set<std::string> allowed = command == "prepare"
            ? std::set<std::string>{"--capture", "--storage-config", "--root", "--base", "--allow-production", "--compact"}
            : std::set<std::string>{"--root", "--execute", "--allow-production", "--retry-transient"};
        std::map<std::string, std::string> args;
        for (int i = 2; i < argc; i += 2) require(i + 1 < argc && allowed.count(argv[i]) &&
            args.emplace(argv[i], argv[i + 1]).second, "invalid or duplicate argument");
        const bool production = args.count("--allow-production") != 0;
        require(!production || args.at("--allow-production") == "yes", "invalid production opt-in");
        const fs::path root = fs::absolute(args.at("--root"));
        const bool retry_transient = args.count("--retry-transient") != 0;
        require(!retry_transient || (command == "run" && args.at("--retry-transient") == "yes"), "invalid retry opt-in");
        if (command == "prepare") {
            const auto b = bundle(args.at("--capture"), true);
            const bool compact = args.count("--compact") != 0;
            require(!compact || args.at("--compact") == "yes", "invalid compact opt-in");
            require(!b.document.contains("verification") || compact, "market verification requires compact data");
            const auto candidate = compact ? compact_bundle(b).manifest : b.factor;
            const auto c = config(args.at("--storage-config"), b.document, production);
            auto transport = std::make_shared<hc::CurlHttpTransport>();
            auto dummy = std::make_shared<hc::S3Credentials>("offline", "offline");
            const hc::S3Store store(c, transport, dummy);
            std::optional<hc::Pointer> base;
            if (args.count("--base")) base = hc::parse_pointer(text(hc::read_file(args.at("--base"), hc::kMaxPointerBytes)));
            hc::create_new_directory(root);
            hc::write_new_file(root / "factor.json", b.factor);
            hc::write_new_file(root / "observation.json", b.proof);
            hc::write_new_file(root / "source-0.json", b.source);
            if (compact) {
                const auto values = compact_bundle(b);
                hc::write_new_file(root / "manifest.json", candidate);
                hc::write_new_file(root / "compact-state.json", values.objects[1]);
                hc::write_new_file(root / "compact-data.json", values.objects[2]);
                hc::write_new_file(root / "compact-proof.json", values.objects[3]);
            }
            const auto profile = hc::read_file(args.at("--storage-config"), 4096);
            hc::write_new_file(root / "storage.json", profile);
            hc::write_new_file(root / "run.lock", "");
            hc::write_new_file(root / "budget-floor", "0");
            hc::JournaledPublisher::prepare_factors(root / "journal", store.scope_id(), candidate,
                b.document.at("symbol"), b.document.at("market"), base, now_ms());
            Json intent{{"version", 1}, {"scope", hc::hex(store.scope_id())}, {"factor", hc::hex(hc::sha256(b.factor))},
                {"proof", hc::hex(hc::sha256(b.proof))}, {"source", hc::hex(hc::sha256(b.source))},
                {"storage", hc::hex(hc::sha256(profile))}, {"expires_ms", now_ms() + 86400000},
                {"attempts", kAttempts}, {"requests_per_attempt", kRequests}, {"upload_per_attempt", kUpload},
                {"download_per_attempt", kDownload}};
            if (compact) {
                intent["compact"] = true; intent["manifest"] = hc::hex(hc::sha256(candidate));
                intent["requests_per_attempt"] = 24;
                intent["download_per_attempt"] = 12 * 1024 * 1024;
            }
            const auto encoded = intent.dump();
            hc::write_new_file(root / "intent.json", encoded);
            hc::write_new_file(root / "ready", hc::hex(hc::sha256(encoded)));
            std::cout << "PASS factor_prepare network=0\n";
            return 0;
        }
        require(args.at("--execute") == "yes", "explicit --execute yes required");
        private_root(root);
        Lock lock(root / "run.lock");
        const auto encoded = hc::read_file(root / "intent.json", 4096);
        require(text(hc::read_file(root / "ready", 64)) == hc::hex(hc::sha256(encoded)), "publication intent checksum differs");
        const auto intent = Json::parse(encoded);
        const auto b = bundle(root, false);
        const bool compact = intent.value("compact", false);
        const auto optimized = compact ? compact_bundle(b) : Compact{b.factor, {}};
        const auto& candidate = optimized.manifest;
        if (compact) require(intent.at("manifest") == hc::hex(hc::sha256(candidate)) &&
                            hc::read_file(root / "manifest.json", hc::kMaxFactorBytes) == candidate,
                            "compact manifest binding differs");
        if (compact) require(hc::read_file(root / "compact-state.json", hc::kMaxMetadataBytes) == optimized.objects[1] &&
                            hc::read_file(root / "compact-data.json", hc::kMaxFactorBytes) == optimized.objects[2] &&
                            hc::read_file(root / "compact-proof.json", hc::kMaxMetadataBytes) == optimized.objects[3],
                            "compact object binding differs");
        require(intent.at("version") == 1 && intent.at("attempts") == kAttempts && intent.at("requests_per_attempt") == (compact ? 24 : kRequests) &&
            intent.at("upload_per_attempt") == kUpload &&
            intent.at("download_per_attempt") == (compact ? 12 * 1024 * 1024 : kDownload) &&
            intent.at("factor") == hc::hex(hc::sha256(b.factor)) && intent.at("proof") == hc::hex(hc::sha256(b.proof)) &&
            intent.at("source") == hc::hex(hc::sha256(b.source)) &&
            intent.at("storage") == hc::hex(hc::file_sha256(root / "storage.json", 4096)), "publication bundle changed");
        const auto until = intent.at("expires_ms").get<int64_t>();
        require(until > now_ms() && until - now_ms() <= 86400000, "publication deadline expired or clock regressed");
        const auto c = config(root / "storage.json", b.document, production);
        {
            auto offline = std::make_shared<hc::CurlHttpTransport>();
            auto dummy = std::make_shared<hc::S3Credentials>("offline", "offline");
            hc::S3Store validation(c, offline, dummy);
            require(intent.at("scope") == hc::hex(validation.scope_id()), "publication scope differs");
            hc::JournaledPublisher journal(root / "journal", validation);
            require(journal.target().manifest_sha256 == hc::sha256(candidate), "journal target differs");
            if (journal.committed() && command == "run") {
                std::cout << "{\"outcome\":\"committed\",\"recovered\":true,\"requests\":0}\n";
                return 0;
            }
        }
        // Reserve the entire invocation before credentials/network. A crash
        // consumes its full slice, never refunding unknown requests or bytes.
        uint64_t attempt = 0;
        bool gap = false;
        for (uint64_t index = 0; index < kAttempts; ++index) {
            const auto path = root / ("attempt-" + std::to_string(index));
            if (!fs::exists(path)) { gap = true; continue; }
            require(!gap && text(hc::read_file(path, 64)) == hc::hex(hc::sha256(encoded)), "persistent attempt ledger differs");
            ++attempt;
        }
        require(attempt < kAttempts, "persistent publication budget exhausted");
        require(text(hc::read_file(root / "budget-floor", 1)) == std::to_string(attempt), "attempt suffix or dispatch floor lost");
        hc::write_new_file(root / ("attempt-" + std::to_string(attempt)), hc::hex(hc::sha256(encoded)));
        {
            Lock floor(root / "budget-floor");
            const auto count = std::to_string(attempt + 1);
            require(::pwrite(floor.fd, count.data(), 1, 0) == 1 && ::fsync(floor.fd) == 0, "budget dispatch floor sync failed");
        }
        const bool prod_scope = c.environment == "production";
        const char* id = std::getenv(prod_scope ? "R2_PRODUCTION_ACCESS_KEY_ID" : "R2_STAGING_ACCESS_KEY_ID");
        const char* secret = std::getenv(prod_scope ? "R2_PRODUCTION_SECRET_ACCESS_KEY" : "R2_STAGING_SECRET_ACCESS_KEY");
        require(id && *id && secret && *secret, "R2 scoped credentials unavailable");
        auto credentials = std::make_shared<hc::S3Credentials>(id, secret);
        auto transport = std::make_shared<hc::CurlHttpTransport>(true, hc::CurlHttpTransport::Reuse::publication);
        diagnostics = std::make_shared<hc::sample::SnapshotDiagnostics>(transport);
        hc::S3Store store(c, diagnostics, credentials, limits(until, compact));
        require(intent.at("scope") == hc::hex(store.scope_id()), "publication scope differs");
        hc::JournaledPublisher journal(root / "journal", store);
        require(journal.target().manifest_sha256 == hc::sha256(candidate), "journal target differs");
        if (command == "verify") {
            stage = diagnostics->stage = "verify";
            const auto before = store.read_current();
            require(before && hc::serialize_pointer(hc::parse_pointer(text(before->bytes))) == hc::serialize_pointer(journal.target()),
                    "remote factor target differs");
            require(store.get(journal.target().manifest_key, hc::kMaxFactorBytes) == candidate, "remote factor differs");
            if (compact) {
                for (const auto& object : optimized.objects)
                    require(store.get(key(object), object.size()) == object, "remote compact provenance differs");
            } else require(store.get(key(b.proof), hc::kMaxMetadataBytes) == b.proof &&
                    store.get(key(b.source), hc::kMaxMetadataBytes) == b.source, "remote factor bundle differs");
            const auto after = store.read_current();
            require(after && after->bytes == before->bytes && after->etag == before->etag, "factor pointer changed during verification");
            const Json report{{"outcome", "verified"}, {"requests", store.usage().requests},
                              {"diagnostics", diagnostics->summary()},
                              {"scope", hc::hex(store.scope_id())},
                              {"publication_seq", journal.target().publication_seq},
                              {"factor_sha256", hc::hex(hc::sha256(candidate))}, {"verified_at_ms", now_ms()},
                              {"factor_valid_now", now_ms() < b.document.at("valid_until_ms").get<int64_t>()}};
            hc::write_new_file(root / ("result-" + std::to_string(attempt) + ".json"), report.dump());
            std::cout << report.dump() << '\n';
            return 0;
        }
        Json compact_objects = Json::array();
        stage = diagnostics->stage = "provenance";
        if (!journal.unresolved()) {
            require(now_ms() < b.document.at("valid_until_ms").get<int64_t>(), "factor expired before publication");
            if (compact) {
                for (const auto& object : optimized.objects) {
                    const auto before = store.usage();
                    if (hc::reuse_or_put_immutable(store, key(object), object) != hc::WriteOutcome::applied)
                        throw hc::Error(hc::ErrorCode::io, "compact object not confirmed");
                    const auto after = store.usage();
                    compact_objects.push_back({{"sha256", hc::hex(hc::sha256(object))}, {"bytes", object.size()},
                        {"requests", after.requests - before.requests},
                        {"upload_reserved", after.upload_reserved - before.upload_reserved},
                        {"reused", after.upload_reserved == before.upload_reserved}});
                }
            } else if (hc::put_immutable(store, key(b.source), b.source) != hc::WriteOutcome::applied ||
                       hc::put_immutable(store, key(b.proof), b.proof) != hc::WriteOutcome::applied)
                throw hc::Error(hc::ErrorCode::io, "factor provenance upload not confirmed");
        }
        stage = diagnostics->stage = "publish_or_recover";
        // Explicit retry retains the journal's original bytes/base and exact CAS.
        // publish_factors checks current first and refuses expired writes/rebases.
        const auto result = journal.resume({}, retry_transient);
        const auto usage = store.usage();
        Json report{{"outcome", result.outcome == hc::PublishOutcome::committed ? "committed" :
                     result.outcome == hc::PublishOutcome::conflict ? "conflict" :
                     result.outcome == hc::PublishOutcome::not_applied ? "not_applied" : "indeterminate"},
                    {"requests", usage.requests}, {"upload_reserved", usage.upload_reserved},
                    {"download_reserved", usage.download_reserved}, {"recovered", result.recovered},
                    {"diagnostics", diagnostics->summary()}, {"stage", stage}};
        if (compact) report["compact_objects"] = std::move(compact_objects);
        if (result.error_code) report["error_code"] = hc::sample::error_name(*result.error_code);
        hc::write_new_file(root / ("result-" + std::to_string(attempt) + ".json"), report.dump());
        std::cout << report.dump() << '\n';
        return result.outcome == hc::PublishOutcome::committed ? 0 : 2;
    } catch (const hc::Error& error) {
        failure_report(hc::sample::error_name(error.code()));
        std::cerr << "FAIL factor_publication: validation, budget or transport failure\n";
        return 2;
    } catch (const std::exception&) {
        failure_report("invalid");
        std::cerr << "FAIL factor_publication: validation, budget or transport failure\n";
        return 2;
    }
}
