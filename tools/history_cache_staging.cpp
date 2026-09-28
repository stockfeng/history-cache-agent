#include "history_cache/staging.h"

#include <cstdlib>
#include <iostream>
#include <map>
#include <sys/resource.h>
#include <sys/stat.h>

namespace hc = history_cache;
namespace st = history_cache::staging;
namespace fs = std::filesystem;
using Json = nlohmann::json;

namespace {

std::string text(const hc::Bytes& bytes) { return {bytes.begin(), bytes.end()}; }
void check(bool condition) { if (!condition) throw hc::Error(hc::ErrorCode::invalid, "staging command rejected"); }

fs::path project_path(const std::string& input, bool artifact) {
    const auto path = fs::absolute(input);
    const fs::path project = HC_STAGING_WORKSPACE;
    const auto allowed = artifact ? project / "artifacts/r2" : project;
    for (const auto& part : path) check(part != ".." && part != ".");
    const auto relative = path.lexically_relative(allowed);
    check(!relative.empty() && relative != "." && *relative.begin() != "..");
    return path;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        ::umask(0077);
        struct rlimit core{0, 0}; check(::setrlimit(RLIMIT_CORE, &core) == 0);
        std::map<std::string, std::string> options;
        bool execute = false, exclusive = false;
        for (int i = 1; i < argc; ++i) {
            const std::string key = argv[i];
            if (key == "--execute") { check(!execute); execute = true; }
            else if (key == "--exclusive-namespace") { check(!exclusive); exclusive = true; }
            else {
                check(key == "--config" || key == "--fixture" || key == "--run-id" || key == "--output" ||
                      key == "--approved-config-sha256" || key == "--approved-intent-sha256" || key == "--inspect-run");
                check(i + 1 < argc && options.emplace(key, argv[++i]).second);
            }
        }
        if (options.count("--inspect-run")) {
            check(!execute && !exclusive && options.size() == 1);
            std::cout << st::inspect_run(project_path(options.at("--inspect-run"), true)).dump() << '\n';
            return 0;
        }
        check(options.count("--config") && options.count("--fixture") && options.count("--run-id"));
        const auto config_path = project_path(options.at("--config"), false);
        const auto fixture = project_path(options.at("--fixture"), false);
        const auto config = text(st::read_input(config_path, 16384));
        const auto manifest_raw = text(st::read_input(fixture / "candidate.json", hc::kMaxMetadataBytes));
        const auto candidate = hc::parse_manifest(manifest_raw);
        check(candidate.entries.size() == 1 && candidate.entries.front().pack.has_value());
        const auto pack = st::read_input(fixture / fs::path(candidate.entries.front().pack->key).filename(), 65536);
        const auto plan = st::Plan::prepare(config, manifest_raw, pack, options.at("--run-id"));
        if (!execute) {
            check(!exclusive && !options.count("--approved-config-sha256") && !options.count("--approved-intent-sha256") && !options.count("--output"));
            std::cout << Json({{"status", "OFFLINE_CPP_PLAN_ONLY"}, {"network_accessed", false}, {"credentials_read", false},
                               {"config_sha256", hc::hex(plan.config_hash())}, {"intent_sha256", hc::hex(plan.intent_hash())},
                               {"intent", plan.intent()}}).dump() << '\n';
            return 0;
        }
        check(options.size() == 6 && exclusive && options.count("--output") && options.count("--approved-config-sha256") && options.count("--approved-intent-sha256"));
        check(hc::CurlHttpTransport::runtime_version() != "not-built");
        const st::Approval approval{true, true, hc::parse_digest(options.at("--approved-config-sha256")),
                                   hc::parse_digest(options.at("--approved-intent-sha256"))};
        const fs::path project = HC_STAGING_WORKSPACE;
        const auto report = st::Executor::run(plan, approval, project_path(options.at("--output"), true),
            project / "artifacts/r2/cpp-staging-registry", [] {
                const auto* id = std::getenv("R2_STAGING_ACCESS_KEY_ID");
                const auto* secret = std::getenv("R2_STAGING_SECRET_ACCESS_KEY");
                check(id && secret);
                auto result = std::make_shared<const hc::S3Credentials>(id, secret);
                ::unsetenv("R2_STAGING_ACCESS_KEY_ID"); ::unsetenv("R2_STAGING_SECRET_ACCESS_KEY");
                return result;
            }, [] { return std::make_shared<hc::CurlHttpTransport>(true); });
        std::cout << Json({{"status", report.at("status")}, {"report", options.at("--output") + "/report.json"},
                           {"usage", report.at("usage")}}).dump() << '\n';
        return report.at("status") == "PASS_STAGING_PROFILE" || report.at("status") == "PASS_STAGING_RETAINED" ? 0 : 1;
    } catch (...) {
        std::cout << "{\"status\":\"REJECTED_OR_INDETERMINATE\",\"hint\":\"Check local input, approvals and retained run evidence; no automatic retry.\"}\n";
        return 2;
    }
}
