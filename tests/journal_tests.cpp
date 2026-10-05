#include "transport_fixture.h"
#include "history_cache/journal.h"
#include "history_cache/reader.h"
#include "history_cache/factor_publisher.h"
#include <nlohmann/json.hpp>

#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace test;

namespace {

fs::path executable;

void mutate(const fs::path& path, const hc::Bytes& value) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_TRUNC | O_NOFOLLOW | O_CLOEXEC);
    check(fd >= 0, "open synthetic mutation failed");
    size_t done = 0;
    while (done < value.size()) {
        const auto count = ::write(fd, value.data() + done, value.size() - done);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { ::close(fd); throw std::runtime_error("synthetic mutation write failed"); }
        done += static_cast<size_t>(count);
    }
    const auto synced = ::fsync(fd);
    ::close(fd);
    check(synced == 0, "synthetic mutation sync failed");
}

void integer(hc::Bytes& output, size_t offset, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) output[offset + i] = static_cast<uint8_t>(value >> ((7 - i) * 8U));
}

hc::Bytes record(const std::string& magic, uint8_t kind, uint64_t index, const hc::Digest& previous) {
    hc::Bytes result(80, 0);
    std::copy(magic.begin(), magic.end(), result.begin()); result[4] = kind;
    integer(result, 8, index);
    std::copy(previous.begin(), previous.end(), result.begin() + 16);
    const auto hash = hc::sha256(hc::Bytes(result.begin(), result.begin() + 48));
    std::copy(hash.begin(), hash.end(), result.begin() + 48);
    return result;
}

hc::Digest tail(const hc::Bytes& event) {
    hc::Digest result{}; std::copy(event.end() - 32, event.end(), result.begin()); return result;
}

void journal_storage() {
    run_case("market_source_revision_guard_and_compact_reuse", [] {
        using Json = nlohmann::json;
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        constexpr int64_t day = 20728, now = day * 86400000 + 1000, until = (day + 3) * 86400000;
        Json data = {{"schema_version", 2}, {"kind", "ddb-adjustment-data-v2"},
            {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "000001.SZ"}, {"market", "SZ"},
            {"model", "cumulative"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
            {"allow_empty", true}, {"source_epoch", "factor-fixture"},
            {"date_encoding", "exchange-civil-days-since-1970"}, {"rows", Json::array()}};
        const auto payload = bytes(data.dump());
        check(hc::reuse_or_put_immutable(store, test::key(payload), payload) == hc::WriteOutcome::applied, "data upload failed");
        Json doc = data;
        doc.erase("rows");
        doc.update(Json{{"schema_version", 3}, {"kind", "ddb-adjustment-reference-v3"},
            {"observed_at_ms", now}, {"valid_until_ms", until}, {"source_proof_sha256", hc::hex(hc::sha256("proof"))},
            {"factor_data_sha256", hc::hex(hc::sha256(payload))}, {"factor_data_bytes", payload.size()},
            {"verification", {{"policy", "market-source-check-v1"}, {"exchange", "XSHE"},
                {"calendar_sha256", hc::hex(hc::sha256("calendar"))}, {"trading_day", day}, {"next_trading_day", day + 3},
                {"check_minute", 480}, {"source_revision", 2}, {"source_completed_at_ms", now}, {"next_check_ms", until},
                {"config_sha256", hc::hex(hc::sha256("config"))}, {"workload_sha256", hc::hex(hc::sha256("workload"))},
                {"source_receipt_sha256", hc::hex(hc::sha256("receipt"))}}}});
        const auto initial = hc::publish_factors(store, bytes(doc.dump()), "000001.SZ", "SZ", {}, [] { return now; });
        check(initial.outcome == hc::PublishOutcome::committed, "market initial publish failed");
        for (int mode = 0; mode < 2; ++mode) {
            auto bad = doc;
            if (mode == 0) bad["verification"]["source_revision"] = 1;
            else bad["verification"]["source_receipt_sha256"] = hc::hex(hc::sha256("tamper"));
            peer->calls.clear();
            rejects(hc::ErrorCode::conflict, [&] {
                (void)hc::publish_factors(store, bytes(bad.dump()), "000001.SZ", "SZ", initial.target, [] { return now; });
            });
            check(std::none_of(peer->calls.begin(), peer->calls.end(), [](const auto& call) {
                return call.method == hc::HttpMethod::put;
            }), "bad source revision wrote data");
        }
        doc["verification"]["source_revision"] = 3;
        doc["verification"]["source_receipt_sha256"] = hc::hex(hc::sha256("next-receipt"));
        check(hc::publish_factors(store, bytes(doc.dump()), "000001.SZ", "SZ", initial.target, [] { return now; }).outcome ==
              hc::PublishOutcome::committed, "fresh source check failed");
    });
    run_case("explicit_factor_retry_same_target_no_rebase_or_expired_write", [] {
        using Json = nlohmann::json;
        const Json doc = {{"schema_version", 1}, {"kind", "ddb-adjustment-snapshot-v1"},
            {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "AAPL"}, {"market", "US"},
            {"model", "futu_ab"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
            {"allow_empty", true}, {"observed_at_ms", 1000}, {"valid_until_ms", 2000},
            {"source_epoch", "factor-fixture"}, {"source_proof_sha256", hc::hex(hc::sha256("proof"))},
            {"date_encoding", "exchange-civil-days-since-1970"}, {"rows", Json::array()}};
        for (int mode = 0; mode < 4; ++mode) {
            Workspace workspace;
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto path = workspace.root / "factor-retry";
            const auto candidate = bytes(doc.dump());
            hc::JournaledPublisher::prepare_factors(path, store.scope_id(), candidate, "AAPL", "US", {}, 1500);
            const auto intent = hc::read_file(path / "intent.r2j", hc::kMaxMetadataBytes);
            peer->fault = mode == 1 ? PointerFault::lost_ack_unreadable : PointerFault::unknown;
            {
                hc::JournaledPublisher journal(path, store, [] { return 1500; });
                check(journal.resume().outcome == hc::PublishOutcome::indeterminate, "missing synthetic failure");
            }
            if (mode == 3) {
                auto other = doc; other["observed_at_ms"] = 1100; other["valid_until_ms"] = 2100;
                const auto competitor = workspace.root / "competitor";
                hc::JournaledPublisher::prepare_factors(competitor, store.scope_id(), bytes(other.dump()), "AAPL", "US", {}, 1500);
                hc::JournaledPublisher writer(competitor, store, [] { return 1500; });
                check(writer.resume().outcome == hc::PublishOutcome::committed, "competitor failed");
            }
            const auto calls = peer->calls.size();
            hc::JournaledPublisher retry(path, store, [mode] { return mode == 2 ? 2000 : 1500; });
            const auto result = retry.resume({}, true);
            check(result.outcome == (mode < 2 ? hc::PublishOutcome::committed : hc::PublishOutcome::indeterminate), "retry outcome");
            if (mode != 0) check(std::none_of(peer->calls.begin() + static_cast<ptrdiff_t>(calls), peer->calls.end(),
                [](const auto& call) { return call.method == hc::HttpMethod::put; }), "retry rewrote committed, expired or conflicting pointer");
            check(hc::read_file(path / "intent.r2j", hc::kMaxMetadataBytes) == intent, "retry changed intent");
        }
    });
    run_case("factor_journal_pins_intent_recovers_expired_ack_and_never_rewrites_unknown", [] {
        using Json = nlohmann::json;
        const Json base_doc = {{"schema_version", 1}, {"kind", "ddb-adjustment-snapshot-v1"},
            {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "000001.SZ"}, {"market", "SZ"},
            {"model", "cumulative"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
            {"allow_empty", true}, {"observed_at_ms", 1000}, {"valid_until_ms", 2000},
            {"source_epoch", "factor-fixture"}, {"source_proof_sha256", hc::hex(hc::sha256("proof"))},
            {"date_encoding", "exchange-civil-days-since-1970"}, {"rows", Json::array()}};
        for (const bool compact : {false, true}) for (const auto fault : {PointerFault::lost_ack_unreadable, PointerFault::unknown}) {
            Workspace workspace;
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto path = workspace.root / "factor-journal";
            auto doc = base_doc;
            if (compact) {
                auto data = doc;
                for (const auto* field : {"observed_at_ms", "valid_until_ms", "source_proof_sha256"}) data.erase(field);
                data["schema_version"] = 2; data["kind"] = "ddb-adjustment-data-v2";
                const auto payload = bytes(data.dump());
                check(hc::reuse_or_put_immutable(store, test::key(payload), payload) == hc::WriteOutcome::applied,
                      "compact journal data prepare failed");
                doc.erase("rows"); doc["schema_version"] = 2; doc["kind"] = "ddb-adjustment-reference-v2";
                doc["factor_data_sha256"] = hc::hex(hc::sha256(payload)); doc["factor_data_bytes"] = payload.size();
            }
            const auto candidate = bytes(doc.dump());
            hc::JournaledPublisher::prepare_factors(path, store.scope_id(), candidate, "000001.SZ", "SZ", {}, 1500);
            const auto original = hc::read_file(path / "intent.r2j", hc::kMaxMetadataBytes);
            peer->fault = fault;
            {
                hc::JournaledPublisher journal(path, store, [] { return 1500; });
                check(journal.resume().outcome == hc::PublishOutcome::indeterminate && journal.unresolved(), "factor unknown lost");
            }
            peer->calls.clear();
            hc::JournaledPublisher restarted(path, store, [] { return 2500; });
            const auto result = restarted.resume();
            check(result.outcome == (fault == PointerFault::unknown ? hc::PublishOutcome::indeterminate : hc::PublishOutcome::committed),
                  "expired factor ACK recovery incorrect");
            check(std::all_of(peer->calls.begin(), peer->calls.end(), [](const auto& call) {
                return call.method == hc::HttpMethod::get;
            }), "factor recovery performed a PUT");
            check(hc::read_file(path / "intent.r2j", hc::kMaxMetadataBytes) == original, "factor intent rewritten");
            if (fault != PointerFault::unknown) {
                peer->calls.clear();
                check(restarted.resume().outcome == hc::PublishOutcome::committed && peer->calls.empty(), "committed journal used network");
            }
        }
    });
    run_case("factor_journal_barriers_scope_and_torn_intent", [] {
        using Json = nlohmann::json;
        const Json doc = {{"schema_version", 1}, {"kind", "ddb-adjustment-snapshot-v1"},
            {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "AAPL"}, {"market", "US"},
            {"model", "futu_ab"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
            {"allow_empty", true}, {"observed_at_ms", 1000}, {"valid_until_ms", 2000},
            {"source_epoch", "factor-fixture"}, {"source_proof_sha256", hc::hex(hc::sha256("proof"))},
            {"date_encoding", "exchange-civil-days-since-1970"}, {"rows", Json::array()}};
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto path = workspace.root / "factor-journal";
        hc::JournaledPublisher::prepare_factors(path, store.scope_id(), bytes(doc.dump()), "AAPL", "US", {}, 1500);
        {
            hc::JournaledPublisher journal(path, store, [] { return 1500; });
            rejects(hc::ErrorCode::io, [&] { (void)journal.resume([](auto step) {
                if (step == hc::JournalStep::before_dispatch_sync) throw hc::Error(hc::ErrorCode::io, "injected");
            }); });
            check(peer->calls.empty(), "factor dispatched before fsync");
        }
        auto cfg = config(); cfg.key_prefix = "r2-history-staging/different-002/";
        hc::S3Store other(cfg, peer, credentials(), limits(), signing_time);
        rejects(hc::ErrorCode::conflict, [&] { hc::JournaledPublisher journal(path, other); });
        auto original = hc::read_file(path / "intent.r2j", hc::kMaxMetadataBytes);
        original.pop_back(); mutate(path / "intent.r2j", original);
        rejects(hc::ErrorCode::corrupt, [&] { hc::JournaledPublisher journal(path, store); });
        check(peer->calls.empty(), "invalid factor journal used network");
    });
    run_case("epoch_replacement_is_explicit_pinned_and_recoverable", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        check(hc::ConditionalPublisher(store).publish(manifest(), 0).outcome == hc::PublishOutcome::committed, "initial");
        const auto base = hc::load_snapshot(store).pointer;
        auto next = manifest(2); next.dataset_epoch = "new-physical-epoch";
        rejects(hc::ErrorCode::conflict, [&] { (void)hc::ConditionalPublisher(store).publish(next, 1); });
        auto wrong = base; wrong.manifest_sha256 = hc::sha256("different");
        wrong.manifest_key = "manifests/v1/" + hc::hex(wrong.manifest_sha256) + ".json";
        check(hc::ConditionalPublisher(store).publish(next, 1, wrong).outcome == hc::PublishOutcome::conflict, "base hash ignored");
        const auto path = workspace.root / "epoch-journal";
        hc::JournaledPublisher::prepare(path, store.scope_id(), next, 1, {}, base);
        peer->fault = PointerFault::lost_ack_unreadable;
        {
            hc::JournaledPublisher journal(path, store);
            check(journal.epoch_base() && hc::serialize_pointer(*journal.epoch_base()) == hc::serialize_pointer(base), "base lost");
            check(journal.resume().outcome == hc::PublishOutcome::indeterminate, "unknown lost");
        }
        hc::JournaledPublisher reopened(path, store);
        check(reopened.resume().outcome == hc::PublishOutcome::committed && reopened.target().publication_seq == 2,
              "same migration could not recover");
        check(hc::load_snapshot(store).manifest.dataset_epoch == next.dataset_epoch, "migration not applied");
    });
    run_case("epoch_replacement_cannot_drop_dates_or_reset_source_versions", [] {
        const auto old = manifest(7);
        auto next = manifest(8); next.dataset_epoch = "new-epoch";
        hc::validate_epoch_replacement(old, next);
        auto bad = next; bad.entries[0].coverage.end_ms--;
        rejects(hc::ErrorCode::conflict, [&] { hc::validate_epoch_replacement(old, bad); });
        bad = next; bad.entries[0].source_version = 7;
        rejects(hc::ErrorCode::conflict, [&] { hc::validate_epoch_replacement(old, bad); });
        bad = next; bad.entries[0].data_version = hc::sha256("changed-schema");
        rejects(hc::ErrorCode::conflict, [&] { hc::validate_epoch_replacement(old, bad); });
        bad = next; bad.entries[0].identity.symbol = "DIFFERENT";
        rejects(hc::ErrorCode::conflict, [&] { hc::validate_epoch_replacement(old, bad); });
    });
    run_case("intent_requires_new_private_directory_and_exact_candidate", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        struct stat status{};
        check(::stat(root.c_str(), &status) == 0 && (status.st_mode & 0777) == 0700, "journal directory not private");
        for (const auto* name : {"ready", "writer.lock", "intent.r2j", "events.r2j", "dispatch.r2j"})
            check(::stat((root / name).c_str(), &status) == 0 && (status.st_mode & 0777) == 0600, "journal file not private");
        hc::JournaledPublisher journal(root, store);
        check(hc::serialize_pointer(journal.target()) == text(pointer()) && !journal.unresolved(), "persisted target changed");
        const auto original = hc::read_file(root / "intent.r2j", hc::kMaxMetadataBytes);
        rejects(hc::ErrorCode::conflict, [&] { hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(2), 1); });
        check(hc::read_file(root / "intent.r2j", hc::kMaxMetadataBytes) == original && peer->calls.empty(), "intent overwritten or storage contacted");
        rejects(hc::ErrorCode::resource_limit, [&] {
            hc::JournaledPublisher::prepare(workspace.root / "invalid", store.scope_id(), manifest(), UINT64_MAX);
        });
        check(!fs::exists(workspace.root / "invalid"), "invalid candidate created journal");
    });
    run_case("failed_intent_fsync_cannot_open_ready_journal_or_dispatch", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        rejects(hc::ErrorCode::io, [&] {
            hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0, [](auto step) {
                if (step == hc::JournalStep::before_intent_sync) throw hc::Error(hc::ErrorCode::io, "injected fsync failure");
            });
        });
        check(!fs::exists(root / "ready"), "unpersisted intent ready");
        rejects(hc::ErrorCode::io, [&] { hc::JournaledPublisher journal(root, store); });
        check(peer->calls.empty(), "unpersisted intent dispatched");
    });
    run_case("attempt_and_dispatch_floor_fsync_precede_all_remote_calls", [] {
        Workspace workspace;
        for (auto step : {hc::JournalStep::before_attempt_sync, hc::JournalStep::before_dispatch_sync}) {
            auto peer = std::make_shared<FakeS3>();
            hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
            const auto root = workspace.root / std::to_string(static_cast<int>(step));
            hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
            {
                hc::JournaledPublisher journal(root, store);
                rejects(hc::ErrorCode::io, [&] {
                    (void)journal.resume([step](auto at) { if (at == step) throw hc::Error(hc::ErrorCode::io, "injected fsync failure"); });
                });
                check(peer->calls.empty() && journal.unresolved(), "failed local barrier contacted store or erased unknown");
                rejects(hc::ErrorCode::io, [&] { (void)journal.resume(); });
            }
            hc::JournaledPublisher restarted(root, store);
            check(restarted.unresolved(), "possible attempt lost on restart");
            check(restarted.resume().outcome == hc::PublishOutcome::committed, "same candidate cannot recover local failure");
        }
    });
    run_case("journal_scope_rejects_new_namespace_but_accepts_rotated_key", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        auto cfg = config(); cfg.key_prefix = "r2-history-staging/different-002/";
        hc::S3Store different(cfg, peer, credentials(), limits(), signing_time);
        rejects(hc::ErrorCode::conflict, [&] { hc::JournaledPublisher journal(root, different); });
        hc::S3Store rotated(config(), peer, std::make_shared<const hc::S3Credentials>("ROTATED", "ROTATEDSECRET"), limits(), signing_time);
        hc::JournaledPublisher journal(root, rotated);
        check(journal.target().publication_seq == 1 && peer->calls.empty(), "scope validation performed I/O or lost original seq");
    });
    run_case("local_lock_symlinks_hardlinks_and_loose_permissions_fail", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        {
            hc::JournaledPublisher writer(root, store);
            rejects(hc::ErrorCode::conflict, [&] { hc::JournaledPublisher second(root, store); });
        }
        fs::create_directory_symlink(root, workspace.root / "alias");
        rejects(hc::ErrorCode::invalid, [&] { hc::JournaledPublisher journal(workspace.root / "alias", store); });
        fs::create_directory_symlink(workspace.root, workspace.root / "parent-alias");
        rejects(hc::ErrorCode::invalid, [&] {
            hc::JournaledPublisher::prepare(workspace.root / "parent-alias" / "unsafe", store.scope_id(), manifest(), 0);
        });
        fs::create_hard_link(root / "intent.r2j", workspace.root / "hardlink");
        rejects(hc::ErrorCode::invalid, [&] { hc::JournaledPublisher journal(root, store); });
        fs::remove(workspace.root / "hardlink");
        fs::permissions(root / "intent.r2j", fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read);
        rejects(hc::ErrorCode::invalid, [&] { hc::JournaledPublisher journal(root, store); });
        fs::permissions(root / "intent.r2j", fs::perms::owner_read | fs::perms::owner_write);
        fs::permissions(root, fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec);
        rejects(hc::ErrorCode::invalid, [&] { hc::JournaledPublisher journal(root, store); });
        check(peer->calls.empty(), "unsafe local journal contacted store");
    });
    run_case("truncated_and_corrupt_intent_never_dispatch", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        const auto original = hc::read_file(root / "intent.r2j", hc::kMaxMetadataBytes);
        for (unsigned mode = 0; mode < 3; ++mode) {
            auto changed = original;
            if (mode == 0) changed.resize(10);
            if (mode == 1) changed.pop_back();
            if (mode == 2) changed[60] ^= 1;
            mutate(root / "intent.r2j", changed);
            rejects(hc::ErrorCode::corrupt, [&] { hc::JournaledPublisher journal(root, store); });
        }
        check(peer->calls.empty(), "invalid intent used");
    });
    run_case("torn_chain_and_full_event_suffix_loss_fail_closed", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        peer->fault = PointerFault::unknown;
        {
            hc::JournaledPublisher journal(root, store);
            check(journal.resume().outcome == hc::PublishOutcome::indeterminate, "expected unknown");
        }
        const auto original = hc::read_file(root / "events.r2j", 10240);
        check(original.size() == 160, "expected attempt/result pair");
        const auto calls = peer->calls.size();
        for (unsigned mode = 0; mode < 3; ++mode) {
            auto changed = original;
            if (mode == 0) changed.pop_back();
            if (mode == 1) changed[40] ^= 1;
            if (mode == 2) changed.clear();
            mutate(root / "events.r2j", changed);
            rejects(hc::ErrorCode::corrupt, [&] { hc::JournaledPublisher journal(root, store); });
        }
        mutate(root / "events.r2j", hc::Bytes(original.begin(), original.begin() + 80));
        hc::JournaledPublisher conservative(root, store);
        check(conservative.unresolved() && peer->calls.size() == calls, "missing result erased possible dispatch");
    });
    run_case("bounded_journal_refuses_another_attempt_before_dispatch", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        auto previous = hc::sha256(hc::read_file(root / "intent.r2j", hc::kMaxMetadataBytes));
        hc::Bytes events, floor;
        for (uint64_t i = 1; i <= 128; ++i) {
            const auto event = record("JEV1", i % 2 ? 1 : 4, i, previous);
            previous = tail(event);
            events.insert(events.end(), event.begin(), event.end());
            if (i % 2) floor = record("JFL1", 0, i, previous);
        }
        mutate(root / "events.r2j", events); mutate(root / "dispatch.r2j", floor);
        hc::JournaledPublisher journal(root, store);
        rejects(hc::ErrorCode::resource_limit, [&] { (void)journal.resume(); });
        check(journal.unresolved() && peer->calls.empty(), "journal cap cleared unknown or contacted peer");
    });
}

void save_peer(const FakeS3& peer, const fs::path& path) {
    nlohmann::json objects = nlohmann::json::array();
    for (const auto& [url, object] : peer.objects)
        objects.push_back({{"url", url}, {"body_hex", hc::hex(object.body.data(), object.body.size())}, {"etag", object.etag}});
    hc::write_new_file(path, nlohmann::json{{"writes", peer.writes}, {"objects", objects}}.dump() + '\n');
}

void load_peer(FakeS3& peer, const fs::path& path) {
    const auto state = nlohmann::json::parse(hc::read_file(path, hc::kMaxMetadataBytes));
    peer.writes = state.at("writes").get<uint64_t>();
    for (const auto& value : state.at("objects"))
        peer.objects.emplace(value.at("url").get<std::string>(), FakeS3::Object{hc::unhex(value.at("body_hex").get<std::string>()),
                                                                            value.at("etag").get<std::string>()});
}

int worker(const std::string& mode, const fs::path& root) {
    ::alarm(10);
    auto peer = std::make_shared<FakeS3>();
    if (mode == "recover") load_peer(*peer, root / "peer.json");
    hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
    hc::JournaledPublisher journal(root / "journal", store);
    if (mode == "after-write") peer->after_pointer = [&] { save_peer(*peer, root / "peer.json"); ::_exit(41); };
    auto result = journal.resume([&](auto step) {
        if (mode == "before-dispatch" && step == hc::JournalStep::attempt_durable) {
            save_peer(*peer, root / "peer.json"); ::_exit(42);
        }
    });
    check(mode == "recover" && result.outcome == hc::PublishOutcome::committed, "worker failed to recover");
    const auto current = hc::load_snapshot(store);
    hc::write_new_file(root / "recovered.json", nlohmann::json{{"publication_seq", result.target.publication_seq},
                       {"current_seq", current.pointer.publication_seq}, {"writes", peer->writes},
                       {"recovered", result.recovered}, {"manifest", hc::serialize_manifest(current.manifest)}}.dump() + '\n');
    return 0;
}

void child(const std::string& mode, const fs::path& root, int expected) {
    const auto pid = ::fork();
    check(pid >= 0, "fork failed");
    if (pid == 0) {
        ::execl(executable.c_str(), executable.c_str(), "worker", mode.c_str(), root.c_str(), static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    pid_t reaped;
    do { reaped = ::waitpid(pid, &status, 0); } while (reaped < 0 && errno == EINTR);
    check(reaped == pid && WIFEXITED(status) && WEXITSTATUS(status) == expected, "journal child failed or exceeded timeout");
}

void process_recovery(const std::string& mode, int exit_code) {
    Workspace workspace;
    auto peer = std::make_shared<FakeS3>();
    hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
    hc::JournaledPublisher::prepare(workspace.root / "journal", store.scope_id(), manifest(), 0);
    child(mode, workspace.root, exit_code);
    {
        hc::JournaledPublisher journal(workspace.root / "journal", store);
        check(journal.unresolved(), "process exit forgot pending attempt");
    }
    child("recover", workspace.root, 0);
    const auto result = nlohmann::json::parse(hc::read_file(workspace.root / "recovered.json", hc::kMaxMetadataBytes));
    check(result.at("publication_seq") == 1 && result.at("current_seq") == 1 && result.at("writes") == 2 &&
          result.at("manifest") == hc::serialize_manifest(manifest()), "restart changed target, advanced sequence or rewrote pointer");
    check(result.at("recovered") == (mode == "after-write"), "unexpected recovery path");
}

void journal_recovery() {
    run_case("known_non_delivery_is_distinct_from_historical_unknown", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        peer->fault = PointerFault::not_sent;
        {
            hc::JournaledPublisher journal(root, store);
            check(journal.resume().outcome == hc::PublishOutcome::not_applied && !journal.unresolved(), "known non-delivery marked historical unknown");
        }
        hc::JournaledPublisher resumed(root, store);
        check(resumed.resume().outcome == hc::PublishOutcome::committed && peer->writes == 2, "explicit same-target retry failed");
    });
    run_case("unknown_survives_restart_and_later_non_delivery", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        peer->fault = PointerFault::unknown;
        {
            hc::JournaledPublisher journal(root, store);
            check(journal.resume().outcome == hc::PublishOutcome::indeterminate, "expected unknown");
        }
        peer->fault = PointerFault::not_sent;
        {
            hc::JournaledPublisher resumed(root, store);
            check(resumed.resume().outcome == hc::PublishOutcome::indeterminate && resumed.unresolved(), "new failure erased historical unknown");
        }
        hc::JournaledPublisher recovered(root, store);
        check(recovered.resume().outcome == hc::PublishOutcome::committed && !recovered.unresolved(), "same target did not resolve unknown");
    });
    run_case("newer_pointer_cannot_clear_unknown_or_rebase_old_candidate", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        peer->fault = PointerFault::unknown;
        {
            hc::JournaledPublisher journal(root, store);
            check(journal.resume().outcome == hc::PublishOutcome::indeterminate, "expected unknown");
        }
        check(hc::ConditionalPublisher(store).publish(manifest(2), 0).outcome == hc::PublishOutcome::committed, "racing publisher failed");
        check(hc::ConditionalPublisher(store).publish(manifest(3), 1).outcome == hc::PublishOutcome::committed, "newer publisher failed");
        const auto writes = peer->writes;
        hc::JournaledPublisher journal(root, store);
        const auto result = journal.resume();
        check(result.outcome == hc::PublishOutcome::indeterminate && result.target.publication_seq == 1 && peer->writes == writes,
              "unknown cleared, target rebased or pointer rewritten");
        rejects(hc::ErrorCode::conflict, [&] { hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(4), 2); });
    });
    run_case("lost_ack_exact_pointer_recovers_without_second_pointer_put", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        peer->fault = PointerFault::lost_ack_unreadable;
        {
            hc::JournaledPublisher journal(root, store);
            check(journal.resume().outcome == hc::PublishOutcome::indeterminate && peer->writes == 2, "ACK loss misclassified");
        }
        hc::JournaledPublisher journal(root, store);
        const auto result = journal.resume();
        check(result.outcome == hc::PublishOutcome::committed && result.recovered && peer->writes == 2 && !journal.unresolved(),
              "ACK recovery repeated mutation");
    });
    run_case("local_checkpoint_failure_after_ack_returns_unknown", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        {
            hc::JournaledPublisher journal(root, store);
            const auto result = journal.resume([](auto step) {
                if (step == hc::JournalStep::before_result_sync) throw hc::Error(hc::ErrorCode::io, "injected checkpoint fsync failure");
            });
            check(result.outcome == hc::PublishOutcome::indeterminate && journal.unresolved() && peer->writes == 2,
                  "ACK acknowledged despite failed checkpoint");
            rejects(hc::ErrorCode::io, [&] { (void)journal.resume(); });
        }
        const auto calls = peer->calls.size();
        hc::JournaledPublisher recovered(root, store);
        const auto result = recovered.resume();
        check(result.outcome == hc::PublishOutcome::committed && result.recovered && peer->calls.size() == calls,
              "readable ACK record was not durably recovered");
    });
    run_case("process_exit_after_remote_write_recovers_from_disk", [] { process_recovery("after-write", 41); });
    run_case("process_exit_after_durable_attempt_replays_original_intent", [] { process_recovery("before-dispatch", 42); });
    run_case("durable_commit_records_historical_not_current_success", [] {
        Workspace workspace;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        const auto root = workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), manifest(), 0);
        {
            hc::JournaledPublisher journal(root, store);
            check(journal.resume().outcome == hc::PublishOutcome::committed, "commit failed");
        }
        check(hc::ConditionalPublisher(store).publish(manifest(2), 1).outcome == hc::PublishOutcome::committed, "second commit failed");
        const auto calls = peer->calls.size();
        hc::JournaledPublisher recovered(root, store);
        const auto result = recovered.resume();
        check(result.outcome == hc::PublishOutcome::committed && result.target.publication_seq == 1 && peer->calls.size() == calls,
              "historical ACK became current-ness assertion or rewrote remote state");
    });
    run_case("journaled_s3_upload_publish_and_query_preserve_pack_format", [] {
        PackFixture fixture;
        auto peer = std::make_shared<FakeS3>();
        hc::S3Store store(config(), peer, credentials(), limits(), signing_time);
        check(hc::put_immutable(store, fixture.candidate.entries[0].pack->key, fixture.data) == hc::WriteOutcome::applied, "pack upload failed");
        const auto root = fixture.workspace.root / "journal";
        hc::JournaledPublisher::prepare(root, store.scope_id(), fixture.candidate, 0);
        hc::JournaledPublisher journal(root, store);
        check(journal.resume().outcome == hc::PublishOutcome::committed, "journaled publication failed");
        const auto snapshot = std::make_shared<const hc::Snapshot>(hc::load_snapshot(store));
        const hc::Query query{identity(), version(), {fixture.rows[1000].timestamp_ms, fixture.rows[1100].timestamp_ms}, 100};
        size_t rows = 0;
        const auto result = hc::read_plan(store, hc::plan_query(snapshot, query, true), [&](const auto& batch) {
            for (const auto& row : batch) { check(hc::same_row(row, fixture.rows[1000 + rows]), "journaled query row changed"); ++rows; }
        });
        check(rows == 100 && result.rows == 100 && snapshot->pointer.publication_seq == 1, "journaled query did not complete");
    });
}

}  // namespace

int main(int argc, char** argv) {
    try {
        executable = fs::absolute(argv[0]);
        if (argc == 4 && std::string(argv[1]) == "worker") return worker(argv[2], argv[3]);
        check(argc == 2, "usage: history-cache-journal-tests SUITE");
        const std::string suite = argv[1];
        if (suite == "journal_storage") journal_storage();
        else if (suite == "journal_recovery") journal_recovery();
        else throw std::runtime_error("unknown suite");
        std::cout << "PASS " << suite << " cases=" << cases << '\n';
        return 0;
    } catch (const std::exception& error) { std::cerr << "FAIL " << error.what() << '\n'; return 1; }
}
