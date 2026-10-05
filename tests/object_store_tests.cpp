#include "history_cache/publisher.h"
#include "history_cache/reader.h"
#include "history_cache/factor_publisher.h"
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <utility>

namespace hc = history_cache;
namespace fs = std::filesystem;

namespace {

size_t cases = 0;

void check(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

template <class Callable> void rejects(hc::ErrorCode code, Callable call) {
    try { call(); }
    catch (const hc::Error& error) {
        check(error.code() == code, "unexpected error code: " + std::string(error.what()));
        return;
    }
    throw std::runtime_error("expected rejection");
}

template <class Callable> void run_case(const std::string& name, Callable call) {
    call();
    ++cases;
    std::cout << "PASS " << name << '\n';
}

hc::Bytes bytes(const std::string& text) { return {text.begin(), text.end()}; }
hc::SeriesIdentity identity() { return {"synthetic", "TEST", "FIXTURE", 60, "none"}; }
hc::Digest version() { return hc::sha256(std::string("test-query-semantics-v1\n")); }

hc::Manifest manifest(uint64_t source = 1) {
    return {"fixture-epoch-1", {{identity(), version(), source, {0, 10000}, 0, std::nullopt}}};
}

std::string manifest_key(const hc::Manifest& value) {
    return "manifests/v1/" + hc::hex(hc::sha256(hc::serialize_manifest(value))) + ".json";
}

enum class Fault { none, not_applied, unknown_without_write, lost_ack, lost_ack_unreadable, throw_after_write, delayed };

class MemoryStore : public hc::ObjectStore {
public:
    hc::Digest scope_id() const override { return hc::sha256(std::string("synthetic-memory-store-v1\n")); }
    std::map<std::string, std::shared_ptr<const hc::Bytes>> objects;
    std::optional<hc::VersionedObject> current;
    Fault create_fault = Fault::none;
    Fault pointer_fault = Fault::none;
    std::function<void()> before_pointer;
    std::function<void()> after_pointer;
    mutable unsigned fail_current_reads = 0;
    mutable unsigned fail_object_reads = 0;
    mutable uint64_t current_reads = 0;
    mutable uint64_t range_bytes = 0;
    mutable uint64_t largest_range = 0;
    mutable uint64_t data_gets = 0;
    uint64_t creates = 0;
    uint64_t pointer_writes = 0;
    uint64_t pointer_mutations = 0;
    std::vector<std::optional<std::string>> conditions;
    std::optional<uint64_t> bad_range_offset;
    bool short_range = false;

    hc::Bytes get(const std::string& key, uint64_t max_bytes) const override {
        const auto reference = hc::parse_immutable_key(key);
        check(max_bytes > 0 && max_bytes <= reference.max_bytes, "invalid get budget");
        if (key.rfind("data/", 0) == 0) ++data_gets;
        if (fail_object_reads > 0) {
            --fail_object_reads;
            throw hc::Error(hc::ErrorCode::io, "injected object read failure");
        }
        const auto found = objects.find(key);
        if (found == objects.end()) throw hc::Error(hc::ErrorCode::missing, "object missing");
        if (found->second->size() > max_bytes) throw hc::Error(hc::ErrorCode::resource_limit, "object read budget");
        return *found->second;
    }

    hc::RangeReader open_range(const std::string& key, uint64_t expected_size) const override {
        const auto reference = hc::parse_immutable_key(key);
        check(expected_size > 0 && expected_size <= reference.max_bytes, "invalid range object size");
        const auto found = objects.find(key);
        if (found == objects.end()) throw hc::Error(hc::ErrorCode::missing, "range object missing");
        if (found->second->size() != expected_size) throw hc::Error(hc::ErrorCode::corrupt, "object size mismatch");
        return [this, data = found->second](uint64_t offset, uint64_t size) {
            check(size > 0 && size <= hc::kMaxBlockBytes, "range allocation exceeds block budget");
            check(offset <= data->size() && size <= data->size() - offset, "range outside object");
            range_bytes += size;
            largest_range = std::max(largest_range, size);
            auto result = hc::Bytes(data->begin() + static_cast<std::ptrdiff_t>(offset),
                                    data->begin() + static_cast<std::ptrdiff_t>(offset + size));
            if (bad_range_offset && offset == *bad_range_offset) {
                if (short_range) result.pop_back();
                else result[0] ^= 1;
            }
            return result;
        };
    }

    std::optional<hc::VersionedObject> read_current() const override {
        ++current_reads;
        if (fail_current_reads > 0) {
            --fail_current_reads;
            throw hc::Error(hc::ErrorCode::io, "injected authoritative read failure");
        }
        return current;
    }

    hc::WriteOutcome create(const std::string& key, const hc::Bytes& value) override {
        const auto reference = hc::parse_immutable_key(key);
        check(!value.empty() && value.size() <= reference.max_bytes, "create budget");
        ++creates;
        const auto fault = std::exchange(create_fault, Fault::none);
        if (fault == Fault::not_applied) return hc::WriteOutcome::not_applied;
        if (fault == Fault::unknown_without_write) return hc::WriteOutcome::indeterminate;
        if (!objects.emplace(key, std::make_shared<const hc::Bytes>(value)).second)
            return hc::WriteOutcome::precondition_failed;
        if (fault == Fault::throw_after_write) throw hc::Error(hc::ErrorCode::io, "create response lost");
        if (fault == Fault::lost_ack) return hc::WriteOutcome::indeterminate;
        return hc::WriteOutcome::applied;
    }

    hc::WriteOutcome write_current(const hc::Bytes& value, const std::optional<std::string>& etag) override {
        check(!value.empty() && value.size() <= hc::kMaxPointerBytes, "pointer write budget");
        (void)hc::parse_pointer(std::string(value.begin(), value.end()));
        if (etag) hc::validate_etag(*etag);
        ++pointer_writes;
        conditions.push_back(etag);
        const auto fault = std::exchange(pointer_fault, Fault::none);
        auto hook = std::exchange(before_pointer, {});
        if (hook) hook();
        if (fault == Fault::not_applied) return hc::WriteOutcome::not_applied;
        if (fault == Fault::unknown_without_write) return hc::WriteOutcome::indeterminate;
        if (fault == Fault::delayed) {
            check(!delayed_, "only one delayed test request");
            delayed_ = Pending{value, etag};
            return hc::WriteOutcome::indeterminate;
        }
        const auto outcome = apply_current(value, etag);
        if (outcome != hc::WriteOutcome::applied) return outcome;
        hook = std::exchange(after_pointer, {});
        if (hook) hook();
        if (fault == Fault::throw_after_write) throw hc::Error(hc::ErrorCode::io, "pointer response lost");
        if (fault == Fault::lost_ack_unreadable) fail_current_reads = 1;
        if (fault == Fault::lost_ack || fault == Fault::lost_ack_unreadable) return hc::WriteOutcome::indeterminate;
        return outcome;
    }

    hc::WriteOutcome complete_delayed() {
        check(bool(delayed_), "no pending request");
        const auto pending = std::exchange(delayed_, std::nullopt);
        return apply_current(pending->value, pending->etag);
    }

    void retag_current() {
        check(bool(current), "retag requires current pointer");
        current->etag = next_etag();
    }

private:
    struct Pending { hc::Bytes value; std::optional<std::string> etag; };
    std::optional<Pending> delayed_;
    uint64_t token_ = 0;

    std::string next_etag() { return "\"opaque-etag/" + std::to_string(++token_) + "\""; }

    hc::WriteOutcome apply_current(const hc::Bytes& value, const std::optional<std::string>& etag) {
        if (etag ? (!current || current->etag != *etag) : bool(current))
            return hc::WriteOutcome::precondition_failed;
        current = hc::VersionedObject{value, next_etag()};
        ++pointer_mutations;
        return hc::WriteOutcome::applied;
    }
};

void committed(const hc::PublishResult& result, uint64_t seq) {
    check(result.outcome == hc::PublishOutcome::committed && result.target.publication_seq == seq,
          "publication did not commit expected sequence");
}

void object_store() {
    run_case("safe_content_addressed_keys", [] {
        const auto body = bytes(hc::serialize_manifest(manifest()));
        const auto key = manifest_key(manifest());
        check(hc::parse_immutable_key(key).sha256 == hc::sha256(body), "manifest key hash");
        const auto data_key = "data/v1/" + hc::hex(hc::sha256(body)) + ".r2b";
        check(hc::parse_immutable_key(data_key).max_bytes == hc::kMaxObjectBytes, "data key budget");
        for (const auto& bad : {"../current.json", "current.json", "data/v1/../../secret", "manifests/v1/abc.json"})
            rejects(hc::ErrorCode::invalid, [&] { (void)hc::parse_immutable_key(bad); });
        MemoryStore store;
        rejects(hc::ErrorCode::invalid, [&] { (void)hc::put_immutable(store, key, bytes("wrong")); });
        rejects(hc::ErrorCode::resource_limit, [&] { (void)hc::put_immutable(store, key, {}); });
        rejects(hc::ErrorCode::resource_limit, [&] {
            (void)hc::put_immutable(store, key, hc::Bytes(hc::kMaxMetadataBytes + 1, 0));
        });
        check(store.creates == 0, "invalid input reached storage");
    });
    run_case("strong_etag_is_opaque_not_a_digest", [] {
        hc::validate_etag("\"opaque-etag/123-not-a-hash\"");
        for (const auto& invalid : {"", "*", "W/\"weak\"", "unquoted", "\"a\",\"b\"", "\"x\r\ny\""})
            rejects(hc::ErrorCode::corrupt, [&] { hc::validate_etag(invalid); });
        rejects(hc::ErrorCode::corrupt, [] { hc::validate_etag("\"" + std::string(1024, 'x') + "\""); });
    });
    run_case("immutable_duplicate_is_verified_not_overwritten", [] {
        MemoryStore store;
        const auto body = bytes(hc::serialize_manifest(manifest()));
        const auto key = manifest_key(manifest());
        check(hc::put_immutable(store, key, body) == hc::WriteOutcome::applied, "create failed");
        check(hc::put_immutable(store, key, body) == hc::WriteOutcome::applied, "identical duplicate failed");
        store.objects.at(key) = std::make_shared<const hc::Bytes>(bytes("corrupt"));
        rejects(hc::ErrorCode::conflict, [&] { (void)hc::put_immutable(store, key, body); });
        check(*store.objects.at(key) == bytes("corrupt"), "different object was overwritten");
    });
    run_case("immutable_lost_ack_requires_full_readback", [] {
        for (auto fault : {Fault::lost_ack, Fault::throw_after_write}) {
            MemoryStore store;
            store.create_fault = fault;
            check(hc::put_immutable(store, manifest_key(manifest()), bytes(hc::serialize_manifest(manifest()))) ==
                  hc::WriteOutcome::applied, "existing identical bytes not recovered");
            check(store.creates == 1, "unexpected create retry");
        }
    });
    run_case("immutable_unknown_or_unreadable_is_not_success", [] {
        MemoryStore store;
        const auto key = manifest_key(manifest());
        const auto body = bytes(hc::serialize_manifest(manifest()));
        store.create_fault = Fault::unknown_without_write;
        check(hc::put_immutable(store, key, body) == hc::WriteOutcome::indeterminate, "missing is not definite failure");
        check(store.objects.empty() && store.creates == 1, "unknown create retried");
        store.create_fault = Fault::not_applied;
        check(hc::put_immutable(store, key, body) == hc::WriteOutcome::not_applied, "definite non-write lost");
        store.fail_object_reads = 1;
        check(hc::put_immutable(store, key, body) == hc::WriteOutcome::indeterminate, "unverified create became success");
        check(hc::put_immutable(store, key, body) == hc::WriteOutcome::applied, "explicit retry failed");
    });
    run_case("snapshot_errors_are_not_missing_current", [] {
        MemoryStore store;
        hc::ConditionalPublisher publisher(store);
        rejects(hc::ErrorCode::missing, [&] { (void)hc::load_snapshot(store); });
        committed(publisher.publish(manifest(), 0), 1);
        rejects(hc::ErrorCode::conflict, [&] { (void)hc::load_snapshot(store, 2); });
        const auto key = manifest_key(manifest());
        const auto saved = store.objects.at(key);
        store.objects.erase(key);
        rejects(hc::ErrorCode::missing, [&] { (void)publisher.publish(manifest(2), 1); });
        check(store.pointer_writes == 1, "missing manifest was treated as empty store");
        store.objects[key] = std::make_shared<const hc::Bytes>(bytes("bad"));
        rejects(hc::ErrorCode::corrupt, [&] { (void)hc::load_snapshot(store); });
        store.objects[key] = saved;
        store.current->etag = "W/\"weak\"";
        rejects(hc::ErrorCode::corrupt, [&] { (void)publisher.publish(manifest(2), 1); });
        store.current->etag = "\"valid\"";
        store.current->bytes = hc::Bytes(hc::kMaxPointerBytes + 1, 0);
        rejects(hc::ErrorCode::resource_limit, [&] { (void)hc::load_snapshot(store); });
    });
}

void conditional_publish() {
    run_case("reuse_immutable_requires_full_get_and_never_puts_on_transport_error", [] {
        MemoryStore store;
        const auto content = bytes("immutable fixture");
        const auto key = "manifests/v1/" + hc::hex(hc::sha256(content)) + ".json";
        check(hc::reuse_or_put_immutable(store, key, content) == hc::WriteOutcome::applied, "initial reusable put failed");
        check(store.creates == 1, "initial reusable create count");
        check(hc::reuse_or_put_immutable(store, key, content) == hc::WriteOutcome::applied && store.creates == 1,
              "unchanged object reuploaded");
        store.fail_object_reads = 1;
        rejects(hc::ErrorCode::io, [&] { (void)hc::reuse_or_put_immutable(store, key, content); });
        check(store.creates == 1, "network failure became write permission");
        store.objects[key] = std::make_shared<const hc::Bytes>(bytes("bad"));
        rejects(hc::ErrorCode::corrupt, [&] { (void)hc::reuse_or_put_immutable(store, key, content); });
        check(store.creates == 1, "corrupt immutable overwritten");
    });
    run_case("reference_factors_require_data_and_refresh_without_data_put", [] {
        using Json = nlohmann::json;
        Json data = {{"schema_version", 2}, {"kind", "ddb-adjustment-data-v2"},
            {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "AAPL"}, {"market", "US"},
            {"model", "futu_ab"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
            {"allow_empty", true}, {"source_epoch", "factor-fixture"},
            {"date_encoding", "exchange-civil-days-since-1970"}, {"rows", Json::array()}};
        const auto payload = bytes(data.dump());
        const auto key = "manifests/v1/" + hc::hex(hc::sha256(payload)) + ".json";
        Json ref = data;
        ref.erase("rows"); ref["kind"] = "ddb-adjustment-reference-v2";
        ref["observed_at_ms"] = 1000; ref["valid_until_ms"] = 2000;
        ref["source_proof_sha256"] = hc::hex(hc::sha256("proof"));
        ref["factor_data_sha256"] = hc::hex(hc::sha256(payload)); ref["factor_data_bytes"] = payload.size();
        MemoryStore store;
        int64_t now = 1500;
        auto publish = [&](const std::optional<hc::Pointer>& base, bool recover = false) {
            return hc::publish_factors(store, bytes(ref.dump()), "AAPL", "US", base, [&] { return now; }, recover);
        };
        rejects(hc::ErrorCode::missing, [&] { (void)publish({}); });
        check(store.pointer_writes == 0, "unresolved data became visible");
        check(hc::reuse_or_put_immutable(store, key, payload) == hc::WriteOutcome::applied, "data upload failed");
        store.pointer_fault = Fault::lost_ack;
        const auto initial = publish({});
        check(initial.outcome == hc::PublishOutcome::committed, "reference lost ACK recovery failed");
        const auto creates = store.creates;
        ref["observed_at_ms"] = 1400; ref["valid_until_ms"] = 2400;
        check(hc::reuse_or_put_immutable(store, key, payload) == hc::WriteOutcome::applied && store.creates == creates,
              "reference refresh reuploaded data");
        const auto next = publish(initial.target);
        check(next.outcome == hc::PublishOutcome::committed && store.creates == creates + 1, "refresh manifest count");
        now = 2500;
        check(publish(initial.target, true).recovered, "expired reference ACK could not recover");
        ref["valid_until_ms"] = 2600;
        check(publish(next.target, true).outcome == hc::PublishOutcome::indeterminate,
              "unknown reference recovery minted freshness");
    });
    run_case("factor_publication_cas_recovery_expiry_and_coverage", [] {
        using Json = nlohmann::json;
        Json document = {{"schema_version", 1}, {"kind", "ddb-adjustment-snapshot-v1"},
            {"algorithm", "upcloud-adjustment-v1"}, {"symbol", "000001.SZ"}, {"market", "SZ"},
            {"model", "cumulative"}, {"first_day", 1}, {"end_day", 100}, {"coverage_complete", true},
            {"allow_empty", true}, {"observed_at_ms", 1000}, {"valid_until_ms", 2000},
            {"source_epoch", "factor-fixture"}, {"source_proof_sha256", hc::hex(hc::sha256("proof"))},
            {"date_encoding", "exchange-civil-days-since-1970"}, {"rows", Json::array()}};
        MemoryStore store;
        int64_t now = 1500;
        auto publish = [&](const std::optional<hc::Pointer>& base) {
            return hc::publish_factors(store, bytes(document.dump()), "000001.SZ", "SZ", base, [&] { return now; });
        };
        store.pointer_fault = Fault::lost_ack;
        const auto initial = publish(std::nullopt);
        check(initial.outcome == hc::PublishOutcome::committed && initial.recovered, "factor lost ack not recovered");
        const auto writes = store.pointer_writes;
        check(publish(std::nullopt).recovered && store.pointer_writes == writes, "factor retry wrote twice");
        store.pointer_fault = Fault::none;
        now = 2500;
        document["observed_at_ms"] = 2400; document["valid_until_ms"] = 3400;
        check(publish(std::nullopt).outcome == hc::PublishOutcome::conflict, "factor stale base accepted");
        document["end_day"] = 90;
        rejects(hc::ErrorCode::conflict, [&] { (void)publish(initial.target); });
        document["end_day"] = 100;
        const auto next = publish(initial.target);
        check(next.outcome == hc::PublishOutcome::committed && next.target.publication_seq == 2,
              "expired factor snapshot could not be renewed");
        document["observed_at_ms"] = 2600; document["valid_until_ms"] = 3600;
        now = 2700;
        store.pointer_fault = Fault::unknown_without_write;
        check(publish(next.target).outcome == hc::PublishOutcome::indeterminate, "unknown factor write became failure");
        store.pointer_fault = Fault::none;
        const auto creates = store.creates;
        now = 3600;
        rejects(hc::ErrorCode::corrupt, [&] { (void)publish(next.target); });
        check(store.creates == creates, "expired candidate uploaded");
    });
    run_case("create_only_then_exact_etag_cas_and_idempotent_replay", [] {
        MemoryStore store;
        hc::ConditionalPublisher publisher(store);
        committed(publisher.publish(manifest(), 0), 1);
        check(!store.conditions.at(0), "initial pointer was not create-only");
        const auto etag = store.current->etag;
        const auto retry = publisher.publish(manifest(), 0);
        committed(retry, 1);
        check(retry.recovered && store.creates == 1 && store.pointer_writes == 1, "idempotent replay wrote again");
        committed(publisher.publish(manifest(2), 1), 2);
        check(store.conditions.at(1) == etag, "CAS did not use exact GET ETag");
        check(hc::load_snapshot(store, 2).manifest.entries[0].source_version == 2, "new snapshot not readable");
    });
    run_case("pointer_ack_loss_is_resolved_without_duplicate_publication", [] {
        for (auto fault : {Fault::lost_ack, Fault::throw_after_write}) {
            MemoryStore store;
            store.pointer_fault = fault;
            hc::ConditionalPublisher publisher(store);
            const auto result = publisher.publish(manifest(), 0);
            committed(result, 1);
            check(result.recovered && store.pointer_mutations == 1 && store.pointer_writes == 1,
                  "lost ACK duplicated pointer commit");
            committed(hc::ConditionalPublisher(store).publish(manifest(), 0), 1);
            check(store.pointer_writes == 1, "publisher reconstruction duplicated commit");
        }
    });
    run_case("ack_loss_and_unreadable_pointer_remain_indeterminate", [] {
        MemoryStore store;
        store.pointer_fault = Fault::lost_ack_unreadable;
        check(hc::ConditionalPublisher(store).publish(manifest(), 0).outcome == hc::PublishOutcome::indeterminate,
              "readback failure was reported as non-application");
        check(store.pointer_mutations == 1 && store.pointer_writes == 1, "unknown result was retried");
        const auto recovered = hc::ConditionalPublisher(store).publish(manifest(), 0);
        committed(recovered, 1);
        check(recovered.recovered && store.pointer_writes == 1, "restart failed to recognize target");
    });
    run_case("unchanged_pointer_cannot_disprove_inflight_write", [] {
        for (const bool initialized : {false, true}) {
            MemoryStore store;
            hc::ConditionalPublisher publisher(store);
            const uint64_t expected = initialized ? 1 : 0;
            if (initialized) committed(publisher.publish(manifest(), 0), 1);
            store.pointer_fault = Fault::unknown_without_write;
            const auto writes = store.pointer_writes;
            check(publisher.publish(manifest(2), expected).outcome == hc::PublishOutcome::indeterminate,
                  "unchanged GET incorrectly ruled out a pending write");
            check(store.pointer_writes == writes + 1 && store.pointer_mutations == expected, "implicit retry");
            committed(publisher.publish(manifest(2), expected), expected + 1);
        }
    });
    run_case("definite_pre_dispatch_failure_does_not_publish", [] {
        MemoryStore store;
        store.pointer_fault = Fault::not_applied;
        hc::ConditionalPublisher publisher(store);
        check(publisher.publish(manifest(), 0).outcome == hc::PublishOutcome::not_applied, "known failure not preserved");
        check(!store.current && store.pointer_writes == 1 && store.current_reads == 1, "failed write changed pointer");
        committed(publisher.publish(manifest(), 0), 1);
    });
    run_case("manifest_failure_never_reaches_pointer", [] {
        for (auto fault : {Fault::not_applied, Fault::unknown_without_write}) {
            MemoryStore store;
            store.create_fault = fault;
            const auto expected = fault == Fault::not_applied ? hc::PublishOutcome::not_applied : hc::PublishOutcome::indeterminate;
            check(hc::ConditionalPublisher(store).publish(manifest(), 0).outcome == expected, "manifest failure changed outcome");
            check(!store.current && store.pointer_writes == 0 && store.creates == 1, "unverified manifest became visible");
        }
        MemoryStore store;
        store.fail_object_reads = 1;
        check(hc::ConditionalPublisher(store).publish(manifest(), 0).outcome == hc::PublishOutcome::indeterminate,
              "manifest verification failure ignored");
        check(store.pointer_writes == 0, "pointer written before manifest verification");
        committed(hc::ConditionalPublisher(store).publish(manifest(), 0), 1);
    });
    run_case("manifest_lost_ack_is_verified_before_pointer", [] {
        MemoryStore store;
        store.create_fault = Fault::lost_ack;
        committed(hc::ConditionalPublisher(store).publish(manifest(), 0), 1);
        check(store.creates == 1 && store.pointer_mutations == 1, "manifest recovery duplicated write");
    });
    run_case("delayed_write_can_commit_after_unknown_result", [] {
        MemoryStore store;
        store.pointer_fault = Fault::delayed;
        hc::ConditionalPublisher publisher(store);
        check(publisher.publish(manifest(), 0).outcome == hc::PublishOutcome::indeterminate, "delayed write not unknown");
        check(!store.current, "delayed write already applied");
        check(store.complete_delayed() == hc::WriteOutcome::applied, "inflight write cannot complete");
        committed(publisher.publish(manifest(), 0), 1);
        check(store.pointer_mutations == 1 && store.pointer_writes == 1, "late ACK recovery advanced twice");
    });
    run_case("explicit_same_attempt_retry_fences_delayed_duplicate", [] {
        MemoryStore store;
        store.pointer_fault = Fault::delayed;
        hc::ConditionalPublisher publisher(store);
        check(publisher.publish(manifest(), 0).outcome == hc::PublishOutcome::indeterminate, "pending result");
        committed(publisher.publish(manifest(), 0), 1);
        check(store.complete_delayed() == hc::WriteOutcome::precondition_failed, "late create-only overwrote current");
        check(store.pointer_mutations == 1 && hc::load_snapshot(store).pointer.publication_seq == 1, "retry advanced seq");
    });
    run_case("new_writer_fences_delayed_old_etag", [] {
        MemoryStore store;
        hc::ConditionalPublisher old_writer(store), new_writer(store);
        committed(old_writer.publish(manifest(), 0), 1);
        store.pointer_fault = Fault::delayed;
        check(old_writer.publish(manifest(2), 1).outcome == hc::PublishOutcome::indeterminate, "old write not pending");
        committed(new_writer.publish(manifest(3), 1), 2);
        check(store.complete_delayed() == hc::WriteOutcome::precondition_failed, "old inflight write overwrote winner");
        check(hc::load_snapshot(store).manifest.entries[0].source_version == 3, "source version decreased");
    });
    run_case("interleaved_competing_candidate_loses_cas", [] {
        MemoryStore store;
        hc::ConditionalPublisher old_writer(store), new_writer(store);
        committed(old_writer.publish(manifest(), 0), 1);
        const auto etag = store.current->etag;
        store.before_pointer = [&] { committed(new_writer.publish(manifest(3), 1), 2); };
        check(old_writer.publish(manifest(2), 1).outcome == hc::PublishOutcome::conflict, "stale writer did not conflict");
        check(store.conditions.at(1) == etag && store.pointer_mutations == 2, "stale ETag was refreshed automatically");
        const auto latest = hc::load_snapshot(store);
        check(latest.pointer.publication_seq == 2 && latest.manifest.entries[0].source_version == 3, "winner overwritten");
        rejects(hc::ErrorCode::conflict, [&] { (void)old_writer.publish(manifest(2), 2); });
        check(store.pointer_mutations == 2, "rebased stale source was published");
    });
    run_case("identical_competing_candidate_recovers_same_target", [] {
        MemoryStore store;
        hc::ConditionalPublisher a(store), b(store);
        store.before_pointer = [&] { committed(b.publish(manifest(), 0), 1); };
        const auto result = a.publish(manifest(), 0);
        committed(result, 1);
        check(result.recovered && store.pointer_mutations == 1, "identical target was committed twice");
    });
    run_case("same_numeric_sequence_is_not_an_etag_condition", [] {
        MemoryStore store;
        hc::ConditionalPublisher publisher(store);
        committed(publisher.publish(manifest(), 0), 1);
        store.before_pointer = [&] { store.retag_current(); };
        check(publisher.publish(manifest(2), 1).outcome == hc::PublishOutcome::conflict, "sequence-only CAS accepted");
        check(store.pointer_mutations == 1, "retagged pointer overwritten with stale ETag");
        committed(publisher.publish(manifest(2), 1), 2);
    });
    run_case("lost_ack_then_superseded_does_not_claim_non_application", [] {
        MemoryStore store;
        hc::ConditionalPublisher a(store), b(store);
        store.pointer_fault = Fault::lost_ack;
        store.after_pointer = [&] { committed(b.publish(manifest(2), 1), 2); };
        check(a.publish(manifest(), 0).outcome == hc::PublishOutcome::indeterminate,
              "newer pointer was incorrectly treated as proof of failure");
        check(store.pointer_mutations == 2, "setup did not include supersession");
        check(a.publish(manifest(), 0).outcome == hc::PublishOutcome::conflict, "old retry silently rebased");
        check(store.pointer_mutations == 2, "superseded retry wrote again");
    });
    run_case("source_epoch_sequence_and_manifest_guards", [] {
        MemoryStore store;
        hc::ConditionalPublisher publisher(store);
        committed(publisher.publish(manifest(2), 0), 1);
        check(publisher.publish(manifest(3), 0).outcome == hc::PublishOutcome::conflict, "stale sequence accepted");
        rejects(hc::ErrorCode::conflict, [&] { (void)publisher.publish(manifest(), 1); });
        auto next = manifest(2);
        next.entries[0].data_version = hc::sha256(std::string("changed"));
        rejects(hc::ErrorCode::conflict, [&] { (void)publisher.publish(next, 1); });
        next = manifest(2); next.entries[0].coverage.end_ms++;
        rejects(hc::ErrorCode::conflict, [&] { (void)publisher.publish(next, 1); });
        next = manifest(3); next.dataset_epoch = "other-epoch";
        rejects(hc::ErrorCode::conflict, [&] { (void)publisher.publish(next, 1); });
        next = manifest(3); next.entries[0].row_count = 1;
        rejects(hc::ErrorCode::invalid, [&] { (void)publisher.publish(next, 1); });
        rejects(hc::ErrorCode::resource_limit, [&] { (void)publisher.publish(manifest(3), UINT64_MAX); });
        check(store.creates == 1 && store.pointer_writes == 1, "rejected candidate wrote storage");
        store.fail_current_reads = 1;
        rejects(hc::ErrorCode::io, [&] { (void)publisher.publish(manifest(3), 1); });
        check(store.pointer_writes == 1, "read failure treated as absence");
        store.current.reset();
        check(publisher.publish(manifest(3), 1).outcome == hc::PublishOutcome::conflict, "missing pointer reset seq");
    });
}

class PackFixture {
public:
    PackFixture() {
        auto pattern = (fs::temp_directory_path() / "history-object-test-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        const auto* directory = ::mkdtemp(buffer.data());
        if (!directory) throw std::runtime_error("mkdtemp failed");
        root_ = directory;
        for (int64_t i = 0; i < 2050; ++i) {
            const auto price = static_cast<float>(i % 100) * 0.125F;
            rows.push_back({1000 + i * 60000, price, price + 2, price - 1, price + 1, i});
        }
        candidate = manifest();
        auto& entry = candidate.entries[0];
        entry.coverage.end_ms = rows.back().timestamp_ms + 60000;
        entry.row_count = rows.size();
        const auto path = root_ / "fixture.r2b";
        entry.pack = hc::write_pack(path, hc::pack_metadata(entry), [&](uint64_t offset, uint32_t count) {
            return std::vector<hc::Row>(rows.begin() + static_cast<std::ptrdiff_t>(offset),
                                      rows.begin() + static_cast<std::ptrdiff_t>(offset + count));
        });
        data = hc::read_file(path, hc::kMaxObjectBytes);
    }
    ~PackFixture() { std::error_code ignored; fs::remove_all(root_, ignored); }
    void upload(MemoryStore& store) const {
        check(hc::put_immutable(store, candidate.entries[0].pack->key, data) == hc::WriteOutcome::applied, "pack upload");
    }
    std::vector<hc::Row> rows;
    hc::Manifest candidate;
    hc::Bytes data;
private:
    fs::path root_;
};

void object_reader() {
    run_case("memory_objects_publish_and_pinned_range_read_end_to_end", [] {
        PackFixture fixture;
        MemoryStore store;
        fixture.upload(store);
        store.data_gets = 0;
        hc::ConditionalPublisher publisher(store);
        committed(publisher.publish(fixture.candidate, 0), 1);
        check(store.range_bytes == fixture.data.size() && store.data_gets == 0,
              "publication verification was not one bounded range pass");
        const auto snapshot = std::make_shared<const hc::Snapshot>(hc::load_snapshot(store));
        hc::Query query{identity(), version(), {fixture.rows[1000].timestamp_ms, fixture.rows[1100].timestamp_ms}, 100};
        const auto plan = hc::plan_query(snapshot, query, true);
        std::vector<hc::Row> received;
        const auto result = hc::read_plan(store, plan, [&](const std::vector<hc::Row>& batch) {
            received.insert(received.end(), batch.begin(), batch.end());
        });
        check(result.rows == 100 && received.size() == 100, "range row count mismatch");
        for (size_t i = 0; i < received.size(); ++i)
            check(hc::same_row(received[i], fixture.rows[1000 + i]), "range row bit mismatch");
        query.max_count = 3;
        check(hc::read_plan(store, hc::plan_query(snapshot, query, true)).last_ms == fixture.rows[1002].timestamp_ms,
              "max_count is not earliest rows");
        auto empty = fixture.candidate;
        empty.entries[0].row_count = 0; empty.entries[0].pack.reset(); empty.entries[0].source_version = 2;
        committed(publisher.publish(empty, 1), 2);
        check(hc::read_plan(store, plan).rows == 100 && plan.snapshot->pointer.publication_seq == 1,
              "inflight plan switched snapshot");
        const auto latest = std::make_shared<const hc::Snapshot>(hc::load_snapshot(store));
        check(hc::read_plan(store, hc::plan_query(latest, query, true)).rows == 0, "confirmed empty not hit");
        query.range.end_ms = empty.entries[0].coverage.end_ms + 1;
        check(hc::plan_query(latest, query, true).result == hc::PlanResult::miss, "unknown suffix falsely hit");
        check(store.data_gets == 0 && store.largest_range <= hc::kMaxBlockBytes, "reader used unbounded pack GET");
    });
    run_case("missing_or_corrupt_pack_cannot_publish", [] {
        PackFixture fixture;
        MemoryStore store;
        hc::ConditionalPublisher publisher(store);
        rejects(hc::ErrorCode::missing, [&] { (void)publisher.publish(fixture.candidate, 0); });
        check(store.creates == 0 && store.pointer_writes == 0, "missing pack became visible");
        fixture.upload(store);
        store.bad_range_offset = 0;
        rejects(hc::ErrorCode::corrupt, [&] { (void)publisher.publish(fixture.candidate, 0); });
        store.bad_range_offset.reset();
        const auto original_key = fixture.candidate.entries[0].pack->key;
        auto wrong = fixture.candidate;
        wrong.entries[0].pack->sha256 = hc::sha256(std::string("wrong-whole-object-hash"));
        wrong.entries[0].pack->key = "data/v1/" + hc::hex(wrong.entries[0].pack->sha256) + ".r2b";
        store.objects[wrong.entries[0].pack->key] = store.objects.at(original_key);
        rejects(hc::ErrorCode::corrupt, [&] { (void)publisher.publish(wrong, 0); });
        check(store.pointer_writes == 0 && store.creates == 1, "whole-object hash was not checked before manifest");
        store.objects.at(original_key) = std::make_shared<const hc::Bytes>(bytes("short"));
        rejects(hc::ErrorCode::corrupt, [&] { (void)publisher.publish(fixture.candidate, 0); });
    });
    run_case("short_range_and_midstream_corruption_fail_entire_read", [] {
        PackFixture fixture;
        MemoryStore store;
        fixture.upload(store);
        committed(hc::ConditionalPublisher(store).publish(fixture.candidate, 0), 1);
        const auto& entry = fixture.candidate.entries[0];
        const auto index = hc::read_pack_index(store.open_range(entry.pack->key, entry.pack->bytes),
                                               *entry.pack, hc::pack_metadata(entry));
        const auto snapshot = std::make_shared<const hc::Snapshot>(hc::load_snapshot(store));
        const hc::Query query{identity(), version(), entry.coverage, hc::kMaxRows};
        const auto plan = hc::plan_query(snapshot, query, true);
        for (const bool short_read : {false, true}) {
            store.bad_range_offset = index.blocks.at(1).offset;
            store.short_range = short_read;
            size_t provisional = 0;
            rejects(hc::ErrorCode::corrupt, [&] {
                (void)hc::read_plan(store, plan, [&](const std::vector<hc::Row>& batch) { provisional += batch.size(); });
            });
            check(provisional == hc::kBlockRows, "unexpected provisional prefix after failure");
        }
    });
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::runtime_error("one suite argument required");
        const std::string suite = argv[1];
        if (suite == "object_store") object_store();
        else if (suite == "conditional_publish") conditional_publish();
        else if (suite == "object_reader") object_reader();
        else throw std::runtime_error("unknown suite");
        std::cout << "PASS " << suite << " cases=" << cases << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
