#include "history_cache/factor_publisher.h"

namespace history_cache {
namespace {
bool same(const Pointer& a, const Pointer& b) {
    return serialize_pointer(a) == serialize_pointer(b);
}
PublishOutcome outcome(WriteOutcome value) {
    if (value == WriteOutcome::applied) return PublishOutcome::committed;
    if (value == WriteOutcome::not_applied) return PublishOutcome::not_applied;
    if (value == WriteOutcome::precondition_failed) return PublishOutcome::conflict;
    return PublishOutcome::indeterminate;
}
}  // namespace

PublishResult publish_factors(ObjectStore& store, const Bytes& bytes,
    const std::string& symbol, const std::string& market,
    const std::optional<Pointer>& expected_base, const std::function<int64_t()>& now_ms, bool recover_only) {
    const auto hash = sha256(bytes);
    const auto candidate = parse_factor_snapshot(bytes, hash, symbol, market, now_ms(), FactorReadPolicy::structural_only);
    const auto seq = expected_base ? expected_base->publication_seq : 0;
    if (seq == UINT64_MAX) throw Error(ErrorCode::resource_limit, "factor sequence exhausted");
    const Pointer target{candidate.source_epoch, seq + 1, "manifests/v1/" + hex(hash) + ".json", hash};
    (void)serialize_pointer(target);
    if (expected_base) (void)serialize_pointer(*expected_base);
    const auto current = store.read_current();
    std::optional<Pointer> observed;
    if (current) {
        validate_etag(current->etag);
        observed = parse_pointer(std::string(current->bytes.begin(), current->bytes.end()));
        if (same(*observed, target)) {
            if (store.get(target.manifest_key, kMaxFactorBytes) != bytes)
                throw Error(ErrorCode::corrupt, "factor recovery content mismatch");
            if (candidate.data_hash != Digest{}) {
                const auto data = store.get("manifests/v1/" + hex(candidate.data_hash) + ".json", candidate.data_bytes);
                (void)resolve_factor_snapshot(bytes, hash, data, symbol, market, now_ms(), FactorReadPolicy::structural_only);
            }
            return {PublishOutcome::committed, target, true};
        }
    }
    // An expired target may still be the one committed before a lost ACK.
    // Read-only recovery must never refresh its TTL or adopt a newer base.
    if (recover_only) return {PublishOutcome::indeterminate, target, false};
    const auto now = now_ms();
    if (now < candidate.observed_at_ms || now >= candidate.valid_until_ms)
        throw Error(ErrorCode::corrupt, "factor candidate expired or future-dated");
    if (bool(observed) != bool(expected_base) || (observed && !same(*observed, *expected_base)))
        return {PublishOutcome::conflict, target, false};
    if (observed) {
        const auto old_bytes = store.get(observed->manifest_key, kMaxFactorBytes);
        // Expired snapshots can be replaced. Validate their original interval,
        // rather than treating expiry as permission to reset the publication.
        const auto old = parse_factor_snapshot(old_bytes, observed->manifest_sha256, symbol, market,
            candidate.observed_at_ms, FactorReadPolicy::structural_only);
        if (old.source_epoch != observed->dataset_epoch || old.source_epoch != candidate.source_epoch ||
            old.factors.model != candidate.factors.model ||
            candidate.factors.first_day > old.factors.first_day || candidate.factors.end_day < old.factors.end_day ||
            candidate.observed_at_ms < old.observed_at_ms || candidate.valid_until_ms < old.valid_until_ms)
            throw Error(ErrorCode::conflict, "factor publication regresses identity, coverage or freshness");
        if (old.source_revision && (candidate.source_revision < old.source_revision ||
            (candidate.source_revision == old.source_revision &&
             (candidate.source_receipt_hash != old.source_receipt_hash || candidate.data_hash != old.data_hash))))
            throw Error(ErrorCode::conflict, "factor source revision regresses or changes at same revision");
    }
    if (candidate.data_hash != Digest{}) {
        const auto data = store.get("manifests/v1/" + hex(candidate.data_hash) + ".json", candidate.data_bytes);
        (void)resolve_factor_snapshot(bytes, hash, data, symbol, market, now_ms());
    }
    const auto stored = put_immutable(store, target.manifest_key, bytes);
    if (stored != WriteOutcome::applied) return {outcome(stored), target, false};
    if (now_ms() >= candidate.valid_until_ms)
        throw Error(ErrorCode::resource_limit, "factor expired before pointer dispatch");
    const auto text = serialize_pointer(target);
    WriteOutcome written;
    try {
        written = store.write_current(Bytes(text.begin(), text.end()),
            current ? std::optional<std::string>(current->etag) : std::nullopt);
    } catch (const Error& error) {
        if (error.code() != ErrorCode::io) throw;
        written = WriteOutcome::indeterminate;
    }
    if (written == WriteOutcome::applied || written == WriteOutcome::not_applied)
        return {outcome(written), target, false};
    try {
        const auto recovered = store.read_current();
        if (recovered) {
            validate_etag(recovered->etag);
            if (same(parse_pointer(std::string(recovered->bytes.begin(), recovered->bytes.end())), target))
                return {PublishOutcome::committed, target, true};
        }
    } catch (const Error&) { return {PublishOutcome::indeterminate, target, false}; }
    return {outcome(written), target, false};
}
}  // namespace history_cache
