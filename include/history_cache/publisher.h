#pragma once

#include "history_cache/catalog.h"
#include "history_cache/object_store.h"

namespace history_cache {

enum class PublishOutcome { committed, not_applied, conflict, indeterminate };

struct PublishResult {
    // committed means the exact target was committed/observed, not that it is still
    // current. Other outcomes cannot resolve an earlier indeterminate invocation.
    PublishOutcome outcome = PublishOutcome::indeterminate;
    Pointer target;
    bool recovered = false;
    std::optional<ErrorCode> error_code = std::nullopt;
};

Snapshot load_snapshot(const ObjectStore& store, uint64_t min_publication_seq = 0);

class ConditionalPublisher {
public:
    explicit ConditionalPublisher(ObjectStore& store) : store_(store) {}

    // Retry with the SAME manifest and expected_seq, retained by the caller before
    // dispatch. Persist that intent outside this class for process-crash recovery.
    // Never adopt a newer sequence to resolve an indeterminate result.
    // No pointer deletion/epoch reset is supported in this namespace.
    // epoch_base is an explicit migration authorization, pinned in the journal.
    [[nodiscard]] PublishResult publish(const Manifest& manifest, uint64_t expected_seq,
                                        const std::optional<Pointer>& epoch_base = std::nullopt);

private:
    ObjectStore& store_;
};

}  // namespace history_cache
