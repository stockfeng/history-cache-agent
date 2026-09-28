#pragma once

#include "history_cache/publisher.h"

#include <memory>

namespace history_cache {

enum class JournalStep {
    before_intent_sync, before_attempt_sync, before_dispatch_sync, attempt_durable, before_result_sync, result_durable
};
using JournalHook = std::function<void(JournalStep)>;

// One owner-only directory per candidate. No reset, automatic rebase or log GC.
// The lock is local only. The remote pointer still requires exact ETag CAS.
class JournaledPublisher {
public:
    static void prepare(const std::filesystem::path& root, const Digest& scope,
                        const Manifest& candidate, uint64_t expected_seq, const JournalHook& hook = {});
    JournaledPublisher(const std::filesystem::path& root, ObjectStore& store);
    ~JournaledPublisher();
    JournaledPublisher(const JournaledPublisher&) = delete;
    JournaledPublisher& operator=(const JournaledPublisher&) = delete;

    // Reads the original candidate/sequence from the journal, not caller memory.
    // A local checkpoint failure after remote ACK is returned as indeterminate.
    [[nodiscard]] PublishResult resume(const JournalHook& hook = {});
    Pointer target() const;
    bool unresolved() const;

private:
    struct State;
    std::unique_ptr<State> state_;
};

}  // namespace history_cache
