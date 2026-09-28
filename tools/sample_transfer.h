#pragma once

#include "history_cache/object_store.h"

namespace history_cache::sample {

struct Receipt {
    bool already_present = false;
    std::optional<WriteOutcome> write;
    Bytes readback;
};

// Only immutable, content-addressed bytes are resumable this way. This is not
// a mutable-pointer retry or proof of ownership of an existing object.
inline Receipt ensure_object(ObjectStore& store, const std::string& key, const Bytes& bytes) {
    const auto reference = parse_immutable_key(key);
    if (bytes.empty() || bytes.size() > reference.max_bytes || sha256(bytes) != reference.sha256)
        throw Error(ErrorCode::invalid, "invalid content-addressed transfer");
    Receipt receipt;
    try {
        receipt.readback = store.get(key, bytes.size());
        receipt.already_present = true;
    } catch (const Error& error) {
        if (error.code() != ErrorCode::missing) throw;
        try {
            receipt.write = store.create(key, bytes);
        } catch (const Error& write_error) {
            if (write_error.code() != ErrorCode::io) throw;
            receipt.write = WriteOutcome::indeterminate;
        }
        // One authoritative read resolves an ACK loss or a racing identical
        // create. Failure stops this invocation; there is no retry loop.
        receipt.readback = store.get(key, bytes.size());
    }
    if (receipt.readback != bytes)
        throw Error(ErrorCode::conflict, "immutable readback differs from pinned bytes");
    return receipt;
}

}  // namespace history_cache::sample
