# Reviewed suspension policy extensions

The `install-policy` Actions operation remains create-only by default. To extend
an installed policy, set `policy_previous_sha256` to its exact current hash and
provide the normal pinned policy release asset and new `policy_sha256`.

The operation requires an ACCEPTED or ROLLED_BACK deployment journal. It retains
every old interval and its evidence verbatim; removal, changed evidence, duplicate
entries, and mismatched previous hashes are rejected. No Cloud operation, R2
request, or running container mutation occurs during policy installation.

Old and new JSON/pin pairs are fsynced under
`/etc/history-cache/policy-upgrades/<old>-<new>/` before replacing source files.
An identical retry resumes an interrupted pair. Other source changes fail closed.
The running Agent reads its immutable release snapshot, so it is unaffected by a
partial source pair. Preflight/deploy refuse an inconsistent pair.

After installation, run normal preflight and journaled Agent deployment to load
the extended policy. Verify UDS policy hash/count before publishing new nullable
data. A normal deployment rollback restores the previous Agent and its policy
snapshot; source backups remain available for an explicitly reviewed restoration.
Do not edit the immutable running snapshot or infer activation from install success.
