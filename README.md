# history-cache

C++17 core and UDS agent for the R2 historical-cache project. This is an
independent Git repository inside the integration workspace. Building or testing
the component does not deploy services or publish data.

## Factor versions versus source checks, 2026-10-06

An already-published, hash-verified factor version does not expire with wall time.
The legacy manifest field `valid_until_ms` remains unchanged on disk and denotes
the next source-check deadline. Agent reads use `FactorReadPolicy::published_version`:
future observations, malformed proofs, missing data, rollback and coverage errors
still fail, but a missed check alone does not disable query or `adjust_rows`.
The normal demand-driven current-pointer TTL and all resource limits are unchanged;
network/validation errors never silently substitute an old version after refresh fails.

Internal UDS replies explicitly declare `factor_freshness_policy=versioned-v1`,
`factor_observed_at_ms`, `factor_next_check_ms`, `factor_evaluated_at_ms` and
`factor_check_status=current|overdue`. The old deadline field is retained without
extension. Compact v2/v3 reads both identify the immutable data hash, so a check-only
publication does not change the data version. No persisted object/schema rewrite.

Deploy the matching Cloud UDS validator before enabling adjusted client traffic.
Old Cloud cannot interpret the new overdue-result policy. Old Agent responses
retain their original strict contract in the upgraded Cloud; SDK bytes do not change.
"Overdue" explicitly means latest-source synchronization is unconfirmed, not proof
that corporate-action coverage is up to date. It is visible in Cloud's served log.

New publication dispatch still requires a timely source observation and CAS/journal
validation. Keeping that write-time guard prevents an abandoned old capture from
replacing a newer publication; it is not an expiration policy for stored factors.
Existing immutable-object reuse means unchanged factor data is not uploaded again.

## Native composite adjustment, 2026-10-02

With `--adjustment yes`, a trusted same-host gateway can send `ADJUST64\n`
followed by one JSON `op=adjust_rows` request. The frame is bounded to
5000 native64 rows plus 8 KiB metadata; ordinary requests remain limited to
8 KiB. It includes the original UTC half-open range, mode, `input_adjust=none`,
raw row hash, `prefix_rows`, `prefix_end_ms`, and an optional same-host
`deadline_mono_ms`. One verified factor version adjusts the combined raw result;
the cumulative forward anchor uses the final returned row, not the prefix.
Only prices change. The response is marked `gateway-raw-composite-v1`, not
an R2 coverage proof or an atomic DDB/R2 snapshot. No DDB credentials are
installed in the agent. Ordinary adjusted PARTIAL requests still fail closed.

The paired gateway uses a bounded internal native64 upstream tail channel,
then projects the final result to the unchanged SDK 48-byte wire format.
No worker, realtime queue or cache capacity is increased. Deployment and
2000-client resource acceptance remain separate gates.

## Data-side publisher connection reuse, 2026-09-30

The snapshot CLI now opts into a separate publication reuse policy: verified,
fully received GET 2xx/404 and PUT 200 responses may keep the connection.
Agent read-only reuse behavior and legacy sample/staging tools are unchanged.
TLS/transport errors and 412 responses discard the handle; conditional writes,
readback validation, journals and request/byte/deadline budgets are unchanged.
No application retry loop is added. Reuse is per CLI process, not across timer ticks.
Snapshot diagnostics include numeric `curl_code` and `tls_verify_result`, without
URLs, credentials or raw error strings. A zero verify result alone does not prove
TLS success; the delivery and failure fields still apply.

Integration loopback-TLS tests measured 18 requests / 18 new connections before,
versus 18 / 1 after, with identical native64 rows. They also cover signed header
reset, conditional replacement/conflict, uncertain write readback, journal recovery
and CA rejection. Full curl-ON CTest: 74/74. No production or live pilot binary
is replaced by this source change; external-R2 stability is not yet measured.

## DDB-native schema, 2026-09-29

The selected archive contract is now `le-ddb-native64-v1`: timestamp, DOUBLE
OHLC, LONG volume/open_oi/close_oi (`<q4d3q`, 64 bytes). No amount is required.
Integration tools expose `--native-kline`; dataset `ddb-history-native64`, pack
row schema 3, and `-native64-001` namespaces isolate it from older formats.
Agent and Cloud internal UDS reads preserve all fields. Cloud prefer explicitly
projects native rows to the existing 48-byte SDK response, matching upcloud's
OHLCV history projection (float32 prices, zero turnover/open_interest). No SDK
upgrade is required; native bytes must never be copied directly to legacy wire rows.

## Complete historical rows, 2026-09-29

UDS queries may request `row_encoding=le-kline48-v1` (48-byte canonical
timestamp/OHLC/volume/turnover/open-interest rows). This uses pack row schema 2,
dataset `ddb-history-kline48`, and a separate `-kline48-001` namespace. Legacy
OHLCV archives cannot satisfy complete-row queries. The snapshot publisher now
supports schema 2 and requires an isolated `-kline48-` namespace. Integration
exporters offer explicit `--complete-kline`; missing source amount/open_oi fails
closed. Existing saved DDB schemas lack amount, so no live complete-row archive
has been published by this migration.
Cloud's opt-in `HISTORY_CACHE_MODE=prefer` can return validated complete rows;
default remains shadow. No deployment is implied by these source changes.

## Realtime-first maintenance, 2026-09-29

Correction: default foreground now fetches R2 on local miss/expiry. Cold reads
are serialized with deadline-bounded waiting and 512 KiB/s receive pacing;
foreground demand cancels background work. Cache-only is opt-in diagnostic mode.
See the latest section of `deploy/README.md`; the following defaults are historical.

Defaults now favor realtime isolation: cache-only foreground, two workers/four
pending sockets, 32 MiB pack cache. Optional explicit warm plans use one paced,
budgeted worker and a renewable same-host idle lease; without a valid lease no
background HTTP starts. Matching Cloud integration permits maintenance only
with zero connected clients. See `deploy/README.md` for opt-in/configuration,
resource templates, diagnostic compatibility and remaining production gates.
The older 64 MiB/on-demand defaults below are historical measurements.

## Agent read performance, 2026-09-29

The agent now reuses verified HTTPS GET connections, coalesces small immutable
packs into one GET, and retains validated compressed packs in a bounded 64 MiB /
256-entry LRU. Real staging HK/US samples remain byte-identical; warm queries
issue zero HTTP requests. Cold queries can still exceed the Cloud shadow deadline.
See `deploy/README.md` for limits, diagnostic switches, numeric timing metrics and
freshness boundaries. This does not enable client cache-serving or deploy images.

## Agent and shadow hardening, 2026-09-28

Follow-up: the agent now requires explicit UDS protocol v1 with UTC-instant
milliseconds and half-open ranges. US legacy ET-wall packs are translated using
read-only IANA New York TZif transitions; returned rows and their SHA-256 are UTC.
Cloud verifies the protocol, hash, row times and OHLCV. Old unversioned clients
are rejected; upgrade the pair together. Runtime `tzdata` is required; ambiguous,
nonexistent or out-of-transition-range US times fail closed. See the parent
`docs/R2-AGENT-UTC-CONTRACT-20260928.md` for 74/74 core, 4/4 Gateway and 14-vector
offline cross-component evidence. Upcloud/SDK US time semantics still need work;
this is not live upstream parity or full-hit deployment acceptance.

`history-cache-agent` is a long-running, read-only UDS service. Each query now
owns fresh S3 stores and one aggregate deadline/request/download budget, rather
than reusing a store whose original 30-second deadline has expired. Local budget,
deadline, DNS, connect and TLS failures are distinguished. Bounded read retries
do not change the conditional publisher's write/recovery contract.

The service binds each pack to its source namespace, caches owning immutable
snapshots, and bounds rows, frame sizes, workers, pending connections and cache
entries. A complete empty range is a HIT; read errors are not coverage MISSes.
The companion Cloud gateway uses an asynchronous, bounded shadow queue after
authentication, market authorization and history admission. It still returns
all real responses from upstream; full-hit routing is not implemented.

Local validation: Debug curl-OFF CTest **74/74**, including 101 queries with a
61-second idle interval, cross-month synthetic reads, concurrent TTL refresh and
UDS lifecycle checks. The related Gateway suite passed **4/4**. Targeted agent
and transport tests passed **4/4** with curl ON and **4/4** with ASan+UBSan; these
are not full sanitizer acceptance or real R2/network tests. No deployment or push
was performed for this change.

Still open: futures trading-day/month routing and multi-partition proofs, US
wall-clock/UTC semantics, exporter/batch regressions, upstream result comparison,
full-hit/SDK integration and production deployment gates. A synthetic cross-month
test does not establish compatible source versions across real DDB partitions.
See `docs/R2-AGENT-HARDENING-20260928.md` in the parent integration workspace.
The dated sections below describe earlier milestones, not current acceptance.

## Native-version snapshot catalog, 2026-09-22

`history-cache-snapshot` consumes the integration exporter's bounded single-chunk
proof, writes an R2H1 pack and standard catalog, and reuses the journaled conditional
publisher and full-coverage reader. It never upgrades the older observations.
One real 000001.SZ two-day snapshot (480 rows, native chunk version 7) was published
in a new staging namespace: 19 requests, four objects, 8360 bytes. Full readback,
independent raw-value/pack decoding, earliest-three-row query and out-of-coverage
MISS passed. Explicit metadata-only revalidation also passed before and after.

This is complete **database-visible data at a pinned single-partition version**,
not upstream finality or live freshness. The exporter rejects changing/inflight
metadata and multiple partitions. Epochs bind source and physical chunk identity;
version is the native counter, not a clock/hash. Revalidation does not withdraw a
catalog automatically. Source restart/restore ABA, cross-partition version domains,
batch snapshot catalogs, ongoing invalidation and production integration remain open.
The CLI only first-publishes to a new namespace; retain its journal on uncertainty.
It has no automatic retry, delete, update or explicit resume command.

Evidence/runbook: `docs/R2-M2-SOURCE-SNAPSHOT.md` and
`artifacts/r2/20260922-m2-snapshot-live-001/README.md` in the parent workspace.
Release CTest includes five snapshot roundtrips and fourteen negative checks.
Native guard: 12/12; original exporter: 6/6; batch orchestration: 12/12.
After the live run, validation was tightened to bind identity/range/limit to the
original query hash. The saved live source still passes independent and local
reader validation; live and final binaries are retained separately in evidence.
The new CLI passes curl-OFF syntax compilation, not a new full curl-OFF or
sanitizer acceptance run. Production services/configuration were not changed.

## Resumable real-data batches, 2026-09-22

`history-cache-sample sync` reads each immutable content-addressed object before
creating it, then performs exact full readback. Existing identical bytes are
reused without claiming ownership; only verified NoSuchKey permits a create-only
PUT. One read can resolve an ambiguous ACK, but there are no retry loops, current
pointer writes or deletes. The legacy `upload` behavior is unchanged.

The integration `scripts/r2/run_ddb_batch.py` now pins explicit stock/date batches,
source artifacts and code, checkpoints attempts before dispatch, inherits a
single-directory lock into children and resumes completed stages. Six real
batches (3 stocks, 2 days, 1440 rows) were uploaded in two separate worker runs:
2 batches then the remaining 4. All independent row comparisons passed; 42 R2
requests created 12 objects totaling 19821 bytes. Completed resume is a local
no-op even without credentials. These remain observations, not a service catalog.

Evidence: `artifacts/r2/20260922-m2-ddb-batch-001/README.md` in the parent workspace.
Release CTest: 70/70; batch orchestration tests: 12/12. No new full sanitizer or
curl-OFF run. At that point coverage/version proof remained open; the subsequent
single-chunk work above does not retroactively change these observation artifacts.

## Real-data M1, 2026-09-21

`history-cache-sample` now encodes bounded DDB query observations with the existing
column/Zstd codec, performs create-only upload and full readback using `S3Store`,
and compares decoded canonical rows. `scripts/r2/export_ddb_sample.py` in the
parent workspace supplies at most 5000 rows from one stock/fixed interval through
independent read-only DDB connections. It does not subscribe to IPC or write tables.

The first real sample is **240 rows of 000001.SZ on 2025-12-01**, read from the
configured History TSDB and uploaded to `history-cache-staging` in 5 requests.
The 1639-byte data object and 1723-byte source descriptor were read back exactly;
independent Python decoding also matches raw DDB-to-float32 canonical values.
Evidence: `artifacts/r2/20260921-m1-r2-live-001/README.md` in the parent workspace.

These are **observations, not complete-coverage R2H1 packs/catalogs**. They reuse
the immutable blob key convention, with explicit descriptor kind and
`pack_format=null`, `coverage_verified=false`, `source_version=null`,
`production_eligible=false`. No current pointer or query-service catalog is
created. Full source commit/revision proof, scaled batch coverage, incremental updates,
agent/Cloud/SDK integration and production deployment remain unimplemented.
Usage and replay: `docs/R2-M1-REAL-SAMPLE.md` in the parent workspace.

## Scope

- `none/1m`, normalized ASCII series identity, explicit complete `[start,end)` coverage.
- Immutable single-series packs; independent checksummed Zstd frames containing
  `HISTORY_COLUMNS_V1` (required OHLCV columns only).
- Canonical manifest/pointer JSON, 128-bit stable series ID, source version
  separate from monotonic publication sequence.
- Local filesystem store: atomic pointer, single-writer lock, expected-sequence
  check, lost-ack recovery and explicit rollback via a new publication sequence.
- Transport-independent immutable-object/Range reader and conditional publisher:
  create-only manifests, exact-ETag pointer CAS, explicit uncertain outcomes and
  authoritative-pointer recovery.
- S3/SigV4 staging adapter with bounded requests, receive limits, pinned ETags,
  deadlines and cancellation. Optional libcurl HTTPS backend is disabled by default.
- Durable owner-only publish intent, hash-chained events and dispatch floor;
  same-candidate restart recovery with persistent historical uncertainty.
- Separately gated `history-cache-staging` CLI: fixed synthetic request plan,
  durable aggregate budget/ownership ledger, local ACK suppression, read-only
  journal reopen, legacy conditional cleanup or explicit no-delete retention.
  Default mode is offline; the two profiles never switch automatically.
- Reader: full coverage only, pinned snapshot, bounded range/block reads,
  earliest `max_count` rows, empty coverage distinct from MISS.
- Synthetic fixture CLI and independent Python decoder tests, including 500,000 rows.

The legacy C++ staging profile failed in one authorized endpoint run: stale
pointer PUT returned 412, but stale pointer DELETE returned 204 and the next
GET confirmed absence. Two synthetic dependencies (9497 bytes) remain as last
observed evidence; no cleanup retry is authorized by that run's unused budget.
Not validated: the full synthetic staging profile, real network fault behavior,
automatic retention/cleanup, or power-loss filesystem behavior. The separate real
single-chunk publication/read path above passed without deletes.
Remaining production work: multi-partition batch coverage/version proof,
distributed fencing, automatic freshness invalidation, Cloud full-hit routing,
SDK invalidation, GC and deployment acceptance. UDS and shadow now exist; the
integration's intraday tools are not wired into the daemon query path.
The local store is a test double, not a statement about R2 conditional-write
semantics or cross-object transactions. A source version in a fixture is not a
verified database commit watermark.

## Conditional publication contract

`ObjectReader` is independent of filesystem paths; `LocalStore` implements it,
and the CLI still uses the original POSIX publisher. `ObjectStore` adds one
authoritative pointer GET (body and strong opaque ETag from the same response),
create-only immutable writes and create-only/If-Match pointer writes. The
S3 adapter derives the R2 endpoint from the account/jurisdiction and restricts
requests to a named staging bucket and `r2-history-staging/<run-id>/` prefix.
The adapter does not prove bucket privacy or token scope. All tests use synthetic
HTTP peers and never enable a network request.

`ConditionalPublisher::publish(manifest, expected_seq)` validates current-coverage
source versions, verifies every referenced pack with a bounded full Range pass,
creates/verifies the immutable manifest, then attempts **one** pointer CAS. ETags
are comparison tokens, never content checksums. There are no implicit write
retries, unconditional overwrites, listing, deletion or remote rollback.

- `committed`: the exact target was acknowledged or observed. It may already be
  superseded; an identical competing publication counts as the same target.
- `not_applied`: this invocation's write is proven not applied. It says nothing
  about previous in-flight requests.
- `conflict`: this invocation saw a different sequence or failed its condition;
  a prior unknown attempt may nevertheless have committed and been superseded.
- `indeterminate`: a dispatched request may still commit, or authoritative
  recovery failed. Missing/unchanged/newer pointer reads do not prove failure.

Callers must retain the exact candidate and expected sequence **before dispatch**;
explicit retries use that same pair and do not rebase to a newer sequence. This
class remains transport independent. `JournaledPublisher` is an optional durable
wrapper, described below; the legacy CLI still uses the POSIX-only publisher.
Pointer deletion, namespace reuse/epoch reset, out-of-band writes and dishonest
adapters are outside this contract. All writers must use these CAS/version rules.
Current-overlap source checks do not implement a historical source-version floor.
The detailed state table and outstanding production gates are documented in
`docs/R2-OBJECT-STORE-CONTRACT.md` in the parent workspace.

## Staging transport and durable intent

`S3Store` accepts injected `HttpTransport` and non-copyable `S3Credentials`.
It does not read credentials from files/environment or log HTTP bodies/headers.
Secrets and temporary HMAC keys are best-effort cleansed; caller copies and
process memory are not a credential-vault boundary.

- Full immutable GETs verify content SHA-256. Range reads require exact 206,
  Content-Range/total/body length, and pin a strong ETag from the first response.
- A pointer body and its opaque ETag come from one uncached GET. Only a complete
  verified 404 with a strictly framed `NoSuchKey` error means absent; permission,
  bucket, framing, TLS and timeout errors do not mean an empty store.
- No implicit retry, redirect, proxy, netrc, cookie, listing or remote rollback.
  PUT is create-only or exact If-Match. Potential delivery without a valid ACK
  remains indeterminate. libcurl uses HTTP/1.1, verified TLS >= 1.2, finite connect
  and request timeouts, and requires matching headers/runtime with async DNS.
- `TransferLimits` is per store operation context, shared by captured Range
  readers. Reservations are not refunded: each call reserves its upload and
  `max(success_limit, 4096)` download bytes before dispatch. Header bytes (32 KiB
  per response), TLS/HTTP overhead and libcurl internal buffers are not byte-budget
  accounting; this is not an exact billing or process-RSS limit.
- Default budget: 32 requests, 4 MiB upload, 16 MiB body download, 30 seconds
  overall / 10 seconds per request / 3 seconds connect. Hard configurable maxima:
  4096 requests, 64 MiB upload/download each, 5 minutes overall, 30 seconds/request.
  A 500k-row pack may need hundreds of Ranges; callers must preflight and choose
  a finite sufficient budget rather than silently raise it or retry.
- Network access additionally requires explicit `CurlHttpTransport(true)`.
  The staging CLI alone loads the two explicit `R2_STAGING_*` variables, after
  matching config/intent hashes, confirmations and durable namespace registration.
  It never reads AWS profiles; no service or deployment integration is included.

### Controlled staging runner

`history-cache-staging --config PATH --fixture DIR --run-id ID` only validates
the fixed 2050-row synthetic fixture and prints `OFFLINE_CPP_PLAN_ONLY`. It does
not read credentials. Live mode additionally requires `--execute`,
`--exclusive-namespace`, a new `--output` leaf and both
`--approved-config-sha256` / `--approved-intent-sha256` hashes. A false resource
confirmation or hash mismatch rejects before reading secrets or creating output.
The curl-OFF binary cannot execute live mode.

The CLI is tied to its build-time workspace (this integration project, or the
repository root for a standalone checkout). Inputs must be project-local; evidence must be
below `artifacts/r2`. The persistent, owner-only 0700 directory
`artifacts/r2/cpp-staging-registry` must be provisioned before a live run. Its
scope markers prevent local namespace reuse; do not remove them to retry.
This is not distributed fencing or protection against a malicious local owner.

The runner itself constructs the three request sequences; it never executes an
external request-list JSON. Requests reserve upload/body-download budgets and
fsync a hash-chained ledger plus dispatch floor before backend calls. Only a
complete create-only ACK, checkpointed before further work, establishes ownership.
Adapter counters are reconciled with the ledger before cleanup or the next phase.
Approved but locally blocked helper reads still consume their reservations.

In `cpp-staging-2050-v1`, cleanup uses a separate deadline, cancellation token and 9-request budget. Each
owned object requires `GET/hash/ETag -> DELETE If-Match -> GET NoSuchKey`, with
pointer absence established before deleting manifest/pack. Unknown writes or
deletes, unexpected identities and unsupported conditions stop deletion without
retry or unconditional fallback. A baseline failure before its planned final
seq 2 may conservatively leave seq 1 and dependencies for manual reconciliation.
Before ordinary cleanup, baseline must reject a DELETE carrying the stale seq 1
ETag with 412. Unsupported or ignored conditional DELETE stops the profile.
The proposal reserves at most 110 requests (109 backend dispatches), versus 108
reservations / 107 backend dispatches on its nominal path. No live run is implied.

`--inspect-run EVIDENCE` validates/reconstructs local evidence only; it does not
resume execution or reset budgets. Validation includes recovery fsync but no new
requests or event append. A missing final report requires inspection, not rerun.
Local ACK suppression is not a real network fault, and same-process journal
reopen is not a kill/exec test. The tested endpoint/adapter did not satisfy the
legacy conditional DELETE contract. See `docs/R2-CPP-STAGING-RUNNER.md` in the parent workspace for gates
and evidence; historical Python smoke is not C++ runner validation.

### Explicit no-delete retention profile

`tests/staging-config-retain-v2.json` is an unconfirmed synthetic template for
`cpp-staging-2050-retain-v2` (config/intent/report schema 2). It retains the same
2050 rows, three publication/read scenarios, PUT CAS negative tests, exact
Ranges, local ACK faults and read-only journal reopen. It omits the legacy
stale-DELETE probe and all cleanup operations. Both the request gate and durable
ledger reject DELETE independently; failures do not attempt cleanup or fallback.
The shared registry still forbids namespace reuse, including across profiles.

- Nominal: 80 reservations / 79 backend requests; conservative plan: 82 / 81.
  Upload bodies: 39288 bytes; nominal/worst download reservations:
  113527157 / 113535349 bytes. An unconfirmed proposal allows 84 reservations,
  64 KiB upload/stored, 128 MiB download, 360 seconds and USD 0.10 request cost.
- Nine exact keys, at most 29271 bytes of this run's own object bodies, are
  retained on the nominal path. This is not a bound on preexisting cloud data.
  Require an owner label, object count, review interval (1 second to 7 days),
  a separate cost proposal through review, and explicit retention confirmations.
- A durable `started` event anchors the review deadline before credentials are
  loaded. Inspection never moves that deadline or performs cloud requests.
  The template suggests a 24-hour **manual review**, not automatic expiry or a
  storage/billing hard cap; objects continue to exist until separately handled.
  No bucket lifecycle rule or background collector is created.
- `PASS_STAGING_RETAINED` means this publication/read profile completed with
  retained ownership, not that cleanup passed. `FAIL_STAGING_RETAINED` stops the
  run; unknown writes, changed identity, unowned observations and unvisited keys
  stay distinct. Per-object last ACK body size/hash/ETag and aggregate retained
  bytes are historical evidence, never a fresh cloud inventory.
- Capability observations cite the recorded request indices. DELETE is always
  `not_tested_not_required`; a synthetic peer or one successful run is not
  generalized endpoint verification. Existing live DELETE failure stays separate.

No new credential is required by the version change: `authorized_credentials_ready`
means the user-authorized credentials remain valid for the approved scope. Exact
new config/intent hashes and explicit object-retention approval are still required
before a new live run. Old source-pinned preparation tools intentionally reject
this new source tree; their historical acceptance is not rebound.

The historical retention matrix has 68 entries. M1 adds `sample_cli`, for 69
CTest entries including independent decode and rejection of false coverage.
The CLI suite now requires six named checks. CI and the acceptance runner verify
the exact manifest, required output and zero skipped cases. Build parallelism is
at most two; tests are serial and existing timeouts remain unchanged.

Prepare a new journal before any publication request, then reopen it on restart:

```cpp
JournaledPublisher::prepare(new_directory, store.scope_id(), candidate, expected_seq);
JournaledPublisher publisher(new_directory, store);
auto result = publisher.resume();
```

The parent directory must already exist. The journal stores the exact canonical
manifest, sequence and credential-independent endpoint/bucket/prefix identity;
it references already uploaded packs and does not duplicate pack payloads.
`openat`/`O_NOFOLLOW`, owned 0700/0600 paths, single-link regular files and a held
nonblocking flock protect local use. The same directory cannot accept another
candidate, even after completion; use a new directory for a new candidate.
This does not serialize distinct journal directories or different machines.

Intent + directory fsync precede a hash-chained attempt record; event + dispatch
floor fsync precede remote calls. Results are appended and fsynced before return.
An ACK followed by local checkpoint failure returns indeterminate. Recovery may
use an intact ACK only after a successful recovery fsync, or the exact remote
target. Missing/old/newer pointers and a later not-applied/conflict never erase
an earlier unknown attempt. `committed` confirms historical success, not that
the target is still current.

Each intent is <= 1 MiB + 84 bytes, events <= 128 x 80 bytes, dispatch floor is
80 bytes. At most 64 complete attempts; exhaustion stops before I/O. No automatic
repair, log rollover, GC or global directory quota. Truncated/corrupt records or
a dispatch floor ahead of the surviving log fail closed. Coordinated rollback
of all journal files, hostile same-user modification and physical power-loss
ordering are not covered. Unknown journals must not be deleted to rebase.

## Build and test

Requires Linux, CMake >= 3.20, C++17, OpenSSL Crypto, libzstd >= 1.4.0,
nlohmann-json >= 3.10, Python >= 3.10. There is no CMake network fetch and no
Python package installation. Limit build concurrency to 2 on the shared host.
`HISTORY_CACHE_CURL=OFF` is the default; HTTP/S3/journal offline tests still run.
With `-DHISTORY_CACHE_CURL=ON`, provide libcurl >= 8.12 development headers and
matching shared runtime. `CURL_INCLUDE_DIR` and `CURL_LIBRARY_RELEASE` may point
to verified isolated headers and the installed runtime; CMake never fetches them.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
timeout --kill-after=5s 180s cmake --build build --parallel 2
timeout --kill-after=5s 300s ctest --test-dir build --output-on-failure --parallel 1
```

For ASan/UBSan, configure a separate directory with
`-DHISTORY_CACHE_SANITIZERS=ON`; do not replace a production build directory.
If the host lacks one runtime, `-DHISTORY_CACHE_SANITIZER_SET=address` enables
ASan only; record UBSan as not run, not as passed. CI requests both by default.

## Reproducible integration acceptance

Run from the integration workspace, choosing a new output directory each time:

```sh
timeout --kill-after=5s 600s python3 -B scripts/r2/run_offline_acceptance.py \
  --output artifacts/r2/local-001 --rows 500000
```

The runner builds from source, runs all suites, generates a deterministic
synthetic fixture, publishes locally, injects failure before/after pointer
commit, recovers without double publishing, reads/compares row hashes, and
rolls back using a newer publication sequence. It records source file hashes,
toolchain, commands, timeouts, source/runner archives and result JSON. No service
is contacted. All 69 CTest entries run serially, including independent Python
SigV4 vectors, 39 transport/journal cases, the original 24 object-store cases,
30 individually bounded staging cases and four checks in the staging CLI suite.
Process recovery tests use fork/exec and a persisted synthetic HTTP-peer fixture;
they exercise real journal fsync/locks but not real S3, sockets or power loss.

To include the curl backend in acceptance (still no network requests), also pass
`--curl-include-dir <project-local-include>` and `--curl-library /usr/lib64/libcurl.so.4`.
The runner hashes the headers/library and checks the runtime version/linkage.

`build/history-cache-tool --help` lists individual offline commands. `init` and
`fixture` require new directories; all writes are explicitly rooted there.
Directories and path components must not be symlinks. The local store is
private to its owner; concurrent hostile mutation by the same OS user is not
a supported security boundary.

## Limits and failure contract

- Pack: 500,000 rows / 32 MiB maximum, whichever is reached first.
- Block: 1,024 rows; both raw and compressed bytes <= 64 KiB; one Zstd frame.
- Metadata: 1 MiB; up to 1,024 interval entries; bounded nesting.
- No unbounded query result buffering; `read_plan` retains one decoded block.
- Row callbacks are provisional until `read_plan` returns successfully. A later
  error invalidates the entire request; this is not a public V2 END implementation.
- Index hash binds TOC and header; selected blocks are hashed/decompressed
  before delivery. Whole-object hash is verified at publication and full audit.
- No automatic GC. Old snapshots remain readable; store growth is the test
  operator's responsibility until a reference-safe, quota-bounded GC exists.

The exact format and integration gates are in the parent workspace:
`docs/R2-FORMAT-V1.md`, `docs/R2-DEVELOPMENT-PLAN.md`, and
`docs/R2-ACCEPTANCE-PLAN.md`. Golden vectors in `tests/golden/` are part of the
format contract. Changes require matching tests and an explicit format decision.
