# Deployment boundary

## GitHub Actions deployment, 2026-10-05 (current)

Formal cloud Agent deployment uses the independent manual
`.github/workflows/deploy.yml`, not the Cloud deployment job or a manual SSH
release. Build/push now depends on the reusable offline-core test workflow.
The deployment adapter invokes the existing journaled deployer, selects exactly
one pinned node, requires a registry digest/reviewed commit, checks resource and
image compatibility, and preserves explicit rollback/recovery/acceptance.
Cloud is never restarted, reconfigured or switched by this job. The existing
production storage profile is required even when targeting test-vps.

See [GITHUB-ACTIONS.md](GITHUB-ACTIONS.md) for Environment secrets, one-time
setup, release inputs, UID/UDS boundaries and failure handling. These files are
local implementation until reviewed/pushed; no GitHub settings or production
node has been changed by adding them. Historical sections below describe earlier
implementation milestones and are not a current rollout status report.

## Local evidence retention and health notifications, 2026-10-02 (latest)

`docker/agent_operations.py archive --archive-root PATH` plans retention of old
terminal deployment journals. Defaults: retain five newest, age over 30 days,
at most eight per run. `--execute` writes private, verified local archives;
`--prune` additionally removes exact source journals after readback verification.
The deployment lock protects active state. Release directories and credentials
are never archived or deleted by this operation. Archive destinations must be
outside the live state tree. No Docker, Cloud or R2 operations are performed.

`agent_operations.py monitor --monitor-state PATH` checks local Agent identity,
state and one UDS PING/PONG; it does not query history or download from R2.
It atomically writes private health.json and Prometheus health.prom, including
the last-check timestamp so an external monitor can detect stopped checks.
The optional `--webhook-config PATH` enables bounded HTTPS JSON notifications:
secrets only on curl stdin, no redirects, normal certificate verification,
2xx required, persistent deduplication, recovery events and per-instance caps
(300s attempt spacing, four/hour, 48/day). Without the option there is no webhook.
Failed/unknown delivery consumes its reservation and is not credited as sent.

Ship adjacent ops_archive.py/ops_alert.py and deployment helpers. Monitor
service/timer templates, monitor.env.example and webhook.example.json are not
installed or enabled by this change. The monitor is a low-priority host-side
one-shot, not an extra Agent thread; its limits are not an Oracle capacity test.
Private metric files need controlled reader permissions and explicitly configured
stale/missing-instance alerts; see deploy/monitor-alerts.example.yml.
The full integration runbook is docs/R2-RETENTION-MONITORING-20261002.md.
Factor publisher retention/monitoring is separate on the DDB export host.
Real notification delivery, sustained timer operation, final archive expiry and
credential-snapshot retirement are still deployment acceptance/policy work.

## Durable recovery and bounded Docker operations, 2026-10-02 (latest)

`deploy-agent.sh` is now a compatibility wrapper for adjacent `deploy_agent.py`.
Ship both helpers (`probe-agent.py`, `snapshot-agent-config.py`) with it.
The private deployment root holds an atomic/fsynced active journal and unique
config snapshots. Every container mutation records intent before dispatch.
Recovery binds old/candidate IDs, a transaction label, image ID and config
fingerprints; unrelated containers or changed previous config block recovery.
The old restart policy must be `no` or `unless-stopped` to avoid booting a retained
backup into the shared socket. Container budgets and Cloud gates are unchanged.

Use `--status` (local journal only), `--recover` for interrupted deployment,
`--rollback` for a committed but unaccepted release, or `--accept` to explicitly
discard the stopped backup. Accept retains config snapshots and audit state.
The next release requires the previous one to be rolled back or accepted.
A pending Docker mutation requires operator verification that daemon work has
settled, then `--recover --acknowledge-unknown`; never auto-pass this flag.
Killing a Docker client does not cancel the daemon's operation. Timeout or an
uncertain CLI failure does not trigger another container mutation automatically.

Docker limits: pull 240s, reads 15s, mutations 30s; shared budgets preflight 300s,
cutover/bake bake+180s, recovery 180s, acceptance 90s. CLI timeout kills its local
process group; disk stalls and daemon-side work are not covered by that deadline.
Local UDS probes stay bounded and perform no history/R2 request.
New snapshots use `release-<transaction>/`; earlier `release.*/config` snapshots
remain untouched. Existing roots must be private/owned. Legacy backup containers
without a journal require manual reconciliation, not automatic adoption.

Offline tests include a genuinely killed deployment test process, fake-Docker
recovery, CLI process-group cleanup, identity/config conflicts and real local UDS.
CI and image-build workflow run the regression, but no real Docker/VPS rollout
or production verification has occurred. Long-term snapshot retention and alert
delivery remain separate work. This section supersedes the older implementation
limitations below; the integration runbook is
`docs/R2-AGENT-DEPLOYMENT-RECOVERY-20261002.md` in history-v2-integration.

## Failure rollback implementation, 2026-10-02

Follow-up: new deployments snapshot credentials and the optional reader profile
into a unique private `/var/lib/history-cache/deployments/release.*/config`
directory before stopping the old agent. Files are fsynced, mode 0400, mounted
read-only, and never printed. Sources must be private regular JSON files owned
by the deployment user. Ship `snapshot-agent-config.py` beside the script.
Changing the original source file no longer changes a newly deployed container's
configuration. Keep snapshot directories while any retained container uses them;
do not put credentials in Git or automatically delete snapshots on failure.
Old containers deployed before this change may still mount mutable source files:
their first rollback still requires those old files to remain unchanged.
CI now runs the fake-Docker and snapshot regressions without a Docker daemon.
These checks do not provide full Agent configuration-schema validation.

`docker/deploy-agent.sh` now locks deployment, pulls an immutable image before
stopping anything, and retains the old container as `history-cache-agent-rollback`.
Startup/bake failures restore the previous container by ID and restart it if it
was previously running. Success retains the stopped backup for operator review;
an existing backup blocks another deployment rather than silently deleting it.
Ship `docker/probe-agent.py` beside the script; host Python 3 is required.
Checks use bounded local UDS PING/PONG and require zero candidate restarts, not
just the presence of a socket. They do not read R2 or establish data readiness.
Resource limits remain unchanged and Cloud containers/gates are never modified.

This supersedes the historical no-rollback statements below, but is not complete
transactional deployment acceptance: SIGKILL/host reboot recovery is manual,
Docker operations have no outer deadline, and bind-mounted configuration must
remain unchanged for restoration to preserve its meaning. The old and new agents
are not run together; there is a short history-service interruption at cutover.
No real Docker deployment, VPS or production validation was performed here.
Offline fake-Docker transition tests cover pull failure, failed run/health,
first-install failure, retained backup and successful cutover. Full recovery
rehearsal and CI integration remain outstanding. Do not treat this as production
deployment approval.

## Explicit production storage profile, 2026-09-30

The agent now accepts `--storage-config /run/config/history-storage.json` with
`--credentials-file`. The profile has exactly `version=1`, `environment`,
`account_id`, `bucket`, `prefix`, `jurisdiction`, and `role=reader`. Production
requires an isolated `production` bucket name and `r2-history-production/` root.
Account must match the credentials. Do not combine it with inline scope flags.
Absent a profile, existing staging restrictions remain. Every agent store is
read-only; use an independently scoped R2 read-only token as well.

`docker/deploy-agent.sh` accepts an optional third argument for the absolute
reader profile path and mounts it read-only. Resource limits and demand-read
defaults are unchanged. This is not production deployment approval: the script
still lacks transactional rollback. No production bucket, container or image
was created by this change. Historical backfill runs on the local DDB data host,
not in the cloud agent. Production intraday scheduling remains separate work.

## Demand-read correction (latest)

The default is now `--foreground-network yes`. A local miss or expired catalog
triggers a bounded R2 read, not automatic DDB fallback. Unchanged current pointers
reuse the manifest and immutable pack. One cold query runs at a time across
foreground and background; waiting queries recheck the shared cache and consume
their original 5s deadline. This coalesces successful overlapping small-pack
loads, not arbitrary failed requests or uncached large Range reads.
Demand GETs use 512 KiB/s application receive pacing; background uses 128 KiB/s.
Cold demand cancels in-flight maintenance and pending demand prevents maintenance
from starting. Hot cache reads do not wait on the cold-operation lock.
The existing two UDS workers/four pending connections and container resource
templates remain. `--foreground-network no` is now an explicit diagnostic mode.
Warm plans may coexist with demand reads. The zero-client lease policy applies
only to optional background maintenance, never to client demand reads.
Cloud remains shadow-only: this change does not switch client responses to R2,
raise its 2s shadow deadline, or establish Oracle capacity acceptance. The
cache-only load test explicitly retains `--foreground-network no`; its old
results are not evidence for R2 download contention. Earlier defaults below
are historical and superseded by this section.

## Realtime-first profile, 2026-09-29

This section supersedes the worker/cache/default-network values below. The
daemon defaults to **cache-only foreground** (`--foreground-network no`), two
UDS workers, four pending connections and a 32 MiB pack LRU. Cold, expired or
uncached-large-pack requests return `MISS/cache_not_ready` without HTTP. No
request automatically queues a warm job. The gateway still serves all real
responses from upstream; its 2-second shadow deadline is unchanged.

`--foreground-network yes` restores on-demand network reads for bounded
diagnostics only; the archived-sample verifier explicitly passes this flag.
It cannot be combined with a nonempty warm plan. Small packs remain <=1 MiB
and <=5000 rows. Snapshot TTL is still 300s, with no stale-on-error extension.

`--warm-plan /run/config/warm.json` accepts a <=16 KiB array of at most eight
explicit UTC-v1 query objects. Every object has exactly these ten fields:
`op`, `symbol`, `start_ms`, `end_ms`, `max_rows`, `protocol_version`,
`timestamp_semantics`, `range_semantics`, `period_seconds`, `adjust`.
Use `op=query`, `protocol_version=1`, `timestamp_semantics=utc-instant-ms`,
`range_semantics=half-open`, `period_seconds=60`, `adjust=none`, max_rows 1..5000
and an explicit half-open interval <=24 hours. This is not a rolling date rule:
update the reviewed plan when the archive date changes. Parent paths are trusted
operator configuration. An absent/empty plan starts no scheduler thread.

One background worker runs at most one job per 30 seconds across all targets;
each attempt (including retries/namespaces) reserves <=6 HTTP requests,
<=4 MiB download capacity and <=5 seconds. GET receive speed is limited by
libcurl to 128 KiB/s. This is application receive pacing, not NIC QoS or a
zero-burst bandwidth guarantee. Unchanged current pointers reuse manifests and
packs. Successful targets are rechecked after 240..261s; failures back off
60..960s. Pauses do not accumulate catch-up work. Up to eight jobs can delay
some refreshes past TTL, in which case foreground requests fall back safely.

The scheduler starts **paused** and requires a renewable idle lease. Opt in on
the matching gateway using `HISTORY_CACHE_IDLE_MAINTENANCE=1` alongside
`HISTORY_CACHE_SOCKET`. Its existing shadow worker checks client-map emptiness
using a nonblocking try-lock at most every 500ms. Any connected client (including
unauthenticated), or failure to acquire the lock, means busy. It does not scan
2000 queues or perform I/O on the realtime thread. This deliberately conservative
policy does not run maintenance under client load, even if CPU happens to be idle.

UDS controls are `maintenance_status` and `maintenance_lease` with boolean `idle`
and integer `valid_until_mono_ms` (absolute same-host CLOCK_MONOTONIC milliseconds,
no more than 2000ms ahead). Old queued permits cannot renew themselves. Peers must
share the same monotonic time namespace; do not use custom time namespaces.
Missing, expired or revoked permits cancel active work; the daemon checks every
200ms and curl observes cancellation in its callbacks. Cancellation is not
instantaneous (DNS/TLS/callback scheduling still matters); the 5s job deadline
is retained. `maintenance_status` exposes only aggregate numeric/boolean state.
The owner-only UDS is the trust boundary; do not grant untrusted local clients
access or add a second permit issuer.

Deployment templates now specify 0.25 CPU, 128 MiB memory hard limit, 96 MiB soft
reservation/high threshold, no swap, 32 tasks, 256 descriptors and lower CPU/IO
weights. Docker uses read-only rootfs, no capabilities, no-new-privileges and a
read-only credentials JSON mount, not secret argv/env. These are **unapplied
templates**, not a measured Oracle capacity guarantee. Configure UID/socket
ownership consistently with Cloud. Validate actual cgroup settings before use;
IO weights do not shape network traffic. Memory caps include more than the LRU;
an OOM terminates the sidecar and Cloud continues on upstream.

The deployment script does not yet enable a warm plan automatically and still
lacks transactional rollback. Do not run it on production as an approved release.
No new images or services have been deployed. The integration document
`docs/R2-AGENT-REALTIME-PRIORITY-20260929.md` records local evidence and remaining
Oracle 2000-client acceptance requirements.

## Historical local implementation, 2026-09-28

`history-cache-agent` is a read-only UDS daemon, with container assets in
`../docker/` and a separate image workflow in `../.github/workflows/build-docker.yml`.
Its intended deployment is beside `cloud_gateway_v2` on the same cloud server,
with a shared Unix socket directory, not beside the local DDB exporter.
The gateway currently supports shadow observation only; upstream still supplies
every real history response.

The daemon uses four workers, a 16-connection pending queue, owner-only socket
permissions and an exclusive socket lock. Gateway and agent containers need
compatible UID and mount permissions. Query budgets are per query, not daemon
lifetime. See `../README.md` for local tests and remaining correctness gaps.

The 2026-09-28 hardening changes have not been deployed to test VPS or production.
The UTC-contract revision requires matching UDS protocol v1 on both sides:
old unversioned clients/responses are rejected. Agent images need `tzdata` and
Gateway images need `libcrypto`; update both images and verify their identities
before interpreting test observations. No existing pack bytes are rewritten.
Before another test deployment, verify source/image identity and market-time
contracts, then compare authenticated upstream and agent results. A socket-only
health check does not prove that R2 reads work. Offline tests do not establish
real TLS stability or long-running service health.

Production is not ready: image publication must depend on tests; credentials
must use the secret-file interface below instead of command line arguments;
deployment must pull/verify before stopping the old container and
provide rollback. Current staging bucket/prefix restrictions also remain in force.
Do not reuse the staging image configuration as an approved production setup.

`history-cache-tool` remains an offline fixture command, not a publisher daemon.
Do not wrap it in a production timer. Publisher scheduling is independent of
`upcloud_gateway.service`. No offline CI test deploys or uploads data, and any
production cutover requires separate approval.

## Earlier read-performance implementation (superseded defaults)

### Read performance options

The agent enables GET connection reuse by default (`--reuse-connections yes`).
It owns at most four curl easy handles, each with at most two cached connections
and a 30-second idle reuse limit. Headers, methods and callbacks are reset
between requests; transport failures discard the handle. The publisher and
other `CurlHttpTransport` callers keep their original non-reuse default.
TLS peer/hostname verification, no redirects and no environment proxy remain enforced.

For immutable packs <=1 MiB and <=5000 rows, one verified full GET replaces
the header/TOC/block Range calls. Successful packs are retained in a process-local
LRU, bounded by 64 MiB and 256 entries. The key includes S3 scope, object key and
digest. Active readers retain owning references; the byte bound is for resident
cache entries, not total RSS. Four workers may also hold active/download buffers.
Larger packs keep the verified Range path and are not cached by this optimization.

`--pack-cache-bytes 0` disables retention; `--full-pack-read-bytes 0` disables
full GET coalescing; `--reuse-connections no` restores fresh connections. These
switches support one-variable-at-a-time diagnostic comparisons. Cache bytes are
limited to 256 MiB and the full-read threshold to 1 MiB.

Cached bytes never establish coverage: a valid snapshot is still required.
Manifest TTL remains 300 seconds; after expiry, refresh failure returns ERROR,
not stale data. This remains pinned-snapshot semantics, not live DDB freshness.
Restart empties memory caches. No automatic background preload or disk cache is
introduced, and a cold miss may still exceed Cloud's 2-second shadow budget.

UDS replies add `metrics` containing bounded numeric counters by stage
(`current`, `manifest`, `index`, `block`, `pack`), new connections, HTTP receive
bytes, failures, snapshot/pack hits and resident pack bytes/count. Curl DNS,
connect, TLS and first-byte times are cumulative from request start, in
microseconds; do not add them together as exclusive phases. No URLs, signed
headers, credentials or response bodies appear in these metrics.

### Secrets and deadlines

`--query-timeout-ms N` overrides the complete agent query deadline (1..30000 ms,
default 5000). This includes every object/Range read and retry, not just one TLS
connection. Use the larger values only for bounded diagnostics; they do not
change Cloud's UDS deadline and are not a remedy for slow production shadow
reads. Requests remain GET-only and retain the existing transfer budgets.

Use `--credentials-file /run/secrets/history-cache.json` instead of the three
inline credential options. The JSON object contains exactly `account_id`,
`access_key_id`, and `secret_access_key` string fields. Account ID must contain
32 hex characters; key/secret must also satisfy the existing S3 credential shape
(1..128 characters, alphanumeric key; alphanumeric or `/+=` secret).
The file is limited to 4096 bytes, must be a regular file owned by the service
UID with mode 0400 or 0600 and one hard link. Final-component symlinks, FIFOs,
directories, malformed input and mixed inline/file options fail before socket
creation. Errors never include JSON content. Parent directories must be trusted.

Mount the file read-only; do not COPY it into an image or commit it. Provision
it through a secret manager or protected editor, not a shell command containing
the secret. Default Docker secret mode 0444 is intentionally rejected: configure
the owner/mode or use an owner-only read-only bind mount. Credentials are read
once at startup; rotation requires a controlled restart. Existing inline options
remain compatible, but are unsuitable for real credentials. This change does
not migrate existing services or rotate previously exposed credentials.
