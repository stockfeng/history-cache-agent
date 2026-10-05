# Agent-only GitHub Actions deployment

## Boundary

The Agent repository owns `.github/workflows/build-docker.yml` and `deploy.yml`.
The former tests/builds/pushes to `ghcr.io/stockfeng/history-cache-agent`; it does
not deploy. The latter is manual, defaults to `preflight`, and contacts exactly
one node. It never drains, stops, rebuilds or reconfigures Cloud, upcloud, DDB,
the uploader, client allowlists or timers. No R2 writes or data health queries
are issued by this deployment workflow.

Targets are pinned in `docker/actions_deploy.py`:

| Target | GitHub Environment | Host |
| --- | --- | --- |
| test-vps | history-cache-test-vps | 132.226.7.147 |
| oracle | history-cache-oracle | 151.145.72.82 |
| aliyun | history-cache-aliyun | 139.196.115.141 |

All three use the **existing production bucket**, not a new staging bucket.
The test node is selected explicitly; production is never implicitly chosen.
The current container UID and credential checks require a root SSH deployment
user, rootful Docker, and no user namespace remap. SSH is only the transport
executed by Actions; no manual production deployment is required.

## One-time GitHub setup

Configure the selected Environment in the Agent repository. Production
environments should require reviewers, disallow self-review where available,
and allow only reviewed release branches/tags. These protections are repository
settings, not enforced merely by adding this workflow. Protect workflow and
deployment-script changes with code review as they execute with node privileges.

Secrets in another repository are **not inherited**. Reuse approved credentials
via Environment/organization secret configuration; do not post them in issues,
logs or commit them. No new Cloudflare account or bucket is needed.

| Secret | Meaning |
| --- | --- |
| AGENT_SSH_KEY | Deployment key, without passphrase; use this OR password |
| AGENT_SSH_PASSWORD | Existing root SSH password; omit when using a key |
| AGENT_SSH_KNOWN_HOSTS | Verified OpenSSH known_hosts entry for the selected pinned IP |
| AGENT_R2_CREDENTIALS_JSON | Optional existing R2 reader credentials JSON |
| AGENT_STORAGE_READER_JSON | Optional matching production reader storage profile JSON |

Provide both R2 JSON secrets on first install, or leave both absent to use
existing private root-owned files on that node:

```text
/etc/history-cache/credentials.json
/etc/history-cache/storage-reader.json
```

The secret JSON formats are the existing Agent formats. Credentials contain
`account_id`, `access_key_id`, `secret_access_key`; the profile contains
`version: 1`, `environment: production`, matching `account_id`,
`bucket: history-cache-production`, `prefix: r2-history-production/`,
`jurisdiction: default` (or `eu`), and `role: reader`. Source files must be
mode 0400/0600. Agent read-only behavior does not prove the token is scoped
read-only in Cloudflare; configure least privilege in the existing account.

The workflow's short-lived `GITHUB_TOKEN` needs GHCR package read access from
the Agent repository. If package ownership is detached from this repository,
grant repository Actions access in the package settings. It is sent over SSH
stdin and used with `docker login --password-stdin` and a private temporary
Docker config, never the node's default login file.

The verified SSH host key must come from the existing trusted inventory or
provider console; the workflow never trusts a fresh `ssh-keyscan` implicitly.
Only port 22 and the pinned root login are supported in this initial adapter.
Host dependencies: bash, tar, mktemp, Python >=3.8, Docker CLI/daemon. GHCR/R2
certificate verification remains enabled. Helpers use only Python stdlib.

## Release sequence

1. Review and commit the complete candidate, including previously untracked
   Agent sources/helpers. Push to the Agent repository; the workflow must be
   present on its default branch to appear in manual Actions dispatch.
2. `build-docker.yml` invokes the full existing offline-core test matrix, then
   builds/pushes the Rocky Linux image. Copy its full `ghcr.io/...@sha256:...`
   and `release_commit` from the Actions summary. Tags such as `latest` cannot
   be deployed. This build currently targets linux/amd64; an arm64 node will
   be rejected, not run through transparent emulation.
3. Select `deploy.yml`, the reviewed workflow branch, one target, `preflight`,
   and the full reviewed `release_commit`. Leave image empty. This checks
   storage JSON locally, Docker enforcement, available resources and retained
   deployment state. It may create private temporary/lock directories but does
   not pull images, start containers or contact R2. Production Cloud must exist
   and be running. It does not establish WAN/R2 data readiness or future load.
4. Dispatch `deploy` on the same target, full commit and exact registry digest.
   Scripts are checked out at that commit. After pull, image architecture,
   Agent entrypoint/UID, RepoDigest and OCI revision must match before any old
   Agent is stopped. Default bake is 120s (30-600s allowed); stock adjustment is
   enabled by default, with no futures/options adjustment implementation added.
5. Inspect the result. `COMMITTED` means startup and bounded local PING/PONG
   bake passed, not a data comparison or live-client cutover. Keep the stopped
   Agent backup until acceptance. Cloud ID/image/PID/restarts/running state are
   compared before/after; a difference fails the operation and never triggers
   a Cloud restart or an automatic undo of unrelated changes.
6. Separately approve Cloud UDS mount/UID compatibility and client allowlist
   rollout through Cloud's own Actions. This workflow intentionally does not
   create that mount on a running Cloud container or change routing.

Thresholds: currently >=256 MiB MemAvailable and >=1 GiB free in DockerRootDir.
They are a conservative startup gate, **not** capacity acceptance for 2000
clients. Runtime limits remain 128 MiB maximum, CPU 0.25, no swap, 32 PIDs,
256 FDs, CPU shares 64, 32 MiB pack cache, no prewarm plan, bridge networking
with no published TCP ports. 128 MiB is a cap, not allocated idle usage.
Docker daemon image pulls/decompression are not constrained by the Agent's
container CPU/network limits: schedule the initial pull during a low-load window
and observe realtime traffic. No image builds run on the VPS.

## Status, rollback and interrupted runs

Dispatch the same workflow with the selected target and reviewed script commit,
empty image, and one explicit operation:

- `status`: private journal phase/transaction/pending operation; not a full health check.
- `rollback`: restore a committed, not yet accepted Agent release. On first
  install it removes only the candidate; Cloud remains unchanged.
- `recover`: reconcile an interrupted deployment using the existing journal.
  Unknown/pending Docker mutations stop for manual reconciliation; the workflow
  never adds `--acknowledge-unknown` or automatically retries mutations.
- `accept`: explicitly remove the stopped Agent backup after review. A new
  deployment is blocked until the prior one is accepted or rolled back.

Use the deployed script revision for recovery unless a reviewed recovery fix
is needed. GitHub cancellation or SSH loss is not proof the daemon has stopped
working. Check journal/daemon state before another operation. Per-target Actions
concurrency does not cancel an in-progress run; a host-side lock also rejects
overlaps. Existing journal locks still protect manual/monitor operations.

Credential snapshots live under `/var/lib/history-cache/deployments/release-*/`
and are mounted read-only. Successful deployments survive removal of temporary
upload files. Recovery needs no new R2 secrets or registry login. Do not delete
snapshots referenced by current/retained containers. A hard kill/reboot may leave
the private `/tmp/history-cache-actions.*` transport directory; reconcile any
active process before deleting it. Routine exits clean these temporary files.

## Local verification and limits

```sh
bash -n docker/deploy-agent.sh
python3 -B -m unittest discover -s tests -p 'test_deploy*.py'
actionlint .github/workflows/deploy.yml .github/workflows/build-docker.yml .github/workflows/ci.yml
```

Tests cover request/secret validation, pinned single-host SSH, no secrets in
argv, archive membership, timeouts/no retries, production storage, private files,
resource/image guards, real journal transitions with fake Docker, explicit
recovery and Cloud state-change detection. They do not execute SSH, real Docker,
GitHub Actions, R2 reads or any production deployment. A newly built GHCR digest
is not automatically identical to an earlier locally frozen candidate.
