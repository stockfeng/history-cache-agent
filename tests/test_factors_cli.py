"""Offline factor publication gates; no network credentials are provided."""

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from zoneinfo import ZoneInfo


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def fixture(root):
    now = time.time_ns() // 1000000
    root.mkdir(mode=0o700)
    schema = [{"name": k, "typeString": v} for k, v in
              (("code", "SYMBOL"), ("ex_div_date", "DATE"), ("forward_adj_factorA", "DOUBLE"),
               ("forward_adj_factorB", "DOUBLE"), ("backward_adj_factorA", "DOUBLE"),
               ("backward_adj_factorB", "DOUBLE"), ("update_time", "TIMESTAMP"))]
    source = {"schema": schema, "batches": [[]], "temporal_units": {"DATE": "D", "TIMESTAMP": "ms"}}
    plan = dict(host="fixture", port=8848, database="dfs://fixture", table="factors", symbol="AAPL", market="US",
                model="futu_ab", allow_empty=True, start="2026-01-01", end="2027-01-01", carry_lower_bound=None)
    state = {"runtime": "3.00.5 fixture", "node": "node1", "paths": [], "chunks": [], "tablets": [],
             "layout": {"engineType": "TSDB", "colDefs": schema, "partitionColumnName": ["ex_div_date", "code"]}}
    proof = {"kind": "ddb-factor-native-snapshot-v1", "snapshot_consistency_proven": True,
             "guard": {"contract": "ddb-factor-native-vector-v1", "before": state, "after": state,
                       "domain": 'code="AAPL"', "scope": "ddb-visible-factor-domain-at-native-version-vector"},
             "plan": plan, "plan_sha256": digest(encode(plan)), "observed_at_ms": now - 1000, "completed_at_ms": now,
             "source_sha256": [digest(encode(source))] * 2}
    binding = {k: plan[k] for k in ("host", "port", "database", "table", "symbol", "market", "model")}
    factor = dict(schema_version=1, kind="ddb-adjustment-snapshot-v1", algorithm="upcloud-adjustment-v1",
                  symbol="AAPL", market="US", model="futu_ab", first_day=20454, end_day=20819, coverage_complete=True,
                  allow_empty=True, observed_at_ms=now-1000, valid_until_ms=now+60000,
                  source_epoch="ddb-factor-" + digest(encode(binding)), source_proof_sha256=digest(encode(proof)),
                  date_encoding="exchange-civil-days-since-1970", rows=[])
    for name, value in (("factor.json", factor), ("observation.json", proof), ("source-0.json", source)):
        (root / name).write_bytes(encode(value))
    return factor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool", required=True)
    args = parser.parse_args()
    env = dict(os.environ)
    for name in ("R2_STAGING_ACCESS_KEY_ID", "R2_STAGING_SECRET_ACCESS_KEY", "R2_PRODUCTION_ACCESS_KEY_ID", "R2_PRODUCTION_SECRET_ACCESS_KEY"):
        env.pop(name, None)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        capture = root / "capture"
        factor = fixture(capture)
        profile = root / "storage.json"
        settings = dict(version=1, environment="staging", account_id="a" * 32, bucket="history-cache-staging",
                        prefix="r2-history-staging/", jurisdiction="default", role="publisher")
        profile.write_bytes(encode(settings))

        def invoke(*command, ok=False):
            result = subprocess.run([args.tool, *command], env=env, capture_output=True, timeout=10)
            assert (result.returncode == 0) == ok, (command, result.stdout, result.stderr)
            return result

        def prepare(target, ok=True):
            return invoke("prepare", "--capture", str(capture), "--storage-config", str(profile), "--root", str(target), ok=ok)

        compact = root / 'compact'
        invoke('prepare', '--capture', str(capture), '--storage-config', str(profile), '--root', str(compact),
               '--compact', 'yes', ok=True)
        reference = json.loads((compact / 'manifest.json').read_bytes())
        data_bytes = (compact / 'compact-data.json').read_bytes()
        state_bytes = (compact / 'compact-state.json').read_bytes()
        proof = json.loads((compact / 'compact-proof.json').read_bytes())
        assert reference['kind'] == 'ddb-adjustment-reference-v2' and 'rows' not in reference
        assert reference['factor_data_sha256'] == digest(data_bytes)
        assert reference['factor_data_bytes'] == len(data_bytes)
        assert proof['guard']['before_sha256'] == proof['guard']['after_sha256'] == digest(state_bytes)
        assert 'before' not in proof['guard'] and 'after' not in proof['guard']
        original_proof = json.loads((capture / 'observation.json').read_bytes())
        original_factor = dict(factor)
        # Synthetic source completion is fixture-only; no live timestamps/TTL are rewritten.
        zone = ZoneInfo('America/New_York')
        local = dt.datetime.fromtimestamp(factor['observed_at_ms'] / 1000, zone)
        first = local.date() - dt.timedelta(days=1)
        following = first + dt.timedelta(days=3)
        boundary = lambda day: int(dt.datetime.combine(day, dt.time(3), zone).timestamp() * 1000)
        scheduled_proof = copy.deepcopy(original_proof)
        scheduled_plan = scheduled_proof['plan']
        scheduled_plan['config_sha256'] = 'a' * 64
        contract = dict(policy='market-source-check-v1', exchange='XNYS', calendar_sha256='b' * 64,
                        trading_day=(first - dt.date(1970, 1, 1)).days,
                        next_trading_day=(following - dt.date(1970, 1, 1)).days, check_minute=180,
                        source_revision=1, source_completed_at_ms=boundary(first), next_check_ms=boundary(following),
                        config_sha256='a' * 64, workload_sha256='c' * 64, source_receipt_sha256='d' * 64)
        scheduled_plan['verification'] = contract
        scheduled_proof['plan_sha256'] = digest(encode(scheduled_plan))
        scheduled_factor = dict(factor, schema_version=3, kind='ddb-adjustment-snapshot-v3',
                                verification=contract, valid_until_ms=boundary(following),
                                source_proof_sha256=digest(encode(scheduled_proof)))
        (capture / 'factor.json').write_bytes(encode(scheduled_factor))
        (capture / 'observation.json').write_bytes(encode(scheduled_proof))
        scheduled = root / 'market-check'
        prepare(root / 'market-noncompact', ok=False)
        invoke('prepare', '--capture', str(capture), '--storage-config', str(profile), '--root', str(scheduled),
               '--compact', 'yes', ok=True)
        assert (scheduled / 'compact-data.json').read_bytes() == data_bytes
        assert json.loads((scheduled / 'manifest.json').read_bytes())['kind'] == 'ddb-adjustment-reference-v3'
        contract['source_revision'] += 1
        # Changing the plan without a corresponding proof digest is rejected before any dispatch.
        (capture / 'factor.json').write_bytes(encode(scheduled_factor))
        invoke('prepare', '--capture', str(capture), '--storage-config', str(profile),
               '--root', str(root / 'bad-market-check'), '--compact', 'yes')
        (capture / 'factor.json').write_bytes(encode(factor))
        (capture / 'observation.json').write_bytes(encode(original_proof))
        changed_proof = dict(original_proof, observed_at_ms=factor['observed_at_ms'] + 1,
                             completed_at_ms=original_proof['completed_at_ms'] + 1)
        factor.update(observed_at_ms=factor['observed_at_ms'] + 1, valid_until_ms=factor['valid_until_ms'] + 1,
                      source_proof_sha256=digest(encode(changed_proof)))
        (capture / 'factor.json').write_bytes(encode(factor))
        (capture / 'observation.json').write_bytes(encode(changed_proof))
        refreshed = root / 'compact-refreshed'
        invoke('prepare', '--capture', str(capture), '--storage-config', str(profile), '--root', str(refreshed),
               '--compact', 'yes', ok=True)
        assert (refreshed / 'compact-data.json').read_bytes() == data_bytes
        assert (refreshed / 'compact-state.json').read_bytes() == state_bytes
        assert (refreshed / 'manifest.json').read_bytes() != (compact / 'manifest.json').read_bytes()
        assert (refreshed / 'compact-proof.json').read_bytes() != (compact / 'compact-proof.json').read_bytes()
        (refreshed / 'compact-data.json').write_bytes(b'{}')
        invoke('run', '--root', str(refreshed), '--execute', 'yes')
        assert not (refreshed / 'attempt-0').exists()
        factor = original_factor
        (capture / 'factor.json').write_bytes(encode(factor))
        (capture / 'observation.json').write_bytes(encode(original_proof))

        job = root / "job"
        prepare(job)
        assert (job / "journal" / "intent.r2j").read_bytes().startswith(b"R2JI0003")
        prepare(job, ok=False)
        for index in range(3):
            failed = invoke("run", "--root", str(job), "--execute", "yes")
            diagnostic = json.loads(failed.stdout)
            assert diagnostic['outcome'] == 'failed' and diagnostic['error_code'] == 'invalid'
            assert 'unused-secret' not in failed.stdout.decode()
            assert (job / ("attempt-" + str(index))).exists()
            assert (job / "budget-floor").read_text() == str(index + 1)
        invoke("run", "--root", str(job), "--execute", "yes")
        assert not (job / "attempt-3").exists()
        # A lost reservation suffix cannot restore a network budget.
        (job / "attempt-2").unlink()
        invoke("run", "--root", str(job), "--execute", "yes")
        assert not (job / "attempt-2").exists()
        changed = root / "changed"
        prepare(changed)
        invoke("verify", "--root", str(changed), "--execute", "yes")
        assert (changed / "attempt-0").exists()
        document = json.loads((changed / "factor.json").read_bytes())
        document["rows"] = [{}]
        (changed / "factor.json").write_bytes(encode(document))
        invoke("run", "--root", str(changed), "--execute", "yes")
        assert not (changed / "attempt-1").exists()
        original = factor["first_day"]
        factor["first_day"] = original - 1
        (capture / "factor.json").write_bytes(encode(factor))
        prepare(root / "wrong-coverage", ok=False)
        factor["first_day"] = original
        factor["coverage_complete"] = False
        (capture / "factor.json").write_bytes(encode(factor))
        prepare(root / "draft", ok=False)
        assert not (root / "draft").exists()
        factor["coverage_complete"] = True
        (capture / "factor.json").write_bytes(encode(factor))
        settings.update(environment="production", bucket="history-cache-production", prefix="r2-history-production/")
        profile.write_bytes(encode(settings))
        prepare(root / "production", ok=False)
        assert not (root / "production").exists()
        production = root / "production"
        invoke("prepare", "--capture", str(capture), "--storage-config", str(profile),
               "--root", str(production), "--allow-production", "yes", ok=True)
        for action in ("run", "verify"):
            invoke(action, "--root", str(production), "--execute", "yes")
            assert not (production / "attempt-0").exists()
        invoke("run", "--root", str(production), "--execute", "yes", "--allow-production", "no")
        assert not (production / "attempt-0").exists()
    print("PASS factor_cli proof_binding draft_reject production_opt_in persistent_budget suffix_loss network0")


if __name__ == "__main__":
    main()
