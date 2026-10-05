"""Bounded Agent deployment-log archiving and local liveness monitoring."""

import argparse
import json
import os
from pathlib import Path
import re
import time

import deploy_agent as deploy
import ops_alert
import ops_archive as archive


def retain(root, destination, *, execute=False, prune=False, keep=5, age_days=30, now=None):
    root, destination = Path(root).absolute(), Path(destination).absolute()
    archive.require(not prune or execute, 'prune requires execute')
    archive.require(2 <= keep <= 100 and 1 <= age_days <= 3650, 'invalid retention bounds')
    archive.require(destination != root and root not in destination.parents and destination not in root.parents,
                    'archive destination must be separate')
    now = int(time.time()) if now is None else now
    deadline = time.monotonic() + 60
    journal = deploy.Journal(root)
    with journal.lock():
        active = journal.read()
        archive.require(active is None or active['phase'] in deploy.TERMINAL, 'deployment in progress; archive deferred')
        records = []
        count = 0
        with os.scandir(root) as scan:
            for item in scan:
                count += 1
                archive.require(count <= 10000 and time.monotonic() < deadline, 'deployment state scan exceeds bound')
                if not re.fullmatch(r'result-[a-f0-9]{32}\.json', item.name):
                    continue
                value = json.loads(archive.bounded_read(Path(item.path), 65536))
                archive.require(value['phase'] in ('ROLLED_BACK', 'ACCEPTED') and value['pending'] is None and
                                item.name == 'result-' + value['transaction'] + '.json', 'nonterminal deployment archive')
                records.append((value['updated_at'], item.name, value['transaction']))
        records.sort(reverse=True)
        chosen = [r for r in records[keep:] if now - r[0] >= age_days * 86400 and
                  (active is None or r[2] != active['transaction'])][:8]
        if not execute:
            return dict(status='PLAN', journals=[r[1] for r in chosen], configuration_snapshots_deleted=0)
        results = []
        owner = deploy.digest(str(root))
        for _, name, transaction in chosen:
            target = destination / owner / transaction
            manifest, _ = archive.create(root, [name], target, {'deployment': transaction, 'owner': owner}, deadline)
            if prune:
                archive.prune(root, manifest, deadline)
            results.append(dict(transaction=transaction, status='PRUNED' if prune else 'VERIFIED'))
        return dict(status='COMPLETE', journals=results, configuration_snapshots_deleted=0)


def health(root, socket_dir, *, docker=None, probe=None, now=None):
    root, socket_dir = Path(root).absolute(), Path(socket_dir).absolute()
    now = time.time() if now is None else now
    docker = docker or deploy.Docker()
    probe = probe or deploy.helper('probe-agent').probe
    docker.budget(20)
    journal = deploy.Journal(root)
    record = journal.read()
    codes, metrics = [], {}
    if record:
        archive.require(record['socket_dir'] == str(socket_dir), 'monitor socket differs from deployment')
        if record['pending']:
            codes.append('deployment_unknown')
        elif record['phase'] not in deploy.TERMINAL:
            codes.append('deployment_incomplete')
        if now < record['updated_at']:
            codes.append('clock_regressed')
        if record['phase'] == 'COMMITTED':
            codes.append('backup_acceptance_pending')
    value = docker.inspect(deploy.NAME)
    if value is None:
        codes.append('agent_missing')
    elif (not value['State']['Running'] or value['State'].get('Paused') or
          value['State'].get('Restarting') or value['State'].get('OOMKilled')):
        codes.append('agent_unhealthy')
    else:
        if record and record['phase'] in ('COMMITTED', 'ACCEPTED'):
            if (value['Id'] != record['candidate'] or deploy.fingerprint(value) != record['candidate_fingerprint'] or
                    deploy.config_files(value, socket_dir) != record['candidate_files']):
                codes.append('agent_identity_changed')
        elif record and record['phase'] == 'ROLLED_BACK' and record['old'] and value['Id'] != record['old']['id']:
            codes.append('agent_identity_changed')
        try:
            probe(str(socket_dir / 'agent.sock'))
        except (OSError, ValueError):
            codes.append('agent_probe_failed')
        metrics['agent_restarts'] = value['RestartCount']
        if value['RestartCount'] > 0:
            codes.append('agent_restarted')
    count = 0
    if root.exists():
        with os.scandir(root) as scan:
            for _ in scan:
                count += 1
                if count >= 10000:
                    codes.append('deployment_state_full')
                    break
    metrics['deployment_state_entries'] = count
    if count >= 1000:
        codes.append('deployment_archive_required')
    critical = set(codes) - {'backup_acceptance_pending', 'agent_restarted', 'deployment_archive_required'}
    return dict(severity='critical' if critical else 'warning' if codes else 'ok',
                codes=sorted(set(codes)), metrics=metrics, network_requests=0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('archive', 'monitor'))
    parser.add_argument('--root', type=Path, default=Path('/var/lib/history-cache/deployments'))
    parser.add_argument('--socket-dir', type=Path, default=Path('/var/lib/history-cache/socket'))
    parser.add_argument('--archive-root', type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--prune', action='store_true')
    parser.add_argument('--keep', type=int, default=5)
    parser.add_argument('--age-days', type=int, default=30)
    parser.add_argument('--monitor-state', type=Path)
    parser.add_argument('--instance', default='history-agent')
    parser.add_argument('--webhook-config', type=Path)
    args = parser.parse_args()
    if args.action == 'archive':
        archive.require(args.archive_root, '--archive-root required')
        result = retain(args.root, args.archive_root, execute=args.execute, prune=args.prune,
                        keep=args.keep, age_days=args.age_days)
    else:
        archive.require(args.monitor_state, '--monitor-state required')
        try:
            report = health(args.root, args.socket_dir)
        except (OSError, ValueError, TypeError, KeyError, deploy.subprocess.SubprocessError):
            report = dict(severity='critical', codes=['monitor_read_failed'], metrics={}, network_requests=0)
        result = ops_alert.publish(args.monitor_state, args.instance, report, webhook_config=args.webhook_config)
    print(json.dumps(result))
    return 2 if result.get('severity') in ('critical', 'warning') or result.get('delivery') == 'failed' else 0


if __name__ == '__main__':
    raise SystemExit(main())
