"""Atomic local health/metrics and opt-in, rate-limited HTTPS notifications."""

import datetime as dt
import fcntl
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

import ops_archive as archive

NAME = re.compile(r'[a-zA-Z0-9_-]{1,64}\Z')
SEVERITY = {'ok': 0, 'warning': 1, 'critical': 2}


def atomic(path, data):
    fd, temporary = tempfile.mkstemp(prefix='.health-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        archive.sync(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def settings(path):
    path = Path(path)
    info = path.stat()
    archive.require(info.st_uid == os.geteuid() and not info.st_mode & 0o077, 'webhook config must be private')
    value = json.loads(archive.bounded_read(path, 4096))
    archive.require(set(value) == {'url', 'bearer_token'}, 'invalid webhook config')
    parsed = urlsplit(value['url'])
    archive.require(parsed.scheme == 'https' and parsed.hostname and not parsed.username and not parsed.password and
                    not parsed.fragment and all(ord(c) >= 32 for c in value['url']), 'invalid webhook HTTPS URL')
    token = value['bearer_token']
    archive.require(isinstance(token, str) and len(token) <= 2048 and all(32 <= ord(c) < 127 for c in token),
                    'invalid webhook token')
    return value


def send(config, event):
    value = settings(config)
    # Secrets go only to stdin, never argv, response files, metrics or stderr.
    lines = ['url = ' + json.dumps(value['url']), 'header = "Content-Type: application/json"',
             'data-binary = ' + json.dumps(json.dumps(event, separators=(',', ':')))]
    if value['bearer_token']:
        lines.append('header = ' + json.dumps('Authorization: Bearer ' + value['bearer_token']))
    child = subprocess.Popen(['curl', '--disable', '--config', '-', '--silent', '--fail',
                              '--proto', '=https', '--max-time', '8', '--connect-timeout', '3',
                              '--retry', '0', '--max-redirs', '0', '--output', os.devnull,
                              '--write-out', '%{http_code}'],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             start_new_session=True)
    try:
        output, _ = child.communicate(('\n'.join(lines) + '\n').encode(), timeout=10)
        return child.returncode == 0 and re.fullmatch(b'2[0-9]{2}', output) is not None
    finally:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=2)


def validate(instance, report):
    archive.require(NAME.fullmatch(instance), 'invalid monitor instance')
    archive.require(set(report) == {'severity', 'codes', 'metrics', 'network_requests'} and
                    report['severity'] in SEVERITY and report['network_requests'] == 0, 'invalid local health report')
    codes, metrics = report['codes'], report['metrics']
    archive.require(isinstance(codes, list) and len(codes) <= 32 and all(NAME.fullmatch(c) for c in codes), 'invalid alert codes')
    archive.require(isinstance(metrics, dict) and len(metrics) <= 32 and
                    all(NAME.fullmatch(k) and type(v) in (int, float) and math.isfinite(v) for k, v in metrics.items()),
                    'invalid monitor metrics')
    archive.require((report['severity'] == 'ok') == (not codes), 'severity and codes differ')


def publish(root, instance, report, *, webhook_config=None, now=None, sender=send):
    validate(instance, report)
    root = archive.private(root)
    now = int(time.time()) if now is None else now
    fd = os.open(root / 'monitor.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state_path = root / 'delivery.json'
        if state_path.exists():
            state = json.loads(archive.bounded_read(state_path, 16384))
            archive.require(state['instance'] == instance, 'monitor state belongs to another instance')
        else:
            state = dict(instance=instance, observed_at=now, delivered=None, sent_at=0,
                         attempted_at=None, day='', hour='', daily=0, hourly=0, failure=False)
        if now < state['observed_at']:
            report = dict(severity='critical', codes=['monitor_clock_regressed'], metrics={}, network_requests=0)
            frozen = True
        else:
            frozen = False
            state['observed_at'] = now
        signature = json.dumps([report['severity'], sorted(set(report['codes']))], separators=(',', ':'))
        current = dict(instance=instance, observed_at=now, **report)
        delivery = 'disabled' if webhook_config is None else 'suppressed'
        if webhook_config and not frozen:
            utc = dt.datetime.fromtimestamp(now, dt.timezone.utc)
            day, hour = utc.strftime('%Y-%m-%d'), utc.strftime('%Y-%m-%dT%H')
            if state['day'] != day:
                state.update(day=day, daily=0)
            if state['hour'] != hour:
                state.update(hour=hour, hourly=0)
            previous_bad = state['delivered'] is not None and json.loads(state['delivered'])[0] != 'ok'
            needed = (signature != state['delivered'] or now - state['sent_at'] >= 3600) and (
                report['severity'] != 'ok' or previous_bad or state['failure'])
            cooled = state['attempted_at'] is None or now - state['attempted_at'] >= 300
            if needed and cooled and state['daily'] < 48 and state['hourly'] < 4:
                state.update(attempted_at=now, daily=state['daily'] + 1, hourly=state['hourly'] + 1, failure=True)
                atomic(state_path, archive.encode(state))
                atomic(root / 'health.json', archive.encode(current))
                event = dict(instance=instance, observed_at=now, severity=report['severity'], codes=report['codes'],
                             event='recovery' if report['severity'] == 'ok' else 'alert')
                try:
                    success = sender(webhook_config, event)
                except (OSError, ValueError, subprocess.SubprocessError):
                    success = False
                if success:
                    state.update(delivered=signature, sent_at=now, failure=False)
                delivery = 'sent' if success else 'failed'
        atomic(state_path, archive.encode(state))
        current.update(delivery=delivery, notification_failed=state['failure'])
        atomic(root / 'health.json', archive.encode(current))
        metrics = dict(report['metrics'], severity=SEVERITY[report['severity']],
                       last_check_unixtime=now, notification_failed=int(state['failure']))
        lines = [f'r2_cache_{k}{{instance="{instance}"}} {v}\n' for k, v in sorted(metrics.items())]
        atomic(root / 'health.prom', ''.join(lines).encode())
        return current
    finally:
        os.close(fd)
