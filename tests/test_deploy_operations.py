import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'docker'))
import agent_operations as agent
import ops_alert as alerts
import ops_archive as archive
from test_deploy_agent import FakeDocker


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / 'source'
        self.root.mkdir()
        (self.root / 'cycle').mkdir()
        (self.root / 'cycle/a').write_bytes(b'evidence')
        (self.root / 'cycle/empty').mkdir()
        self.target = self.base / 'archives/bundle'

    def create(self):
        return archive.create(self.root, ['cycle'], self.target, {'cycle': 1}, time.monotonic() + 10)

    def test_archive_readback_idempotence_then_prune(self):
        manifest, digest = self.create()
        self.assertEqual(self.create(), (manifest, digest))
        self.assertEqual(archive.verify(self.target, time.monotonic() + 10), (manifest, digest))
        archive.prune(self.root, manifest, time.monotonic() + 10)
        self.assertFalse((self.root / 'cycle').exists())
        archive.prune(self.root, manifest, time.monotonic() + 10)
        self.assertTrue((self.target / 'evidence.tar.gz').exists())

    def test_changed_or_added_local_file_stops_prune(self):
        manifest, _ = self.create()
        (self.root / 'cycle/a').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            archive.prune(self.root, manifest, time.monotonic() + 10)
        (self.root / 'cycle/a').write_bytes(b'evidence')
        (self.root / 'cycle/extra').write_bytes(b'extra')
        with self.assertRaises(ValueError):
            archive.prune(self.root, manifest, time.monotonic() + 10)
        self.assertTrue((self.root / 'cycle/a').exists())

    def test_archive_corruption_and_missing_member_rejected(self):
        self.create()
        (self.target / 'evidence.tar.gz').write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            archive.verify(self.target, time.monotonic() + 10)

    def test_unsafe_input_and_deadline(self):
        (self.root / 'cycle/link').symlink_to(self.root / 'cycle/a')
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.create()
        (self.root / 'cycle/link').unlink()
        with self.assertRaises(ValueError):
            archive.create(self.root, ['cycle'], self.root / 'nested', {}, time.monotonic() + 10)
        with self.assertRaises(ValueError):
            archive.inventory(self.root, ['../source'], time.monotonic() + 10)
        with self.assertRaises(ValueError):
            archive.inventory(self.root, ['cycle'], 0)

    def test_partial_prune_is_resumable(self):
        (self.root / 'cycle/b').write_bytes(b'evidence2')
        manifest, _ = self.create()
        (self.root / 'cycle/a').unlink()
        archive.prune(self.root, manifest, time.monotonic() + 10)
        self.assertFalse((self.root / 'cycle').exists())

    def test_entry_byte_and_disk_bounds(self):
        with patch.object(archive, 'MAX_BYTES', 2), self.assertRaises(ValueError):
            self.create()
        with patch.object(archive, 'MAX_FILES', 1), self.assertRaises(ValueError):
            self.create()
        with patch.object(archive.shutil, 'disk_usage', return_value=Mock(free=0)), self.assertRaises(ValueError):
            self.create()
        self.assertFalse(self.target.exists())

    def test_source_changed_during_readback_never_published(self):
        verify = archive.verify
        def change(path, deadline):
            result = verify(path, deadline)
            (self.root / 'cycle/a').write_bytes(b'changed')
            return result
        with patch.object(archive, 'verify', side_effect=change), self.assertRaises(ValueError):
            self.create()
        self.assertFalse(self.target.exists())


class AlertTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'monitor'
        self.report = dict(severity='critical', codes=['agent_missing'], metrics={}, network_requests=0)
        self.sender = Mock(return_value=True)

    def publish(self, now=10000, report=None, enabled=True):
        return alerts.publish(self.root, 'test', self.report if report is None else report,
                              now=now, webhook_config='unused' if enabled else None, sender=self.sender)

    def test_local_only_no_notifications_or_secret_fields(self):
        result = self.publish(enabled=False)
        self.sender.assert_not_called()
        self.assertEqual(result['delivery'], 'disabled')
        self.assertIn('r2_cache_severity{instance="test"} 2', (self.root / 'health.prom').read_text())
        self.assertEqual((self.root / 'health.json').stat().st_mode & 0o777, 0o600)

    def test_dedup_reminder_recovery_and_restart(self):
        self.assertEqual(self.publish()['delivery'], 'sent')
        self.assertEqual(self.publish(10001)['delivery'], 'suppressed')
        self.assertEqual(self.publish(13600)['delivery'], 'sent')
        ok = dict(severity='ok', codes=[], metrics={}, network_requests=0)
        self.assertEqual(self.publish(13900, ok)['delivery'], 'sent')
        self.assertEqual(self.sender.call_args.args[1]['event'], 'recovery')
        self.assertEqual(self.publish(18000, ok)['delivery'], 'suppressed')
        self.assertEqual(self.sender.call_count, 3)

    def test_failed_delivery_not_credited_and_rate_budget_persists(self):
        self.sender.return_value = False
        for offset in range(4):
            self.assertEqual(self.publish(10800 + offset * 300)['delivery'], 'failed')
        self.assertEqual(self.publish(12000)['delivery'], 'suppressed')
        self.assertEqual(self.sender.call_count, 4)
        state = json.loads((self.root / 'delivery.json').read_bytes())
        self.assertIsNone(state['delivered'])
        self.assertEqual(state['daily'], 4)
        self.assertIn('notification_failed{instance="test"} 1', (self.root / 'health.prom').read_text())

    def test_crash_after_reservation_does_not_immediately_redeliver(self):
        self.sender.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.publish()
        self.sender.side_effect = None
        self.assertEqual(self.publish(10001)['delivery'], 'suppressed')
        self.assertEqual(self.sender.call_count, 1)

    def test_clock_regression_never_resets_delivery_budget(self):
        self.publish()
        result = self.publish(9999)
        self.assertEqual(result['codes'], ['monitor_clock_regressed'])
        self.assertEqual(self.sender.call_count, 1)

    def test_initial_failed_alert_then_healthy_sends_recovery(self):
        self.sender.return_value = False
        self.publish()
        self.sender.return_value = True
        ok = dict(severity='ok', codes=[], metrics={}, network_requests=0)
        result = self.publish(10300, ok)
        self.assertEqual(result['delivery'], 'sent')
        self.assertFalse(result['notification_failed'])
        self.assertEqual(self.sender.call_args.args[1]['event'], 'recovery')

    def test_daily_cap_and_next_day_no_budget_refund(self):
        self.sender.return_value = False
        for hour in range(12):
            for slot in range(4):
                self.publish(86400 + hour * 3600 + slot * 300)
        self.assertEqual(self.sender.call_count, 48)
        self.assertEqual(self.publish(86400 + 13 * 3600)['delivery'], 'suppressed')
        self.publish(2 * 86400)
        self.assertEqual(self.sender.call_count, 49)

    def test_health_code_and_metric_labels_cannot_leak_free_text(self):
        bad = dict(self.report, codes=['token:SECRET'])
        with self.assertRaises(ValueError):
            self.publish(report=bad)
        bad = dict(self.report, metrics={'query': 'SECRET'})
        with self.assertRaises(ValueError):
            self.publish(report=bad)

    def test_webhook_only_https_no_redirect_or_argv_secrets(self):
        path = Path(self.temp.name) / 'webhook.json'
        path.write_text(json.dumps({'url': 'https://fixture.invalid/notify', 'bearer_token': 'SECRET'}))
        path.chmod(0o600)
        with patch.object(alerts.subprocess, 'Popen') as spawn, patch.object(alerts.os, 'killpg'):
            spawn.return_value.pid = 12345
            spawn.return_value.returncode = 0
            spawn.return_value.communicate.return_value = (b'204', b'')
            self.assertTrue(alerts.send(path, {'event': 'alert'}))
            command = spawn.call_args.args[0]
            self.assertNotIn('SECRET', ' '.join(command))
            self.assertNotIn('--location', command)
            self.assertNotIn('--insecure', command)
            self.assertIn(b'SECRET', spawn.return_value.communicate.call_args.args[0])
            spawn.return_value.communicate.return_value = (b'302', b'')
            self.assertFalse(alerts.send(path, {'event': 'alert'}))
        path.write_text(json.dumps({'url': 'http://fixture.invalid', 'bearer_token': ''}))
        with self.assertRaises(ValueError):
            alerts.settings(path)


class AgentOperationsTests(unittest.TestCase):
    def test_logs_only_no_credentials_or_active_log_pruned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'state'
            root.mkdir(mode=0o700)
            secret = root / ('release-' + 'c' * 32)
            secret.mkdir()
            (secret / 'credentials.json').write_bytes(b'DO NOT ARCHIVE')
            for index in range(4):
                tx = '%032x' % index
                (root / ('result-' + tx + '.json')).write_text(json.dumps(
                    dict(phase='ACCEPTED', pending=None, transaction=tx, updated_at=index)))
            destination = Path(directory) / 'archive'
            plan = agent.retain(root, destination, keep=2, age_days=1, now=200000)
            self.assertEqual(len(plan['journals']), 2)
            result = agent.retain(root, destination, execute=True, prune=True, keep=2, age_days=1, now=200000)
            self.assertEqual(result['configuration_snapshots_deleted'], 0)
            self.assertEqual((secret / 'credentials.json').read_bytes(), b'DO NOT ARCHIVE')
            self.assertEqual(len(list(root.glob('result-*.json'))), 2)

    def test_agent_health_without_history_or_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = FakeDocker()
            probe = Mock()
            result = agent.health(root, root / 'socket', docker=docker, probe=probe)
            self.assertEqual(result['severity'], 'ok')
            self.assertEqual(docker.calls, [])
            probe.assert_called_once()
            docker.containers.clear()
            result = agent.health(root, root / 'socket', docker=docker, probe=probe)
            self.assertIn('agent_missing', result['codes'])


if __name__ == '__main__':
    unittest.main()
