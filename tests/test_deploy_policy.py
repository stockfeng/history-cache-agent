"""Create-only policy delivery; no Docker, SSH or R2 operations."""

import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'docker'))
import actions_deploy as runner
import actions_remote as remote


class PolicyDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bundle = self.root / 'bundle'
        self.bundle.mkdir(mode=0o700)
        self.config = self.root / 'config'
        self.data = json.dumps(dict(schema_version=1, policy='a-share-null-placeholder-v1', intervals=[
            dict(symbol='000001.SZ', start_ms=1000, end_ms=2000, evidence='synthetic fixture')])).encode()
        self.pin = hashlib.sha256(self.data).hexdigest()
        for name, data in [('suspensions.json', self.data), ('suspensions.sha256', (self.pin + '\n').encode())]:
            path = self.bundle / name
            path.write_bytes(data)
            path.chmod(0o400)
        self.patcher = patch.object(remote, 'CONFIG_ROOT', self.config)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_install_and_idempotent(self):
        self.assertEqual(remote.install_policy(self.bundle)['status'], 'POLICY_INSTALLED')
        self.assertEqual(remote.install_policy(self.bundle)['status'], 'POLICY_ALREADY_INSTALLED')
        for path in self.config.iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)
            self.assertEqual(path.stat().st_nlink, 1)
        self.assertEqual(remote.suspension_config()[2], dict(sha256=self.pin, intervals=1))

    def test_partial_identical_delivery_can_finish(self):
        self.config.mkdir(mode=0o700)
        path = self.config / 'suspensions.json'
        path.write_bytes(self.data)
        path.chmod(0o400)
        self.assertEqual(remote.install_policy(self.bundle)['status'], 'POLICY_INSTALLED')

    def test_different_existing_policy_not_overwritten(self):
        remote.install_policy(self.bundle)
        path = self.config / 'suspensions.sha256'
        path.chmod(0o600)
        path.write_text('0' * 64 + '\n')
        with self.assertRaises(ValueError):
            remote.install_policy(self.bundle)
        self.assertEqual(path.read_text(), '0' * 64 + '\n')

    def test_symlink_and_hash_mismatch_fail_before_install(self):
        self.config.mkdir(mode=0o700)
        path = self.config / 'suspensions.json'
        path.symlink_to(self.bundle / 'suspensions.json')
        with self.assertRaises(ValueError):
            remote.install_policy(self.bundle)
        path.unlink()
        pin = self.bundle / 'suspensions.sha256'
        pin.chmod(0o600)
        pin.write_text('0' * 64 + '\n')
        with self.assertRaises(ValueError):
            remote.install_policy(self.bundle)
        self.assertFalse(path.exists())

    def test_transport_contains_only_policy_and_helpers(self):
        request = dict(target='oracle', operation='install-policy', image='', revision='a' * 40,
                       bake=120, adjustment=True)
        remote.validate_request(request)
        env = dict(AGENT_POLICY_DIRECTORY=str(self.bundle), AGENT_POLICY_SHA256=self.pin,
                   AGENT_R2_CREDENTIALS_JSON='must-not-transmit', AGENT_GHCR_TOKEN='must-not-transmit')
        payload = runner.bundle(request, env)
        self.assertNotIn(b'must-not-transmit', payload)
        with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
            self.assertEqual(set(archive.getnames()), set(runner.HELPERS) |
                             {'request.json', 'suspensions.json', 'suspensions.sha256'})

    def extension(self, rows=None):
        value = json.loads(self.data)
        value['intervals'] = rows if rows is not None else value['intervals'] + [
            dict(symbol='000002.SZ', start_ms=3000, end_ms=4000, evidence='additional fixture')]
        data = json.dumps(value).encode()
        pin = hashlib.sha256(data).hexdigest()
        for name, payload in [('suspensions.json', data), ('suspensions.sha256', (pin + '\n').encode()),
                              ('policy-previous.sha256', (self.pin + '\n').encode())]:
            path = self.bundle / name
            path.chmod(0o600) if path.exists() else None
            path.write_bytes(payload)
            path.chmod(0o400)
        return data, pin

    def test_additive_extension_preserves_backups_and_is_idempotent(self):
        remote.install_policy(self.bundle)
        data, pin = self.extension()
        result = remote.install_policy(self.bundle)
        self.assertEqual(result['status'], 'POLICY_EXTENDED')
        self.assertFalse(result['containers_changed'])
        backup = Path(result['backup'])
        self.assertEqual((backup / 'old-suspensions.json').read_bytes(), self.data)
        self.assertEqual((backup / 'new-suspensions.json').read_bytes(), data)
        self.assertEqual(remote.suspension_config()[2], dict(sha256=pin, intervals=2))
        self.assertEqual(remote.install_policy(self.bundle), result)

    def test_extension_rejects_removed_interval_and_changed_evidence(self):
        remote.install_policy(self.bundle)
        original = json.loads(self.data)['intervals'][0]
        for rows in ([], [dict(original, evidence='changed')], [original, original]):
            self.extension(rows)
            with self.assertRaises(ValueError):
                remote.install_policy(self.bundle)
            self.assertEqual((self.config / 'suspensions.json').read_bytes(), self.data)

    def test_extension_rejects_wrong_previous_pin(self):
        remote.install_policy(self.bundle)
        self.extension()
        path = self.bundle / 'policy-previous.sha256'
        path.chmod(0o600)
        path.write_text('f' * 64 + '\n')
        with self.assertRaises(ValueError):
            remote.install_policy(self.bundle)
        self.assertEqual(remote.suspension_config()[1], self.pin)

    def test_partial_pair_resumes_without_changing_preimage(self):
        remote.install_policy(self.bundle)
        data, pin = self.extension()
        real = remote.policy_write
        def fail_pin(path, payload, replace=False):
            if path == self.config / 'suspensions.sha256' and replace:
                raise OSError('simulated interrupted pair')
            return real(path, payload, replace)
        with patch.object(remote, 'policy_write', side_effect=fail_pin):
            with self.assertRaises(OSError):
                remote.install_policy(self.bundle)
        self.assertEqual((self.config / 'suspensions.json').read_bytes(), data)
        with self.assertRaises(ValueError):
            remote.suspension_config()
        result = remote.install_policy(self.bundle)
        self.assertEqual(remote.suspension_config()[1], pin)
        self.assertEqual((Path(result['backup']) / 'old-suspensions.json').read_bytes(), self.data)

    def test_interrupted_backup_creation_resumes(self):
        remote.install_policy(self.bundle)
        self.extension()
        real = remote.policy_write
        def fail_marker(path, payload, replace=False):
            if path.name == 'intent.json':
                raise OSError('interrupted before marker')
            return real(path, payload, replace)
        with patch.object(remote, 'policy_write', side_effect=fail_marker):
            with self.assertRaises(OSError):
                remote.install_policy(self.bundle)
        self.assertEqual(remote.suspension_config()[1], self.pin)
        self.assertEqual(remote.install_policy(self.bundle)['status'], 'POLICY_EXTENDED')

    def test_upgrade_transport_binds_previous_hash(self):
        request = dict(target='oracle', operation='install-policy', image='', revision='a' * 40,
                       bake=120, adjustment=True)
        data, pin = self.extension()
        env = dict(AGENT_POLICY_DIRECTORY=str(self.bundle), AGENT_POLICY_SHA256=pin,
                   AGENT_POLICY_PREVIOUS_SHA256=self.pin)
        with tarfile.open(fileobj=io.BytesIO(runner.bundle(request, env))) as archive:
            self.assertEqual(archive.extractfile('policy-previous.sha256').read(), (self.pin + '\n').encode())
        env['AGENT_POLICY_PREVIOUS_SHA256'] = pin
        with self.assertRaises(ValueError):
            runner.bundle(request, env)


if __name__ == '__main__':
    unittest.main()
