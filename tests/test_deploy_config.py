import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / 'docker' / 'snapshot-agent-config.py'
spec = importlib.util.spec_from_file_location('snapshot_config', MODULE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SnapshotTests(unittest.TestCase):
    def test_policy_snapshot_pin_and_failures_before_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, policy = root / 'credentials', root / 'policy'
            source.write_text('{}')
            source.chmod(0o600)
            value = {'schema_version': 1, 'policy': 'a-share-null-placeholder-v1', 'intervals': [
                {'symbol': '000001.SZ', 'start_ms': 1000, 'end_ms': 2000, 'evidence': 'synthetic fixture'}]}
            data = json.dumps(value).encode()
            policy.write_bytes(data)
            policy.chmod(0o600)
            expected = hashlib.sha256(data).hexdigest()
            for file, pin in ((policy, None), (None, expected), (policy, 'f' * 64)):
                with self.assertRaises(ValueError):
                    module.snapshot(root / 'bad', source, suspensions=file, suspensions_sha256=pin)
                self.assertFalse((root / 'bad').exists())
            metadata = module.snapshot(root / 'release', source, suspensions=policy, suspensions_sha256=expected)
            policy.write_text('{}')
            self.assertEqual(metadata, {'sha256': expected, 'intervals': 1})
            self.assertEqual((root / 'release/suspensions.json').read_bytes(), data)
            self.assertEqual((root / 'release/suspensions.json').stat().st_mode & 0o777, 0o400)

    def test_policy_schema_is_bounded_and_unambiguous(self):
        base = {'schema_version': 1, 'policy': 'a-share-null-placeholder-v1', 'intervals': []}
        entry = dict(symbol='000001.SZ', start_ms=1000, end_ms=2000, evidence='synthetic fixture')
        bad = [b'{}', b'[' * 2000, b' ' * (module.POLICY_LIMIT + 1),
               json.dumps(base)[:-1].encode() + b',"schema_version":1}']
        for change in ({'schema_version': True}, {'schema_version': 1.0}, {'extra': 1},
                       {'intervals': [entry] * 10001}):
            bad.append(json.dumps(dict(base, **change)).encode())
        for change in ({'symbol': '00700.HK'}, {'evidence': '  '}, {'start_ms': True},
                       {'end_ms': 2**64 - 1}, {'start_ms': -1}, {'extra': 1}):
            bad.append(json.dumps(dict(base, intervals=[dict(entry, **change)])).encode())
        for data in bad:
            with self.assertRaises(ValueError):
                module.validate_policy(data, hashlib.sha256(data).hexdigest())
        data = json.dumps(dict(base, intervals=[entry])).encode()
        self.assertEqual(module.validate_policy(data, hashlib.sha256(data).hexdigest())['intervals'], 1)

    def test_private_snapshot_independent_of_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.write_text('{"token":"fixture"}')
            source.chmod(0o600)
            target = root / 'config'
            module.snapshot(target, source)
            source.write_text('{"token":"changed"}')
            result = target / 'credentials.json'
            self.assertEqual(json.loads(result.read_bytes()), {'token': 'fixture'})
            self.assertEqual(result.stat().st_mode & 0o777, 0o400)
            self.assertEqual(target.stat().st_mode & 0o777, 0o700)
            with self.assertRaises(FileExistsError):
                module.snapshot(target, source)

    def test_reject_public_symlink_invalid_and_oversized(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.write_text('{}')
            source.chmod(0o644)
            with self.assertRaises(ValueError):
                module.snapshot(root / 'config', source)
            source.chmod(0o600)
            link = root / 'link'
            link.symlink_to(source)
            with self.assertRaises(OSError):
                module.snapshot(root / 'config', link)
            for data in ('[]', 'invalid', ' ' * 16385):
                source.write_text(data)
                with self.assertRaises(ValueError):
                    module.snapshot(root / 'config', source)
            self.assertFalse((root / 'config').exists())


if __name__ == '__main__':
    unittest.main()
