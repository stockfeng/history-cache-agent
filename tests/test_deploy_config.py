import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / 'docker' / 'snapshot-agent-config.py'
spec = importlib.util.spec_from_file_location('snapshot_config', MODULE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SnapshotTests(unittest.TestCase):
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
