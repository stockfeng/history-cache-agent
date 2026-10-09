import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'docker'))
import actions_remote as remote
import deploy_agent as deploy


class FingerprintDiagnosticsTests(unittest.TestCase):
    def test_detects_order_only_without_weakening_real_gate(self):
        before = dict(Id='a' * 64, Image='fixture', Config={}, HostConfig={'OomKillDisable': False},
                      Mounts=[dict(Destination='/first', Source='/a'), dict(Destination='/second', Source='/b')])
        expected = deploy.fingerprint(before)
        after = copy.deepcopy(before)
        after['Mounts'].reverse()
        observed = remote.fingerprint_diagnostics(after, expected)
        self.assertFalse(observed['oom_equivalent'])
        self.assertTrue(observed['matches_with_mount_reordering'])
        after['Mounts'][0]['Source'] = '/changed'
        self.assertFalse(remote.fingerprint_diagnostics(after, expected)['matches_with_mount_reordering'])


if __name__ == '__main__':
    unittest.main()
