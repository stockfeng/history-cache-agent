import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'docker'))
import actions_remote as remote
import deploy_agent as deploy


class FingerprintDiagnosticsTests(unittest.TestCase):
    def test_accepts_order_only_with_legacy_journal(self):
        before = dict(Id='a' * 64, Image='fixture', Config={}, HostConfig={'OomKillDisable': False},
                      Mounts=[dict(Destination='/first', Source='/a'), dict(Destination='/second', Source='/b')])
        expected = deploy.legacy_fingerprint(before)
        after = copy.deepcopy(before)
        after['Mounts'].reverse()
        observed = remote.fingerprint_diagnostics(after, expected)
        self.assertTrue(observed['oom_equivalent'])
        self.assertTrue(observed['matches_with_mount_reordering'])
        after['Mounts'][0]['Source'] = '/changed'
        self.assertFalse(remote.fingerprint_diagnostics(after, expected)['matches_with_mount_reordering'])

    def test_all_other_config_changes_rejected(self):
        before = dict(Id='a' * 64, Image='fixture', Config={'Cmd': ['old']},
                      HostConfig={'OomKillDisable': False, 'Memory': 128},
                      Mounts=[dict(Destination='/first', Source='/a', RW=False),
                              dict(Destination='/second', Source='/b', RW=False)])
        for expected in (deploy.fingerprint(before), deploy.legacy_fingerprint(before)):
            reordered = copy.deepcopy(before)
            reordered['Mounts'].reverse()
            reordered['HostConfig']['OomKillDisable'] = None
            self.assertTrue(deploy.matches_fingerprint(reordered, expected))
            for field in ('Id', 'Image', 'Config', 'HostConfig', 'Mounts'):
                after = copy.deepcopy(reordered)
                if field in ('Id', 'Image'):
                    after[field] = 'different'
                elif field == 'Config':
                    after[field]['Cmd'] = ['different']
                elif field == 'HostConfig':
                    after[field]['Memory'] = 256
                else:
                    after[field][0]['RW'] = True
                self.assertFalse(deploy.matches_fingerprint(after, expected), field)
        self.assertEqual(deploy.fingerprint(before), deploy.fingerprint(dict(before, Mounts=before['Mounts'][::-1])))


if __name__ == '__main__':
    unittest.main()
