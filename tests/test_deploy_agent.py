"""Offline deployment state/recovery tests. No real Docker or remote services."""

import copy
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / 'docker' / 'deploy_agent.py'
spec = importlib.util.spec_from_file_location('deploy_agent', MODULE)
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)

OLD, NEW = 'a' * 64, 'b' * 64
IMAGE = 'fixture@sha256:' + 'c' * 64


def container(identity, name, running=True):
    return dict(Id=identity, Name='/' + name, Image='sha256:' + 'd' * 64,
                Config={'Labels': {}}, Mounts=[], HostConfig={'RestartPolicy': {'Name': 'unless-stopped'}},
                State=dict(Running=running, Paused=False, Restarting=False, OOMKilled=False), RestartCount=0)


class FakeDocker:
    def __init__(self, old=True):
        self.containers = {OLD: container(OLD, deploy.NAME)} if old else {}
        self.calls = []
        self.timeouts = []
        self.fail_before = None
        self.fail_after = None
        self.on_mutation = None

    def budget(self, seconds):
        self.timeouts.append(seconds)

    def inspect(self, name):
        for item in self.containers.values():
            if item['Name'] == '/' + name or item['Id'] == name:
                return copy.deepcopy(item)
        return None

    def call(self, args, seconds=15):
        self.calls.append(list(args))
        action = args[0]
        if self.fail_before == action:
            raise deploy.DeployError('injected command failure')
        if action == 'pull':
            return ''
        if args[:2] == ['image', 'inspect']:
            return json.dumps([{'Id': 'sha256:' + 'e' * 64}])
        if action == 'stop':
            self.containers[args[-1]]['State']['Running'] = False
        elif action == 'rename':
            self.containers[args[1]]['Name'] = '/' + args[2]
        elif action == 'create':
            item = container(NEW, deploy.NAME, False)
            item['Image'] = 'sha256:' + 'e' * 64
            label = args[args.index('--label') + 1].split('=', 1)
            item['Config']['Labels'] = dict([label])
            selected = IMAGE if IMAGE in args else 'sha256:' + 'e' * 64
            item['Config']['Cmd'] = args[args.index(selected) + 1:]
            for i, arg in enumerate(args):
                if arg == '-v':
                    mount = args[i + 1].split(':')
                    item['Mounts'].append(dict(Type='bind', Source=mount[0], Destination=mount[1],
                                               RW=len(mount) == 2 or mount[2] != 'ro'))
            self.containers[NEW] = item
        elif action == 'start':
            self.containers[args[1]]['State']['Running'] = True
        elif action == 'rm':
            del self.containers[args[-1]]
        else:
            raise AssertionError('unexpected command: ' + repr(args))
        if self.on_mutation:
            self.on_mutation(args)
        if self.fail_after == action:
            raise subprocess.TimeoutExpired('fake-docker', seconds)
        return NEW if action == 'create' else ''


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.credentials = self.root / 'credentials'
        self.credentials.write_text('{"token":"fixture"}')
        self.credentials.chmod(0o600)
        self.docker = FakeDocker()
        self.now = 0
        self.clock = patch.object(deploy.time, 'monotonic', side_effect=lambda: self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.app = self.instance()

    def sleep(self, seconds):
        self.now += seconds

    def instance(self):
        return deploy.Deployment(self.root / 'state', self.root / 'socket',
                                 self.docker, probe=lambda _: None, sleep=self.sleep)

    def run_deploy(self):
        return self.app.deploy(IMAGE, 1, self.credentials)

    def interrupted(self, action, after=True):
        setattr(self.docker, 'fail_after' if after else 'fail_before', action)
        with self.assertRaises(deploy.UnknownOutcome):
            self.run_deploy()
        self.assertEqual(self.app.journal.read()['pending'], action)
        self.docker.fail_after = self.docker.fail_before = None

    def test_success_retains_old_and_isolates_config(self):
        self.assertEqual(self.run_deploy(), 'COMMITTED')
        self.assertFalse(self.docker.inspect(deploy.BACKUP)['State']['Running'])
        self.assertTrue(self.docker.inspect(deploy.NAME)['State']['Running'])
        record = self.app.journal.read()
        self.credentials.write_text('{"token":"changed"}')
        self.assertEqual(json.loads((Path(record['release']) / 'credentials.json').read_bytes()), {'token': 'fixture'})
        self.assertNotIn('token', (self.app.journal.root / 'active.json').read_text())
        self.assertFalse(any('cloud_gateway' in str(c) for c in self.docker.calls))
        create = next(c for c in self.docker.calls if c[0] == 'create')
        for flag, value in [('--memory', '128m'), ('--memory-swap', '128m'), ('--cpus', '0.25'), ('--pids-limit', '32')]:
            self.assertEqual(create[create.index(flag) + 1], value)

    def policy_input(self):
        source = self.root / 'suspensions.json'
        value = {'schema_version': 1, 'policy': 'a-share-null-placeholder-v1', 'intervals': [
            dict(symbol='000001.SZ', start_ms=1000, end_ms=2000, evidence='synthetic' + 'x' * 70000)]}
        data = json.dumps(value).encode()
        source.write_bytes(data)
        source.chmod(0o600)
        return source, hashlib.sha256(data).hexdigest()

    def policy_probe(self, expected):
        original = deploy.helper
        def helper(name):
            value = original(name)
            if name == 'probe-agent':
                value.suspension_policy = lambda path, actual: self.assertEqual(actual, expected)
            return value
        return patch.object(deploy, 'helper', side_effect=helper)

    def test_policy_frozen_mounted_and_checked_during_accept(self):
        source, sha = self.policy_input()
        with self.policy_probe(dict(sha256=sha, intervals=1)):
            self.assertEqual(self.app.deploy(IMAGE, 1, self.credentials, suspensions=source,
                                             suspensions_sha256=sha), 'COMMITTED')
            source.write_text('{}')
            record = self.app.journal.read()
            frozen = Path(record['release']) / 'suspensions.json'
            self.assertEqual(record['candidate_files'][str(frozen)], sha)
            create = next(call for call in self.docker.calls if call[0] == 'create')
            self.assertEqual(create[create.index('--suspensions-sha256') + 1], sha)
            frozen.chmod(0o600)
            frozen.write_text('{}')
            with self.assertRaises(deploy.DeployError):
                self.instance().accept()
            self.assertIn(OLD, self.docker.containers)
            self.assertEqual(self.instance().rollback(), 'ROLLED_BACK')

    def test_wrong_policy_pin_never_stops_old(self):
        source, _ = self.policy_input()
        with self.assertRaises(ValueError):
            self.app.deploy(IMAGE, 1, self.credentials, suspensions=source, suspensions_sha256='f' * 64)
        self.assertFalse(any(c[0] in ('stop', 'rename', 'create', 'start') for c in self.docker.calls))
        self.assertTrue(self.docker.containers[OLD]['State']['Running'])

    def test_policy_cannot_disappear_and_old_policy_survives_unknown_start(self):
        source, sha = self.policy_input()
        self.docker.containers[OLD]['Mounts'] = [dict(Type='bind', RW=False, Source=str(source),
            Destination='/run/config/history-suspensions.json')]
        with self.assertRaises(deploy.DeployError):
            self.run_deploy()
        self.assertEqual(self.docker.calls, [])
        self.docker.fail_after = 'start'
        with self.assertRaises(deploy.UnknownOutcome):
            self.app.deploy(IMAGE, 1, self.credentials, suspensions=source, suspensions_sha256=sha)
        self.docker.fail_after = None
        self.assertEqual(self.instance().recover(True), 'ROLLED_BACK')
        self.assertEqual(deploy.config_files(self.docker.inspect(deploy.NAME), self.app.socket_dir)[str(source)], sha)

    def test_loaded_policy_mismatch_rolls_back(self):
        source, sha = self.policy_input()
        original = deploy.helper
        def helper(name):
            value = original(name)
            if name == 'probe-agent':
                def mismatch(*_):
                    raise ValueError('policy mismatch')
                value.suspension_policy = mismatch
            return value
        with patch.object(deploy, 'helper', side_effect=helper), self.assertRaises(ValueError):
            self.app.deploy(IMAGE, 1, self.credentials, suspensions=source, suspensions_sha256=sha)
        self.assertEqual(self.app.journal.read()['phase'], 'ROLLED_BACK')
        self.assertTrue(self.docker.containers[OLD]['State']['Running'])

    def test_only_policy_mount_has_larger_size_budget(self):
        source, sha = self.policy_input()
        item = container(OLD, deploy.NAME)
        item['Mounts'] = [dict(Type='bind', RW=False, Source=str(source),
                             Destination='/run/config/history-suspensions.json')]
        self.assertEqual(deploy.config_files(item, self.app.socket_dir), {str(source): sha})
        item['Mounts'][0]['Destination'] = '/run/config/other.json'
        with self.assertRaises(deploy.DeployError):
            deploy.config_files(item, self.app.socket_dir)

    def test_fingerprint_only_normalizes_explicit_false_null_oom_default(self):
        before = container(NEW, deploy.NAME)
        before['HostConfig']['OomKillDisable'] = False
        after = copy.deepcopy(before)
        after['HostConfig']['OomKillDisable'] = None
        self.assertTrue(deploy.matches_fingerprint(after, deploy.fingerprint(before)))
        self.assertTrue(deploy.matches_fingerprint(before, deploy.fingerprint(after)))
        for change in ({'OomKillDisable': True}, {'OomKillDisable': 0}, {'Memory': 1},
                       {'NanoCpus': 1000000000}, {'Privileged': True}):
            changed = copy.deepcopy(after)
            changed['HostConfig'].update(change)
            self.assertFalse(deploy.matches_fingerprint(changed, deploy.fingerprint(before)))
        missing = copy.deepcopy(after)
        del missing['HostConfig']['OomKillDisable']
        self.assertFalse(deploy.matches_fingerprint(missing, deploy.fingerprint(before)))

    def test_accept_after_docker_materializes_oom_default(self):
        def normalize(args):
            if args[0] == 'create':
                self.docker.containers[NEW]['HostConfig']['OomKillDisable'] = False
            elif args[0] == 'start':
                self.docker.containers[NEW]['HostConfig']['OomKillDisable'] = None
        self.docker.on_mutation = normalize
        self.assertEqual(self.run_deploy(), 'COMMITTED')
        recorded = self.app.journal.read()['candidate_fingerprint']
        self.assertEqual(self.instance().accept(), 'ACCEPTED')
        self.assertEqual(self.app.journal.read()['candidate_fingerprint'], recorded)
        self.assertTrue(self.docker.containers[NEW]['State']['Running'])
        self.assertNotIn(OLD, self.docker.containers)

    def test_pull_failure_preserves_running_old(self):
        self.docker.fail_before = 'pull'
        with self.assertRaises(deploy.DeployError):
            self.run_deploy()
        self.assertTrue(self.docker.containers[OLD]['State']['Running'])

    def test_local_exact_image_adjustment_and_host_network_are_explicit(self):
        image = 'sha256:' + 'e' * 64
        self.assertEqual(self.app.deploy(image, 1, self.credentials, local_image=True,
                                        adjustment=True, network='host'), 'COMMITTED')
        self.assertFalse(any(c[0] == 'pull' for c in self.docker.calls))
        create = next(c for c in self.docker.calls if c[0] == 'create')
        self.assertEqual(create[create.index('--network') + 1], 'host')
        self.assertEqual(create[create.index('--adjustment') + 1], 'yes')
        self.assertTrue(self.app.journal.read()['local_image'])
        self.assertEqual(self.instance().rollback(), 'ROLLED_BACK')

    def test_local_tag_or_wrong_image_identity_rejected_before_old_stop(self):
        for image, local in [('mutable:latest', True), ('sha256:' + 'e' * 64, False),
                             ('sha256:' + 'f' * 64, True)]:
            with self.assertRaises(deploy.DeployError):
                self.app.deploy(image, 1, self.credentials, local_image=local)
        self.assertTrue(self.docker.containers[OLD]['State']['Running'])
        self.assertFalse(any(c[0] == 'stop' for c in self.docker.calls))
        self.assertIsNone(self.app.journal.read())
        self.assertFalse(any(c[0] == 'stop' for c in self.docker.calls))

    def test_uncertain_mutation_requires_ack_and_new_process_restores(self):
        self.interrupted('create')
        calls = len(self.docker.calls)
        restarted = self.instance()
        with self.assertRaisesRegex(deploy.DeployError, 'quiescence'):
            restarted.recover()
        self.assertEqual(len(self.docker.calls), calls)
        self.assertEqual(restarted.recover(True), 'ROLLED_BACK')
        self.assertEqual(self.docker.inspect(deploy.NAME)['Id'], OLD)
        self.assertTrue(self.docker.containers[OLD]['State']['Running'])
        calls = len(self.docker.calls)
        self.assertEqual(self.instance().recover(), 'ROLLED_BACK')
        self.assertEqual(len(self.docker.calls), calls)

    def test_failed_mutation_before_apply_is_also_unknown(self):
        self.interrupted('create', after=False)
        self.assertEqual(self.instance().recover(True), 'ROLLED_BACK')
        self.assertEqual(set(self.docker.containers), {OLD})

    def test_unknown_stop_rename_start_boundaries(self):
        for action in ('stop', 'rename', 'start'):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                self.docker = FakeDocker()
                self.app = self.instance()
                self.interrupted(action)
                self.assertEqual(self.instance().recover(True), 'ROLLED_BACK')
                self.assertEqual(set(self.docker.containers), {OLD})
                self.assertEqual(self.docker.inspect(deploy.NAME)['Id'], OLD)

    def test_health_failure_rolls_back_without_ack(self):
        def probe(_):
            if self.docker.inspect(deploy.NAME)['Id'] == NEW:
                raise OSError('bad candidate')
        self.app.probe = probe
        with self.assertRaises(OSError):
            self.run_deploy()
        self.assertEqual(self.app.journal.read()['phase'], 'ROLLED_BACK')
        self.assertTrue(self.docker.containers[OLD]['State']['Running'])

    def test_initial_install_failure_restores_absence(self):
        self.docker.containers.clear()
        self.app.probe = lambda _: (_ for _ in ()).throw(OSError('bad candidate'))
        with self.assertRaises(OSError):
            self.run_deploy()
        self.assertEqual(self.docker.containers, {})
        self.assertEqual(self.app.journal.read()['phase'], 'ROLLED_BACK')

    def test_stopped_old_stays_stopped_after_recovery(self):
        self.docker.containers[OLD]['State']['Running'] = False
        self.interrupted('create')
        self.instance().recover(True)
        self.assertFalse(self.docker.containers[OLD]['State']['Running'])

    def test_conflicting_container_never_deleted(self):
        self.interrupted('create')
        self.docker.containers[NEW]['Config']['Labels'] = {}
        calls = len(self.docker.calls)
        with self.assertRaisesRegex(deploy.DeployError, 'unrelated'):
            self.instance().recover(True)
        self.assertFalse(any(c[0] == 'rm' for c in self.docker.calls[calls:]))

    def test_changed_old_config_blocks_before_deleting_candidate(self):
        self.interrupted('create')
        self.docker.containers[OLD]['Config']['changed'] = True
        with self.assertRaisesRegex(deploy.DeployError, 'configuration changed'):
            self.instance().recover(True)
        self.assertIn(NEW, self.docker.containers)

    def test_changed_old_mounted_file_blocks_recovery(self):
        self.docker.containers[OLD]['Mounts'] = [dict(Type='bind', RW=False, Source=str(self.credentials),
                                                    Destination='/run/secrets/history-cache.json')]
        self.interrupted('create')
        self.credentials.write_text('{"token":"changed"}')
        with self.assertRaisesRegex(deploy.DeployError, 'mounted config changed'):
            self.instance().recover(True)
        self.assertIn(NEW, self.docker.containers)

    def test_pending_deployment_blocks_new_pull(self):
        self.interrupted('stop')
        calls = len(self.docker.calls)
        with self.assertRaisesRegex(deploy.DeployError, 'existing deployment'):
            self.instance().deploy(IMAGE, 1, self.credentials)
        self.assertEqual(calls, len(self.docker.calls))

    def test_accept_then_next_release_archives_journal(self):
        self.run_deploy()
        transaction = self.app.journal.read()['transaction']
        with self.assertRaises(deploy.DeployError):
            self.instance().deploy(IMAGE, 1, self.credentials)
        self.assertEqual(self.instance().accept(), 'ACCEPTED')
        self.assertNotIn(OLD, self.docker.containers)
        self.docker.fail_before = 'pull'
        with self.assertRaises(deploy.DeployError):
            self.instance().deploy(IMAGE, 1, self.credentials)
        self.assertTrue((self.app.journal.root / ('result-' + transaction + '.json')).exists())

    def test_unknown_backup_removal_recovers_accept_not_rollback(self):
        self.run_deploy()
        self.docker.fail_after = 'rm'
        with self.assertRaises(deploy.UnknownOutcome):
            self.instance().accept()
        self.docker.fail_after = None
        self.assertEqual(self.instance().recover(True), 'ACCEPTED')
        self.assertEqual(self.docker.inspect(deploy.NAME)['Id'], NEW)

    def test_lock_rejects_concurrent_deployment(self):
        with self.app.journal.lock(), self.assertRaises(BlockingIOError):
            self.instance().deploy(IMAGE, 1, self.credentials)
        self.assertEqual(self.docker.calls, [])

    def test_restart_policy_admission(self):
        self.docker.containers[OLD]['HostConfig']['RestartPolicy']['Name'] = 'always'
        with self.assertRaisesRegex(deploy.DeployError, 'restart policy'):
            self.run_deploy()
        self.assertEqual(self.docker.calls, [])

    def test_accept_rejects_modified_candidate_without_deleting_backup(self):
        self.run_deploy()
        self.docker.containers[NEW]['Config']['changed'] = True
        with self.assertRaisesRegex(deploy.DeployError, 'candidate configuration changed'):
            self.instance().accept()
        self.assertIn(OLD, self.docker.containers)

    def test_invalid_phase_and_symlink_journal_rejected(self):
        self.interrupted('stop')
        path = self.app.journal.root / 'active.json'
        record = json.loads(path.read_bytes())
        record['phase'] = 'invented'
        path.write_text(json.dumps(record))
        calls = len(self.docker.calls)
        with self.assertRaisesRegex(deploy.DeployError, 'phase'):
            self.instance().recover(True)
        path.rename(path.with_name('moved.json'))
        path.symlink_to(path.with_name('moved.json'))
        with self.assertRaisesRegex(deploy.DeployError, 'unsafe deployment journal'):
            self.instance().recover(True)
        self.assertEqual(len(self.docker.calls), calls)

    def test_explicit_post_commit_rollback(self):
        self.run_deploy()
        calls = len(self.docker.calls)
        self.assertEqual(self.instance().recover(), 'COMMITTED')
        self.assertEqual(calls, len(self.docker.calls))
        self.assertEqual(self.instance().rollback(), 'ROLLED_BACK')
        self.assertEqual(self.docker.inspect(deploy.NAME)['Id'], OLD)

    def test_candidate_restart_rolls_back(self):
        def after(args):
            if args[0] == 'start' and args[1] == NEW:
                self.docker.containers[NEW]['RestartCount'] = 1
        self.docker.on_mutation = after
        with self.assertRaisesRegex(deploy.DeployError, 'restarted'):
            self.run_deploy()
        self.assertEqual(self.app.journal.read()['phase'], 'ROLLED_BACK')

    def test_unknown_rollback_removal_can_resume(self):
        self.run_deploy()
        self.docker.fail_after = 'rm'
        with self.assertRaises(deploy.UnknownOutcome):
            self.instance().rollback()
        self.docker.fail_after = None
        self.assertEqual(self.instance().recover(True), 'ROLLED_BACK')
        self.assertEqual(self.docker.inspect(deploy.NAME)['Id'], OLD)

    def test_prepared_state_recovery_and_unrelated_backup(self):
        self.interrupted('stop', after=False)
        self.docker.containers['f' * 64] = container('f' * 64, deploy.BACKUP)
        calls = len(self.docker.calls)
        with self.assertRaisesRegex(deploy.DeployError, 'unrelated rollback'):
            self.instance().recover(True)
        self.assertEqual(calls, len(self.docker.calls))

    def test_mutation_is_journaled_before_dispatch(self):
        phases = []
        def inspect_intent(args):
            record = self.app.journal.read()
            self.assertEqual(record['pending'], args[0])
            phases.append(record['phase'])
        self.docker.on_mutation = inspect_intent
        self.run_deploy()
        self.assertEqual(phases, ['STOP_OLD', 'RENAME_OLD', 'CREATE', 'START'])

    def test_sigkill_process_then_recover_from_disk(self):
        child_root = self.root / 'child'
        child_root.mkdir()
        state = child_root / 'daemon.json'
        code = '''
import json, os, signal, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from test_deploy_agent import FakeDocker, deploy, IMAGE
root = Path(sys.argv[2])
docker = FakeDocker()
def crash(args):
    if args[0] == 'create':
        (root / 'daemon.json').write_text(json.dumps(docker.containers))
        os.kill(os.getpid(), signal.SIGKILL)
docker.on_mutation = crash
deploy.Deployment(root / 'state', root / 'socket', docker, probe=lambda _: None).deploy(
    IMAGE, 1, Path(sys.argv[3]))
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, str(Path(__file__).parent),
                                 str(child_root), str(self.credentials)], capture_output=True, timeout=10)
        self.assertEqual(result.returncode, -9, result.stderr)
        self.root = child_root
        self.docker.containers = json.loads(state.read_bytes())
        restarted = self.instance()
        self.assertEqual(restarted.journal.read()['pending'], 'create')
        self.assertEqual(restarted.recover(True), 'ROLLED_BACK')
        self.assertEqual(set(self.docker.containers), {OLD})


class DockerDeadlineTests(unittest.TestCase):
    def test_real_timed_out_fake_cli_and_grandchild_are_killed(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'escaped'
            code = ('import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",' +
                    repr('import time,pathlib; time.sleep(1); pathlib.Path(' + repr(str(marker)) + ').touch()') +
                    ']); time.sleep(10)')
            spawn = subprocess.Popen
            docker = deploy.Docker()
            docker.budget(5)
            with patch.object(deploy.subprocess, 'Popen', side_effect=lambda args, **kw: spawn(
                    [sys.executable, '-c', code], **kw)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    docker.call(['stop', OLD], .2)
            time.sleep(1)
            self.assertFalse(marker.exists())
    def test_timeout_kills_cli_group_and_does_not_retry(self):
        docker = deploy.Docker()
        docker.budget(5)
        with patch.object(deploy.subprocess, 'Popen') as spawn, patch.object(deploy.os, 'killpg') as kill:
            child = spawn.return_value
            child.pid = 12345
            child.wait.side_effect = [subprocess.TimeoutExpired('docker', 1), 0]
            with self.assertRaises(subprocess.TimeoutExpired):
                docker.call(['stop', OLD], 1)
            self.assertEqual(spawn.call_count, 1)
            kill.assert_called_once_with(12345, deploy.signal.SIGKILL)
            self.assertLessEqual(child.wait.call_args_list[0].kwargs['timeout'], 1)

    def test_total_budget_prevents_dispatch(self):
        docker = deploy.Docker()
        docker.deadline = 0
        with patch.object(deploy.subprocess, 'Popen') as spawn, self.assertRaises(deploy.DeployError):
            docker.call(['pull', IMAGE], 240)
        spawn.assert_not_called()

    def test_failed_inspect_is_not_container_absence(self):
        docker = deploy.Docker()
        with patch.object(docker, 'call', side_effect=[OLD + '\n', deploy.DeployError('daemon down')]):
            with self.assertRaises(deploy.DeployError):
                docker.inspect(deploy.NAME)
        with patch.object(docker, 'call', return_value=''):
            self.assertIsNone(docker.inspect(deploy.NAME))


if __name__ == '__main__':
    unittest.main()
