"""Offline Actions transport/preflight tests. No SSH, Docker daemon or R2 access."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'docker'))
import actions_deploy as runner
import actions_remote as remote
import test_deploy_agent as deploy_fixture
from test_deploy_agent import FakeDocker

ROOT = Path(__file__).resolve().parents[1]
REVISION = 'a' * 40
IMAGE = 'ghcr.io/stockfeng/history-cache-agent@sha256:' + 'b' * 64
CREDENTIALS = dict(account_id='c' * 32, access_key_id='fixture-key', secret_access_key='fixture-secret')
PROFILE = dict(version=1, environment='production', account_id='c' * 32, bucket='history-cache-production',
               prefix='r2-history-production/', jurisdiction='default', role='reader')


def request(operation='deploy'):
    return dict(target='oracle', operation=operation, image=IMAGE if operation == 'deploy' else '',
                revision=REVISION, bake=30, adjustment=True)


def environment():
    return dict(AGENT_TARGET='oracle', AGENT_OPERATION='deploy', AGENT_IMAGE=IMAGE,
                AGENT_RELEASE_COMMIT=REVISION, AGENT_BAKE_SECONDS='30', AGENT_ADJUSTMENT='true',
                AGENT_SSH_PASSWORD='fixture-password', AGENT_SSH_KNOWN_HOSTS='fixture-host-key',
                AGENT_GHCR_USER='fixture-user', AGENT_GHCR_TOKEN='fixture-token')


def private_json(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)


class RequestTests(unittest.TestCase):
    def test_exact_request(self):
        self.assertEqual(runner.make_request(environment()), request())
        for operation in remote.OPERATIONS:
            remote.validate_request(request(operation))

    def test_reject_mutable_foreign_injected_digest_and_invalid_controls(self):
        bad = [('image', 'latest'), ('image', IMAGE + '; echo injected'),
               ('image', IMAGE.replace('history-cache-agent', 'cloud_gateway_v2')),
               ('revision', 'main'), ('target', 'all'), ('operation', 'shell'), ('bake', True),
               ('bake', 29), ('bake', 601), ('adjustment', 'true')]
        for key, value in bad:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                changed = request()
                changed[key] = value
                remote.validate_request(changed)
        changed = request('rollback')
        changed['image'] = IMAGE
        with self.assertRaises(ValueError):
            remote.validate_request(changed)

    def test_production_reader_only(self):
        remote.validate_storage(json.dumps(CREDENTIALS).encode(), json.dumps(PROFILE).encode())
        for key, value in [('environment', 'staging'), ('role', 'publisher'), ('version', True),
                           ('prefix', 'r2-history-staging/'), ('account_id', 'd' * 32),
                           ('bucket', 'history-cache-staging')]:
            profile = dict(PROFILE, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                remote.validate_storage(json.dumps(CREDENTIALS).encode(), json.dumps(profile).encode())
        duplicate = json.dumps(CREDENTIALS)[:-1] + ', "account_id": "' + 'c' * 32 + '"}'
        with self.assertRaises(ValueError):
            remote.validate_storage(duplicate.encode(), json.dumps(PROFILE).encode())


class TransportTests(unittest.TestCase):
    def test_password_only_in_environment_single_pinned_target(self):
        for target, host in runner.HOSTS.items():
            with self.subTest(target=target), tempfile.TemporaryDirectory() as name:
                value = dict(request(), target=target)
                command, env = runner.ssh_command(value, environment(), Path(name))
                self.assertEqual(command.count('root@' + host), 1)
                self.assertEqual(command[-1], runner.REMOTE_COMMAND)
                self.assertIn('StrictHostKeyChecking=yes', command)
                self.assertNotIn('fixture-password', ' '.join(command))
                self.assertEqual(env['SSHPASS'], 'fixture-password')
                self.assertFalse(any(key.startswith('AGENT_') for key in env))
                self.assertNotIn('cloud_gateway', command[-1])

    def test_key_and_missing_ambiguous_auth(self):
        with tempfile.TemporaryDirectory() as name:
            env = environment()
            env.pop('AGENT_SSH_PASSWORD')
            env['AGENT_SSH_KEY'] = 'fixture-private-key'
            command, child = runner.ssh_command(request(), env, Path(name))
            self.assertEqual(command[0], 'ssh')
            self.assertIn('BatchMode=yes', command)
            self.assertNotIn('SSHPASS', child)
            self.assertEqual((Path(name) / 'key').stat().st_mode & 0o777, 0o600)
            for changes in ({'AGENT_SSH_PASSWORD': 'both'}, {'AGENT_SSH_KEY': ''},
                            {'AGENT_SSH_KNOWN_HOSTS': ''}):
                with self.assertRaises(ValueError):
                    runner.ssh_command(request(), dict(env, **changes), Path(name))

    def test_bundle_allowlist_no_ssh_secrets(self):
        env = environment()
        env.update(AGENT_R2_CREDENTIALS_JSON=json.dumps(CREDENTIALS), AGENT_STORAGE_READER_JSON=json.dumps(PROFILE))
        payload = runner.bundle(request(), env)
        with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
            self.assertEqual(set(archive.getnames()), set(runner.HELPERS) |
                             {'request.json', 'registry.json', 'credentials.json', 'storage.json'})
            self.assertTrue(all(item.mode == 0o600 and item.isfile() for item in archive))
        self.assertNotIn(b'fixture-password', payload)
        self.assertNotIn(b'fixture-host-key', payload)
        payload = runner.bundle(request('status'), env)
        self.assertNotIn(b'fixture-secret', payload)
        self.assertNotIn(b'fixture-token', payload)

    def test_existing_server_config_allowed_but_partial_secret_rejected(self):
        with tarfile.open(fileobj=io.BytesIO(runner.bundle(request(), environment()))) as archive:
            self.assertNotIn('credentials.json', archive.getnames())
        with self.assertRaises(ValueError):
            runner.bundle(request(), dict(environment(), AGENT_R2_CREDENTIALS_JSON=json.dumps(CREDENTIALS)))

    def test_main_bounded_one_ssh_call_and_no_retry(self):
        with patch.dict(os.environ, environment(), clear=True), patch.object(runner.subprocess, 'Popen') as run:
            run.return_value.returncode = 255
            with self.assertRaises(ValueError):
                runner.main()
            self.assertEqual(run.call_count, 1)
            self.assertTrue(run.call_args.kwargs['start_new_session'])
            communicate = run.return_value.communicate.call_args.kwargs
            self.assertEqual(communicate['timeout'], 1200)
            self.assertIsInstance(communicate['input'], bytes)
            key_path = next(value for value in run.call_args.args[0] if value.startswith('UserKnownHostsFile='))
            self.assertFalse(Path(key_path.split('=', 1)[1]).exists())

    def test_transport_timeout_kills_local_group_without_retry(self):
        with patch.dict(os.environ, environment(), clear=True), \
                patch.object(runner.subprocess, 'Popen') as run, patch.object(runner.os, 'killpg') as kill:
            run.return_value.communicate.side_effect = [subprocess.TimeoutExpired('ssh', 1200), (None, None)]
            run.return_value.pid = 123
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.main()
            kill.assert_called_once_with(123, runner.signal.SIGKILL)
            self.assertEqual(run.call_count, 1)

    def test_actual_stdin_tar_extraction_and_cleanup_without_ssh(self):
        payload = runner.bundle(request('status'), environment())
        with tempfile.TemporaryDirectory() as name:
            # The fake interpreter checks transport material, never executes the deployment adapter.
            interpreter = Path(name) / 'python3'
            interpreter.write_text('#!/bin/sh\nset -eu\ntest "$1" = "-B"\n'
                                   'test -f "$(dirname "$2")/request.json"\nprintf "%s" "$(dirname "$2")"\n')
            interpreter.chmod(0o700)
            result = subprocess.run(['bash', '-c', runner.REMOTE_COMMAND], input=payload,
                                    env=dict(os.environ, PATH=name + ':' + os.environ.get('PATH', '/usr/bin')),
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=True)
            extracted = Path(result.stdout.decode())
            self.assertTrue(str(extracted).startswith('/tmp/history-cache-actions.'))
            self.assertFalse(extracted.exists())


class HostTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        private_json(self.root / 'credentials.json', CREDENTIALS)
        private_json(self.root / 'storage.json', PROFILE)

    def test_private_config_reject_symlink_permissions_and_hardlink(self):
        self.assertEqual(remote.config_paths(self.root), (self.root / 'credentials.json', self.root / 'storage.json'))
        source = self.root / 'credentials.json'
        source.chmod(0o644)
        with self.assertRaises(ValueError):
            remote.config_paths(self.root)
        source.chmod(0o600)
        link = self.root / 'link'
        link.symlink_to(source)
        with self.assertRaises(ValueError):
            remote.private_read(link)
        link.unlink()
        os.link(source, link)
        with self.assertRaises(ValueError):
            remote.private_read(source)

    def test_registry_uses_private_temporary_config_and_stdin(self):
        private_json(self.root / 'registry.json', dict(user='fixture-user', token='fixture-token'))
        with patch.dict(os.environ, {}, clear=True), patch.object(remote.subprocess, 'run') as run:
            run.return_value.returncode = 0
            with remote.registry_session(self.root):
                directory = Path(os.environ['DOCKER_CONFIG'])
                self.assertTrue(directory.is_dir())
            self.assertFalse(directory.exists())
            self.assertNotIn('DOCKER_CONFIG', os.environ)
            self.assertEqual(run.call_args.kwargs['input'], b'fixture-token')
            self.assertNotIn('fixture-token', ' '.join(run.call_args.args[0]))

    def test_resource_gates_before_deploy(self):
        info = dict(Architecture='x86_64', OSType='linux', MemoryLimit=True, SwapLimit=True,
                    CpuCfsQuota=True, PidsLimit=True, SecurityOptions=[], DockerRootDir=str(self.root))
        disk = SimpleNamespace(f_bavail=2048, f_frsize=remote.MIB)
        with patch.object(remote, 'docker_read', return_value=json.dumps(info)), \
                patch.object(remote.Path, 'read_text', return_value='MemAvailable: 524288 kB\n'), \
                patch.object(remote.os, 'statvfs', return_value=disk), \
                patch.object(remote, 'SOCKET_ROOT', self.root / 'socket'):
            self.assertEqual(remote.resource_preflight()['architecture'], 'amd64')
        for key, value in [('MemoryLimit', False), ('SwapLimit', False), ('Architecture', 'unknown'),
                           ('SecurityOptions', ['name=rootless'])]:
            with patch.object(remote, 'docker_read', return_value=json.dumps(dict(info, **{key: value}))), \
                    self.assertRaises(ValueError):
                remote.resource_preflight()
        with patch.object(remote, 'docker_read', return_value=json.dumps(info)), \
                patch.object(remote.Path, 'read_text', return_value='MemAvailable: 1000 kB\n'), \
                self.assertRaises(ValueError):
            remote.resource_preflight()
        with patch.object(remote, 'docker_read', return_value=json.dumps(info)), \
                patch.object(remote.os, 'statvfs', return_value=SimpleNamespace(f_bavail=1, f_frsize=4096)), \
                self.assertRaises(ValueError):
            remote.resource_preflight()

    def test_image_checks_platform_revision_and_entrypoint_before_mutation(self):
        value = dict(Os='linux', Architecture='amd64', RepoDigests=[IMAGE],
                     Config=dict(User='', Entrypoint=['/opt/history-cache/bin/history-cache-agent'],
                                 Labels={'org.opencontainers.image.revision': REVISION}))
        docker = remote.CheckedDocker('amd64', REVISION)
        with patch.object(remote.deploy.Docker, 'call', return_value=json.dumps([value])):
            docker.call(['image', 'inspect', IMAGE])
        for field, bad in [('Architecture', 'arm64'), ('RepoDigests', []),
                           ('Config', dict(value['Config'], User='1000')),
                           ('Config', dict(value['Config'], Labels={})),
                           ('Config', dict(value['Config'], Entrypoint=['/bin/sh'])),
                           ('Config', dict(value['Config'], Volumes={'/data': {}}))]:
            changed = dict(value, **{field: bad})
            with patch.object(remote.deploy.Docker, 'call', return_value=json.dumps([changed])), \
                    self.assertRaises(ValueError):
                docker.call(['image', 'inspect', IMAGE])

    def test_adapter_real_journal_deploy_then_rollback(self):
        docker = FakeDocker(old=False)
        app = remote.deploy.Deployment(self.root / 'state', self.root / 'socket', docker, probe=lambda _: None)
        original = app.deploy
        # Exercise real state transitions without a 30-second sleep or a Docker daemon.
        def shortened(image, bake, credentials, profile, **kwargs):
            self.assertEqual(bake, 30)
            return original(image, 1, credentials, profile, **kwargs)
        with patch.object(remote.deploy, 'Deployment', return_value=app), \
                patch.object(deploy_fixture, 'IMAGE', IMAGE), \
                patch.object(remote, 'CheckedDocker', return_value=docker), \
                patch.object(remote, 'resource_preflight', return_value={'architecture': 'amd64'}), \
                patch.object(remote, 'registry_session', return_value=contextlib.nullcontext()), \
                patch.object(app, 'deploy', side_effect=shortened):
            before = list(docker.calls)
            self.assertEqual(remote.operate(request('preflight'), self.root)['status'], 'PREFLIGHT_OK')
            self.assertEqual(before, docker.calls)
            self.assertEqual(remote.operate(request(), self.root)['status'], 'COMMITTED')
            self.assertEqual(remote.operate(request('status'), self.root)['phase'], 'COMMITTED')
            with self.assertRaises(ValueError):
                remote.operate(request(), self.root)
            self.assertEqual(remote.operate(request('rollback'), self.root)['status'], 'ROLLED_BACK')
            self.assertEqual(docker.containers, {})
            self.assertFalse(any('cloud_gateway' in str(call) for call in docker.calls))

    def test_recover_never_acknowledges_unknown(self):
        with patch.object(remote.deploy, 'Deployment') as factory:
            remote.operate(request('recover'), self.root)
            factory.return_value.recover.assert_called_once_with()

    def test_main_cloud_change_detected_without_any_cloud_remediation(self):
        private_json(self.root / 'request.json', request())
        with patch.object(remote, 'require_root'), \
                patch.object(remote, '__file__', str(self.root / 'actions_remote.py')), \
                patch.object(remote, 'STATE_ROOT', self.root / 'state'), \
                patch.object(remote, 'cloud_state', side_effect=['running true', 'changed true']), \
                patch.object(remote, 'operate', return_value={'status': 'COMMITTED'}) as operate, \
                patch.dict(os.environ, {'DOCKER_HOST': 'tcp://invalid', 'DOCKER_CONTEXT': 'unrelated'}):
            with self.assertRaisesRegex(remote.ActionsError, 'Cloud state changed'):
                remote.main()
            operate.assert_called_once()
            self.assertEqual(os.environ['DOCKER_HOST'], 'unix:///var/run/docker.sock')
            self.assertNotIn('DOCKER_CONTEXT', os.environ)

    def test_missing_production_cloud_blocks_before_operate(self):
        private_json(self.root / 'request.json', request())
        with patch.object(remote, 'require_root'), \
                patch.object(remote, '__file__', str(self.root / 'actions_remote.py')), \
                patch.object(remote, 'STATE_ROOT', self.root / 'state'), \
                patch.object(remote, 'cloud_state', return_value=None), \
                patch.object(remote, 'operate') as operate, patch.dict(os.environ, {}):
            with self.assertRaisesRegex(remote.ActionsError, 'Cloud must already be running'):
                remote.main()
            operate.assert_not_called()

    def test_production_root_guard_remains_required(self):
        with patch.object(remote.os, 'geteuid', return_value=1000):
            with self.assertRaisesRegex(remote.ActionsError, 'root is required'):
                remote.require_root()


class WorkflowTests(unittest.TestCase):
    def test_manual_single_node_workflow_and_build_gate(self):
        workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        self.assertIn('workflow_dispatch:', workflow)
        self.assertNotIn('\n  push:', workflow)
        self.assertNotIn('matrix:', workflow)
        self.assertIn('environment: history-cache-${{ inputs.target }}', workflow)
        self.assertIn('cancel-in-progress: false', workflow)
        self.assertNotIn('--acknowledge-unknown', workflow)
        self.assertIn('python3 -B docker/actions_deploy.py', workflow)
        build = (ROOT / '.github/workflows/build-docker.yml').read_text()
        self.assertIn('needs: regression', build)
        self.assertIn('steps.build.outputs.digest', build)
        self.assertIn('workflow_call:', (ROOT / '.github/workflows/ci.yml').read_text())

    def test_ctest_report_and_matrix_guard_use_same_absolute_path(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn('--output-junit "$GITHUB_WORKSPACE/build/ctest.xml"', workflow)
        self.assertIn('--junit "$GITHUB_WORKSPACE/build/ctest.xml"', workflow)
        self.assertIn('fail-fast: false', workflow)


if __name__ == '__main__':
    unittest.main()
