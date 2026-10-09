"""Journaled, bounded Agent-only Docker deployment. No Cloud or R2 operations."""

import argparse
import copy
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid

NAME = 'history-cache-agent'
BACKUP = NAME + '-rollback'
LABEL = 'org.history-cache.deployment'
IMAGE = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[a-f0-9]{64}\Z')
LOCAL_IMAGE = re.compile(r'sha256:[a-f0-9]{64}\Z')
CONTAINER_ID = re.compile(r'[a-f0-9]{64}\Z')
TERMINAL = {'COMMITTED', 'ROLLED_BACK', 'ACCEPTED'}
PHASES = TERMINAL | {'PREPARED', 'STOP_OLD', 'RENAME_OLD', 'CREATE', 'START', 'BAKING',
                     'ROLLING_BACK', 'ACCEPTING'}


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate journal key')
        result[key] = value
    return result


def helper(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DeployError(ValueError):
    pass


class UnknownOutcome(DeployError):
    pass


def require(condition, message):
    if not condition:
        raise DeployError(message)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def private_directory(path):
    require(path.absolute() == path.resolve(), 'unsafe deployment directory')
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.stat()
    require(info.st_uid == os.geteuid() and not info.st_mode & 0o077, 'deployment directory must be private and owned')
    sync_directory(path.parent)


class Journal:
    def __init__(self, root):
        self.root = Path(root).absolute()

    @contextmanager
    def lock(self):
        private_directory(self.root)
        fd = os.open(self.root / 'deploy.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o077,
                    'unsafe deployment lock')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def read(self):
        path = self.root / 'active.json'
        if not path.exists():
            require(not path.is_symlink(), 'unsafe deployment journal')
            return None
        info = path.stat()
        require(not path.is_symlink() and stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                not info.st_mode & 0o077 and info.st_size <= 65536, 'unsafe deployment journal')
        record = json.loads(path.read_bytes(), object_pairs_hook=unique_pairs)
        require(type(record['version']) is int and record['version'] == 1 and
                re.fullmatch('[a-f0-9]{32}', record['transaction']), 'invalid journal')
        require(record['phase'] in PHASES and record['pending'] in (None, 'stop', 'rename', 'create', 'start', 'rm'),
                'invalid deployment phase or pending operation')
        require(record['phase'] not in TERMINAL or record['pending'] is None, 'terminal journal has pending operation')
        require(record['name'] == NAME and record['backup'] == BACKUP, 'journal container scope differs')
        require(record['old'] is None or CONTAINER_ID.fullmatch(record['old']['id']), 'invalid old container ID')
        require(record['candidate'] is None or CONTAINER_ID.fullmatch(record['candidate']), 'invalid candidate ID')
        require((IMAGE.fullmatch(record['image']) or
                 (record.get('local_image') is True and LOCAL_IMAGE.fullmatch(record['image']) and
                  record['image'] == record['image_id'])) and LOCAL_IMAGE.fullmatch(record['image_id']),
                'invalid journal image')
        require(record['release'] == str(self.root / ('release-' + record['transaction'])), 'release path differs')
        return record

    def write(self, record):
        record['updated_at'] = int(time.time())
        data = (json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n').encode()
        require(len(data) <= 65536, 'deployment journal too large')
        fd, temporary = tempfile.mkstemp(prefix='.journal-', dir=self.root)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.root / 'active.json')
            sync_directory(self.root)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def archive(self, record):
        target = self.root / ('result-' + record['transaction'] + '.json')
        data = (json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n').encode()
        if target.exists():
            require(not target.is_symlink(), 'unsafe journal archive')
            require(target.read_bytes() == data, 'archive differs')
            return
        with target.open('xb') as stream:
            os.chmod(target, 0o600)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        sync_directory(self.root)


class Docker:
    """CLI deadlines do not cancel daemon work: mutation timeouts stay unknown."""
    def __init__(self, executable='docker', socket=None):
        self.deadline = 0
        require(socket is None or (socket.startswith('/') and '\n' not in socket), 'invalid Docker socket')
        self.command = [executable] + (['-H', 'unix://' + socket] if socket else [])

    def budget(self, seconds):
        self.deadline = time.monotonic() + seconds

    def call(self, args, seconds=15):
        remaining = min(seconds, self.deadline - time.monotonic())
        require(remaining > 0, 'Docker operation budget exhausted')
        # Pull output can be unbounded. Never log inspect results (Config may contain secrets).
        capture = args[0] in ('inspect', 'ps') or args[:2] == ['image', 'inspect']
        with tempfile.TemporaryFile() as output:
            child = subprocess.Popen([*self.command, *args], stdout=output if capture else subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                rc = child.wait(timeout=remaining)
            except BaseException:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=5)
                raise
            require(rc == 0, 'Docker command failed: ' + args[0])
            if not capture:
                return ''
            require(output.tell() <= 1024 * 1024, 'Docker response too large')
            output.seek(0)
            return output.read().decode()

    def inspect(self, identity):
        require(CONTAINER_ID.fullmatch(identity) or identity in (NAME, BACKUP), 'invalid inspect target')
        # Absence is only accepted after a successful list, not a failed inspect.
        ids = self.call(['ps', '-aq', '--no-trunc', '--filter', 'name=^/' + identity + '$']) if not CONTAINER_ID.fullmatch(identity) else None
        if ids is not None:
            found = ids.split()
            require(len(found) <= 1, 'ambiguous container identity')
            if not found:
                return None
            identity = found[0]
            require(CONTAINER_ID.fullmatch(identity), 'invalid listed container ID')
        value = json.loads(self.call(['inspect', identity]))
        require(len(value) == 1 and value[0]['Id'] == identity, 'inspect identity differs')
        return value[0]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def fingerprint(container):
    value = {key: container[key] for key in ('Id', 'Image', 'Config', 'HostConfig', 'Mounts')}
    value['Mounts'] = sorted(value['Mounts'], key=lambda item: json.dumps(item, sort_keys=True))
    return digest(value)


def legacy_fingerprint(container):
    return digest({key: container[key] for key in ('Id', 'Image', 'Config', 'HostConfig', 'Mounts')})


def matches_fingerprint(container, expected):
    if fingerprint(container) == expected or legacy_fingerprint(container) == expected:
        return True
    # Docker can materialize false as null after start; both leave OOM killing enabled.
    host = container['HostConfig']
    variants = [container]
    if 'OomKillDisable' in host and (host['OomKillDisable'] is None or host['OomKillDisable'] is False):
        equivalent = copy.deepcopy(container)
        equivalent['HostConfig']['OomKillDisable'] = False if host['OomKillDisable'] is None else None
        variants.append(equivalent)
    for variant in variants:
        if fingerprint(variant) == expected or legacy_fingerprint(variant) == expected:
            return True
        # Old journals hashed Docker's unordered Mounts array. Retain their exact
        # hashes without changing any mount content or any other configuration.
        if len(variant['Mounts']) <= 4:
            for mounts in itertools.permutations(variant['Mounts']):
                legacy = dict(variant, Mounts=list(mounts))
                if legacy_fingerprint(legacy) == expected:
                    return True
    return False


def config_files(container, socket_dir):
    files = {}
    require(len(container['Mounts']) <= 16, 'too many previous mounts')
    for mount in container['Mounts']:
        if mount['Destination'] == '/run/history-cache':
            require(mount['Type'] == 'bind' and mount['Source'] == str(socket_dir), 'previous socket mount differs')
            continue
        require(mount['Type'] == 'bind' and not mount['RW'], 'unsupported previous writable or volume mount')
        path = Path(mount['Source'])
        limit = 1024 * 1024 if mount['Destination'] == '/run/config/history-suspensions.json' else 65536
        require(path.absolute() == path.resolve() and path.is_file() and path.stat().st_size <= limit,
                'unsafe previous configuration mount')
        with path.open('rb') as stream:
            data = stream.read(limit + 1)
        require(len(data) <= limit, 'previous configuration mount grew beyond limit')
        files[str(path)] = hashlib.sha256(data).hexdigest()
    return files


def require_policy_retained(container, suspensions):
    if container:
        configured = '--suspensions-file' in (container['Config'].get('Cmd') or []) or any(
            m['Destination'] == '/run/config/history-suspensions.json' for m in container['Mounts'])
        require(not configured or suspensions is not None, 'refusing to remove an active suspension policy')


class Deployment:
    def __init__(self, root=Path('/var/lib/history-cache/deployments'),
                 socket_dir=Path('/var/lib/history-cache/socket'), docker=None, probe=None, sleep=time.sleep):
        self.journal = Journal(root)
        self.socket_dir = Path(socket_dir).absolute()
        self.docker = docker or Docker()
        self.probe = probe or helper('probe-agent').probe
        self.sleep = sleep
        self.record = None

    def save(self, phase=None):
        if phase:
            self.record['phase'] = phase
        self.journal.write(self.record)

    def mutate(self, args, seconds=30):
        require(self.record['pending'] is None, 'unresolved Docker operation')
        # Only the verb is persisted; argv may contain operator-provided paths.
        self.record['pending'] = args[0]
        self.save()
        try:
            result = self.docker.call(args, seconds)
        except BaseException as error:
            raise UnknownOutcome('Docker mutation unresolved; inspect daemon then recover with explicit acknowledgement') from error
        self.record['pending'] = None
        try:
            self.save()
        except BaseException as error:
            self.record['pending'] = args[0]
            raise UnknownOutcome('Docker mutation acknowledgement was not durably recorded') from error
        return result

    def healthy(self, identity, fresh=False):
        value = self.docker.inspect(NAME)
        require(value is not None and value['Id'] == identity and value['State']['Running'] and
                not value['State'].get('Paused') and not value['State'].get('Restarting') and
                not value['State'].get('OOMKilled'), 'Agent is not running normally')
        require(not fresh or value['RestartCount'] == 0, 'candidate restarted')
        self.probe(str(self.socket_dir / 'agent.sock'))
        if self.record and identity == self.record['candidate'] and self.record.get('suspension_policy'):
            helper('probe-agent').suspension_policy(str(self.socket_dir / 'agent.sock'),
                                                  self.record['suspension_policy'])

    def wait_ready(self, identity, fresh=False):
        for attempt in range(10):
            try:
                self.healthy(identity, fresh)
                return
            except (ValueError, OSError):
                if attempt == 9:
                    raise
                self.sleep(1)

    def check_old(self, old):
        require(old is not None and old['Id'] == self.record['old']['id'] and
                matches_fingerprint(old, self.record['old']['fingerprint']), 'previous container configuration changed')
        require(config_files(old, self.socket_dir) == self.record['old']['files'], 'previous mounted config changed')

    def candidate(self, value):
        require(value['Config'].get('Labels', {}).get(LABEL) == self.record['transaction'] and
                value['Image'] == self.record['image_id'] and
                (self.record['candidate'] is None or value['Id'] == self.record['candidate']),
                'unrelated container occupies deployment name')
        return value['Id']

    def deploy(self, image, bake, credentials, profile=None, *, local_image=False, adjustment=False, network='bridge',
               suspensions=None, suspensions_sha256=None):
        require((LOCAL_IMAGE.fullmatch(image) if local_image else IMAGE.fullmatch(image)) and
                type(bake) is int and 1 <= bake <= 3600 and network in ('bridge', 'host'),
                'invalid image digest, network or bake duration')
        with self.journal.lock():
            previous = self.journal.read()
            require(previous is None or previous['phase'] in ('ROLLED_BACK', 'ACCEPTED'),
                    'existing deployment requires recover or accept')
            if previous:
                self.journal.archive(previous)
            self.docker.budget(300)
            require(self.docker.inspect(BACKUP) is None, 'rollback container already exists')
            old = self.docker.inspect(NAME)
            require_policy_retained(old, suspensions)
            if old:
                require(old['HostConfig']['RestartPolicy']['Name'] in ('no', 'unless-stopped'),
                        'previous restart policy is unsafe for retained backup')
                require(not old['State'].get('Paused') and not old['State'].get('Restarting'), 'previous container not stable')
                old_record = dict(id=old['Id'], running=old['State']['Running'],
                                  fingerprint=fingerprint(old), files=config_files(old, self.socket_dir))
            else:
                old_record = None
            if not local_image:
                self.docker.call(['pull', image], 240)
            image_info = json.loads(self.docker.call(['image', 'inspect', image]))
            require(len(image_info) == 1 and re.fullmatch('sha256:[a-f0-9]{64}', image_info[0]['Id']), 'invalid image identity')
            require(not local_image or image_info[0]['Id'] == image, 'local image ID differs')
            transaction = uuid.uuid4().hex
            release = self.journal.root / ('release-' + transaction)
            policy = helper('snapshot-agent-config').snapshot(release, credentials, profile,
                                                             suspensions, suspensions_sha256)
            require(self.socket_dir == self.socket_dir.resolve(), 'unsafe socket directory')
            self.socket_dir.mkdir(parents=True, exist_ok=True)
            self.record = dict(version=1, transaction=transaction, name=NAME, backup=BACKUP,
                               image=image, image_id=image_info[0]['Id'], local_image=local_image,
                               adjustment=adjustment, network=network, old=old_record, candidate=None,
                               candidate_fingerprint=None, candidate_files=None,
                               suspension_policy=policy,
                               release=str(release), socket_dir=str(self.socket_dir),
                               phase='PREPARED', pending=None, acknowledgements=0)
            self.save()
            self.docker.budget(bake + 180)
            try:
                if old:
                    self.check_old(self.docker.inspect(NAME))
                    self.save('STOP_OLD')
                    self.mutate(['stop', '--time', '10', old['Id']])
                    self.save('RENAME_OLD')
                    self.mutate(['rename', old['Id'], BACKUP])
                self.save('CREATE')
                args = ['create', '--name', NAME, '--label', LABEL + '=' + transaction, '--network', network,
                        '--restart', 'unless-stopped', '--memory', '128m', '--memory-reservation', '96m',
                        '--memory-swap', '128m', '--cpus', '0.25', '--cpu-shares', '64', '--pids-limit', '32',
                        '--blkio-weight', '100', '--ulimit', 'nofile=256:256', '--cap-drop', 'ALL',
                        '--security-opt', 'no-new-privileges', '--read-only',
                        '-v', str(self.socket_dir) + ':/run/history-cache',
                        '-v', str(release / 'credentials.json') + ':/run/secrets/history-cache.json:ro']
                if profile:
                    args += ['-v', str(release / 'storage.json') + ':/run/config/history-storage.json:ro']
                if policy:
                    args += ['-v', str(release / 'suspensions.json') + ':/run/config/history-suspensions.json:ro']
                args += [image, '--socket', '/run/history-cache/agent.sock',
                         '--credentials-file', '/run/secrets/history-cache.json']
                if profile:
                    args += ['--storage-config', '/run/config/history-storage.json']
                if policy:
                    args += ['--suspensions-file', '/run/config/history-suspensions.json',
                             '--suspensions-sha256', policy['sha256']]
                args += ['--foreground-network', 'yes', '--pack-cache-bytes', '33554432']
                if adjustment:
                    args += ['--adjustment', 'yes']
                self.mutate(args)
                value = self.docker.inspect(NAME)
                require(value is not None, 'candidate missing after create')
                self.record['candidate'] = self.candidate(value)
                self.record['candidate_fingerprint'] = fingerprint(value)
                self.record['candidate_files'] = config_files(value, self.socket_dir)
                self.save('START')
                self.mutate(['start', self.record['candidate']])
                self.save('BAKING')
                self.wait_ready(self.record['candidate'], True)
                end = time.monotonic() + bake
                while time.monotonic() < end:
                    self.healthy(self.record['candidate'], True)
                    self.sleep(max(0, min(1, end - time.monotonic())))
                self.healthy(self.record['candidate'], True)
                require(config_files(self.docker.inspect(NAME), self.socket_dir) == self.record['candidate_files'],
                        'candidate mounted config changed during bake')
                self.save('COMMITTED')
                return 'COMMITTED'
            except UnknownOutcome:
                raise
            except BaseException:
                if not self.record['pending'] and self.record['phase'] != 'COMMITTED':
                    self.docker.budget(180)
                    self.restore()
                raise

    def restore(self):
        self.save('ROLLING_BACK')
        current, backup = self.docker.inspect(NAME), self.docker.inspect(BACKUP)
        old_record = self.record['old']
        old = None
        if old_record:
            candidates = [v for v in (current, backup) if v and v['Id'] == old_record['id']]
            require(len(candidates) == 1, 'previous container missing or renamed outside deployment scope')
            old = candidates[0]
            self.check_old(old)
        require(backup is None or (old is not None and backup['Id'] == old['Id']), 'unrelated rollback container')
        if current and (old is None or current['Id'] != old['Id']):
            identity = self.candidate(current)
            self.record['candidate'] = identity
            self.save()
            self.mutate(['rm', '-f', identity])
            require(self.docker.inspect(NAME) is None, 'candidate still present')
        if old:
            if old['Name'] == '/' + BACKUP:
                self.mutate(['rename', old['Id'], NAME])
            restored = self.docker.inspect(NAME)
            self.check_old(restored)
            if old_record['running']:
                if not restored['State']['Running']:
                    self.mutate(['start', old['Id']])
                self.wait_ready(old['Id'])
            elif restored['State']['Running']:
                self.mutate(['stop', '--time', '10', old['Id']])
            require(self.docker.inspect(NAME)['State']['Running'] == old_record['running'], 'previous running state differs')
        self.save('ROLLED_BACK')

    def recover(self, acknowledge=False):
        with self.journal.lock():
            self.record = self.journal.read()
            require(self.record is not None, 'no deployment to recover')
            require(self.record['socket_dir'] == str(self.socket_dir), 'socket path differs')
            if self.record['phase'] in TERMINAL:
                return self.record['phase']
            if self.record['pending']:
                require(acknowledge, 'unknown Docker mutation; verify daemon quiescence and use --acknowledge-unknown')
                self.record['acknowledgements'] += 1
                self.record['pending'] = None
                self.save()
            self.docker.budget(180)
            if self.record['phase'] == 'ACCEPTING':
                self.finish_accept()
            else:
                self.restore()
            return self.record['phase']

    def accept(self):
        with self.journal.lock():
            self.record = self.journal.read()
            require(self.record and self.record['phase'] in ('COMMITTED', 'ACCEPTING', 'ACCEPTED'), 'no committed release to accept')
            require(self.record['socket_dir'] == str(self.socket_dir), 'socket path differs')
            require(not self.record['pending'], 'unresolved backup removal requires operator reconciliation')
            if self.record['phase'] == 'ACCEPTED':
                return 'ACCEPTED'
            self.docker.budget(90)
            return self.finish_accept()

    def rollback(self):
        with self.journal.lock():
            self.record = self.journal.read()
            require(self.record and self.record['phase'] == 'COMMITTED' and not self.record['pending'],
                    'explicit rollback requires an unaccepted committed release')
            require(self.record['socket_dir'] == str(self.socket_dir), 'socket path differs')
            self.docker.budget(180)
            self.restore()
            return 'ROLLED_BACK'

    def finish_accept(self):
        current = self.docker.inspect(NAME)
        require(current is not None, 'committed candidate missing')
        self.candidate(current)
        require(matches_fingerprint(current, self.record['candidate_fingerprint']) and
                config_files(current, self.socket_dir) == self.record['candidate_files'], 'candidate configuration changed')
        self.healthy(current['Id'])
        backup = self.docker.inspect(BACKUP)
        if backup:
            require(self.record['old'] is not None, 'unexpected backup')
            self.check_old(backup)
            require(not backup['State']['Running'], 'refusing to remove running backup')
            self.save('ACCEPTING')
            self.mutate(['rm', backup['Id']])
        else:
            require(self.record['old'] is None or self.record['phase'] == 'ACCEPTING', 'backup disappeared before acceptance')
        self.save('ACCEPTED')
        return 'ACCEPTED'


def main():
    global NAME, BACKUP
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('image', nargs='?')
    parser.add_argument('bake', nargs='?', type=int, default=120)
    parser.add_argument('profile', nargs='?', type=Path)
    parser.add_argument('--recover', action='store_true')
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--accept', action='store_true', help='explicitly discard the stopped rollback container')
    parser.add_argument('--rollback', action='store_true', help='restore the retained version after commit, before accept')
    parser.add_argument('--acknowledge-unknown', action='store_true')
    parser.add_argument('--local-image', action='store_true', help='use an already loaded exact sha256 image ID, without pulling')
    parser.add_argument('--adjustment', action='store_true')
    parser.add_argument('--suspensions-file', type=Path)
    parser.add_argument('--suspensions-sha256')
    parser.add_argument('--network', choices=('bridge', 'host'), default='bridge')
    parser.add_argument('--engine', default='docker')
    parser.add_argument('--docker-socket')
    parser.add_argument('--state-root', type=Path, default=Path('/var/lib/history-cache/deployments'))
    parser.add_argument('--socket-dir', type=Path, default=Path('/var/lib/history-cache/socket'))
    parser.add_argument('--credentials', type=Path, default=Path('/etc/history-cache/credentials.json'))
    parser.add_argument('--name', choices=('history-cache-agent', 'history-cache-agent-test'), default=NAME)
    args = parser.parse_args()
    NAME, BACKUP = args.name, args.name + '-rollback'
    require(sum((bool(args.image), args.recover, args.status, args.accept, args.rollback)) == 1,
            'choose deploy, recover, status, rollback or accept')
    require(not args.acknowledge_unknown or args.recover, 'acknowledgement only applies to recovery')
    require(args.image or not (args.local_image or args.adjustment or args.network != 'bridge' or
                              args.suspensions_file or args.suspensions_sha256),
            'deployment options require an image')
    deployment = Deployment(args.state_root, args.socket_dir, Docker(args.engine, args.docker_socket))
    if args.status:
        with deployment.journal.lock():
            record = deployment.journal.read()
        print(json.dumps({key: record[key] for key in ('transaction', 'phase', 'pending', 'acknowledgements')}
                         if record else {'phase': 'EMPTY'}))
        return
    def interrupted(*_):
        raise InterruptedError('deployment interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    if args.recover:
        result = deployment.recover(args.acknowledge_unknown)
    elif args.accept:
        result = deployment.accept()
    elif args.rollback:
        result = deployment.rollback()
    else:
        result = deployment.deploy(args.image, args.bake, args.credentials, args.profile,
                                   local_image=args.local_image, adjustment=args.adjustment, network=args.network,
                                   suspensions=args.suspensions_file, suspensions_sha256=args.suspensions_sha256)
    print(json.dumps({'status': result, 'cloud_modified': False, 'r2_data_health_tested': False}))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        print('deployment incomplete; inspect --status and retained journal before recovery', file=sys.stderr)
        raise SystemExit(1)
