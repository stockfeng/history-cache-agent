"""Host-side Actions adapter for the journaled Agent deployer; no Cloud mutations."""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

import deploy_agent as deploy

IMAGE = re.compile(r'ghcr\.io/stockfeng/history-cache-agent@sha256:[a-f0-9]{64}\Z')
OPERATIONS = ('preflight', 'deploy', 'status', 'rollback', 'recover', 'accept')
CONFIG_ROOT = Path('/etc/history-cache')
STATE_ROOT = Path('/var/lib/history-cache/deployments')
SOCKET_ROOT = Path('/var/lib/history-cache/socket')
MIB = 1024 * 1024


class ActionsError(ValueError):
    pass


def require(value, message):
    if not value:
        raise ActionsError(message)


def validate_request(value):
    require(isinstance(value, dict) and set(value) ==
            {'target', 'operation', 'image', 'revision', 'bake', 'adjustment'}, 'invalid request schema')
    require(value['target'] in ('test-vps', 'oracle', 'aliyun') and value['operation'] in OPERATIONS,
            'invalid target or operation')
    require(isinstance(value['revision'], str) and re.fullmatch('[a-f0-9]{40}', value['revision']),
            'full reviewed commit required')
    require(type(value['bake']) is int and 30 <= value['bake'] <= 600 and
            type(value['adjustment']) is bool, 'invalid bake or adjustment')
    require(isinstance(value['image'], str) and
            (IMAGE.fullmatch(value['image']) if value['operation'] == 'deploy' else not value['image']),
            'deploy requires the Agent registry digest; other operations require an empty image')


def parse_object(data, limit):
    require(0 < len(data) <= limit, 'invalid configuration size')
    value = json.loads(data, object_pairs_hook=deploy.unique_pairs)
    require(isinstance(value, dict), 'invalid configuration object')
    return value


def validate_storage(credentials, profile):
    credentials, profile = parse_object(credentials, 4096), parse_object(profile, 4096)
    require(set(credentials) == {'account_id', 'access_key_id', 'secret_access_key'},
            'invalid reader credentials schema')
    account = credentials['account_id']
    require(isinstance(account, str) and re.fullmatch('[a-f0-9]{32}', account), 'invalid account identifier')
    for field in ('access_key_id', 'secret_access_key'):
        value = credentials[field]
        require(isinstance(value, str) and 0 < len(value) <= 1024 and
                all(33 <= ord(ch) <= 126 for ch in value), 'invalid credential field')
    require(set(profile) == {'version', 'environment', 'account_id', 'bucket', 'prefix', 'jurisdiction', 'role'},
            'invalid reader profile schema')
    require(type(profile['version']) is int and profile['version'] == 1 and
            profile['account_id'] == account and profile['environment'] == 'production' and
            profile['role'] == 'reader' and profile['prefix'] == 'r2-history-production/' and
            profile['jurisdiction'] in ('default', 'eu'), 'production reader profile required')
    bucket = profile['bucket']
    require(isinstance(bucket, str) and re.fullmatch('[a-z0-9][a-z0-9-]{1,61}[a-z0-9]', bucket) and
            '-production-' in '-' + bucket + '-' and '-staging-' not in '-' + bucket + '-',
            'production bucket required')


def private_read(path, limit=4096):
    require(path.absolute() == path.resolve(), 'symlink configuration path rejected')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and info.st_nlink == 1 and
                stat.S_IMODE(info.st_mode) in (0o400, 0o600) and info.st_size <= limit,
                'private owned configuration file required')
        data = stream.read(limit + 1)
    require(len(data) <= limit, 'configuration too large')
    return data


def config_paths(bundle):
    credentials, profile = bundle / 'credentials.json', bundle / 'storage.json'
    require(credentials.exists() == profile.exists(), 'incomplete bundled storage configuration')
    if not credentials.exists():
        credentials, profile = CONFIG_ROOT / 'credentials.json', CONFIG_ROOT / 'storage-reader.json'
    validate_storage(private_read(credentials), private_read(profile))
    return credentials, profile


def docker_read(args):
    # Do not print daemon/inspect errors or full Config (may contain secrets).
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(['docker', *args], stdout=output, stderr=subprocess.DEVNULL, timeout=15)
        require(result.returncode == 0 and output.tell() <= 1024 * 1024, 'Docker preflight read failed')
        output.seek(0)
        return output.read().decode()


def cloud_state():
    ids = docker_read(['ps', '-aq', '--no-trunc', '--filter', 'name=^/cloud_gateway_v2$']).split()
    require(len(ids) <= 1, 'ambiguous Cloud identity')
    if not ids:
        return None
    require(re.fullmatch('[a-f0-9]{64}', ids[0]), 'invalid Cloud identity')
    fields = '{{json .Id}} {{json .Image}} {{json .State.Pid}} {{json .RestartCount}} {{json .State.Running}}'
    return docker_read(['inspect', '--format', fields, ids[0]]).strip()


def resource_preflight():
    info = json.loads(docker_read(['info', '--format', '{{json .}}']))
    architecture = {'x86_64': 'amd64', 'aarch64': 'arm64', 'amd64': 'amd64', 'arm64': 'arm64'}.get(info['Architecture'])
    require(info['OSType'] == 'linux' and architecture is not None, 'unsupported Docker platform')
    require(all(info.get(key) is True for key in ('MemoryLimit', 'SwapLimit', 'CpuCfsQuota', 'PidsLimit')),
            'Docker must enforce memory/swap/CPU/PID limits')
    require(not any('rootless' in option or 'userns' in option for option in info.get('SecurityOptions', [])),
            'rootless/userns requires a separately verified credential and UDS mapping')
    disk = os.statvfs(info['DockerRootDir'])
    free = disk.f_bavail * disk.f_frsize
    available = None
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            available = int(line.split()[1]) * 1024
    require(free >= 1024 * MIB and available is not None and available >= 256 * MIB,
            'need at least 1 GiB Docker disk and 256 MiB currently available RAM')
    if SOCKET_ROOT.exists():
        item = SOCKET_ROOT.stat()
        require(SOCKET_ROOT.resolve() == SOCKET_ROOT and stat.S_ISDIR(item.st_mode) and
                item.st_uid == 0 and not item.st_mode & 0o022, 'unsafe existing UDS directory')
    return {'architecture': architecture, 'disk_free_mib': free // MIB, 'mem_available_mib': available // MIB}


class CheckedDocker(deploy.Docker):
    def __init__(self, architecture, revision):
        super().__init__()
        self.architecture, self.revision = architecture, revision

    def call(self, args, seconds=15):
        result = super().call(args, seconds)
        if args[:2] == ['image', 'inspect']:
            images = json.loads(result)
            require(len(images) == 1, 'ambiguous image')
            image = images[0]
            config = image['Config']
            require(image['Os'] == 'linux' and image['Architecture'] == self.architecture,
                    'image architecture differs from Docker host')
            require(config.get('User', '') in ('', '0', 'root') and
                    config.get('Entrypoint') == ['/opt/history-cache/bin/history-cache-agent'] and
                    not config.get('Volumes'), 'unexpected image UID, entrypoint or volumes')
            require((config.get('Labels') or {}).get('org.opencontainers.image.revision') == self.revision,
                    'image revision differs from reviewed deployment scripts')
            require(args[2] in image.get('RepoDigests', []), 'pulled image digest differs')
        return result


@contextmanager
def registry_session(bundle):
    registry = parse_object(private_read(bundle / 'registry.json', 8192), 8192)
    require(set(registry) == {'user', 'token'} and isinstance(registry['user'], str) and
            re.fullmatch('[A-Za-z0-9][A-Za-z0-9-]{0,99}', registry['user']) and
            isinstance(registry['token'], str) and 0 < len(registry['token']) <= 4096, 'invalid registry credentials')
    with tempfile.TemporaryDirectory(prefix='registry-', dir=bundle) as name:
        os.environ['DOCKER_CONFIG'] = name
        try:
            result = subprocess.run(['docker', 'login', 'ghcr.io', '--username', registry['user'], '--password-stdin'],
                                    input=registry['token'].encode(), stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL, timeout=30)
            require(result.returncode == 0, 'GHCR login failed')
            yield
        finally:
            os.environ.pop('DOCKER_CONFIG', None)


def operate(request, bundle):
    operation = request['operation']
    app = deploy.Deployment(STATE_ROOT, SOCKET_ROOT)
    if operation == 'status':
        with app.journal.lock():
            record = app.journal.read()
        return {key: record[key] for key in ('transaction', 'phase', 'pending')} if record else {'phase': 'EMPTY'}
    if operation in ('preflight', 'deploy'):
        credentials, profile = config_paths(bundle)
        resources = resource_preflight()
        with app.journal.lock():
            record = app.journal.read()
        require(record is None or record['phase'] in ('ROLLED_BACK', 'ACCEPTED'),
                'existing release requires explicit accept, rollback or recovery')
        app.docker.budget(30)
        require(app.docker.inspect(deploy.BACKUP) is None, 'retained Agent backup requires reconciliation')
        if operation == 'preflight':
            return dict(resources, status='PREFLIGHT_OK', image_pulled=False, r2_data_health_tested=False)
        app.docker = CheckedDocker(resources['architecture'], request['revision'])
        with registry_session(bundle):
            result = app.deploy(request['image'], request['bake'], credentials, profile,
                                adjustment=request['adjustment'], network='bridge')
    elif operation == 'rollback':
        result = app.rollback()
    elif operation == 'recover':
        result = app.recover()  # Never acknowledge an unresolved daemon mutation automatically.
    else:
        result = app.accept()
    return {'status': result, 'r2_data_health_tested': False}


def require_root():
    require(os.geteuid() == 0, 'root is required for the current Docker/UDS credential mapping')


def main():
    require_root()
    for key in list(os.environ):
        if key.startswith('DOCKER_'):
            del os.environ[key]
    os.environ['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
    bundle = Path(__file__).resolve().parent
    request = parse_object(private_read(bundle / 'request.json'), 4096)
    validate_request(request)
    deploy.private_directory(STATE_ROOT)
    fd = os.open(STATE_ROOT / 'actions.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        info = os.fstat(lock.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o077,
                'unsafe Actions lock')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = cloud_state()
        if request['target'] != 'test-vps' and request['operation'] in ('preflight', 'deploy'):
            require(before is not None and before.endswith(' true'), 'production Cloud must already be running')
        try:
            result = operate(request, bundle)
        finally:
            require(before == cloud_state(), 'Cloud state changed during operation; investigate, never auto-restart Cloud')
        print(json.dumps(dict(result, target=request['target'], operation=request['operation'],
                              release_commit=request['revision'], cloud_state_unchanged=True)))


if __name__ == '__main__':
    try:
        main()
    except ActionsError as error:
        print('Agent operation blocked: ' + str(error), file=sys.stderr)
        raise SystemExit(1)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        # Parsers and OS errors may contain secret inputs; never print their exception text.
        print('Agent operation incomplete; inspect status and retained journal before retry/recovery.', file=sys.stderr)
        raise SystemExit(1)
