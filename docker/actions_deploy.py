"""GitHub runner transport. One pinned SSH host; no remote Git or Cloud commands."""

import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tarfile
import tempfile

from actions_remote import validate_request, validate_storage, deploy

HOSTS = {'test-vps': '132.226.7.147', 'oracle': '151.145.72.82', 'aliyun': '139.196.115.141'}
HELPERS = ('actions_remote.py', 'deploy_agent.py', 'probe-agent.py', 'snapshot-agent-config.py')
# The command is constant. Inputs and secrets travel only inside the encrypted stdin tar.
REMOTE_COMMAND = '''set -eu
umask 077
work=$(mktemp -d /tmp/history-cache-actions.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT
tar -xf - -C "$work"
python3 -B "$work/actions_remote.py"
'''


def make_request(env):
    request = dict(target=env.get('AGENT_TARGET', ''), operation=env.get('AGENT_OPERATION', ''),
                   image=env.get('AGENT_IMAGE', ''), revision=env.get('AGENT_RELEASE_COMMIT', ''),
                   bake=int(env.get('AGENT_BAKE_SECONDS', '120')),
                   adjustment=env.get('AGENT_ADJUSTMENT', 'true') == 'true')
    if env.get('AGENT_ADJUSTMENT', 'true') not in ('true', 'false'):
        raise ValueError('invalid adjustment flag')
    validate_request(request)
    return request


def bundle(request, env):
    files = {name: Path(__file__).with_name(name).read_bytes() for name in HELPERS}
    files['request.json'] = json.dumps(request).encode()
    if request['operation'] == 'install-policy':
        source = Path(env.get('AGENT_POLICY_DIRECTORY', ''))
        data = (source / 'suspensions.json').read_bytes()
        expected = env.get('AGENT_POLICY_SHA256', '')
        metadata = deploy.helper('snapshot-agent-config').validate_policy(data, expected)
        if not metadata['intervals']:
            raise ValueError('empty activation policy rejected')
        files['suspensions.json'] = data
        files['suspensions.sha256'] = (expected + '\n').encode()
    if request['operation'] in ('deploy', 'preflight'):
        credentials = env.get('AGENT_R2_CREDENTIALS_JSON', '')
        profile = env.get('AGENT_STORAGE_READER_JSON', '')
        if bool(credentials) != bool(profile):
            raise ValueError('provide both R2 secrets or use both existing server files')
        if credentials:
            validate_storage(credentials.encode(), profile.encode())
            files['credentials.json'] = credentials.encode()
            files['storage.json'] = profile.encode()
    if request['operation'] == 'deploy':
        user, token = env.get('AGENT_GHCR_USER', ''), env.get('AGENT_GHCR_TOKEN', '')
        if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9-]{0,99}', user) or not token or len(token) > 4096:
            raise ValueError('missing or invalid GHCR read credentials')
        files['registry.json'] = json.dumps({'user': user, 'token': token}).encode()
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w') as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o600
            archive.addfile(info, io.BytesIO(data))
    return output.getvalue()


def ssh_command(request, env, directory):
    key, password = env.get('AGENT_SSH_KEY', ''), env.get('AGENT_SSH_PASSWORD', '')
    known_hosts = env.get('AGENT_SSH_KNOWN_HOSTS', '')
    if bool(key) == bool(password) or not known_hosts.strip():
        raise ValueError('configure exactly one SSH key/password and pinned known_hosts')
    hostfile = directory / 'known_hosts'
    hostfile.write_text(known_hosts + '\n')
    hostfile.chmod(0o600)
    command = ['ssh', '-F', '/dev/null', '-T', '-o', 'ConnectTimeout=10', '-o', 'ConnectionAttempts=1',
               '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(hostfile),
               '-o', 'GlobalKnownHostsFile=/dev/null', '-o', 'ServerAliveInterval=15',
               '-o', 'ServerAliveCountMax=3', '-o', 'IdentitiesOnly=yes', '-o', 'IdentityAgent=none',
               '-o', 'ClearAllForwardings=yes', '-o', 'LogLevel=ERROR']
    child_env = {k: v for k, v in env.items() if not k.startswith('AGENT_')}
    if key:
        keyfile = directory / 'key'
        keyfile.write_text(key + '\n')
        keyfile.chmod(0o600)
        command += ['-i', str(keyfile), '-o', 'BatchMode=yes', '-o', 'PreferredAuthentications=publickey']
    else:
        child_env['SSHPASS'] = password
        command = ['sshpass', '-e', *command, '-o', 'PreferredAuthentications=password',
                   '-o', 'PubkeyAuthentication=no', '-o', 'NumberOfPasswordPrompts=1']
    command += ['root@' + HOSTS[request['target']], REMOTE_COMMAND]
    return command, child_env


def main():
    request = make_request(os.environ)
    payload = bundle(request, os.environ)
    with tempfile.TemporaryDirectory(prefix='history-cache-actions-') as name:
        command, env = ssh_command(request, os.environ, Path(name))
        child = subprocess.Popen(command, stdin=subprocess.PIPE, env=env, start_new_session=True)
        try:
            child.communicate(input=payload, timeout=1200)
        except BaseException:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.communicate(timeout=5)
            raise
        if child.returncode:
            raise ValueError('remote operation incomplete; inspect status before any retry or recovery')


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError):
        print('Agent Actions failed; no automatic retry or Cloud operation. Check retained Agent journal.',
              file=sys.stderr)
        raise SystemExit(1)
