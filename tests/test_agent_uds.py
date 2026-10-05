"""Local UDS framing/shutdown tests; no query may reach the network backend."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="hc-uds-") as directory:
        path = str(Path(directory) / "agent.sock")
        credentials = Path(directory) / 'credentials.json'
        valid = json.dumps({'account_id': 'a' * 32, 'access_key_id': 'SYNTHETICKEY',
                            'secret_access_key': 'SYNTHETICSECRET'})
        credentials.write_text(valid)
        credentials.chmod(0o600)
        command = [args.agent, '--socket', path, '--foreground-network', 'no', '--credentials-file', str(credentials)]

        def rejected(candidate=command):
            result = subprocess.run(candidate, capture_output=True, timeout=3)
            assert result.returncode != 0
            assert not Path(path).exists()
            assert b'SYNTHETICSECRET' not in result.stdout + result.stderr

        for mode in (0o644, 0o640, 0o000, 0o700):
            credentials.chmod(mode)
            rejected()
        credentials.chmod(0o600)
        for content in ('', 'x' * 4097, '{"SYNTHETICSECRET":', '[]', '{}',
                        valid.replace('SYNTHETICKEY', ''),
                        valid.replace('SYNTHETICSECRET', 'bad\\nsecret')):
            credentials.write_text(content)
            rejected()
        credentials.write_text(valid)
        rejected(command + ['--account', ''])
        rejected(command + ['--credentials-file', str(credentials)])
        for timeout in ('0', '30001', '-1', '1.5', '1x', '999999999999999999999999'):
            rejected(command + ['--query-timeout-ms', timeout])
        rejected(command + ['--reuse-connections', 'maybe'])
        rejected(command + ['--foreground-network', 'maybe'])
        rejected(command + ['--adjustment', 'maybe'])
        plan = Path(directory) / 'warm.json'
        plan.write_text('{}')
        rejected(command + ['--warm-plan', str(plan)])
        for option, value in (('--pack-cache-bytes', '-1'), ('--pack-cache-bytes', '268435457'),
                              ('--full-pack-read-bytes', '1048577')):
            rejected(command + [option, value])
        link = Path(directory) / 'link'
        link.symlink_to(credentials)
        rejected(command[:-1] + [str(link)])
        link.unlink()
        os.link(credentials, link)
        rejected()
        link.unlink()
        fifo = Path(directory) / 'fifo'
        os.mkfifo(fifo, 0o600)
        rejected(command[:-1] + [str(fifo)])
        rejected(command[:-1] + [directory])
        rejected(command[:-1] + [str(Path(directory) / 'missing')])
        profile = Path(directory) / 'storage.json'
        storage = {'version': 1, 'environment': 'production', 'account_id': 'a' * 32,
                   'bucket': 'history-cache-production', 'prefix': 'r2-history-production/',
                   'jurisdiction': 'default', 'role': 'reader'}
        for change in ({'role': 'publisher'}, {'account_id': 'b' * 32}, {'bucket': 'history-cache-staging'},
                       {'prefix': 'r2-history-staging/'}):
            profile.write_text(json.dumps({**storage, **change}))
            rejected(command + ['--storage-config', str(profile)])
        profile.write_text(json.dumps(storage))
        rejected(command + ['--storage-config', str(profile), '--bucket', 'history-cache-production'])
        # The full UDS test also proves production reader startup without HTTP.
        command += ['--storage-config', str(profile)]
        credentials.chmod(0o400)
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        clients = []
        try:
            for _ in range(100):
                if Path(path).exists():
                    break
                assert child.poll() is None, "daemon exited during startup"
                time.sleep(0.02)
            assert b'SYNTHETICSECRET' not in Path('/proc/{}/cmdline'.format(child.pid)).read_bytes()
            second = subprocess.run(command, capture_output=True, timeout=3)
            assert second.returncode != 0, "second daemon stole socket"
            def connect():
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                connection.settimeout(3)
                connection.connect(path)
                clients.append(connection)
                return connection

            for request, status in (({"op": "ping"}, "PONG"),
                                    ({"op": "maintenance_status"}, "OK"),
                                    ({"op": "maintenance_lease", "idle": False, "valid_until_mono_ms": 0}, "OK"),
                                    ({"op": "query", "symbol": "", "start_ms": 0}, "ERROR")):
                client = connect()
                data = (json.dumps(request) + "\n").encode()
                for part in (data[:3], data[3:]):
                    client.sendall(part)
                response = b""
                while b"\n" not in response:
                    chunk = client.recv(4096)
                    assert chunk, "short response"
                    response += chunk
                assert json.loads(response)["status"] == status
                client.close()

            client = connect()
            client.sendall((json.dumps({'op': 'query', 'symbol': '000001.SZ',
                'start_ms': 1790784000000, 'end_ms': 1790784060000, 'max_rows': 5,
                'protocol_version': 1, 'timestamp_semantics': 'utc-instant-ms',
                'range_semantics': 'half-open'}) + '\n').encode())
            response = json.loads(client.makefile('rb').readline())
            assert response['status'] == 'MISS' and response['reason'] == 'cache_not_ready'
            assert response['metrics']['http'] == {}, 'default foreground used network'
            client.close()

            def control(request):
                client = connect()
                client.sendall((json.dumps(request) + '\n').encode())
                with client.makefile('rb') as stream:
                    result = json.loads(stream.readline())
                client.close()
                return result

            adjusted = control({'op': 'query', 'symbol': '000001.SZ',
                'start_ms': 1790784000000, 'end_ms': 1790784060000, 'max_rows': 5,
                'protocol_version': 1, 'timestamp_semantics': 'utc-instant-ms',
                'range_semantics': 'half-open', 'row_encoding': 'le-ddb-native64-v1', 'adjust': 'forward'})
            assert adjusted['status'] == 'MISS' and adjusted['reason'] == 'unsupported_series'
            assert adjusted['metrics']['http'] == {}, 'disabled adjustment used network'
            client = connect()
            for part in (b'ADJU', b'ST64\n', b'{"op":"adjust_rows","padding":"', b'x' * 10000, b'"}\n'):
                client.sendall(part)
            assert json.loads(client.makefile('rb').readline())['status'] == 'ERROR'
            client.close()
            client = connect()
            client.sendall(b'ADJUST64\n{"op":"ping"}\n')
            assert json.loads(client.makefile('rb').readline())['reason'] == 'invalid_request'
            client.close()
            assert control({'op': 'adjust_rows'})['reason'] == 'invalid_request'

            assert control({'op': 'maintenance_lease', 'idle': True,
                            'valid_until_mono_ms': int(time.monotonic() * 1000) - 1})['lease_ms'] == 0
            assert not control({'op': 'maintenance_status'})['permitted']
            assert control({'op': 'maintenance_lease', 'idle': True,
                            'valid_until_mono_ms': int(time.monotonic() * 1000) + 500})['lease_ms'] > 0
            assert control({'op': 'maintenance_status'})['permitted']
            time.sleep(0.6)
            assert not control({'op': 'maintenance_status'})['permitted']
            assert control({'op': 'maintenance_status'})['attempts'] == 0

            client = connect()
            client.sendall(b"x" * 8193)
            try:
                assert client.recv(1) == b"", "oversized frame accepted"
            except ConnectionResetError:
                pass
            client.close()
            for _ in range(4):
                connect()  # Idle clients must not prevent bounded shutdown.
            started = time.monotonic()
            child.terminate()
            assert child.wait(timeout=7) == 0
            child.communicate(timeout=3)
            plan.write_text(json.dumps([{'op': 'query', 'symbol': '000001.SZ',
                'start_ms': 1790784000000, 'end_ms': 1790784060000, 'max_rows': 5,
                'protocol_version': 1, 'timestamp_semantics': 'utc-instant-ms',
                'range_semantics': 'half-open', 'period_seconds': 60, 'adjust': 'none'}]))
            child = subprocess.Popen(command + ['--warm-plan', str(plan)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert b'listening on' in child.stdout.readline()
            state = control({'op': 'maintenance_status'})
            assert state['targets'] == 1 and state['attempts'] == 0 and not state['permitted']
            child.terminate()
            assert child.wait(timeout=7) == 0
            assert time.monotonic() - started < 7
            assert not Path(path).exists(), "socket not removed"
            child.communicate(timeout=3)
            credentials.chmod(0o600)
            stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            stale.bind(path)
            stale.close()
            child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert b"listening on" in child.stdout.readline(), "stale socket recovery failed"
            child.terminate()
            assert child.wait(timeout=7) == 0
        finally:
            for client in clients:
                client.close()
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=3)
    print("PASS agent_uds fragmented_request oversized_frame idle_shutdown exclusive_restart network=0")


if __name__ == "__main__":
    main()
