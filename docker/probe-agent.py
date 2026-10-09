"""Bounded local UDS liveness probe. Never issues history queries or R2 reads."""

import json
import socket
import sys
import time


def request(path, operation):
    deadline = time.monotonic() + 2
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(path)
        client.sendall((json.dumps({'op': operation}, separators=(',', ':')) + '\n').encode())
        data = bytearray()
        while b'\n' not in data:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('agent probe deadline')
            client.settimeout(remaining)
            chunk = client.recv(1024)
            if not chunk:
                raise ValueError('incomplete agent response')
            data.extend(chunk)
            if len(data) > 1024:
                raise ValueError('oversized agent response')
        return json.loads(data)


def probe(path):
    if request(path, 'ping') != {'status': 'PONG'}:
        raise ValueError('unexpected agent response')


def suspension_policy(path, expected):
    value = request(path, 'suspension_policy_status')
    if value != dict(status='OK', price_null_encoding='ddb-double-null-v1', **expected):
        raise ValueError('loaded suspension policy differs from deployment snapshot')
    return value


if __name__ == '__main__':
    try:
        probe(sys.argv[1])
    except (OSError, ValueError, IndexError):
        raise SystemExit(1)
