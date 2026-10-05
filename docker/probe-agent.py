"""Bounded local UDS liveness probe. Never issues history queries or R2 reads."""

import json
import socket
import sys
import time


def probe(path):
    deadline = time.monotonic() + 2
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(path)
        client.sendall(b'{"op":"ping"}\n')
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
        if json.loads(data) != {'status': 'PONG'}:
            raise ValueError('unexpected agent response')


if __name__ == '__main__':
    try:
        probe(sys.argv[1])
    except (OSError, ValueError, IndexError):
        raise SystemExit(1)
