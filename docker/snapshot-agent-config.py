"""Create a private, durable deployment config snapshot without printing secrets."""

import json
import os
from pathlib import Path
import stat
import sys


def snapshot(destination, credentials, profile=None):
    destination = Path(destination)
    if destination.absolute() != destination.resolve():
        raise ValueError('unsafe snapshot path')
    sources = [('credentials.json', credentials, 16384)]
    if profile:
        sources.append(('storage.json', profile, 4096))
    payloads = []
    for name, source, limit in sources:
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
                raise ValueError('invalid configuration file')
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError('configuration must be private and owned by deployment user')
            data = stream.read(limit + 1)
            if len(data) > limit or not isinstance(json.loads(data), dict):
                raise ValueError('invalid configuration JSON')
            payloads.append((name, data, info.st_uid, info.st_gid))
    destination.mkdir(mode=0o700)
    for name, data, uid, gid in payloads:
        fd = os.open(destination / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fchown(stream.fileno(), uid, gid)
            os.fsync(stream.fileno())
    for path in (destination, destination.parent):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


if __name__ == '__main__':
    try:
        snapshot(*sys.argv[1:])
    except (OSError, ValueError, TypeError):
        print('configuration snapshot failed', file=sys.stderr)
        raise SystemExit(1)
