"""Create a private, durable deployment config snapshot without printing secrets."""

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

POLICY_LIMIT = 1024 * 1024


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate configuration key')
        result[key] = value
    return result


def validate_policy(data, expected):
    if not isinstance(expected, str) or not re.fullmatch('[a-f0-9]{64}', expected):
        raise ValueError('suspension policy SHA256 required')
    if not 0 < len(data) <= POLICY_LIMIT or hashlib.sha256(data).hexdigest() != expected:
        raise ValueError('suspension policy size or SHA256 differs')
    try:
        value = json.loads(data, object_pairs_hook=unique_pairs)
        if (not isinstance(value, dict) or set(value) != {'schema_version', 'policy', 'intervals'} or
                type(value['schema_version']) is not int or value['schema_version'] != 1 or
                value['policy'] != 'a-share-null-placeholder-v1' or
                not isinstance(value['intervals'], list) or len(value['intervals']) > 10000):
            raise ValueError('invalid suspension policy')
        for item in value['intervals']:
            if (not isinstance(item, dict) or set(item) != {'symbol', 'start_ms', 'end_ms', 'evidence'} or
                    not isinstance(item['symbol'], str) or not re.fullmatch('[0-9]{6}\\.(SH|SZ)', item['symbol']) or
                    type(item['start_ms']) is not int or type(item['end_ms']) is not int or
                    not 0 < item['start_ms'] < item['end_ms'] <= 32503680000000 - 28800000 or
                    not isinstance(item['evidence'], str) or not item['evidence'].strip(' \t\n\r\f\v')):
                raise ValueError('unconfirmed suspension interval')
    except (RecursionError, TypeError, KeyError) as error:
        raise ValueError('invalid suspension policy') from error
    return {'sha256': expected, 'intervals': len(value['intervals'])}


def snapshot(destination, credentials, profile=None, suspensions=None, suspensions_sha256=None):
    destination = Path(destination)
    if destination.absolute() != destination.resolve():
        raise ValueError('unsafe snapshot path')
    sources = [('credentials.json', credentials, 16384)]
    if profile:
        sources.append(('storage.json', profile, 4096))
    if bool(suspensions) != bool(suspensions_sha256):
        raise ValueError('suspension file and SHA256 must be supplied together')
    if suspensions:
        sources.append(('suspensions.json', suspensions, POLICY_LIMIT))
    payloads = []
    policy = None
    for name, source, limit in sources:
        if Path(source).absolute().parent != Path(source).resolve().parent:
            raise ValueError('unsafe configuration parent path')
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
                raise ValueError('invalid configuration file')
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise ValueError('configuration must be private and owned by deployment user')
            data = stream.read(limit + 1)
            if name == 'suspensions.json':
                policy = validate_policy(data, suspensions_sha256)
            elif len(data) > limit or not isinstance(json.loads(data, object_pairs_hook=unique_pairs), dict):
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
    return policy


if __name__ == '__main__':
    try:
        snapshot(*sys.argv[1:])
    except (OSError, ValueError, TypeError):
        print('configuration snapshot failed', file=sys.stderr)
        raise SystemExit(1)
