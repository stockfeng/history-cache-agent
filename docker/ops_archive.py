"""Bounded local evidence archives; no extraction, remote access or secret discovery."""

import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tarfile
import tempfile
import time

MAX_BYTES = 256 * 1024**2
MAX_FILES = 10000


def require(value, message):
    if not value:
        raise ValueError(message)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def private(path):
    path = Path(path).absolute()
    require(path == path.resolve(), 'unsafe archive directory')
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(path.stat().st_uid == os.geteuid() and not path.stat().st_mode & 0o077, 'private directory required')
    sync(path.parent)
    return path


def safe_name(name):
    require(isinstance(name, str) and name and '\\' not in name and
            not PurePosixPath(name).is_absolute() and
            all(p not in ('', '.', '..') for p in name.split('/')), 'unsafe archive member')
    return name


def bounded_read(path, limit):
    path = Path(path)
    require(path.absolute() == path.resolve(), 'symlink in archive input')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit,
                'invalid or oversized archive file')
        raw = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(raw) <= limit and (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns),
                'archive source changed')
        return raw


def inventory(root, names, deadline):
    root = Path(root).absolute()
    require(root == root.resolve(), 'unsafe archive source root')
    pending = [safe_name(n) for n in names]
    seen, files, dirs, total = set(), [], [], 0
    while pending:
        require(time.monotonic() < deadline and len(seen) < MAX_FILES, 'archive time/entry budget exceeded')
        name = pending.pop()
        require(name not in seen, 'overlapping archive roots')
        seen.add(name)
        path = root / name
        require(not path.is_symlink(), 'symlink in archive source')
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            dirs.append(name)
            with os.scandir(path) as scan:
                for item in scan:
                    require(len(pending) + len(seen) < MAX_FILES, 'archive entry budget exceeded')
                    pending.append(safe_name(name + '/' + item.name))
        else:
            raw = bounded_read(path, min(32 * 1024**2, MAX_BYTES - total))
            total += len(raw)
            files.append(dict(name=name, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
    return dict(files=sorted(files, key=lambda f: f['name']), directories=sorted(dirs), bytes=total)


def verify(bundle, deadline):
    bundle = Path(bundle)
    raw = bounded_read(bundle / 'manifest.json', 4 * 1024**2)
    manifest = json.loads(raw)
    require(manifest['version'] == 1 and len(manifest['files']) <= MAX_FILES and
            0 <= manifest['bytes'] <= MAX_BYTES, 'invalid archive manifest')
    names = [safe_name(f['name']) for f in manifest['files']]
    require(names == sorted(set(names)) and sum(f['bytes'] for f in manifest['files']) == manifest['bytes'],
            'invalid archive file list')
    require(len(manifest['directories']) + len(names) <= MAX_FILES, 'archive entries exceed limit')
    for name in manifest['directories']:
        safe_name(name)
    archive = bundle / 'evidence.tar.gz'
    require(archive.absolute() == archive.resolve() and archive.is_file() and
            archive.stat().st_size <= MAX_BYTES + 16 * 1024**2, 'invalid evidence archive')
    hasher = hashlib.sha256()
    with archive.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            require(time.monotonic() < deadline, 'archive verify deadline exceeded')
            hasher.update(chunk)
    require(hasher.hexdigest() == manifest['archive_sha256'], 'archive checksum differs')
    # Never extract an archive. Compare each regular member with its exact manifest entry.
    with tarfile.open(archive, mode='r|gz') as tar:
        for expected in manifest['files']:
            require(time.monotonic() < deadline, 'archive verify deadline exceeded')
            member = tar.next()
            require(member and member.isfile() and member.name == expected['name'] and
                    member.size == expected['bytes'], 'archive member differs')
            content_hash = hashlib.sha256()
            with tar.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    require(time.monotonic() < deadline, 'archive verify deadline exceeded')
                    content_hash.update(chunk)
            require(content_hash.hexdigest() == expected['sha256'], 'archive content differs')
        require(tar.next() is None, 'extra archive member')
    return manifest, hashlib.sha256(raw).hexdigest()


def create(root, names, destination, binding, deadline):
    root, destination = Path(root).absolute(), Path(destination).absolute()
    require(root != destination and root not in destination.parents and destination not in root.parents,
            'archive must be outside source tree')
    private(destination.parent)
    require(destination == destination.resolve(), 'unsafe archive destination')
    expected = dict(version=1, binding=binding, **inventory(root, names, deadline))
    if destination.exists():
        existing, digest = verify(destination, deadline)
        require({k: v for k, v in existing.items() if k != 'archive_sha256'} == expected, 'existing archive differs')
        return existing, digest
    require(shutil.disk_usage(destination.parent).free >= expected['bytes'] + 32 * 1024**2,
            'insufficient archive disk headroom')
    temporary = Path(tempfile.mkdtemp(prefix='.archive-', dir=destination.parent))
    try:
        archive = temporary / 'evidence.tar.gz'
        with archive.open('xb') as output:
            os.chmod(archive, 0o600)
            with gzip.GzipFile(fileobj=output, mode='wb', compresslevel=1, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode='w|') as tar:
                    for item in expected['files']:
                        require(time.monotonic() < deadline, 'archive deadline exceeded')
                        raw = bounded_read(root / item['name'], min(32 * 1024**2, item['bytes']))
                        require(hashlib.sha256(raw).hexdigest() == item['sha256'], 'source changed before archiving')
                        header = tarfile.TarInfo(item['name'])
                        header.size, header.mode, header.mtime = len(raw), 0o600, 0
                        tar.addfile(header, io.BytesIO(raw))
            output.flush()
            os.fsync(output.fileno())
        hasher = hashlib.sha256()
        with archive.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                hasher.update(chunk)
        manifest = dict(expected, archive_sha256=hasher.hexdigest())
        with (temporary / 'manifest.json').open('xb') as stream:
            os.chmod(stream.name, 0o600)
            stream.write(encode(manifest))
            stream.flush()
            os.fsync(stream.fileno())
        result = verify(temporary, deadline)
        require(inventory(root, names, deadline) == {k: expected[k] for k in ('files', 'directories', 'bytes')},
                'source changed during archiving')
        sync(temporary)
        os.rename(temporary, destination)
        sync(destination.parent)
        return result
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def prune(root, manifest, deadline):
    """Caller must hold producer lock and persist PRUNING before allowing missing files."""
    root = Path(root).absolute()
    expected = {item['name']: item for item in manifest['files']}
    allowed = set(expected) | set(manifest['directories'])
    top = sorted(n for n in allowed if '/' not in n)
    present = [n for n in top if (root / n).exists() or (root / n).is_symlink()]
    current = inventory(root, present, deadline)
    require(set(current['directories']) <= set(manifest['directories']) and
            all(item['name'] in expected and item == expected[item['name']] for item in current['files']),
            'changed or additional file prevents pruning')
    for item in current['files']:
        require(time.monotonic() < deadline, 'prune deadline exceeded')
        path = root / item['name']
        require(hashlib.sha256(bounded_read(path, item['bytes'])).hexdigest() == item['sha256'], 'source changed before prune')
        path.unlink()
        sync(path.parent)
    for name in sorted(current['directories'], key=lambda n: (n.count('/'), n), reverse=True):
        (root / name).rmdir()
        sync((root / name).parent)
