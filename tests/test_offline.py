"""Independent binary checks and replay of the offline CLI; standard library only."""

import argparse
import ctypes
import ctypes.util
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile


START = 1_704_067_200_000


def run(tool, *args, expected=0):
    result = subprocess.run(
        [str(tool), *map(str, args)], capture_output=True, text=True, timeout=10,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    if result.returncode != expected:
        raise AssertionError(f"{args}: rc={result.returncode}\n{result.stdout}\n{result.stderr}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"


def fixture_row(index):
    price = 100 + (index % 1000) * 0.125
    return (START + index * 60_000, price, price + 1, price - 1, price + 0.5, index * 17 + 1)


def rows_hash(start, end):
    digest = hashlib.sha256()
    for index in range(start, end):
        digest.update(struct.pack("<q4fq", *fixture_row(index)))
    return digest.hexdigest()


def identity_hash(identity):
    fields = [identity[name] for name in ("dataset", "market", "symbol")]
    fields += [str(identity["period_seconds"]), identity["adjust"]]
    text = "history-series-v1\n" + "".join(f"{len(field)}:{field}\n" for field in fields)
    return hashlib.sha256(text.encode("ascii")).hexdigest()[:32]


def decode_columns(raw, expected_schema=1):
    schema, encoding, flags, count, timestamp_bytes, reserved = struct.unpack_from("!BBHIII", raw)
    assert expected_schema in (1, 2, 3)
    assert (schema, encoding, flags, reserved) == (2 if expected_schema == 3 else 1, 2, 0xFF if expected_schema >= 2 else 0x3F, 0)
    assert len(raw) == 16 + timestamp_bytes + count * (56 if expected_schema == 3 else 40 if expected_schema == 2 else 24)
    timestamps = [struct.unpack_from("!q", raw, 16)[0]]
    offset = 24
    delta = 0
    for index in range(1, count):
        unsigned = 0
        for shift in range(0, 70, 7):
            byte = raw[offset]
            offset += 1
            unsigned |= (byte & 127) << shift
            if byte < 128:
                break
        difference = (unsigned >> 1) ^ -(unsigned & 1)
        delta = difference if index == 1 else delta + difference
        assert delta > 0
        timestamps.append(timestamps[-1] + delta)
    assert offset == 16 + timestamp_bytes
    prices = []
    for _ in range(4):
        prices.append(struct.unpack_from(f"<{count}" + ("d" if expected_schema == 3 else "f"), raw, offset))
        offset += count * (8 if expected_schema == 3 else 4)
    volume = struct.unpack_from(f"<{count}q", raw, offset)
    if expected_schema == 3:
        open_oi = struct.unpack_from(f"<{count}q", raw, offset + count * 8)
        close_oi = struct.unpack_from(f"<{count}q", raw, offset + count * 16)
        return list(zip(timestamps, *prices, volume, open_oi, close_oi))
    if expected_schema == 2:
        offset += count * 8
        turnover = struct.unpack_from(f"<{count}d", raw, offset)
        interest = struct.unpack_from(f"<{count}q", raw, offset + count * 8)
        return list(zip(timestamps, *prices, volume, turnover, interest))
    return list(zip(timestamps, *prices, volume))


def zstd_decoder():
    name = ctypes.util.find_library("zstd")
    if not name:
        raise AssertionError("libzstd must be installed with the C++ build dependencies")
    library = ctypes.CDLL(name)
    library.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
    library.ZSTD_decompress.restype = ctypes.c_size_t
    library.ZSTD_isError.argtypes = [ctypes.c_size_t]
    library.ZSTD_isError.restype = ctypes.c_uint

    def decompress(frame, size):
        assert frame[:4] == b"\x28\xb5\x2f\xfd" and frame[4] & 4
        output = ctypes.create_string_buffer(size)
        source = ctypes.create_string_buffer(frame)
        actual = library.ZSTD_decompress(output, size, source, len(frame))
        assert not library.ZSTD_isError(actual) and actual == size
        return output.raw

    return decompress


def verify_pack(path, entry, decompress):
    descriptor = entry["object"]
    object_hash = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(65536):
            object_hash.update(chunk)
    assert object_hash.hexdigest() == descriptor["sha256"]
    assert path.stat().st_size == descriptor["bytes"]
    with path.open("rb") as source:
        header = source.read(160)
        assert struct.unpack_from("!4sHHHHI", header) == (b"R2H1", 1, 160, 1, 1, 60)
        assert header[16:32].hex() == entry["series_id"]
        assert header[32:64].hex() == entry["data_version"]
        version, start, end, first, last, total = struct.unpack_from("!QqqqqQ", header, 64)
        assert version == entry["source_version"]
        assert (start, end, total) == (entry["coverage_start_ms"], entry["coverage_end_ms"], entry["rows"])
        count, toc_size, toc_offset, data_offset, data_bytes = struct.unpack_from("!IIQQQ", header, 112)
        assert (toc_size, toc_offset, data_offset) == (80, 160, 160 + 80 * count)
        assert data_offset + data_bytes == descriptor["bytes"] and header[144:] == bytes(16)
        toc = source.read(count * 80)
        assert hashlib.sha256(header + toc).hexdigest() == descriptor["index_sha256"]
        result_hash = hashlib.sha256()
        rows_seen = 0
        cursor = data_offset
        for index in range(count):
            seq, rows, block_first, block_last, raw_size, size, offset, reserved, digest = struct.unpack_from(
                "!IIqqIIQQ32s", toc, index * 80)
            assert seq == index and reserved == 0 and offset == cursor
            assert 0 < rows <= 1024 and 0 < raw_size <= 65536 and 0 < size <= 65536
            source.seek(offset)
            frame = source.read(size)
            assert hashlib.sha256(frame).digest() == digest
            decoded = decode_columns(decompress(frame, raw_size))
            assert len(decoded) == rows
            assert (decoded[0][0], decoded[-1][0]) == (block_first, block_last)
            for row in decoded:
                expected = struct.pack("<q4fq", *fixture_row(rows_seen))
                assert struct.pack("<q4fq", *row) == expected
                result_hash.update(expected)
                rows_seen += 1
            cursor += size
        assert rows_seen == total and cursor == descriptor["bytes"]
        assert (first, last) == (fixture_row(0)[0], fixture_row(rows_seen - 1)[0])
        return result_hash.hexdigest()


def exercise(tool, base, count, decompress):
    directory = base / f"case-{count}"
    directory.mkdir()
    fixture = directory / "fixture"
    root = directory / "store"
    meta = run(tool, "fixture", "--output", fixture, "--rows", count)
    assert meta["rows_sha256"] == rows_hash(0, count)
    candidate_bytes = (fixture / "candidate.json").read_bytes()
    candidate = json.loads(candidate_bytes)
    assert canonical(candidate) == candidate_bytes
    entry = candidate["entries"][0]
    assert entry["series_id"] == identity_hash(entry["identity"])
    if count:
        assert verify_pack(fixture / Path(entry["object"]["key"]).name, entry, decompress) == meta["rows_sha256"]
    else:
        assert entry["object"] is None
    run(tool, "init", "--root", root)
    publish_args = ("publish", "--root", root, "--candidate", fixture / "candidate.json",
                    "--pack-dir", fixture, "--expected-seq", "0")
    if count < 500_000:
        for stage in ("after-objects", "after-manifest"):
            run(tool, *publish_args, "--fail", stage, expected=2)
            assert not (root / "current.json").exists()
        run(tool, *publish_args, "--fail", "after-pointer", expected=2)
        assert json.loads((root / "current.json").read_bytes())["publication_seq"] == 1
        assert not (root / "checkpoint.json").exists()
    pointer = run(tool, *publish_args)
    assert pointer["publication_seq"] == 1
    assert (root / "checkpoint.json").read_bytes() == (root / "current.json").read_bytes()
    golden_path = Path(__file__).parent / "golden" / "catalog-v1.json"
    if count == 0:
        golden = json.loads(golden_path.read_bytes())
        assert candidate_bytes.hex() == golden["empty_manifest_hex"]
        assert canonical(pointer).hex() == golden["empty_pointer_hex"]
    if count == 1:
        golden = json.loads((golden_path.parent / "pack-v1.json").read_bytes())
        pack_path = directory / "fixed-golden.r2b"
        pack_path.write_bytes(bytes.fromhex(golden["pack_hex"]))
        assert verify_pack(pack_path, golden["manifest"]["entries"][0], decompress) == rows_hash(0, 1)
    read_args = ("read", "--root", root, "--series-id", meta["series_id"], "--data-version", meta["data_version"])
    result = run(tool, *read_args, "--start", meta["coverage_start_ms"], "--end", meta["coverage_end_ms"])
    assert result["result"] == "HIT" and result["rows"] == count and result["rows_sha256"] == meta["rows_sha256"]
    if count > 1025:
        start, end = 1020, 1030
        ranged = run(tool, *read_args, "--start", fixture_row(start)[0], "--end", fixture_row(end)[0], "--max-count", 3)
        assert ranged["rows"] == 3 and ranged["rows_sha256"] == rows_hash(start, start + 3)
    missing = run(tool, *read_args, "--start", meta["coverage_start_ms"],
                  "--end", meta["coverage_end_ms"] + 1, expected=3)
    assert missing["result"] == "MISS"
    if count < 500_000:
        rolled = run(tool, "rollback", "--root", root, "--manifest-key", pointer["manifest_key"],
                     "--manifest-sha256", pointer["manifest_sha256"], "--expected-seq", 1)
        assert rolled["publication_seq"] == 2 and rolled["manifest_sha256"] == pointer["manifest_sha256"]
    run(tool, *read_args, "--start", meta["coverage_start_ms"], "--end", meta["coverage_end_ms"],
        "--min-seq", 3, expected=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool", type=Path, required=True)
    cases = parser.add_mutually_exclusive_group()
    cases.add_argument("--small-only", action="store_true")
    cases.add_argument("--large-only", action="store_true")
    args = parser.parse_args()
    decompress = zstd_decoder()
    counts = (500_000,) if args.large_only else (0, 1, 240, 2050) if args.small_only else (0, 1, 240, 2050, 500_000)
    with tempfile.TemporaryDirectory(prefix="history-cache-offline-") as directory:
        for count in counts:
            exercise(args.tool.resolve(), Path(directory), count, decompress)
            print(f"PASS offline CLI and independent binary/row decoder: {count} rows", flush=True)


if __name__ == "__main__":
    main()
