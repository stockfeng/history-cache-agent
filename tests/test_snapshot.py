"""Snapshot CLI and independent real-row-capable R2H1 decoder; no network."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import tempfile

from test_offline import canonical, decode_columns, identity_hash, zstd_decoder


def check(condition, message):
    if not condition:
        raise ValueError(message)


def sha(value):
    return hashlib.sha256(value).hexdigest()


def read(path, maximum=1024 * 1024):
    check(path.is_file() and path.stat().st_size <= maximum, "invalid file or input size")
    return path.read_bytes()


def decode_pack(payload, entry):
    descriptor = entry["object"]
    check(len(payload) == descriptor["bytes"] and sha(payload) == descriptor["sha256"]
          and descriptor["key"] == "data/v1/" + sha(payload) + ".r2b", "pack hash or size mismatch")
    check(len(payload) >= 240, "short pack")
    header = payload[:160]
    check(struct.unpack_from("!4sHHHHI", header) == (b"R2H1", 1, 160, 1, 1, 60), "pack format mismatch")
    check(header[16:32].hex() == entry["series_id"] == identity_hash(entry["identity"]), "series mismatch")
    check(header[32:64].hex() == entry["data_version"], "data version mismatch")
    version, start, end, first, last, total = struct.unpack_from("!QqqqqQ", header, 64)
    check((version, start, end, total) == (entry["source_version"], entry["coverage_start_ms"],
                                        entry["coverage_end_ms"], entry["rows"]), "pack metadata differs")
    count, toc_size, toc_offset, data_offset, data_bytes = struct.unpack_from("!IIQQQ", header, 112)
    check(0 < total <= 5000 and count == (total + 1023) // 1024 and
          (toc_size, toc_offset, data_offset) == (80, 160, 160 + 80 * count)
          and data_offset + data_bytes == len(payload) and header[144:] == bytes(16), "invalid pack bounds")
    check(sha(payload[:data_offset]) == descriptor["index_sha256"], "index hash mismatch")
    decompress = zstd_decoder()
    decoded = bytearray()
    cursor = data_offset
    previous = -1
    for index in range(count):
        seq, rows, block_first, block_last, raw_size, size, offset, reserved, digest = struct.unpack_from(
            "!IIqqIIQQ32s", payload, 160 + 80 * index)
        check(seq == index and reserved == 0 and offset == cursor and
              rows == min(1024, total - 1024 * index) and 0 < raw_size <= 65536 and
              0 < size <= 65536 and offset + size <= len(payload), "invalid block bounds")
        frame = payload[offset:offset + size]
        check(hashlib.sha256(frame).digest() == digest, "block checksum mismatch")
        values = decode_columns(decompress(frame, raw_size))
        check(len(values) == rows and (values[0][0], values[-1][0]) == (block_first, block_last), "block rows differ")
        for row in values:
            check(start <= row[0] < end and row[0] > previous and row[-1] >= 0, "invalid decoded row")
            previous = row[0]
            decoded.extend(struct.pack("<q4fq", *row))
        cursor += size
    check(len(decoded) == total * 32 and cursor == len(payload)
          and (struct.unpack_from("<q", decoded)[0], previous) == (first, last), "pack totals differ")
    return bytes(decoded)


def fixture(count):
    start = 1_764_518_400_000
    mapping = {"host": "192.0.2.10", "port": 8848, "database": "dfs://history", "table": "stock",
               "time_column": "trade_time", "code_column": "code", "timestamp_offset_ms": 28800000}
    identity = {"dataset": "ddb-history-snapshot", "market": "SZ", "symbol": "000001.SZ",
                "period_seconds": 60, "adjust": "none"}
    schema = {"engine_type": "TSDB", "partition_columns": ["trade_time", "code"],
              "sort_columns": ["code", "trade_time"], "columns": [
                  {"name": name, "typeString": kind} for name, kind in (
                      ("code", "SYMBOL"), ("trade_time", "TIMESTAMP"), ("duration", "INT"),
                      ("open", "DOUBLE"), ("high", "DOUBLE"), ("low", "DOUBLE"),
                      ("close", "DOUBLE"), ("volume", "LONG"))]}
    path = "/history/202512M/Key16/abc"
    chunk_id = "7f4bab27-10f2-7abe-0947-64ad394cdd53"
    state = {"runtime": "3.00.5 2026.02.06 LINUX x86_64", "node": "local8848", "partition": path, "schema": schema,
             "chunk": {"chunkId": chunk_id, "dfsPath": path, "site": "local8848", "type": 1,
                       "flag": 0, "version": 7, "state": 0, "resolved": False,
                       "versionList": "cid : 123,stock=>123:5000; # "},
             "tablet": {"chunkId": chunk_id, "dfsPath": path, "tableName": "stock", "version": 7,
                        "rowNum": 5000, "createCids": "[]", "latestPhysicalDir": "stock_2"}}
    semantics = {"query_semantics": "upcloud-none-1m-double-to-float32-offset-v1", "mapping": mapping, "schema": schema}
    origin = {"contract": "ddb-single-chunk-guard-v1", "mapping": mapping,
              "runtime": state["runtime"], "node": state["node"], "partition": path,
              "chunk_id": chunk_id, "physical_table": "stock_2"}
    raw = b"".join(struct.pack("<q4dq", start + 34200000 + i * 60000 + 28800000,
                                10.01 + i / 100, 11.25 + i / 100, 9.875 + i / 100,
                                10.501 + i / 100, 2**53 + i) for i in range(count))
    rows = b"".join(struct.pack("<q4fq", timestamp - 28800000, *values)
                    for timestamp, *values in struct.iter_unpack("<q4dq", raw))
    query = (f'select top 5001 long(trade_time) as ddb_timestamp_ms, open, high, low, close, volume '
             f'from loadTable("dfs://history", "stock") where code=`000001.SZ and duration=60 '
             f'and trade_time>=timestamp({start + 28800000}) '
             f'and trade_time<timestamp({start + 4 * 86400000 + 28800000}) '
             'order by ddb_timestamp_ms asc, open asc, high asc, low asc, close asc, volume asc')
    source = {"schema_version": 1, "kind": "ddb-single-chunk-snapshot", "contract": origin["contract"],
              "identity": identity, "source": {**mapping, "config_sha256": "a" * 64,
                                                 "schema_sha256": sha(canonical(schema)), "query_sha256": sha(query.encode())},
              "query_semantics": semantics["query_semantics"], "requested_start_ms": start,
              "requested_end_ms": start + 4 * 86400000, "max_rows": 5000, "query_row_limit": 5001,
              "max_partitions": 1, "production_eligible": False, "upstream_finality_proven": False,
              "coverage_verified": True, "source_snapshot_proven": True,
              "coverage_scope": "ddb-visible-single-partition-at-native-version", "source_version": 7,
              "dataset_epoch": "ddb-chunk-" + sha(canonical(origin)), "data_version": sha(canonical(semantics)),
              "row_count": count, "rows_sha256": sha(rows), "raw_rows_sha256": sha(raw),
              "freshness_policy": "explicit-revalidation-required", "guard": {"before": state, "after": copy.deepcopy(state)}}
    return source, rows


def invoke(tool, *args, expected=0):
    result = subprocess.run([str(tool), *map(str, args)], capture_output=True, text=True, timeout=10)
    check(result.returncode == expected, f"unexpected CLI result: {result.stdout} {result.stderr}")
    return json.loads(result.stdout or result.stderr)


def exercise(tool, root, count):
    source, rows = fixture(count)
    directory = root / f"rows-{count}"
    directory.mkdir()
    source_path, rows_path = directory / "source.json", directory / "rows.bin"
    source_path.write_bytes(canonical(source))
    rows_path.write_bytes(rows)
    candidate = directory / "candidate"
    result = invoke(tool, "encode", "--source", source_path, "--rows", rows_path, "--output", candidate)
    check(result["rows"] == count and not result["production_eligible"], "encode summary differs")
    manifest = json.loads(read(candidate / "candidate.json"))
    entry = manifest["entries"][0]
    check(entry["coverage_complete"] and entry["source_version"] == 7, "manifest source version differs")
    if count:
        pack = read(candidate / Path(entry["object"]["key"]).name)
        check(decode_pack(pack, entry) == rows, "independent pack rows differ")
    else:
        check(entry["object"] is None, "empty interval has an object")
    result = invoke(tool, "local", "--candidate", candidate, "--output", directory / "full")
    check(result["result"] == "HIT" and result["rows"] == count and result["rows_sha256"] == sha(rows), "full HIT differs")
    first = source["requested_start_ms"] + 34200000
    result = invoke(tool, "local", "--candidate", candidate, "--output", directory / "limited",
                    "--start", first, "--end", source["requested_end_ms"], "--max-count", 3)
    check(result["rows"] == min(count, 3) and result["rows_sha256"] == sha(rows[:96]), "max_count differs")
    result = invoke(tool, "local", "--candidate", candidate, "--output", directory / "miss",
                    "--end", source["requested_end_ms"] + 1)
    check(result["result"] == "MISS" and not (directory / "miss" / "rows.bin").exists(), "uncovered range was delivered")
    result = invoke(tool, "plan", "--candidate", candidate, "--account", "a" * 32,
                    "--bucket", "history-cache-staging", "--run-id", "snapshot-offline")
    check(result["max_requests"] == 48 and len(result["objects"]) == (4 if count else 3), "transfer scope differs")
    result = invoke(tool, "plan", "--candidate", candidate, "--account", "a" * 32,
                    "--bucket", "history-cache-staging", "--run-id", "snapshot-upgrade",
                    "--expected-seq", "7")
    check(result["expected_seq"] == 7 and result["target"]["publication_seq"] == 8,
          "upgrade publication plan differs")
    return source_path, rows_path, source


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="history-cache-snapshot-") as temporary:
        root = Path(temporary)
        for count in (0, 1, 240, 2050, 5000):
            source_path, rows_path, source = exercise(args.tool, root, count)
        cases = [lambda s: s.update(coverage_verified=False), lambda s: s.update(production_eligible=True),
                 lambda s: s.update(upstream_finality_proven=True), lambda s: s.update(source_version=8),
                 lambda s: s.update(dataset_epoch="wrong"), lambda s: s.update(data_version="a" * 64),
                 lambda s: s.update(row_count=5001), lambda s: s.update(rows_sha256="0" * 64),
                 lambda s: s["guard"]["after"]["chunk"].update(version=8),
                 lambda s: s["guard"]["before"]["chunk"].update(state=1),
                 lambda s: s["identity"].update(symbol="000002.SZ"),
                 lambda s: s.update(requested_end_ms=s["requested_end_ms"] + 86400000),
                 lambda s: s.update(max_rows=4999, query_row_limit=5000),
                 lambda s: s["source"].update(query_sha256="0" * 64)]
        for index, edit in enumerate(cases):
            changed = copy.deepcopy(source)
            edit(changed)
            source_path.write_bytes(canonical(changed))
            invoke(args.tool, "encode", "--source", source_path, "--rows", rows_path,
                   "--output", root / f"bad-{index}", expected=2)
    print("PASS snapshot_cli roundtrip=5 negative_checks=14 network=0")


if __name__ == "__main__":
    main()
