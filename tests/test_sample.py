"""Offline query-observation CLI checks; never enable network access."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile

from test_offline import decode_columns, zstd_decoder


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool", type=Path, required=True)
    args = parser.parse_args()

    def run(*items, success=True):
        result = subprocess.run([str(args.tool), *map(str, items)], capture_output=True, text=True,
                                timeout=10, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        if (result.returncode == 0) != success:
            raise AssertionError(result.stderr or result.stdout)
        return json.loads(result.stdout) if result.returncode == 0 else None

    decompress = zstd_decoder()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        start = 1764518400000
        for count in (1, 240, 1025, 5000):
            rows = b"".join(struct.pack("<q4fq", start + index * 60000, 10.01, 11.0, 9.0, 10.5,
                                        2**53 + index + 1) for index in range(count))
            source = {"schema_version": 1, "kind": "ddb-query-observation", "coverage_verified": False,
                      "source_version": None, "row_count": count, "rows_sha256": hashlib.sha256(rows).hexdigest(),
                      "requested_start_ms": start, "requested_end_ms": start + 7 * 86400000,
                      "identity": {"dataset": "ddb-query-observation", "market": "SZ", "symbol": "000001.SZ",
                                   "period_seconds": 60, "adjust": "none"},
                      "source": {key: "a" * 64 for key in ("config_sha256", "schema_sha256", "query_sha256")}}
            row_file, source_file = root / f"{count}.rows", root / f"{count}.json"
            row_file.write_bytes(rows)
            source_file.write_text(json.dumps(source))
            sample = root / f"sample-{count}"
            descriptor = run("encode", "--source", source_file, "--rows", row_file, "--output", sample)
            decoded = root / f"decoded-{count}"
            run("decode", "--sample", sample, "--output", decoded)
            assert decoded.read_bytes() == rows
            object_bytes = (sample / "sample.r2b").read_bytes()
            independent = bytearray()
            for block in descriptor["blocks"]:
                frame = object_bytes[block["offset"]:block["offset"] + block["bytes"]]
                assert hashlib.sha256(frame).hexdigest() == block["sha256"]
                raw = decompress(frame, block["raw_bytes"])
                for row in decode_columns(raw):
                    independent.extend(struct.pack("<q4fq", *row))
            assert independent == rows
            assert descriptor["pack_format"] is None and not descriptor["production_eligible"]
            plan = run("plan", "--sample", sample, "--account", "a" * 32,
                       "--bucket", "history-cache-staging", "--run-id", "sample-offline-only")
            assert plan["retained_objects"] == 2 and plan["delete_requests"] == 0
            assert plan["current_pointer_written"] is False and plan["max_requests"] == 8
            run("plan", "--sample", sample, "--account", "a" * 32,
                "--bucket", "history-cache-production", "--run-id", "sample-offline-only", success=False)
            run("encode", "--source", source_file, "--rows", row_file, "--output", sample, success=False)
            broken = root / f"broken-{count}"
            broken.mkdir()
            (broken / "observation.json").write_text(json.dumps(descriptor))
            altered = bytearray(object_bytes)
            altered[-1] ^= 1
            (broken / "sample.r2b").write_bytes(altered)
            run("decode", "--sample", broken, "--output", root / "must-not-exist", success=False)
            source["coverage_verified"] = True
            source_file.write_text(json.dumps(source))
            run("encode", "--source", source_file, "--rows", row_file,
                "--output", root / "false-coverage", success=False)
        print("PASS sample_cli roundtrip=4 independent_decode=4 negative_checks=16 network=0")


if __name__ == "__main__":
    main()
