#!/usr/bin/env python3
"""Independent stdlib SigV4 oracle using only publicly synthetic credentials."""

import argparse
import hashlib
import hmac
import json
from pathlib import Path


def vectors():
    host = "a" * 32 + ".r2.cloudflarestorage.com"
    path = "/history-cache-staging/r2-history-staging/offline-001/current.json"
    timestamp = "20260921T010203Z"
    scope = "20260921/auto/s3/aws4_request"
    cases = []
    for method, body, fields in (
        ("GET", b"", {"range": "bytes=10-99", "if-match": '"opaque/Case-123"'}),
        ("PUT", b"synthetic-pointer-bytes\n", {"if-none-match": "*", "content-type": "application/octet-stream"}),
        ("PUT", bytes(range(256)), {"if-match": '"opaque/not-an-md5"', "cache-control": "no-cache, no-store"}),
    ):
        payload = hashlib.sha256(body).hexdigest()
        headers = {**fields, "host": host, "x-amz-date": timestamp, "x-amz-content-sha256": payload}
        names = ";".join(sorted(headers))
        canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
        canonical = "\n".join([method, path, "", canonical_headers, names, payload])
        canonical_hash = hashlib.sha256(canonical.encode("ascii")).hexdigest()
        message = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{canonical_hash}"
        key = b"AWS4SYNTHETIC/SECRET+NOTREAL="
        for item in ("20260921", "auto", "s3", "aws4_request"):
            key = hmac.digest(key, item.encode("ascii"), "sha256")
        signature = hmac.new(key, message.encode("ascii"), hashlib.sha256).hexdigest()
        authorization = (f"AWS4-HMAC-SHA256 Credential=SYNTHETICKEYID/{scope}, "
                         f"SignedHeaders={names}, Signature={signature}")
        cases.append({"method": method, "host": host, "path": path, "body_hex": body.hex(), "headers": fields,
                      "payload_sha256": payload, "canonical_request_sha256": canonical_hash,
                      "authorization": authorization})
    return {"schema_version": 1, "scope": "synthetic-only-auto-s3", "amz_date": timestamp, "cases": cases}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", action="store_true")
    args = parser.parse_args()
    expected = vectors()
    if args.emit:
        print(json.dumps(expected, sort_keys=True, indent=2))
    else:
        actual = json.loads((Path(__file__).parent / "golden" / "sigv4-v1.json").read_text(encoding="utf-8"))
        if actual != expected:
            raise SystemExit("SigV4 golden differs from independent stdlib oracle")
        print("PASS sigv4_reference vectors=3")
        host = "a" * 32 + ".r2.cloudflarestorage.com"
        path = "/history-cache-staging/r2-history-staging/offline-001/current.json"
        payload = hashlib.sha256(b"").hexdigest()
        fields = {"host": host, "if-match": '"opaque/Case-123"', "x-amz-date": "20260921T010203Z", "x-amz-content-sha256": payload}
        names = ";".join(sorted(fields))
        canonical = "\n".join(["DELETE", path, "", "".join(f"{key}:{fields[key]}\n" for key in sorted(fields)), names, payload])
        scope = "20260921/auto/s3/aws4_request"
        message = "AWS4-HMAC-SHA256\n20260921T010203Z\n" + scope + "\n" + hashlib.sha256(canonical.encode("ascii")).hexdigest()
        key = b"AWS4SYNTHETIC/SECRET+NOTREAL="
        for value in ("20260921", "auto", "s3", "aws4_request"):
            key = hmac.digest(key, value.encode("ascii"), "sha256")
        signature = hmac.digest(key, message.encode("ascii"), "sha256").hex()
        golden = json.loads((Path(__file__).parent / "golden/sigv4-delete-v1.json").read_text())
        if golden != {"authorization": f"AWS4-HMAC-SHA256 Credential=SYNTHETICKEYID/{scope}, SignedHeaders={names}, Signature={signature}"}:
            raise SystemExit("DELETE SigV4 differs from independent oracle")
        print("PASS sigv4_delete_reference vectors=1")
