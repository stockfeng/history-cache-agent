"""Fail CI if required CTest entries or named staging CLI checks are omitted."""

import argparse
import json
from pathlib import Path
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", type=Path, required=True)
    args = parser.parse_args()
    required = {"columns", "pack", "catalog", "store", "reader", "object_store", "conditional_publish", "object_reader",
                "offline_interop", "offline_streaming_500k", "http_protocol", "sigv4", "s3_protocol", "curl_offline",
                "sigv4_reference", "journal_storage", "journal_recovery", "staging_cli", "sample_cli", "sample_transfer", "snapshot_cli"}
    manifest = json.loads((Path(__file__).parent / "staging-cases.json").read_text())
    counts = {"staging_plan": 5, "staging_execution": 13, "staging_ledger": 9, "staging_delete": 3, "staging_retention": 20}
    assert set(manifest) == set(counts), "incomplete staging suite manifest"
    for suite, labels in manifest.items():
        assert len(labels) == len(set(labels)) == counts[suite], "incomplete or duplicate staging cases"
        required.update(suite + "." + label for label in labels)
    root = ET.parse(args.junit).getroot()
    tests = root.findall("testcase")
    assert len(tests) == len(required) == 71 and root.attrib["tests"] == "71" and root.attrib["failures"] == "0", "incomplete CTest run"
    assert {item.attrib["name"] for item in tests} == required, "CTest names differ from required matrix"
    assert all(item.attrib.get("status") == "run" and item.find("skipped") is None and item.find("failure") is None for item in tests), "nonpassing case"
    by_name = {item.attrib["name"]: (item.findtext("system-out") or "").splitlines() for item in tests}
    for suite, labels in manifest.items():
        for label in labels:
            lines = by_name[suite + "." + label]
            assert "PASS " + label in lines and f"PASS {suite} cases=1" in lines, "missing named staging case output"
    cli = by_name["staging_cli"]
    assert "PASS staging_cli cases=6" in cli and len([line for line in cli if line.startswith("PASS ") and " cases=" not in line]) == 6, "missing CLI checks"
    assert "PASS sample_cli roundtrip=4 independent_decode=4 negative_checks=16 network=0" in by_name["sample_cli"], "missing sample CLI checks"
    assert "PASS sample_transfer cases=7" in by_name["sample_transfer"], "missing resumable transfer checks"
    assert "PASS snapshot_cli roundtrip=5 negative_checks=14 network=0" in by_name["snapshot_cli"], "missing snapshot CLI checks"
    print("PASS ctest_matrix tests=71 staging_cases=50 cli_checks=6 sample_cli=1 sample_transfer=7 snapshot_cli=1")


if __name__ == "__main__":
    main()
