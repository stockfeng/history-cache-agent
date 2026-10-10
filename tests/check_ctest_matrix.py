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
                "sigv4_reference", "journal_storage", "journal_recovery", "staging_cli", "sample_cli", "sample_transfer", "snapshot_cli",
                "agent_service", "agent_lifetime", "agent_uds", "adjustment", "factor_cli"}
    manifest = json.loads((Path(__file__).parent / "staging-cases.json").read_text())
    counts = {"staging_plan": 5, "staging_execution": 13, "staging_ledger": 9, "staging_delete": 3, "staging_retention": 20}
    assert set(manifest) == set(counts), "incomplete staging suite manifest"
    for suite, labels in manifest.items():
        assert len(labels) == len(set(labels)) == counts[suite], "incomplete or duplicate staging cases"
        required.update(suite + "." + label for label in labels)
    root = ET.parse(args.junit).getroot()
    tests = root.findall("testcase")
    assert len(tests) == len(required) == 76 and root.attrib["tests"] == "76" and root.attrib["failures"] == "0", "incomplete CTest run"
    assert {item.attrib["name"] for item in tests} == required, "CTest names differ from required matrix"
    assert all(item.attrib.get("status") == "run" and item.find("skipped") is None and item.find("failure") is None for item in tests), "nonpassing case"
    by_name = {item.attrib["name"]: (item.findtext("system-out") or "").splitlines() for item in tests}
    assert all('test output was removed' not in line for lines in by_name.values() for line in lines), \
        'truncated JUnit output; run CTest with --test-output-size-passed 8192'
    for suite, labels in manifest.items():
        for label in labels:
            lines = by_name[suite + "." + label]
            assert "PASS " + label in lines and f"PASS {suite} cases=1" in lines, "missing named staging case output"
    cli = by_name["staging_cli"]
    assert "PASS staging_cli cases=6" in cli and len([line for line in cli if line.startswith("PASS ") and " cases=" not in line]) == 6, "missing CLI checks"
    assert "PASS sample_cli roundtrip=4 independent_decode=4 negative_checks=16 network=0" in by_name["sample_cli"], "missing sample CLI checks"
    assert "PASS sample_transfer cases=12" in by_name["sample_transfer"], "missing resumable transfer checks"
    for name in ("diagnostics_bounded_redacted_and_no_retry", "diagnosed_journal_unknown_recovers_same_target",
                 "verified_remote_objects_reused_but_not_current_or_creates",
                 "verified_cache_budget_and_corruption_fail_closed", "verified_cache_preserves_unknown_pointer_recovery"):
        assert "PASS " + name in by_name["sample_transfer"], "missing publication diagnostics/recovery check"
    assert "PASS snapshot_cli roundtrip=5 negative_checks=14 network=0" in by_name["snapshot_cli"], "missing snapshot CLI checks"
    assert "PASS snapshot_complete roundtrip=5 fields8 namespace_isolation network=0" in by_name["snapshot_cli"], "missing complete snapshot checks"
    assert "PASS snapshot_native roundtrip=5 double_ohlc two_oi namespace_isolation network=0" in by_name["snapshot_cli"], "missing native snapshot checks"
    assert "PASS snapshot_worker direct_compact_parity empty_days bounds recovery proof_guards network=0" in by_name["snapshot_cli"], "missing bounded worker checks"
    assert "PASS snapshot_symbols dotted_us hyphenated_us derivatives query_literals network=0" in by_name["snapshot_cli"], "missing symbol checks"
    for name in ("agent_service", "agent_lifetime", "agent_uds"):
        assert any(line.startswith("PASS " + name + " ") for line in by_name[name]), "missing agent checks"
    assert "PASS agent_market UTC regular390 extended960 max_rows empty month DST hash" in by_name["agent_service"], "missing UTC market checks"
    assert "PASS agent_epoch replacement rollback same_sequence_guard" in by_name["agent_service"], "missing epoch cache checks"
    for name in ("epoch_replacement_is_explicit_pinned_and_recoverable",
                 "epoch_replacement_cannot_drop_dates_or_reset_source_versions"):
        assert "PASS " + name in by_name["journal_storage"], "missing epoch replacement checks"
    assert "PASS agent_cache cold3 warm0 subrange LRU bytes revision corruption range_fallback" in by_name["agent_service"], "missing verified pack cache checks"
    assert "PASS agent_maintenance cache_only warm refresh rate cadence lease_cancel singleflight hot2000" in by_name["agent_service"], "missing realtime-priority checks"
    assert "PASS agent_demand cold_default concurrent8_get3 expired_current_only revision pacing" in by_name["agent_service"], "missing demand checks"
    assert "PASS agent_complete kline48 exact_fields cold3 warm0 range legacy_miss UTC" in by_name["agent_service"], "missing complete kline checks"
    assert "PASS suspension NULL visibility paging empty partial factors composite HK_zero_volume" in by_name["agent_service"], "missing suspension checks"
    assert "PASS agent_adjustment default_off cold5 hot0 native64 anchor revision rollback expiry partial_reject" in by_name["agent_service"], "missing factor snapshot integration checks"
    assert any(line.startswith("PASS adjustment ") for line in by_name["adjustment"]), "missing adjustment checks"
    for name in ("factor_journal_pins_intent_recovers_expired_ack_and_never_rewrites_unknown",
                 "factor_journal_barriers_scope_and_torn_intent"):
        assert "PASS " + name in by_name["journal_storage"], "missing factor journal checks"
    assert "PASS factor_publication_cas_recovery_expiry_and_coverage" in by_name["conditional_publish"], "missing factor publish checks"
    assert "PASS agent_adjustment_us local_date affine_native same_sequence_guard" in by_name["agent_service"], "missing US adjustment checks"
    assert "PASS factor_cli proof_binding draft_reject production_opt_in persistent_budget suffix_loss network0" in by_name["factor_cli"], "missing factor CLI checks"
    print("PASS ctest_matrix tests=76 staging_cases=50 cli_checks=6 sample_cli=1 sample_transfer=12 snapshot_cli=1 agent=3 adjustment=1 factor_cli=1")


if __name__ == "__main__":
    main()
