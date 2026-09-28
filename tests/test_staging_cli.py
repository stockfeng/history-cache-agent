"""Offline staging CLI guardrails; never invokes an approved live execution."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from test_offline import canonical, rows_hash, verify_pack, zstd_decoder


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool", type=Path, required=True)
    parser.add_argument("--staging", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()
    workspace = args.workspace.resolve(strict=True)
    environment = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "PYTHONDONTWRITEBYTECODE": "1"}
    for key in ("ASAN_OPTIONS", "UBSAN_OPTIONS"):
        if key in os.environ:
            environment[key] = os.environ[key]
    artifacts = workspace / "artifacts/r2"
    artifacts.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cli-offline-", dir=artifacts) as directory:
        root = Path(directory)
        environment.update(HOME=str(root / "empty-home"), TMPDIR=str(root))
        (root / "empty-home").mkdir(mode=0o700)
        config = json.loads((Path(__file__).parent / "staging-config-v1.json").read_text())
        config_path = root / "draft.json"
        config_path.write_bytes(canonical(config))
        fixture = root / "fixture"

        def run(command, expected=0):
            result = subprocess.run(list(map(str, command)), cwd=workspace, env=environment,
                                    capture_output=True, text=True, timeout=6)
            assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
            assert not result.stderr, result.stderr
            return json.loads(result.stdout)

        run([args.tool, "fixture", "--output", fixture, "--rows", "2050"])
        manifest = json.loads((fixture / "candidate.json").read_text())
        entry = manifest["entries"][0]
        pack = fixture / Path(entry["object"]["key"]).name
        assert verify_pack(pack, entry, zstd_decoder()) == rows_hash(0, 2050)
        base = [args.staging, "--config", config_path, "--fixture", fixture, "--run-id", "cli-offline-001"]
        plan = run(base)
        assert plan["status"] == "OFFLINE_CPP_PLAN_ONLY" and not plan["network_accessed"] and not plan["credentials_read"]
        assert plan["intent"]["planned"] == {"reserved_requests": 110, "upload_body_bytes": 39288,
                                             "download_reserved_bytes": 113678069}
        print("PASS cli_default_offline_plan_and_independent_2050_rows")
        approvals = ["--execute", "--exclusive-namespace", "--output", root / "run",
                     "--approved-config-sha256", plan["config_sha256"], "--approved-intent-sha256", plan["intent_sha256"]]
        assert run([*base, *approvals], 2)["status"] == "REJECTED_OR_INDETERMINATE"
        assert not (root / "run").exists()
        config["confirmations"] = {key: True for key in config["confirmations"]}
        config_path.write_bytes(canonical(config))
        run([*base, *approvals], 2)
        assert not (root / "run").exists()
        print("PASS cli_false_confirmations_and_stale_approval_rejected_before_output")
        for extra in (["--execute"], ["--exclusive-namespace"], ["--unknown", "ignored"],
                      ["--run-id", "duplicate"], ["--inspect-run", root / "missing"], ["--output", root / "run"]):
            run([*base, *extra], 2)
        run([args.staging, "--inspect-run", root / "missing"], 2)
        print("PASS cli_explicit_options_and_inspect_only_guards")
        link, fifo = root / "config-link", root / "fifo"
        link.symlink_to(config_path)
        os.mkfifo(fifo, 0o600)
        for path in (link, fifo, root / "../escape", "/dev/null"):
            run([args.staging, "--config", path, "--fixture", fixture, "--run-id", "cli-offline-001"], 2)
        print("PASS cli_project_scope_symlink_fifo_and_traversal_guards")
        retain_config = json.loads((Path(__file__).parent / "staging-config-retain-v2.json").read_text())
        config_path.write_bytes(canonical(retain_config))
        retained = run(base)
        assert retained["status"] == "OFFLINE_CPP_PLAN_ONLY"
        intent = retained["intent"]
        assert intent["profile"] == "cpp-staging-2050-retain-v2" and intent["planned"] == {
            "reserved_requests": 82, "upload_body_bytes": 39288, "download_reserved_bytes": 113535349}
        assert intent["retention"]["planned_objects"] == 9 and intent["retention"]["planned_body_bytes"] == 29271
        assert all(not scenario["cleanup"] and all(step["method"] != "DELETE" for step in scenario["business"])
                   for scenario in intent["scenarios"])
        assert not retained["network_accessed"] and not retained["credentials_read"]
        print("PASS cli_retention_plan_has_no_delete_and_explicit_inventory")
        retain_approvals = ["--execute", "--exclusive-namespace", "--output", root / "run",
                           "--approved-config-sha256", retained["config_sha256"], "--approved-intent-sha256", retained["intent_sha256"]]
        run([*base, *retain_approvals], 2)
        retain_config["confirmations"] = {key: True for key in retain_config["confirmations"]}
        config_path.write_bytes(canonical(retain_config))
        run([*base, *retain_approvals], 2)
        assert not (root / "run").exists()
        retain_config["cleanup"] = "verified_created_objects_only"
        config_path.write_bytes(canonical(retain_config))
        run(base, 2)
        print("PASS cli_retention_needs_new_confirmations_and_hashes_without_fallback")
    print("PASS staging_cli cases=6")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
