from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"
ENV = {**os.environ, "PYTHONPATH": str(SRC_DIR)}


def run_mco(*args):
    return subprocess.run(
        [sys.executable, "-m", "meme_cohort_observatory", *args],
        cwd=PROJECT_ROOT,
        env=ENV,
        text=True,
        capture_output=True,
        check=False,
    )


def test_offline_collect_report_export_and_doctor():
    with tempfile.TemporaryDirectory() as temp:
        collected = run_mco(
            "collect", "--fixtures", str(FIXTURES), "--state-dir", temp,
            "--observed-at", "2026-07-14T02:00:00Z",
        )
        assert collected.returncode == 0, collected.stderr
        assert (Path(temp) / "cohorts.sqlite3").exists()
        assert (Path(temp) / "latest_summary.json").exists()

        report = run_mco("report", "--state-dir", temp, "--format", "markdown")
        assert report.returncode == 0, report.stderr
        assert "# Launch Cohort Report" in report.stdout
        for chain in ("base", "bsc", "robinhood", "solana"):
            assert f"| {chain} |" in report.stdout
        assert "Discovery is not a meme classification" in report.stdout

        exported = run_mco("export", "--state-dir", temp, "--format", "jsonl")
        assert exported.returncode == 0, exported.stderr
        first = json.loads(exported.stdout.splitlines()[0])
        assert first["table"] in {"tokens", "observations", "coverage_runs"}

        doctor = run_mco("doctor", "--state-dir", temp, "--json")
        assert doctor.returncode == 0, doctor.stderr
        payload = json.loads(doctor.stdout)
        assert payload["status"] == "pass"
        assert payload["checks"]["wallet_surface"] is False
        assert payload["checks"]["trade_surface"] is False


def test_public_surface_has_no_private_path_or_execution_surface():
    forbidden_import_roots = {"web3", "eth_account", "solders", "solana", "ccxt"}
    forbidden_calls = {
        "send_transaction", "sign_transaction", "broadcast_transaction",
        "swap", "approve", "transfer", "place_order", "create_order",
    }
    for path in SRC_DIR.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        private_prefix = "/Users/" + "zhangxu"
        assert private_prefix not in text
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not ({alias.name.split(".")[0] for alias in node.names} & forbidden_import_roots)
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden_import_roots
            elif isinstance(node, ast.Call):
                target = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else None
                assert target not in forbidden_calls


def test_root_help_lists_all_public_commands():
    result = run_mco("--help")
    assert result.returncode == 0, result.stderr
    for command in ("collect", "report", "export", "inspect-token", "doctor"):
        assert command in result.stdout
