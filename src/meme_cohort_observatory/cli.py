"""Command-line interface for Meme Cohort Observatory."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from .adapters import (
    NETWORK_CONFIG,
    RequestBudget,
    default_adapters,
    default_refresh_adapters,
    fixture_adapters,
)
from .cohort import collect_once
from .doctor import render_doctor
from .exporting import export_state
from .lifecycle import build_lifecycle_report, inspect_token, render_token_path
from .reporting import load_summary, render_report
from .runtime_lock import SingleInstanceLock
from .store import CohortStore


DEFAULT_STATE_DIR = Path("~/.local/state/meme-cohort-observatory").expanduser()


def parse_observed_at(value):
    if value is None:
        return datetime.now(timezone.utc)
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _add_state_dir(parser):
    parser.add_argument(
        "--state-dir",
        default=str(DEFAULT_STATE_DIR),
        help="SQLite and summary directory (default: %(default)s)",
    )


def build_parser():
    parser = argparse.ArgumentParser(
        prog="mco",
        description="Track newly observable token pools with coverage-aware cohort telemetry.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    collect = commands.add_parser("collect", help="run one bounded collection cycle")
    _add_state_dir(collect)
    collect.add_argument("--fixtures", help="use deterministic JSON fixtures instead of public APIs")
    collect.add_argument("--network", action="append", choices=sorted(NETWORK_CONFIG))
    collect.add_argument("--timeout", type=float, default=6.0)
    collect.add_argument("--observed-at", help="UTC ISO timestamp override for replay")
    collect.add_argument("--run-budget", type=float, default=480.0)
    collect.add_argument("--lock-file", help="override the non-blocking process lock path")

    report = commands.add_parser("report", help="render a deterministic coverage-aware report")
    _add_state_dir(report)
    report.add_argument("--format", choices=("table", "markdown", "json"), default="table")
    report.add_argument("--output", help="write to a file instead of stdout")

    export = commands.add_parser("export", help="export SQLite rows")
    _add_state_dir(export)
    export.add_argument("--format", choices=("jsonl", "json"), default="jsonl")
    export.add_argument("--table", action="append", dest="tables")
    export.add_argument("--output", help="write to a file instead of stdout")

    inspect = commands.add_parser("inspect-token", help="inspect one token's recorded cohort lifecycle")
    _add_state_dir(inspect)
    inspect.add_argument("chain")
    inspect.add_argument("token_address")
    inspect.add_argument("--format", choices=("markdown", "json"), default="markdown")
    inspect.add_argument("--output", help="write to a file instead of stdout")

    doctor = commands.add_parser("doctor", help="run non-network safety and local-state checks")
    doctor.add_argument("--state-dir")
    doctor.add_argument("--json", action="store_true", dest="as_json")
    return parser


def _write_output(text, destination):
    if destination:
        path = Path(destination).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


def run_collect(args):
    if args.timeout <= 0 or args.timeout > 30:
        raise ValueError("--timeout must be > 0 and <= 30 seconds")
    if args.run_budget <= 0 or args.run_budget > 540:
        raise ValueError("--run-budget must be > 0 and <= 540 seconds")
    state_dir = Path(args.state_dir).expanduser().resolve()
    lock_path = Path(args.lock_file).expanduser().resolve() if args.lock_file else state_dir / "collector.lock"
    lock = SingleInstanceLock(lock_path)
    if not lock.acquire():
        print(json.dumps({"status": "skipped", "reason": "already_running"}))
        return 0
    try:
        networks = args.network or sorted(NETWORK_CONFIG)
        observed_at = parse_observed_at(args.observed_at)
        if args.fixtures:
            adapters = fixture_adapters(Path(args.fixtures), networks)
            refresh_adapters = {}
        else:
            budget = RequestBudget(args.run_budget)
            adapters = default_adapters(networks, timeout=args.timeout, budget=budget)
            refresh_adapters = default_refresh_adapters(networks, timeout=args.timeout, budget=budget)
        with CohortStore(state_dir) as store:
            result = collect_once(store, adapters, observed_at=observed_at, refresh_adapters=refresh_adapters)
        result["status"] = "complete"
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        lock.release()


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    # Preserve the old collector.py calling shape for internal migration tests,
    # but keep root help as root help so users can discover all commands.
    if argv and argv[0].startswith("-") and argv[0] not in {"-h", "--help"}:
        argv.insert(0, "collect")
    args = build_parser().parse_args(argv)
    try:
        if args.command == "collect":
            return run_collect(args)
        if args.command == "report":
            lifecycle = build_lifecycle_report(args.state_dir)
            _write_output(render_report(load_summary(args.state_dir), args.format, lifecycle), args.output)
            return 0
        if args.command == "export":
            _write_output(export_state(args.state_dir, args.format, args.tables), args.output)
            return 0
        if args.command == "inspect-token":
            payload = inspect_token(args.state_dir, args.chain, args.token_address)
            _write_output(render_token_path(payload, args.format), args.output)
            return 0
        if args.command == "doctor":
            sys.stdout.write(render_doctor(args.state_dir, args.as_json))
            return 0
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "error", "error": f"{exc.__class__.__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        return 1
    raise AssertionError(f"unknown command: {args.command}")
