#!/usr/bin/env python3
"""Replay captured Robicop check_coin payloads against canonical outcomes.

This script performs no network calls and never mutates MCO state. It exists to
evaluate whether a captured external risk signal would have been useful while
keeping that signal outside the canonical admission/lifecycle pipeline.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from meme_cohort_observatory.robicop_shadow import replay_golden_cases  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        nargs="?",
        default=str(PROJECT_ROOT / "tests" / "fixtures" / "robicop_shadow_golden.json"),
        help="JSON fixture containing a top-level cases array",
    )
    parser.add_argument("--output", help="Optional output JSON path")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    cases = payload.get("cases") if isinstance(payload, dict) else None
    if not isinstance(cases, list):
        raise SystemExit("input must contain a top-level 'cases' array")

    report = replay_golden_cases(cases)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
