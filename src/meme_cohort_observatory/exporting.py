"""Deterministic exports from the local SQLite state."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


DEFAULT_TABLES = (
    "tokens",
    "observations",
    "tracking_cohort",
    "valuation_tracks",
    "events",
    "coverage_runs",
    "pool_events",
    "pool_event_tokens",
    "source_cursors",
    "block_coverage_runs",
)


def export_state(state_dir: str | Path, format_name: str = "jsonl", tables=None) -> str:
    database = Path(state_dir).expanduser().resolve() / "cohorts.sqlite3"
    if not database.exists():
        raise FileNotFoundError(f"database not found: {database}")
    selected = tuple(tables or DEFAULT_TABLES)
    unknown = sorted(set(selected) - set(DEFAULT_TABLES))
    if unknown:
        raise ValueError(f"unsupported tables: {', '.join(unknown)}")
    payload = {}
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        existing = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for table in selected:
            if table not in existing:
                continue
            rows = [
                dict(row)
                for row in connection.execute(
                    f'SELECT * FROM "{table}" ORDER BY rowid'
                )
            ]
            payload[table] = rows
    if format_name == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if format_name == "jsonl":
        lines = [
            json.dumps({"table": table, "row": row}, ensure_ascii=False, sort_keys=True)
            for table, rows in payload.items()
            for row in rows
        ]
        return ("\n".join(lines) + "\n") if lines else ""
    raise ValueError(f"unsupported export format: {format_name}")
