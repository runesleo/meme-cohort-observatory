"""Read-only lifecycle analytics over the cohort SQLite state."""

from __future__ import annotations

import json
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path

from .cohort import canonical_address

CHECKPOINTS_MINUTES = (("10m", 10), ("1h", 60), ("1d", 1440), ("7d", 10080))
VALUATION_KINDS = ("market_cap", "fdv")
TIERS = ("RAW_C1_CROSSING", "RAW_C3_CROSSING", "RAW_C10_CROSSING")
TIER_THRESHOLDS = {"RAW_C1_CROSSING": 1_000_000.0, "RAW_C3_CROSSING": 3_000_000.0, "RAW_C10_CROSSING": 10_000_000.0}


def _db_path(state_dir: str | Path) -> Path:
    path = Path(state_dir).expanduser().resolve() / "cohorts.sqlite3"
    if not path.exists():
        raise FileNotFoundError(f"database not found: {path}")
    return path


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _minutes(start: str | None, end: str | None) -> float | None:
    left, right = _parse_utc(start), _parse_utc(end)
    if left is None or right is None:
        return None
    return round((right - left).total_seconds() / 60.0, 3)


def _median(values):
    cleaned = [float(value) for value in values if value is not None]
    return round(float(statistics.median(cleaned)), 4) if cleaned else None


def _row_dict(row):
    return dict(row) if row is not None else None


def _first_checkpoint_observation(observations: list[dict], origin: str, minutes: int, kind: str):
    origin_dt = _parse_utc(origin)
    if origin_dt is None:
        return None
    target_seconds = minutes * 60
    candidates = []
    for row in observations:
        if row.get("valuation_kind") != kind or row.get("valuation_usd") is None:
            continue
        observed = _parse_utc(row.get("observed_at"))
        if observed is None:
            continue
        age_seconds = (observed - origin_dt).total_seconds()
        if age_seconds >= target_seconds:
            candidates.append((observed, age_seconds, row))
    if not candidates:
        return None
    observed, age_seconds, row = min(candidates, key=lambda item: item[0])
    age_minutes = age_seconds / 60.0
    return {
        "observed_at": row.get("observed_at"),
        "valuation_usd": row.get("valuation_usd"),
        "liquidity_usd": row.get("liquidity_usd"),
        "age_minutes": round(age_minutes, 3),
        "lag_minutes": round(age_minutes - minutes, 3),
        "source": row.get("source"),
    }


def _token_path_from_connection(connection: sqlite3.Connection, chain: str, token_address: str) -> dict:
    normalized_chain = chain.strip().lower()
    normalized_token = canonical_address(normalized_chain, token_address)
    token = _row_dict(
        connection.execute(
            "SELECT * FROM tokens WHERE chain = ? AND token_address = ?",
            (normalized_chain, normalized_token),
        ).fetchone()
    )
    if token is None:
        raise ValueError(f"token not found in state: {normalized_chain}:{normalized_token}")
    cohort = _row_dict(
        connection.execute(
            "SELECT * FROM tracking_cohort WHERE chain = ? AND token_address = ?",
            (normalized_chain, normalized_token),
        ).fetchone()
    )
    observations = [
        dict(row)
        for row in connection.execute(
            """
            SELECT * FROM observations
            WHERE chain = ? AND token_address = ?
            ORDER BY observed_at
            """,
            (normalized_chain, normalized_token),
        ).fetchall()
    ]
    tracks = [
        dict(row)
        for row in connection.execute(
            """
            SELECT * FROM valuation_tracks
            WHERE chain = ? AND token_address = ?
            ORDER BY valuation_kind
            """,
            (normalized_chain, normalized_token),
        ).fetchall()
    ]
    events = [
        dict(row)
        for row in connection.execute(
            """
            SELECT * FROM events
            WHERE chain = ? AND token_address = ?
            ORDER BY crossed_at, valuation_kind, threshold_usd
            """,
            (normalized_chain, normalized_token),
        ).fetchall()
    ]
    origin = (cohort or {}).get("pool_created_at") or token.get("first_seen_at")
    checkpoints = {}
    for label, minutes in CHECKPOINTS_MINUTES:
        checkpoints[label] = {
            kind: _first_checkpoint_observation(observations, origin, minutes, kind)
            for kind in VALUATION_KINDS
        }
    crossing_paths = []
    for event in events:
        item = dict(event)
        item["age_minutes_since_pool_created"] = _minutes(origin, event.get("crossed_at"))
        item["minutes_since_admission"] = _minutes(
            (cohort or {}).get("admitted_at"), event.get("crossed_at")
        )
        crossing_paths.append(item)
    return {
        "schema": "mco_token_path.v1",
        "chain": normalized_chain,
        "token_address": normalized_token,
        "comparable_cohort_member": cohort is not None,
        "token": token,
        "cohort": cohort,
        "valuation_tracks": tracks,
        "threshold_crossings": crossing_paths,
        "checkpoints": checkpoints,
        "observation_count": len(observations),
        "observations": observations,
        "interpretation": {
            "checkpoint_semantics": "first recorded observation at or after pool-age checkpoint; lag_minutes is reported and may be large",
            "crossing_semantics": "observed threshold crossing only; not reconstructed intraperiod ATH",
            "market_cap_fdv_separate": True,
            "buy_signal": False,
        },
    }


def inspect_token(state_dir: str | Path, chain: str, token_address: str) -> dict:
    with sqlite3.connect(_db_path(state_dir)) as connection:
        connection.row_factory = sqlite3.Row
        return _token_path_from_connection(connection, chain, token_address)


def build_lifecycle_report(state_dir: str | Path) -> dict:
    with sqlite3.connect(_db_path(state_dir)) as connection:
        connection.row_factory = sqlite3.Row
        cohort_rows = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM tracking_cohort ORDER BY chain, admitted_at, token_address"
            ).fetchall()
        ]
        by_chain: dict[str, list[dict]] = {}
        for row in cohort_rows:
            by_chain.setdefault(row["chain"], []).append(row)
        chains = {}
        for chain, members in sorted(by_chain.items()):
            token_paths = [
                _token_path_from_connection(connection, chain, member["token_address"])
                for member in members
            ]
            crossing_stats = {}
            for kind in VALUATION_KINDS:
                tracked_paths = [
                    path for path in token_paths
                    if any(track.get("valuation_kind") == kind for track in path["valuation_tracks"])
                ]
                crossing_stats[kind] = {}
                for tier in TIERS:
                    latencies = []
                    threshold = TIER_THRESHOLDS[tier]
                    ever_observed = 0
                    for path in tracked_paths:
                        track = next(
                            (item for item in path["valuation_tracks"] if item.get("valuation_kind") == kind),
                            None,
                        )
                        if track is not None and track.get("max_valuation_usd") is not None and float(track["max_valuation_usd"]) >= threshold:
                            ever_observed += 1
                        event = next(
                            (
                                item
                                for item in path["threshold_crossings"]
                                if item["valuation_kind"] == kind and item["tier"] == tier
                            ),
                            None,
                        )
                        if event and event["age_minutes_since_pool_created"] is not None:
                            latencies.append(event["age_minutes_since_pool_created"])
                    crossing_stats[kind][tier] = {
                        "crossed": len(latencies),
                        "transition_events": len(latencies),
                        "ever_observed_ge": ever_observed,
                        "track_n": len(tracked_paths),
                        "cohort_n": len(members),
                        "observed_rate": round(len(latencies) / len(tracked_paths), 4) if tracked_paths else None,
                        "ever_observed_rate": round(ever_observed / len(tracked_paths), 4) if tracked_paths else None,
                        "median_minutes_since_pool_created": _median(latencies),
                        "latencies_minutes": sorted(latencies),
                    }
            checkpoint_stats = {}
            for label, _minutes_target in CHECKPOINTS_MINUTES:
                checkpoint_stats[label] = {}
                for kind in VALUATION_KINDS:
                    tracked_paths = [
                        path for path in token_paths
                        if any(track.get("valuation_kind") == kind for track in path["valuation_tracks"])
                    ]
                    snapshots = [
                        path["checkpoints"][label][kind]
                        for path in tracked_paths
                        if path["checkpoints"][label][kind] is not None
                    ]
                    multiples = []
                    for member, path in zip(members, token_paths):
                        snapshot = path["checkpoints"][label][kind]
                        if (
                            snapshot is not None
                            and member.get("admission_valuation_kind") == kind
                            and member.get("admission_valuation_usd")
                        ):
                            multiples.append(
                                float(snapshot["valuation_usd"])
                                / float(member["admission_valuation_usd"])
                            )
                    checkpoint_stats[label][kind] = {
                        "observed": len(snapshots),
                        "track_n": len(tracked_paths),
                        "cohort_n": len(members),
                        "observed_rate": round(len(snapshots) / len(tracked_paths), 4) if tracked_paths else None,
                        "median_lag_minutes": _median([row["lag_minutes"] for row in snapshots]),
                        "median_multiple_from_same_kind_admission": _median(multiples),
                    }
            inclusion = [row.get("inclusion_probability") for row in members]
            sample_fraction = [row.get("sample_fraction") for row in members]
            missed = [int(row.get("missed_refresh_slots") or 0) for row in members]
            chains[chain] = {
                "comparable_cohort_n": len(members),
                "admission": {
                    "inclusion_probability_min": min(inclusion) if inclusion else None,
                    "inclusion_probability_median": _median(inclusion),
                    "inclusion_probability_max": max(inclusion) if inclusion else None,
                    "sample_fraction_median": _median(sample_fraction),
                    "formal_confidence_interval": None,
                },
                "refresh_quality": {
                    "tokens_with_missed_slots": sum(1 for value in missed if value > 0),
                    "total_missed_slots": sum(missed),
                    "median_missed_slots": _median(missed),
                    "max_missed_slots": max(missed) if missed else 0,
                },
                "crossings": crossing_stats,
                "checkpoints": checkpoint_stats,
            }
        return {
            "schema": "mco_lifecycle_report.v1",
            "cohort_n": len(cohort_rows),
            "chains": chains,
            "interpretation": {
                "denominator": "tracking_cohort members only; incomplete discovery frames are excluded at admission time",
                "threshold_semantics": "ever_observed_ge uses per-track maximum observation; transition_events require an observed below-to-above transition in the same valuation kind and may be fewer",
                "checkpoint_semantics": "first recorded observation at or after checkpoint; lag is explicit, so this is observability rather than exact-time survival",
                "uncertainty": "sample fractions and inclusion probabilities are exposed; no formal confidence interval is claimed",
                "market_cap_fdv_separate": True,
                "ath_reconstruction": False,
                "buy_signal": False,
            },
        }


def render_token_path(path: dict, format_name: str = "markdown") -> str:
    if format_name == "json":
        return json.dumps(path, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if format_name != "markdown":
        raise ValueError(f"unsupported inspect format: {format_name}")
    cohort = path.get("cohort") or {}
    lines = [
        f"# Token lifecycle · {path['chain']}:{path['token_address']}",
        "",
        f"Comparable cohort member: `{str(path['comparable_cohort_member']).lower()}`",
        f"Pool created: `{cohort.get('pool_created_at') or path['token'].get('first_seen_at')}`",
        f"Admission value: `{cohort.get('admission_valuation_usd')}` ({cohort.get('admission_valuation_kind')})",
        f"Missed refresh slots: `{cohort.get('missed_refresh_slots') if cohort else 'N/A'}`",
        "",
        "## Threshold crossings",
        "",
        "| Track | Tier | Crossed at | Age from pool (min) | Observed value |",
        "|---|---|---|---:|---:|",
    ]
    for event in path["threshold_crossings"]:
        lines.append(
            f"| {event['valuation_kind']} | {event['tier']} | {event['crossed_at']} | "
            f"{event['age_minutes_since_pool_created']} | {event['observed_valuation_usd']} |"
        )
    if not path["threshold_crossings"]:
        lines.append("| — | — | — | — | — |")
    lines += ["", "## Checkpoints", "", "| Checkpoint | Track | Observed at | Lag (min) | Value | Liquidity |", "|---|---|---|---:|---:|---:|"]
    for label, tracks in path["checkpoints"].items():
        for kind in VALUATION_KINDS:
            row = tracks[kind]
            if row is None:
                lines.append(f"| {label} | {kind} | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |")
            else:
                lines.append(
                    f"| {label} | {kind} | {row['observed_at']} | {row['lag_minutes']} | "
                    f"{row['valuation_usd']} | {row['liquidity_usd']} |"
                )
    lines += [
        "",
        "## Limits",
        "",
        "- Checkpoints use the first recorded observation at or after the target age and always expose lag.",
        "- Market cap and FDV are separate tracks.",
        "- Crossings are observed values, not reconstructed intraperiod ATHs.",
        "- This report does not authorize or recommend a trade.",
    ]
    return "\n".join(lines) + "\n"
