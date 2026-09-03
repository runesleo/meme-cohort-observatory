import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

from meme_cohort_observatory.lifecycle import build_lifecycle_report, inspect_token
from meme_cohort_observatory.store import CohortStore

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def _seed_state(state_dir: Path):
    token_a = "0x1111111111111111111111111111111111111111"
    token_b = "0x2222222222222222222222222222222222222222"
    with CohortStore(state_dir) as store:
        c = store.connection
        tokens = [
            (token_a, "2026-09-01T00:10:00Z", 80_000.0, 1_500_000.0, 1),
            (token_b, "2026-09-01T00:10:00Z", 90_000.0, 140_000.0, 0),
        ]
        for token, first_seen, first_value, current_value, missed in tokens:
            c.execute(
                """
                INSERT INTO tokens (
                    chain, token_address, first_seen_at, first_pool_address, first_source,
                    first_valuation_usd, first_valuation_kind, discovery_type, candidate_scope,
                    token_origin, meme_classification, current_observed_at, current_pool_address,
                    current_source, current_valuation_usd, current_valuation_kind,
                    current_liquidity_usd, max_market_cap_usd, max_fdv_usd, first_seen_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "bsc", token, first_seen, "0xpool" + token[-4:], "fixture",
                    first_value, "market_cap", "unfiltered_new_pools", "unfiltered_new_pools",
                    "unverified", "unclassified", "2026-09-02T00:10:00Z",
                    "0xpool" + token[-4:], "fixture", current_value, "market_cap",
                    50_000.0, current_value, None, None,
                ),
            )
            c.execute(
                """
                INSERT INTO tracking_cohort (
                    chain, token_address, admitted_at, pool_created_at,
                    admission_valuation_usd, admission_valuation_kind,
                    admission_policy_version, admission_bucket, inclusion_probability,
                    sample_fraction, last_refresh_attempt_at, next_refresh_due_at,
                    next_refresh_slot_minutes, missed_refresh_slots, completed_at, completion_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "bsc", token, "2026-09-01T00:10:00Z", "2026-09-01T00:05:00Z",
                    first_value, "market_cap", "c100_admission_v1", "2026-09-01T00:10Z",
                    0.5, 0.5, "2026-09-02T00:10:00Z", "2026-09-02T02:10:00Z",
                    1560, missed, None, None,
                ),
            )
        obs = [
            (token_a, "2026-09-01T00:10:00Z", 80_000.0, 30_000.0),
            (token_a, "2026-09-01T00:20:00Z", 110_000.0, 32_000.0),
            (token_a, "2026-09-01T01:10:00Z", 1_200_000.0, 80_000.0),
            (token_a, "2026-09-02T00:10:00Z", 1_500_000.0, 90_000.0),
            (token_b, "2026-09-01T00:10:00Z", 90_000.0, 25_000.0),
            (token_b, "2026-09-01T00:20:00Z", 95_000.0, 26_000.0),
            (token_b, "2026-09-01T01:10:00Z", 120_000.0, 28_000.0),
            (token_b, "2026-09-02T00:10:00Z", 140_000.0, 29_000.0),
        ]
        for token, at, value, liquidity in obs:
            c.execute(
                """
                INSERT INTO observations (
                    chain, token_address, observed_at, pool_address, source, valuation_usd,
                    valuation_kind, liquidity_usd, volume_24h_usd, buys_24h, sells_24h,
                    pool_created_at, discovery_type, candidate_scope, token_origin,
                    meme_classification, observation_phase
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "bsc", token, at, "0xpool" + token[-4:], "fixture", value,
                    "market_cap", liquidity, 10_000.0, 10, 5, "2026-09-01T00:05:00Z",
                    "refresh" if at != "2026-09-01T00:10:00Z" else "unfiltered_new_pools",
                    "known_token_refresh" if at != "2026-09-01T00:10:00Z" else "unfiltered_new_pools",
                    "unverified", "unclassified",
                    "tracking" if at != "2026-09-01T00:10:00Z" else "discovery",
                ),
            )
        c.execute(
            """
            INSERT INTO valuation_tracks (
                chain, token_address, valuation_kind, first_observed_at, first_valuation_usd,
                current_observed_at, current_valuation_usd, max_valuation_usd, first_seen_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("bsc", token_a, "market_cap", "2026-09-01T00:10:00Z", 80_000.0,
             "2026-09-02T00:10:00Z", 1_500_000.0, 1_500_000.0, None),
        )
        c.execute(
            """
            INSERT INTO valuation_tracks (
                chain, token_address, valuation_kind, first_observed_at, first_valuation_usd,
                current_observed_at, current_valuation_usd, max_valuation_usd, first_seen_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("bsc", token_b, "market_cap", "2026-09-01T00:10:00Z", 90_000.0,
             "2026-09-02T00:10:00Z", 140_000.0, 140_000.0, None),
        )
        c.execute(
            """
            INSERT INTO events (
                chain, token_address, tier, threshold_usd, crossed_at,
                observed_valuation_usd, valuation_kind
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("bsc", token_a, "RAW_C1_CROSSING", 1_000_000.0,
             "2026-09-01T01:10:00Z", 1_200_000.0, "market_cap"),
        )
        c.commit()
        store.write_summary(state_dir / "latest_summary.json")
    return token_a, token_b


def test_lifecycle_report_uses_comparable_cohort_denominator_and_explicit_lag():
    with tempfile.TemporaryDirectory() as temp:
        state = Path(temp)
        token_a, _ = _seed_state(state)
        report = build_lifecycle_report(state)
        bsc = report["chains"]["bsc"]
        assert report["cohort_n"] == 2
        assert bsc["comparable_cohort_n"] == 2
        c1 = bsc["crossings"]["market_cap"]["RAW_C1_CROSSING"]
        assert c1["crossed"] == 1
        assert c1["transition_events"] == 1
        assert c1["ever_observed_ge"] == 1
        assert c1["track_n"] == 2
        assert c1["observed_rate"] == 0.5
        assert c1["median_minutes_since_pool_created"] == 65.0
        assert bsc["refresh_quality"]["total_missed_slots"] == 1
        ten = bsc["checkpoints"]["10m"]["market_cap"]
        assert ten["observed"] == 2
        assert ten["track_n"] == 2
        assert ten["median_lag_minutes"] == 5.0
        assert ten["median_multiple_from_same_kind_admission"] > 1.2
        seven_day = bsc["checkpoints"]["7d"]["market_cap"]
        assert seven_day["observed"] == 0
        path = inspect_token(state, "BSC", token_a.upper())
        assert path["comparable_cohort_member"] is True
        assert path["threshold_crossings"][0]["age_minutes_since_pool_created"] == 65.0
        assert path["checkpoints"]["7d"]["market_cap"] is None
        assert path["interpretation"]["buy_signal"] is False


def test_cli_report_and_inspect_token_are_read_only_and_machine_readable():
    with tempfile.TemporaryDirectory() as temp:
        state = Path(temp)
        token_a, _ = _seed_state(state)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC)
        report = subprocess.run(
            [sys.executable, "-m", "meme_cohort_observatory", "report", "--state-dir", str(state), "--format", "markdown"],
            text=True, capture_output=True, env=env, check=False,
        )
        assert report.returncode == 0, report.stderr
        assert "Comparable cohort lifecycle" in report.stdout
        assert "MCAP ever observed ≥$1M" in report.stdout
        assert "FDV ever observed ≥$1M" in report.stdout
        inspected = subprocess.run(
            [sys.executable, "-m", "meme_cohort_observatory", "inspect-token", "--state-dir", str(state), "bsc", token_a, "--format", "json"],
            text=True, capture_output=True, env=env, check=False,
        )
        assert inspected.returncode == 0, inspected.stderr
        payload = json.loads(inspected.stdout)
        assert payload["schema"] == "mco_token_path.v1"
        assert payload["threshold_crossings"][0]["tier"] == "RAW_C1_CROSSING"
        assert payload["interpretation"]["buy_signal"] is False
