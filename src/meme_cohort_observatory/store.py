"""SQLite persistence and atomic summaries for the cohort collector.

asset-version: v1.5
updated: 2026-07-15
owner_surface: Meme Cohort Observatory
behavior_change: Persist RH raw pool events, candidate-token enrichment state, block coverage, and reorg-aware source cursors atomically.
rollback: Delete the local collector state directory after reverting this fix commit.
"""

import json
import hashlib
import os
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .cohort import THRESHOLDS, canonical_address, first_seen_class_for, isoformat


ADMISSION_POLICY_VERSION = "c100_admission_v1"
TRACKING_POLICY_VERSION = "anchored_age_tiered_v2"
DEFAULT_ADMISSION_CAP = 5
DEFAULT_REFRESH_CAP = 300


def tracking_cadence_minutes(age):
    if age.total_seconds() < 0:
        return None
    hours = age.total_seconds() / 3600.0
    if hours <= 1:
        return 10
    if hours <= 6:
        return 30
    if hours <= 24:
        return 120
    if hours <= 72:
        return 360
    if hours <= 168:
        return 720
    return None


def tracking_schedule_offsets_minutes():
    """Return immutable admission-anchored phase-1 lookup slots through day 7."""
    return tuple(
        list(range(10, 61, 10))
        + list(range(90, 361, 30))
        + list(range(480, 1441, 120))
        + list(range(1800, 4321, 360))
        + list(range(5040, 10081, 720))
    )


TRACKING_SLOT_OFFSETS_MINUTES = tracking_schedule_offsets_minutes()


def _next_slot_after(admitted_at, after_at):
    elapsed_minutes = (after_at - admitted_at).total_seconds() / 60.0
    for offset in TRACKING_SLOT_OFFSETS_MINUTES:
        if offset > elapsed_minutes:
            return offset, admitted_at + timedelta(minutes=offset)
    return None, None


def _ten_minute_bucket(value):
    bucket = value.astimezone(timezone.utc).replace(
        minute=(value.minute // 10) * 10, second=0, microsecond=0
    )
    return bucket.strftime("%Y-%m-%dT%H:%MZ")


SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens (
    chain TEXT NOT NULL,
    token_address TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    first_pool_address TEXT NOT NULL,
    first_source TEXT NOT NULL,
    first_valuation_usd REAL,
    first_valuation_kind TEXT,
    discovery_type TEXT NOT NULL,
    candidate_scope TEXT NOT NULL,
    token_origin TEXT NOT NULL,
    meme_classification TEXT NOT NULL,
    current_observed_at TEXT NOT NULL,
    current_pool_address TEXT NOT NULL,
    current_source TEXT NOT NULL,
    current_valuation_usd REAL,
    current_valuation_kind TEXT,
    current_liquidity_usd REAL,
    max_market_cap_usd REAL,
    max_fdv_usd REAL,
    first_seen_class TEXT,
    PRIMARY KEY (chain, token_address)
);

CREATE TABLE IF NOT EXISTS observations (
    chain TEXT NOT NULL,
    token_address TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    pool_address TEXT NOT NULL,
    source TEXT NOT NULL,
    valuation_usd REAL,
    valuation_kind TEXT,
    liquidity_usd REAL,
    volume_24h_usd REAL,
    buys_24h INTEGER,
    sells_24h INTEGER,
    pool_created_at TEXT,
    discovery_type TEXT NOT NULL,
    candidate_scope TEXT NOT NULL,
    token_origin TEXT NOT NULL,
    meme_classification TEXT NOT NULL,
    observation_phase TEXT NOT NULL,
    PRIMARY KEY (chain, token_address, observed_at)
);

CREATE TABLE IF NOT EXISTS tracking_cohort (
    chain TEXT NOT NULL,
    token_address TEXT NOT NULL,
    admitted_at TEXT NOT NULL,
    pool_created_at TEXT NOT NULL,
    admission_valuation_usd REAL NOT NULL,
    admission_valuation_kind TEXT,
    admission_policy_version TEXT NOT NULL,
    admission_bucket TEXT NOT NULL,
    inclusion_probability REAL NOT NULL,
    sample_fraction REAL NOT NULL,
    last_refresh_attempt_at TEXT NOT NULL,
    next_refresh_due_at TEXT,
    next_refresh_slot_minutes INTEGER,
    missed_refresh_slots INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    completion_reason TEXT,
    PRIMARY KEY (chain, token_address)
);

CREATE TABLE IF NOT EXISTS valuation_tracks (
    chain TEXT NOT NULL,
    token_address TEXT NOT NULL,
    valuation_kind TEXT NOT NULL,
    first_observed_at TEXT NOT NULL,
    first_valuation_usd REAL NOT NULL,
    current_observed_at TEXT NOT NULL,
    current_valuation_usd REAL NOT NULL,
    max_valuation_usd REAL NOT NULL,
    first_seen_class TEXT,
    PRIMARY KEY (chain, token_address, valuation_kind)
);

CREATE TABLE IF NOT EXISTS events (
    chain TEXT NOT NULL,
    token_address TEXT NOT NULL,
    tier TEXT NOT NULL,
    threshold_usd REAL NOT NULL,
    crossed_at TEXT NOT NULL,
    observed_valuation_usd REAL NOT NULL,
    valuation_kind TEXT NOT NULL,
    PRIMARY KEY (chain, token_address, tier, valuation_kind)
);

CREATE TABLE IF NOT EXISTS coverage_runs (
    run_id TEXT NOT NULL,
    chain TEXT NOT NULL,
    started_at TEXT NOT NULL,
    status TEXT NOT NULL,
    discovery_status TEXT NOT NULL,
    tracking_status TEXT NOT NULL,
    pool_rows_seen INTEGER NOT NULL,
    discovered_tokens INTEGER NOT NULL,
    observations_written INTEGER NOT NULL,
    discovery_observations_written INTEGER NOT NULL,
    tracking_observations_written INTEGER NOT NULL,
    discovery_type TEXT NOT NULL,
    candidate_scope TEXT NOT NULL,
    page_count INTEGER NOT NULL,
    oldest_pool_created_at TEXT,
    newest_pool_created_at TEXT,
    coverage_scope TEXT NOT NULL,
    gap_reason TEXT,
    missing_fields_json TEXT NOT NULL,
    errors_json TEXT NOT NULL,
    watermark_at TEXT,
    admission_eligible INTEGER NOT NULL,
    admission_admitted INTEGER NOT NULL,
    admission_dropped INTEGER NOT NULL,
    admission_policy_version TEXT NOT NULL,
    admission_bucket TEXT NOT NULL,
    admission_inclusion_probability REAL NOT NULL,
    admission_sample_fraction REAL NOT NULL,
    admission_frame_complete INTEGER NOT NULL,
    admission_block_reason TEXT,
    refresh_eligible_due INTEGER NOT NULL,
    refresh_selected INTEGER NOT NULL,
    refresh_returned INTEGER NOT NULL,
    refresh_usable INTEGER NOT NULL,
    refresh_refreshed INTEGER NOT NULL,
    refresh_deferred INTEGER NOT NULL,
    refresh_errors INTEGER NOT NULL,
    refresh_scope TEXT NOT NULL,
    refresh_complete INTEGER NOT NULL,
    PRIMARY KEY (run_id, chain)
);

CREATE TABLE IF NOT EXISTS pool_events (
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    protocol TEXT NOT NULL,
    emitter_address TEXT NOT NULL,
    token0_address TEXT,
    token1_address TEXT,
    pool_address TEXT,
    pool_id TEXT,
    block_number INTEGER NOT NULL,
    block_hash TEXT,
    event_timestamp TEXT,
    venue_status TEXT NOT NULL,
    enrichment_status TEXT NOT NULL DEFAULT 'pending',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    orphaned INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chain, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS pool_event_tokens (
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    token_address TEXT NOT NULL,
    token_role TEXT NOT NULL,
    enrichment_status TEXT NOT NULL DEFAULT 'pending',
    enrichment_attempts INTEGER NOT NULL DEFAULT 0,
    last_enrichment_attempt_at TEXT,
    enriched_at TEXT,
    PRIMARY KEY (chain, tx_hash, log_index, token_address),
    FOREIGN KEY (chain, tx_hash, log_index)
      REFERENCES pool_events(chain, tx_hash, log_index) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS source_cursors (
    chain TEXT NOT NULL,
    source TEXT NOT NULL,
    next_block INTEGER NOT NULL,
    last_block INTEGER,
    last_block_hash TEXT,
    updated_at TEXT NOT NULL,
    last_success_run_id TEXT NOT NULL,
    PRIMARY KEY (chain, source)
);

CREATE TABLE IF NOT EXISTS block_coverage_runs (
    run_id TEXT NOT NULL,
    chain TEXT NOT NULL,
    source TEXT NOT NULL,
    started_at TEXT NOT NULL,
    status TEXT NOT NULL,
    coverage_scope TEXT NOT NULL,
    from_block INTEGER,
    to_block INTEGER,
    latest_indexed_block INTEGER,
    event_rows_seen INTEGER NOT NULL,
    event_rows_inserted INTEGER NOT NULL,
    event_tokens_queued INTEGER NOT NULL,
    pending_enrichment_after INTEGER NOT NULL,
    enrichment_complete INTEGER NOT NULL,
    cursor_before_next_block INTEGER,
    cursor_after_next_block INTEGER,
    gap_reason TEXT,
    errors_json TEXT NOT NULL,
    PRIMARY KEY (run_id, chain, source)
);

CREATE INDEX IF NOT EXISTS idx_observations_chain_time
ON observations(chain, observed_at);

CREATE INDEX IF NOT EXISTS idx_events_chain_tier
ON events(chain, tier);

CREATE INDEX IF NOT EXISTS idx_coverage_chain_time
ON coverage_runs(chain, started_at);

CREATE INDEX IF NOT EXISTS idx_pool_events_chain_block
ON pool_events(chain, block_number DESC);

CREATE INDEX IF NOT EXISTS idx_pool_event_tokens_pending
ON pool_event_tokens(chain, enrichment_status, token_address);

CREATE INDEX IF NOT EXISTS idx_block_coverage_chain_time
ON block_coverage_runs(chain, started_at DESC);
"""


COVERAGE_COLUMN_MIGRATIONS = {
    "discovery_status": "TEXT NOT NULL DEFAULT 'unmeasured'",
    "tracking_status": "TEXT NOT NULL DEFAULT 'unmeasured'",
    "discovery_observations_written": "INTEGER NOT NULL DEFAULT 0",
    "tracking_observations_written": "INTEGER NOT NULL DEFAULT 0",
    "admission_eligible": "INTEGER NOT NULL DEFAULT 0",
    "admission_admitted": "INTEGER NOT NULL DEFAULT 0",
    "admission_dropped": "INTEGER NOT NULL DEFAULT 0",
    "admission_policy_version": "TEXT NOT NULL DEFAULT 'c100_admission_v1'",
    "admission_bucket": "TEXT NOT NULL DEFAULT ''",
    "admission_inclusion_probability": "REAL NOT NULL DEFAULT 0",
    "admission_sample_fraction": "REAL NOT NULL DEFAULT 0",
    "admission_frame_complete": "INTEGER NOT NULL DEFAULT 0",
    "admission_block_reason": "TEXT",
    "refresh_eligible_due": "INTEGER NOT NULL DEFAULT 0",
    "refresh_selected": "INTEGER NOT NULL DEFAULT 0",
    "refresh_returned": "INTEGER NOT NULL DEFAULT 0",
    "refresh_usable": "INTEGER NOT NULL DEFAULT 0",
    "refresh_refreshed": "INTEGER NOT NULL DEFAULT 0",
    "refresh_deferred": "INTEGER NOT NULL DEFAULT 0",
    "refresh_errors": "INTEGER NOT NULL DEFAULT 0",
    "refresh_scope": "TEXT NOT NULL DEFAULT 'tracking_not_migrated'",
    "refresh_complete": "INTEGER NOT NULL DEFAULT 0",
}

OBSERVATION_COLUMN_MIGRATIONS = {
    "observation_phase": "TEXT NOT NULL DEFAULT 'discovery'",
}

TRACKING_COLUMN_MIGRATIONS = {
    "next_refresh_due_at": "TEXT",
    "next_refresh_slot_minutes": "INTEGER",
    "missed_refresh_slots": "INTEGER NOT NULL DEFAULT 0",
}


def _parse_utc(value):
    if value is None:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class CohortStore:
    def __init__(self, state_dir):
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.database_path = self.state_dir / "cohorts.sqlite3"
        self.connection = sqlite3.connect(str(self.database_path))
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)
        self._migrate_schema()
        self.connection.commit()

    def _add_missing_columns(self, table, migrations):
        columns = {
            row[1]
            for row in self.connection.execute(
                "PRAGMA table_info(%s)" % table
            ).fetchall()
        }
        for name, declaration in migrations.items():
            if name not in columns:
                self.connection.execute(
                    "ALTER TABLE %s ADD COLUMN %s %s"
                    % (table, name, declaration)
                )

    def _migrate_schema(self):
        self._add_missing_columns(
            "observations", OBSERVATION_COLUMN_MIGRATIONS
        )
        self._add_missing_columns("coverage_runs", COVERAGE_COLUMN_MIGRATIONS)
        self._add_missing_columns(
            "tracking_cohort", TRACKING_COLUMN_MIGRATIONS
        )
        self.connection.execute(
            """
            UPDATE coverage_runs
            SET discovery_status = status
            WHERE discovery_status = 'unmeasured'
            """
        )
        self.connection.execute(
            """
            UPDATE coverage_runs
            SET discovery_observations_written = observations_written
            WHERE discovery_observations_written = 0
              AND tracking_observations_written = 0
              AND observations_written > 0
            """
        )
        self._backfill_valuation_tracks()
        self._backfill_tracking_schedule()
        self.connection.execute("PRAGMA user_version = 5")

    def _backfill_valuation_tracks(self):
        rows = self.connection.execute(
            """
            SELECT chain, token_address, valuation_kind, observed_at,
                   valuation_usd, candidate_scope
            FROM observations
            WHERE valuation_kind IN ('market_cap', 'fdv')
              AND valuation_usd IS NOT NULL
            ORDER BY chain, token_address, valuation_kind, observed_at
            """
        ).fetchall()
        grouped = {}
        for chain, token, kind, observed_at, value, scope in rows:
            key = (chain, token, kind)
            entry = grouped.setdefault(
                key,
                {
                    "first_at": observed_at,
                    "first_value": value,
                    "first_class": first_seen_class_for(value, scope),
                    "current_at": observed_at,
                    "current_value": value,
                    "max_value": value,
                },
            )
            if _parse_utc(observed_at) < _parse_utc(entry["first_at"]):
                entry["first_at"] = observed_at
                entry["first_value"] = value
                entry["first_class"] = first_seen_class_for(value, scope)
            if _parse_utc(observed_at) > _parse_utc(entry["current_at"]):
                entry["current_at"] = observed_at
                entry["current_value"] = value
            entry["max_value"] = max(entry["max_value"], value)
        for (chain, token, kind), entry in grouped.items():
            self.connection.execute(
                """
                INSERT OR IGNORE INTO valuation_tracks (
                    chain, token_address, valuation_kind, first_observed_at,
                    first_valuation_usd, current_observed_at,
                    current_valuation_usd, max_valuation_usd, first_seen_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain,
                    token,
                    kind,
                    entry["first_at"],
                    entry["first_value"],
                    entry["current_at"],
                    entry["current_value"],
                    entry["max_value"],
                    entry["first_class"],
                ),
            )

    def _backfill_tracking_schedule(self):
        rows = self.connection.execute(
            """
            SELECT chain, token_address, admitted_at, last_refresh_attempt_at,
                   next_refresh_due_at
            FROM tracking_cohort
            """
        ).fetchall()
        for chain, token, admitted_text, last_text, next_due_text in rows:
            if next_due_text:
                continue
            admitted = _parse_utc(admitted_text)
            last_attempt = _parse_utc(last_text)
            slot, due_at = _next_slot_after(admitted, last_attempt)
            if due_at is None:
                slot = TRACKING_SLOT_OFFSETS_MINUTES[-1]
                due_at = admitted + timedelta(minutes=slot)
            self.connection.execute(
                """
                UPDATE tracking_cohort
                SET next_refresh_due_at = ?, next_refresh_slot_minutes = ?
                WHERE chain = ? AND token_address = ?
                """,
                (isoformat(due_at), slot, chain, token),
            )

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def get_chain_watermark(self, chain):
        row = self.connection.execute(
            """
            SELECT watermark_at FROM coverage_runs
            WHERE chain = ? AND status != 'error' AND watermark_at IS NOT NULL
            ORDER BY started_at DESC LIMIT 1
            """,
            (chain.strip().lower(),),
        ).fetchone()
        return _parse_utc(row[0]) if row else None

    def get_source_cursor(self, chain, source):
        from .adapters import BlockCursor

        chain = chain.strip().lower()
        source = source.strip().lower()
        row = self.connection.execute(
            """
            SELECT next_block, last_block, last_block_hash
            FROM source_cursors WHERE chain = ? AND source = ?
            """,
            (chain, source),
        ).fetchone()
        if row is None:
            return None
        return BlockCursor(chain, source, int(row[0]), row[1], row[2])

    def get_pending_pool_event_tokens(self, chain, limit=60):
        chain = chain.strip().lower()
        limit = max(0, int(limit))
        return [
            row[0]
            for row in self.connection.execute(
                """
                SELECT t.token_address
                FROM pool_event_tokens t
                JOIN pool_events e
                  ON e.chain = t.chain AND e.tx_hash = t.tx_hash
                 AND e.log_index = t.log_index
                WHERE t.chain = ? AND e.orphaned = 0
                  AND t.enrichment_status != 'usable'
                GROUP BY t.token_address
                ORDER BY MIN(t.enrichment_attempts),
                         MIN(COALESCE(t.last_enrichment_attempt_at, '')),
                         MIN(e.block_number), t.token_address
                LIMIT ?
                """,
                (chain, limit),
            ).fetchall()
        ]

    def _ingest_pool_events(self, chain, pool_events, observed_iso):
        inserted_events = 0
        inserted_tokens = 0
        for event in pool_events:
            event_chain = event.chain.strip().lower()
            if event_chain != chain:
                raise ValueError("pool event chain mismatch")
            tx_hash = str(event.tx_hash or "").lower()
            if not tx_hash:
                raise ValueError("pool event transaction hash is required")
            candidate_tokens = tuple(
                canonical_address(chain, token)
                for token in event.candidate_token_addresses
                if canonical_address(chain, token)
                and canonical_address(chain, token)
                != "0x0000000000000000000000000000000000000000"
            )
            token0_address = (
                canonical_address(chain, event.token0_address)
                if event.token0_address
                else None
            )
            token1_address = (
                canonical_address(chain, event.token1_address)
                if event.token1_address
                else None
            )
            pool_address = (
                canonical_address(chain, event.pool_address)
                if event.pool_address
                else None
            )
            emitter_address = canonical_address(chain, event.emitter_address)
            immutable_payload = (
                event.source,
                event.event_type,
                event.protocol,
                emitter_address,
                token0_address,
                token1_address,
                pool_address,
                event.pool_id,
            )
            enrichment_status = "pending" if candidate_tokens else "not_applicable"
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO pool_events (
                    chain, tx_hash, log_index, source, event_type, protocol,
                    emitter_address, token0_address, token1_address,
                    pool_address, pool_id, block_number, block_hash,
                    event_timestamp, venue_status, enrichment_status,
                    first_seen_at, last_seen_at, orphaned
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    chain,
                    tx_hash,
                    int(event.log_index),
                    event.source,
                    event.event_type,
                    event.protocol,
                    emitter_address,
                    token0_address,
                    token1_address,
                    pool_address,
                    event.pool_id,
                    int(event.block_number),
                    event.block_hash,
                    isoformat(event.event_timestamp) if event.event_timestamp else None,
                    event.venue_status,
                    enrichment_status,
                    observed_iso,
                    observed_iso,
                ),
            )
            if cursor.rowcount:
                inserted_events += 1
            else:
                existing_payload = self.connection.execute(
                    """
                    SELECT source, event_type, protocol, emitter_address,
                           token0_address, token1_address, pool_address, pool_id
                    FROM pool_events
                    WHERE chain = ? AND tx_hash = ? AND log_index = ?
                    """,
                    (chain, tx_hash, int(event.log_index)),
                ).fetchone()
                if tuple(existing_payload or ()) != immutable_payload:
                    raise RuntimeError("immutable pool event payload conflict")
                self.connection.execute(
                    """
                    UPDATE pool_events SET
                        block_number = ?, block_hash = ?, event_timestamp = ?,
                        venue_status = ?, last_seen_at = ?, orphaned = 0
                    WHERE chain = ? AND tx_hash = ? AND log_index = ?
                    """,
                    (
                        int(event.block_number),
                        event.block_hash,
                        isoformat(event.event_timestamp) if event.event_timestamp else None,
                        event.venue_status,
                        observed_iso,
                        chain,
                        tx_hash,
                        int(event.log_index),
                    ),
                )
            for token in dict.fromkeys(candidate_tokens):
                if token == token0_address:
                    role = "token0"
                elif token == token1_address:
                    role = "token1"
                else:
                    role = "candidate"
                token_cursor = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO pool_event_tokens (
                        chain, tx_hash, log_index, token_address, token_role,
                        enrichment_status, enrichment_attempts
                    ) VALUES (?, ?, ?, ?, ?, 'pending', 0)
                    """,
                    (chain, tx_hash, int(event.log_index), token, role),
                )
                inserted_tokens += int(bool(token_cursor.rowcount))
        return inserted_events, inserted_tokens

    def mark_pool_event_tokens_enriched(
        self,
        chain,
        returned_token_addresses,
        observed_at,
        usable_token_addresses=None,
        attempted_token_addresses=None,
    ):
        chain = chain.strip().lower()
        observed_iso = isoformat(observed_at)
        returned = {
            canonical_address(chain, token) for token in returned_token_addresses
        }
        usable = {
            canonical_address(chain, token)
            for token in (usable_token_addresses or set())
        }
        attempted = {
            canonical_address(chain, token)
            for token in (attempted_token_addresses or returned | usable)
        }
        for token in attempted:
            self.connection.execute(
                """
                UPDATE pool_event_tokens SET
                    enrichment_attempts = enrichment_attempts + 1,
                    last_enrichment_attempt_at = ?
                WHERE chain = ? AND token_address = ?
                  AND enrichment_status != 'usable'
                """,
                (observed_iso, chain, token),
            )
        for token in returned - usable:
            self.connection.execute(
                """
                UPDATE pool_event_tokens SET enrichment_status = 'resolved'
                WHERE chain = ? AND token_address = ?
                  AND enrichment_status != 'usable'
                """,
                (chain, token),
            )
        for token in usable:
            self.connection.execute(
                """
                UPDATE pool_event_tokens SET
                    enrichment_status = 'usable', enriched_at = ?
                WHERE chain = ? AND token_address = ?
                """,
                (observed_iso, chain, token),
            )
        self.connection.execute(
            """
            UPDATE pool_events SET enrichment_status = CASE
                WHEN NOT EXISTS (
                    SELECT 1 FROM pool_event_tokens t
                    WHERE t.chain = pool_events.chain
                      AND t.tx_hash = pool_events.tx_hash
                      AND t.log_index = pool_events.log_index
                ) THEN 'not_applicable'
                WHEN NOT EXISTS (
                    SELECT 1 FROM pool_event_tokens t
                    WHERE t.chain = pool_events.chain
                      AND t.tx_hash = pool_events.tx_hash
                      AND t.log_index = pool_events.log_index
                      AND t.enrichment_status != 'usable'
                ) THEN 'usable'
                WHEN EXISTS (
                    SELECT 1 FROM pool_event_tokens t
                    WHERE t.chain = pool_events.chain
                      AND t.tx_hash = pool_events.tx_hash
                      AND t.log_index = pool_events.log_index
                      AND t.enrichment_status IN ('resolved', 'usable')
                ) THEN 'partial'
                ELSE 'pending'
            END
            WHERE chain = ?
            """,
            (chain,),
        )

    def _upsert_source_cursor(self, cursor, run_id, observed_iso):
        self.connection.execute(
            """
            INSERT INTO source_cursors (
                chain, source, next_block, last_block, last_block_hash,
                updated_at, last_success_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chain, source) DO UPDATE SET
                next_block = excluded.next_block,
                last_block = excluded.last_block,
                last_block_hash = excluded.last_block_hash,
                updated_at = excluded.updated_at,
                last_success_run_id = excluded.last_success_run_id
            WHERE excluded.next_block >= source_cursors.next_block
            """,
            (
                cursor.chain.strip().lower(),
                cursor.source.strip().lower(),
                int(cursor.next_block),
                cursor.last_block,
                cursor.last_block_hash,
                observed_iso,
                run_id,
            ),
        )
        persisted = self.get_source_cursor(cursor.chain, cursor.source)
        if persisted != cursor:
            raise RuntimeError("source cursor upsert did not persist proposed cursor")

    def _upsert_block_coverage(
        self,
        run_id,
        chain,
        source,
        observed_iso,
        status,
        coverage_scope,
        block_from,
        block_to,
        latest_indexed_block,
        event_rows_seen,
        event_rows_inserted,
        event_tokens_queued,
        pending_after,
        enrichment_complete,
        cursor_before,
        next_cursor,
        gap_reason,
        errors,
    ):
        self.connection.execute(
            """
            INSERT OR REPLACE INTO block_coverage_runs (
                run_id, chain, source, started_at, status, coverage_scope,
                from_block, to_block, latest_indexed_block, event_rows_seen,
                event_rows_inserted, event_tokens_queued,
                pending_enrichment_after, enrichment_complete,
                cursor_before_next_block, cursor_after_next_block,
                gap_reason, errors_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                chain,
                source,
                observed_iso,
                status,
                coverage_scope,
                block_from,
                block_to,
                latest_indexed_block,
                int(event_rows_seen),
                int(event_rows_inserted),
                int(event_tokens_queued),
                int(pending_after),
                1 if enrichment_complete else 0,
                cursor_before.next_block if cursor_before else None,
                next_cursor.next_block if next_cursor else None,
                gap_reason,
                json.dumps(list(errors), ensure_ascii=False),
            ),
        )

    def plan_admission(
        self,
        chain,
        candidates,
        observed_at,
        since,
        discovery_type,
        candidate_scope,
        coverage_scope,
        row_loss_count=0,
        enrichment_complete=True,
        cap=DEFAULT_ADMISSION_CAP,
    ):
        chain = chain.strip().lower()
        cap = max(0, int(cap))
        bucket = _ten_minute_bucket(observed_at)
        gecko_comparable = (
            discovery_type == "unfiltered_new_pools"
            and candidate_scope == "unfiltered_new_pools"
            and coverage_scope == "watermark_closed"
            and int(row_loss_count) == 0
        )
        comparable = gecko_comparable
        if int(row_loss_count) > 0:
            block_reason = "discovery_row_loss"
        elif (
            discovery_type == "onchain_pool_events"
            and coverage_scope == "block_range_closed"
            and not enrichment_complete
        ):
            block_reason = "pending_event_enrichment"
        elif discovery_type == "onchain_pool_events":
            block_reason = "event_pair_unlinked"
        elif comparable:
            block_reason = None
        else:
            block_reason = "discovery_not_comparable"
        base = {
            "eligible": 0,
            "admitted": 0,
            "dropped": 0,
            "policy_version": ADMISSION_POLICY_VERSION,
            "bucket": bucket,
            "inclusion_probability": 0.0,
            "sample_fraction": 0.0,
            "frame_complete": comparable,
            "block_reason": block_reason,
            "selected_candidates": [],
        }
        if not comparable:
            return base

        already_admitted = {
            row[0]
            for row in self.connection.execute(
                "SELECT token_address FROM tracking_cohort WHERE chain = ?", (chain,)
            ).fetchall()
        }
        in_interval = {}
        for raw in candidates:
            item = raw.normalized(observed_at)
            if item.chain != chain or item.token_address in already_admitted:
                continue
            if item.pool_created_at is None:
                continue
            if not (since < item.pool_created_at <= observed_at):
                continue
            in_interval.setdefault(item.token_address, []).append(item)

        eligible = []
        for token_address, rows in in_interval.items():
            # A representative pool for admission is selected only among rows
            # created inside the interval. A deeper closing-page pool must not
            # erase a genuine new-pool row for the same token.
            rows.sort(
                key=lambda item: (
                    item.pool_created_at,
                    -(item.liquidity_usd or 0.0),
                    item.pool_address,
                )
            )
            representative = rows[0]
            token_row = self.connection.execute(
                """
                SELECT first_seen_at FROM tokens
                WHERE chain = ? AND token_address = ?
                """,
                (chain, token_address),
            ).fetchone()
            historical_valuations = self.connection.execute(
                """
                SELECT observed_at, valuation_usd, valuation_kind
                FROM observations
                WHERE chain = ? AND token_address = ?
                  AND valuation_usd IS NOT NULL
                  AND valuation_kind IN ('market_cap', 'fdv')
                """,
                (chain, token_address),
            ).fetchall()
            historical_valuations.sort(key=lambda row: _parse_utc(row[0]))

            if token_row is not None:
                # Recovery exception: a token first observed after the still-
                # open watermark may be reconsidered once that interval is
                # fetched completely. Tokens first seen at/before the closed
                # watermark can never later "fall into" C100.
                if _parse_utc(token_row[0]) <= since:
                    continue
            if historical_valuations:
                first_value = historical_valuations[0][1]
                first_kind = historical_valuations[0][2]
            else:
                valued_rows = [
                    item
                    for item in rows
                    if item.valuation_usd is not None
                    and item.valuation_kind in ("market_cap", "fdv")
                ]
                if not valued_rows:
                    continue
                valued_rows.sort(
                    key=lambda item: (
                        item.pool_created_at,
                        -(item.liquidity_usd or 0.0),
                        item.pool_address,
                    )
                )
                representative = valued_rows[0]
                first_value = representative.valuation_usd
                first_kind = representative.valuation_kind
            if first_value <= 0 or first_value > 100_000.0:
                continue
            eligible.append(
                replace(
                    representative,
                    valuation_usd=first_value,
                    valuation_kind=first_kind,
                )
            )
        eligible.sort(
            key=lambda item: (
                hashlib.sha256(
                    (
                        "%s:%s:%s:%s"
                        % (
                            ADMISSION_POLICY_VERSION,
                            bucket,
                            chain,
                            item.token_address,
                        )
                    ).encode("utf-8")
                ).hexdigest(),
                item.token_address,
            )
        )
        selected = eligible[:cap]
        eligible_count = len(eligible)
        admitted_count = len(selected)
        sample_fraction = (
            float(admitted_count) / float(eligible_count)
            if eligible_count
            else 1.0
        )
        base.update(
            {
                "eligible": eligible_count,
                "admitted": admitted_count,
                "dropped": eligible_count - admitted_count,
                "inclusion_probability": sample_fraction,
                "sample_fraction": sample_fraction,
                "selected_candidates": selected,
            }
        )
        return base

    def get_refresh_plan(self, chain, observed_at, cap=DEFAULT_REFRESH_CAP):
        chain = chain.strip().lower()
        cap = max(0, int(cap))
        due = []
        rows = self.connection.execute(
            """
            SELECT token_address, next_refresh_due_at, last_refresh_attempt_at
            FROM tracking_cohort
            WHERE chain = ? AND completed_at IS NULL
            """,
            (chain,),
        ).fetchall()
        for token_address, next_due_at, last_attempt_at in rows:
            next_due = _parse_utc(next_due_at)
            last_attempt = _parse_utc(last_attempt_at)
            if next_due is not None and observed_at >= next_due:
                due.append((next_due, last_attempt, token_address))
        due.sort(key=lambda item: (item[0], item[1], item[2]))
        selected = due[:cap]
        return {
            "eligible_due": len(due),
            "selected": len(selected),
            "deferred": len(due) - len(selected),
            "token_addresses": [item[2] for item in selected],
            "scope": "tracking_cohort_%s_hard_cap_%d"
            % (TRACKING_POLICY_VERSION, cap),
        }

    def mark_refresh_attempts(
        self, chain, token_addresses, observed_at, usable_token_addresses=None
    ):
        if not token_addresses:
            return
        chain = chain.strip().lower()
        usable = set(usable_token_addresses or [])
        with self.connection:
            for token in token_addresses:
                row = self.connection.execute(
                    """
                    SELECT admitted_at, next_refresh_due_at,
                           next_refresh_slot_minutes, missed_refresh_slots
                    FROM tracking_cohort
                    WHERE chain = ? AND token_address = ?
                      AND completed_at IS NULL
                    """,
                    (chain, token),
                ).fetchone()
                if row is None:
                    continue
                admitted = _parse_utc(row[0])
                current_slot = int(row[2] or TRACKING_SLOT_OFFSETS_MINUTES[0])
                elapsed_minutes = max(
                    0.0, (observed_at - admitted).total_seconds() / 60.0
                )
                due_offsets = [
                    offset
                    for offset in TRACKING_SLOT_OFFSETS_MINUTES
                    if current_slot <= offset <= elapsed_minutes
                ]
                missed_increment = max(0, len(due_offsets) - 1)
                next_slot, next_due = _next_slot_after(admitted, observed_at)
                completed_at = None
                completion_reason = None
                if next_due is None:
                    if token in usable:
                        completed_at = isoformat(observed_at)
                        completion_reason = "phase1_7d_tracking_window_complete"
                    else:
                        # A failed terminal lookup stays retryable rather than
                        # disappearing merely because wall time crossed day 7.
                        next_slot = TRACKING_SLOT_OFFSETS_MINUTES[-1]
                        next_due = observed_at + timedelta(minutes=10)
                self.connection.execute(
                    """
                    UPDATE tracking_cohort SET
                        last_refresh_attempt_at = ?, next_refresh_due_at = ?,
                        next_refresh_slot_minutes = ?,
                        missed_refresh_slots = missed_refresh_slots + ?,
                        completed_at = COALESCE(?, completed_at),
                        completion_reason = COALESCE(?, completion_reason)
                    WHERE chain = ? AND token_address = ?
                      AND completed_at IS NULL
                    """,
                    (
                        isoformat(observed_at),
                        isoformat(next_due) if next_due else None,
                        next_slot,
                        missed_increment,
                        completed_at,
                        completion_reason,
                        chain,
                        token,
                    ),
                )

    def ingest_chain(
        self,
        run_id,
        chain,
        observed_at,
        candidates,
        status,
        errors,
        pool_rows_seen,
        discovery_type,
        candidate_scope,
        page_count,
        oldest_pool_created_at,
        newest_pool_created_at,
        coverage_scope,
        gap_reason,
        missing_fields,
        discovered_tokens=None,
        discovery_status=None,
        tracking_status=None,
        admission=None,
        refresh=None,
        pool_events=None,
        block_source=None,
        cursor_before=None,
        next_cursor=None,
        block_from=None,
        block_to=None,
        latest_indexed_block=None,
        enrichment_complete=True,
        pending_token_addresses=None,
        enrichment_attempted_token_addresses=None,
        rewind_from_block=None,
        block_scan_status=None,
        block_scan_errors=None,
    ):
        chain = chain.strip().lower()
        observed_iso = isoformat(observed_at)
        effective_status = status
        effective_errors = list(errors)
        written = 0
        discovery_written = 0
        tracking_written = 0
        discovery_status = discovery_status or status
        tracking_status = tracking_status or "not_requested"
        pool_events = list(pool_events or [])
        pending_token_addresses = list(pending_token_addresses or [])
        enrichment_attempted_token_addresses = list(
            enrichment_attempted_token_addresses or []
        )
        pool_events_inserted = 0
        event_tokens_queued = 0
        if block_source:
            block_source = block_source.strip().lower()
            for event in pool_events:
                if event.source.strip().lower() != block_source:
                    raise ValueError("pool event source mismatch")
                if int(event.log_index) < 0 or int(event.block_number) < 0:
                    raise ValueError("pool event index/block must be nonnegative")
                if (
                    block_from is not None
                    and block_to is not None
                    and int(block_from) <= int(block_to)
                    and not int(block_from)
                    <= int(event.block_number)
                    <= int(block_to)
                ):
                    raise ValueError("pool event outside declared block range")
            if next_cursor is not None and coverage_scope != "block_range_closed":
                raise ValueError("block gap cannot advance source cursor")
            if next_cursor is not None:
                if (
                    next_cursor.chain.strip().lower() != chain
                    or next_cursor.source.strip().lower() != block_source
                ):
                    raise ValueError("source cursor identity mismatch")
                if block_from is not None and block_to is not None:
                    expected_next = (
                        int(block_to) + 1
                        if int(block_from) <= int(block_to)
                        else (cursor_before.next_block if cursor_before else int(block_from))
                    )
                    if int(next_cursor.next_block) != int(expected_next):
                        raise ValueError("source cursor does not close block range")
                    if int(block_from) <= int(block_to):
                        if int(next_cursor.last_block) != int(block_to):
                            raise ValueError("source cursor last block mismatch")
                        if not next_cursor.last_block_hash:
                            raise ValueError("source cursor last block hash required")
            persisted_before = self.get_source_cursor(chain, block_source)
            if cursor_before != persisted_before:
                raise RuntimeError("source cursor changed before atomic ingest")
        if discovered_tokens is None:
            discovered_tokens = sum(
                1 for item in candidates if item.observation_phase == "discovery"
            )
        admission = admission or {
            "eligible": 0,
            "admitted": 0,
            "dropped": 0,
            "policy_version": ADMISSION_POLICY_VERSION,
            "bucket": _ten_minute_bucket(observed_at),
            "inclusion_probability": 0.0,
            "sample_fraction": 0.0,
            "frame_complete": False,
            "block_reason": "admission_not_evaluated",
            "selected_candidates": [],
        }
        refresh = refresh or {
            "eligible_due": 0,
            "selected": 0,
            "returned": 0,
            "usable": 0,
            "refreshed": 0,
            "deferred": 0,
            "errors": 0,
            "scope": "tracking_not_requested",
            "complete": True,
        }
        with self.connection:
            if (
                rewind_from_block is not None
                and coverage_scope == "block_range_closed"
            ):
                self.connection.execute(
                    """
                    UPDATE pool_events SET orphaned = 1
                    WHERE chain = ? AND block_number >= ?
                    """,
                    (chain, int(rewind_from_block)),
                )
            if block_source:
                pool_events_inserted, event_tokens_queued = self._ingest_pool_events(
                    chain, pool_events, observed_iso
                )
            for item in candidates:
                observation = self._observation_values(item, observed_iso)
                existing = self.connection.execute(
                    """
                    SELECT pool_address, source, valuation_usd, valuation_kind,
                           liquidity_usd, volume_24h_usd, buys_24h, sells_24h,
                           pool_created_at, discovery_type, candidate_scope,
                           token_origin, meme_classification, observation_phase
                    FROM observations
                    WHERE chain = ? AND token_address = ? AND observed_at = ?
                    """,
                    (chain, item.token_address, observed_iso),
                ).fetchone()
                if existing is not None:
                    if tuple(existing) != observation:
                        effective_status = "degraded"
                        effective_errors.append(
                            "same_timestamp_conflict:%s:%s"
                            % (chain, item.token_address)
                        )
                    continue

                self.connection.execute(
                    """
                    INSERT INTO observations (
                        chain, token_address, observed_at, pool_address, source,
                        valuation_usd, valuation_kind, liquidity_usd,
                        volume_24h_usd, buys_24h, sells_24h, pool_created_at,
                        discovery_type, candidate_scope, token_origin,
                        meme_classification, observation_phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (chain, item.token_address, observed_iso) + observation,
                )
                written += 1
                if item.observation_phase == "tracking":
                    tracking_written += 1
                else:
                    discovery_written += 1
                self._ingest_valuation_track(chain, item, observed_iso)

                previous = self.connection.execute(
                    """
                    SELECT current_observed_at, current_valuation_usd,
                           current_valuation_kind, first_seen_at
                    FROM tokens WHERE chain = ? AND token_address = ?
                    """,
                    (chain, item.token_address),
                ).fetchone()
                if previous is None:
                    self._insert_token(chain, item, observed_iso)
                    continue

                current_observed_at, current_value, current_kind, first_seen_at = previous
                if _parse_utc(observed_iso) > _parse_utc(current_observed_at):
                    self._update_current(chain, item, observed_iso)
                else:
                    self._update_maxima(chain, item)
                    if _parse_utc(observed_iso) < _parse_utc(first_seen_at):
                        self._update_first_seen(chain, item, observed_iso)

            if block_source:
                returned_tokens = {
                    canonical_address(chain, item.token_address)
                    for item in candidates
                }
                usable_tokens = {
                    canonical_address(chain, item.token_address)
                    for item in candidates
                    if item.valuation_kind in ("market_cap", "fdv")
                    and item.valuation_usd is not None
                }
                attempted_tokens = returned_tokens | {
                    canonical_address(chain, token)
                    for token in enrichment_attempted_token_addresses
                }
                self.mark_pool_event_tokens_enriched(
                    chain,
                    returned_tokens,
                    observed_at,
                    usable_token_addresses=usable_tokens,
                    attempted_token_addresses=attempted_tokens,
                )

            self._insert_admissions(chain, observed_iso, admission)
            watermark_at = None
            if (
                effective_status != "error"
                and coverage_scope
                in ("watermark_closed", "fixture_snapshot", "block_range_closed")
                and (
                    discovery_type
                    not in ("unfiltered_new_pools", "onchain_pool_events")
                    or admission.get("frame_complete", False)
                )
            ):
                watermark_at = observed_iso
            self._upsert_coverage(
                run_id=run_id,
                chain=chain,
                observed_iso=observed_iso,
                status=effective_status,
                discovery_status=discovery_status,
                tracking_status=tracking_status,
                pool_rows_seen=pool_rows_seen,
                discovered_tokens=discovered_tokens,
                observations_written=written,
                discovery_observations_written=discovery_written,
                tracking_observations_written=tracking_written,
                discovery_type=discovery_type,
                candidate_scope=candidate_scope,
                page_count=page_count,
                oldest_pool_created_at=oldest_pool_created_at,
                newest_pool_created_at=newest_pool_created_at,
                coverage_scope=coverage_scope,
                gap_reason=gap_reason,
                missing_fields=missing_fields,
                errors=effective_errors,
                watermark_at=watermark_at,
                admission=admission,
                refresh=refresh,
            )
            if block_source:
                pending_after = len(
                    self.get_pending_pool_event_tokens(chain, limit=1_000_000)
                )
                self._upsert_block_coverage(
                    run_id=run_id,
                    chain=chain,
                    source=block_source,
                    observed_iso=observed_iso,
                    status=(
                        block_scan_status
                        or (
                            "success"
                            if coverage_scope == "block_range_closed"
                            else "error"
                        )
                    ),
                    coverage_scope=coverage_scope,
                    block_from=block_from,
                    block_to=block_to,
                    latest_indexed_block=latest_indexed_block,
                    event_rows_seen=pool_rows_seen,
                    event_rows_inserted=pool_events_inserted,
                    event_tokens_queued=event_tokens_queued,
                    pending_after=pending_after,
                    enrichment_complete=(enrichment_complete and pending_after == 0),
                    cursor_before=cursor_before,
                    next_cursor=next_cursor,
                    gap_reason=gap_reason,
                    errors=(
                        list(block_scan_errors)
                        if block_scan_errors is not None
                        else effective_errors
                    ),
                )
                if (
                    coverage_scope == "block_range_closed"
                    and effective_status != "error"
                    and next_cursor is not None
                ):
                    self._upsert_source_cursor(next_cursor, run_id, observed_iso)
        return {
            "status": effective_status,
            "errors": effective_errors,
            "observations_written": written,
            "discovery_observations_written": discovery_written,
            "tracking_observations_written": tracking_written,
            "pool_events_inserted": pool_events_inserted,
            "event_tokens_queued": event_tokens_queued,
        }

    @staticmethod
    def _observation_values(item, observed_iso):
        del observed_iso
        return (
            item.pool_address,
            item.source,
            item.valuation_usd,
            item.valuation_kind,
            item.liquidity_usd,
            item.volume_24h_usd,
            item.buys_24h,
            item.sells_24h,
            isoformat(item.pool_created_at) if item.pool_created_at else None,
            item.discovery_type,
            item.candidate_scope,
            item.token_origin,
            item.meme_classification,
            item.observation_phase,
        )

    def _insert_admissions(self, chain, observed_iso, admission):
        for item in admission.get("selected_candidates", []):
            self.connection.execute(
                """
                INSERT OR IGNORE INTO tracking_cohort (
                    chain, token_address, admitted_at, pool_created_at,
                    admission_valuation_usd, admission_valuation_kind,
                    admission_policy_version, admission_bucket,
                    inclusion_probability, sample_fraction,
                    last_refresh_attempt_at, next_refresh_due_at,
                    next_refresh_slot_minutes, missed_refresh_slots
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain,
                    item.token_address,
                    observed_iso,
                    isoformat(item.pool_created_at),
                    item.valuation_usd,
                    item.valuation_kind,
                    admission["policy_version"],
                    admission["bucket"],
                    admission["inclusion_probability"],
                    admission["sample_fraction"],
                    observed_iso,
                    isoformat(
                        _parse_utc(observed_iso)
                        + timedelta(minutes=TRACKING_SLOT_OFFSETS_MINUTES[0])
                    ),
                    TRACKING_SLOT_OFFSETS_MINUTES[0],
                    0,
                ),
            )

    def _insert_token(self, chain, item, observed_iso):
        max_mcap = item.valuation_usd if item.valuation_kind == "market_cap" else None
        max_fdv = item.valuation_usd if item.valuation_kind == "fdv" else None
        self.connection.execute(
            """
            INSERT INTO tokens (
                chain, token_address, first_seen_at, first_pool_address,
                first_source, first_valuation_usd, first_valuation_kind,
                discovery_type, candidate_scope, token_origin,
                meme_classification, current_observed_at, current_pool_address,
                current_source, current_valuation_usd, current_valuation_kind,
                current_liquidity_usd, max_market_cap_usd, max_fdv_usd,
                first_seen_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chain,
                item.token_address,
                observed_iso,
                item.pool_address,
                item.source,
                item.valuation_usd,
                item.valuation_kind,
                item.discovery_type,
                item.candidate_scope,
                item.token_origin,
                item.meme_classification,
                observed_iso,
                item.pool_address,
                item.source,
                item.valuation_usd,
                item.valuation_kind,
                item.liquidity_usd,
                max_mcap,
                max_fdv,
                first_seen_class_for(item.valuation_usd, item.candidate_scope),
            ),
        )

    def _update_first_seen(self, chain, item, observed_iso):
        self.connection.execute(
            """
            UPDATE tokens SET
                first_seen_at = ?, first_pool_address = ?, first_source = ?,
                first_valuation_usd = ?, first_valuation_kind = ?,
                discovery_type = ?, candidate_scope = ?, token_origin = ?,
                meme_classification = ?, first_seen_class = ?
            WHERE chain = ? AND token_address = ?
            """,
            (
                observed_iso,
                item.pool_address,
                item.source,
                item.valuation_usd,
                item.valuation_kind,
                item.discovery_type,
                item.candidate_scope,
                item.token_origin,
                item.meme_classification,
                first_seen_class_for(item.valuation_usd, item.candidate_scope),
                chain,
                item.token_address,
            ),
        )

    def _update_current(self, chain, item, observed_iso):
        self.connection.execute(
            """
            UPDATE tokens SET
                current_observed_at = ?, current_pool_address = ?,
                current_source = ?, current_valuation_usd = ?,
                current_valuation_kind = ?, current_liquidity_usd = ?
            WHERE chain = ? AND token_address = ?
            """,
            (
                observed_iso,
                item.pool_address,
                item.source,
                item.valuation_usd,
                item.valuation_kind,
                item.liquidity_usd,
                chain,
                item.token_address,
            ),
        )
        self._update_maxima(chain, item)

    def _update_maxima(self, chain, item):
        if item.valuation_usd is None:
            return
        if item.valuation_kind == "market_cap":
            column = "max_market_cap_usd"
        elif item.valuation_kind == "fdv":
            column = "max_fdv_usd"
        else:
            return
        self.connection.execute(
            """
            UPDATE tokens SET {column} = CASE
                WHEN {column} IS NULL OR {column} < ? THEN ? ELSE {column} END
            WHERE chain = ? AND token_address = ?
            """.format(column=column),
            (item.valuation_usd, item.valuation_usd, chain, item.token_address),
        )

    def _ingest_valuation_track(self, chain, item, observed_iso):
        if item.valuation_kind not in ("market_cap", "fdv"):
            return
        if item.valuation_usd is None:
            return
        previous = self.connection.execute(
            """
            SELECT first_observed_at, current_observed_at, current_valuation_usd,
                   max_valuation_usd
            FROM valuation_tracks
            WHERE chain = ? AND token_address = ? AND valuation_kind = ?
            """,
            (chain, item.token_address, item.valuation_kind),
        ).fetchone()
        if previous is None:
            self.connection.execute(
                """
                INSERT INTO valuation_tracks (
                    chain, token_address, valuation_kind, first_observed_at,
                    first_valuation_usd, current_observed_at,
                    current_valuation_usd, max_valuation_usd, first_seen_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain,
                    item.token_address,
                    item.valuation_kind,
                    observed_iso,
                    item.valuation_usd,
                    observed_iso,
                    item.valuation_usd,
                    item.valuation_usd,
                    first_seen_class_for(item.valuation_usd, item.candidate_scope),
                ),
            )
            return

        first_observed_at, current_observed_at, current_value, max_value = previous
        if _parse_utc(observed_iso) > _parse_utc(current_observed_at):
            self._record_crossings(
                chain,
                item,
                current_value,
                item.valuation_kind,
                observed_iso,
            )
            self.connection.execute(
                """
                UPDATE valuation_tracks SET
                    current_observed_at = ?, current_valuation_usd = ?,
                    max_valuation_usd = CASE
                        WHEN max_valuation_usd < ? THEN ? ELSE max_valuation_usd END
                WHERE chain = ? AND token_address = ? AND valuation_kind = ?
                """,
                (
                    observed_iso,
                    item.valuation_usd,
                    item.valuation_usd,
                    item.valuation_usd,
                    chain,
                    item.token_address,
                    item.valuation_kind,
                ),
            )
        elif _parse_utc(observed_iso) < _parse_utc(first_observed_at):
            self.connection.execute(
                """
                UPDATE valuation_tracks SET
                    first_observed_at = ?, first_valuation_usd = ?,
                    first_seen_class = ?,
                    max_valuation_usd = CASE
                        WHEN max_valuation_usd < ? THEN ? ELSE max_valuation_usd END
                WHERE chain = ? AND token_address = ? AND valuation_kind = ?
                """,
                (
                    observed_iso,
                    item.valuation_usd,
                    first_seen_class_for(item.valuation_usd, item.candidate_scope),
                    item.valuation_usd,
                    item.valuation_usd,
                    chain,
                    item.token_address,
                    item.valuation_kind,
                ),
            )

    def _record_crossings(
        self, chain, item, previous_value, previous_kind, observed_iso
    ):
        current_value = item.valuation_usd
        current_kind = item.valuation_kind
        if (
            previous_value is None
            or current_value is None
            or previous_kind not in ("market_cap", "fdv")
            or current_kind != previous_kind
        ):
            return
        for tier, threshold in THRESHOLDS:
            if previous_value < threshold <= current_value:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO events (
                        chain, token_address, tier, threshold_usd, crossed_at,
                        observed_valuation_usd, valuation_kind
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chain,
                        item.token_address,
                        tier,
                        threshold,
                        observed_iso,
                        current_value,
                        current_kind,
                    ),
                )

    def _upsert_coverage(
        self,
        run_id,
        chain,
        observed_iso,
        status,
        discovery_status,
        tracking_status,
        pool_rows_seen,
        discovered_tokens,
        observations_written,
        discovery_observations_written,
        tracking_observations_written,
        discovery_type,
        candidate_scope,
        page_count,
        oldest_pool_created_at,
        newest_pool_created_at,
        coverage_scope,
        gap_reason,
        missing_fields,
        errors,
        watermark_at,
        admission,
        refresh,
    ):
        self.connection.execute(
            """
            INSERT INTO coverage_runs (
                run_id, chain, started_at, status, discovery_status,
                tracking_status, pool_rows_seen, discovered_tokens,
                observations_written, discovery_observations_written,
                tracking_observations_written, discovery_type,
                candidate_scope, page_count, oldest_pool_created_at,
                newest_pool_created_at, coverage_scope, gap_reason,
                missing_fields_json, errors_json, watermark_at,
                admission_eligible, admission_admitted, admission_dropped,
                admission_policy_version, admission_bucket,
                admission_inclusion_probability, admission_sample_fraction,
                admission_frame_complete, admission_block_reason,
                refresh_eligible_due, refresh_selected, refresh_returned,
                refresh_usable, refresh_refreshed, refresh_deferred,
                refresh_errors, refresh_scope, refresh_complete
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(run_id, chain) DO UPDATE SET
                status = excluded.status,
                discovery_status = excluded.discovery_status,
                tracking_status = excluded.tracking_status,
                pool_rows_seen = excluded.pool_rows_seen,
                discovered_tokens = excluded.discovered_tokens,
                observations_written = excluded.observations_written,
                discovery_observations_written = excluded.discovery_observations_written,
                tracking_observations_written = excluded.tracking_observations_written,
                discovery_type = excluded.discovery_type,
                candidate_scope = excluded.candidate_scope,
                page_count = excluded.page_count,
                oldest_pool_created_at = excluded.oldest_pool_created_at,
                newest_pool_created_at = excluded.newest_pool_created_at,
                coverage_scope = excluded.coverage_scope,
                gap_reason = excluded.gap_reason,
                missing_fields_json = excluded.missing_fields_json,
                errors_json = excluded.errors_json,
                watermark_at = excluded.watermark_at,
                admission_eligible = excluded.admission_eligible,
                admission_admitted = excluded.admission_admitted,
                admission_dropped = excluded.admission_dropped,
                admission_policy_version = excluded.admission_policy_version,
                admission_bucket = excluded.admission_bucket,
                admission_inclusion_probability = excluded.admission_inclusion_probability,
                admission_sample_fraction = excluded.admission_sample_fraction,
                admission_frame_complete = excluded.admission_frame_complete,
                admission_block_reason = excluded.admission_block_reason,
                refresh_eligible_due = excluded.refresh_eligible_due,
                refresh_selected = excluded.refresh_selected,
                refresh_returned = excluded.refresh_returned,
                refresh_usable = excluded.refresh_usable,
                refresh_refreshed = excluded.refresh_refreshed,
                refresh_deferred = excluded.refresh_deferred,
                refresh_errors = excluded.refresh_errors,
                refresh_scope = excluded.refresh_scope,
                refresh_complete = excluded.refresh_complete
            """,
            (
                run_id,
                chain,
                observed_iso,
                status,
                discovery_status,
                tracking_status,
                int(pool_rows_seen),
                int(discovered_tokens),
                int(observations_written),
                int(discovery_observations_written),
                int(tracking_observations_written),
                discovery_type,
                candidate_scope,
                int(page_count),
                isoformat(oldest_pool_created_at) if oldest_pool_created_at else None,
                isoformat(newest_pool_created_at) if newest_pool_created_at else None,
                coverage_scope,
                gap_reason,
                json.dumps(list(missing_fields), ensure_ascii=False),
                json.dumps(list(errors), ensure_ascii=False),
                watermark_at,
                int(admission["eligible"]),
                int(admission["admitted"]),
                int(admission["dropped"]),
                admission["policy_version"],
                admission["bucket"],
                float(admission["inclusion_probability"]),
                float(admission["sample_fraction"]),
                1 if admission["frame_complete"] else 0,
                admission["block_reason"],
                int(refresh["eligible_due"]),
                int(refresh["selected"]),
                int(refresh.get("returned", refresh.get("refreshed", 0))),
                int(refresh.get("usable", refresh.get("refreshed", 0))),
                int(refresh["refreshed"]),
                int(refresh["deferred"]),
                int(refresh["errors"]),
                refresh["scope"],
                1 if refresh["complete"] else 0,
            ),
        )

    def record_coverage_error(self, run_id, chain, observed_at, message):
        with self.connection:
            self._upsert_coverage(
                run_id=run_id,
                chain=chain.strip().lower(),
                observed_iso=isoformat(observed_at),
                status="error",
                discovery_status="error",
                tracking_status="not_run_source_error",
                pool_rows_seen=0,
                discovered_tokens=0,
                observations_written=0,
                discovery_observations_written=0,
                tracking_observations_written=0,
                discovery_type="unknown",
                candidate_scope="unknown",
                page_count=0,
                oldest_pool_created_at=None,
                newest_pool_created_at=None,
                coverage_scope="error",
                gap_reason="source_error",
                missing_fields=[],
                errors=[message],
                watermark_at=None,
                admission={
                    "eligible": 0,
                    "admitted": 0,
                    "dropped": 0,
                    "policy_version": ADMISSION_POLICY_VERSION,
                    "bucket": _ten_minute_bucket(observed_at),
                    "inclusion_probability": 0.0,
                    "sample_fraction": 0.0,
                    "frame_complete": False,
                    "block_reason": "source_error",
                },
                refresh={
                    "eligible_due": 0,
                    "selected": 0,
                    "returned": 0,
                    "usable": 0,
                    "refreshed": 0,
                    "deferred": 0,
                    "errors": 0,
                    "scope": "tracking_not_run_source_error",
                    "complete": False,
                },
            )

    def record_block_coverage_error(
        self, run_id, chain, source, observed_at, cursor_before, message
    ):
        chain = chain.strip().lower()
        source = source.strip().lower()
        with self.connection:
            pending_after = len(
                self.get_pending_pool_event_tokens(chain, limit=1_000_000)
            )
            self._upsert_block_coverage(
                run_id=run_id,
                chain=chain,
                source=source,
                observed_iso=isoformat(observed_at),
                status="error",
                coverage_scope="block_gap",
                block_from=None,
                block_to=None,
                latest_indexed_block=None,
                event_rows_seen=0,
                event_rows_inserted=0,
                event_tokens_queued=0,
                pending_after=pending_after,
                enrichment_complete=False,
                cursor_before=cursor_before,
                next_cursor=None,
                gap_reason="source_error",
                errors=[message],
            )

    def _valuation_headline(self, chain, kind):
        if kind == "unknown":
            token_count = self.connection.execute(
                """
                SELECT COUNT(*) FROM tokens
                WHERE chain = ? AND first_valuation_kind IS NULL
                """,
                (chain,),
            ).fetchone()[0]
        else:
            token_count = self.connection.execute(
                """
                SELECT COUNT(*) FROM valuation_tracks
                WHERE chain = ? AND valuation_kind = ?
                """,
                (chain, kind),
            ).fetchone()[0]
        crossings = dict(
            self.connection.execute(
                """
                SELECT tier, COUNT(*) FROM events
                WHERE chain = ? AND valuation_kind = ? GROUP BY tier
                """,
                (chain, kind),
            ).fetchall()
        )
        if kind == "unknown":
            first_seen = {}
        else:
            first_seen = dict(
                self.connection.execute(
                    """
                    SELECT first_seen_class, COUNT(*) FROM valuation_tracks
                    WHERE chain = ? AND valuation_kind = ?
                      AND first_seen_class IS NOT NULL
                    GROUP BY first_seen_class
                    """,
                    (chain, kind),
                ).fetchall()
            )
        return {
            "token_count": token_count,
            "raw_threshold_crossings": {
                tier: crossings.get(tier, 0) for tier, _ in THRESHOLDS
            },
            "first_seen_high": first_seen,
        }

    def _block_discovery_summary(self, chain):
        cursor = self.connection.execute(
            """
            SELECT source, next_block, last_block, last_block_hash, updated_at,
                   last_success_run_id
            FROM source_cursors WHERE chain = ?
            ORDER BY updated_at DESC LIMIT 1
            """,
            (chain,),
        ).fetchone()
        coverage = self.connection.execute(
            """
            SELECT source, started_at, status, coverage_scope, from_block,
                   to_block, latest_indexed_block, event_rows_seen,
                   event_rows_inserted, event_tokens_queued,
                   pending_enrichment_after, enrichment_complete,
                   cursor_before_next_block, cursor_after_next_block,
                   gap_reason, errors_json
            FROM block_coverage_runs WHERE chain = ?
            ORDER BY started_at DESC LIMIT 1
            """,
            (chain,),
        ).fetchone()
        pending_stats = self.connection.execute(
            """
            SELECT COUNT(*),
                   SUM(CASE WHEN attempts = 0 THEN 1 ELSE 0 END),
                   MIN(first_block), MIN(last_attempt), MAX(attempts)
            FROM (
                SELECT t.token_address,
                       MIN(t.enrichment_attempts) AS attempts,
                       MIN(e.block_number) AS first_block,
                       MIN(t.last_enrichment_attempt_at) AS last_attempt
                FROM pool_event_tokens t
                JOIN pool_events e
                  ON e.chain = t.chain AND e.tx_hash = t.tx_hash
                 AND e.log_index = t.log_index
                WHERE t.chain = ? AND e.orphaned = 0
                  AND t.enrichment_status != 'usable'
                GROUP BY t.token_address
            ) pending
            """,
            (chain,),
        ).fetchone()
        rows = self.connection.execute(
            """
            SELECT tx_hash, log_index, event_type, protocol, emitter_address,
                   token0_address, token1_address, pool_address, pool_id,
                   block_number, block_hash, event_timestamp, venue_status,
                   enrichment_status, source
            FROM pool_events
            WHERE chain = ? AND orphaned = 0
            ORDER BY block_number DESC, log_index DESC LIMIT 20
            """,
            (chain,),
        ).fetchall()
        recent = []
        for row in rows:
            candidate_tokens = [
                item[0]
                for item in self.connection.execute(
                    """
                    SELECT token_address FROM pool_event_tokens
                    WHERE chain = ? AND tx_hash = ? AND log_index = ?
                    ORDER BY token_address
                    """,
                    (chain, row[0], row[1]),
                ).fetchall()
            ]
            recent.append(
                {
                    "tx_hash": row[0],
                    "log_index": row[1],
                    "event_type": row[2],
                    "protocol": row[3],
                    "emitter_address": row[4],
                    "token0_address": row[5],
                    "token1_address": row[6],
                    "candidate_token_addresses": candidate_tokens,
                    "pool_address": row[7],
                    "pool_id": row[8],
                    "block_number": row[9],
                    "block_hash": row[10],
                    "event_timestamp": row[11],
                    "venue_status": row[12],
                    "enrichment_status": row[13],
                    "source": row[14],
                }
            )
        return {
            "scope": (
                "chain-wide standard V2/V3/V4 pool events plus registered "
                "Pons/NOXA launch events; unknown custom AMMs, private bonding "
                "curves and tokens without these events remain out of coverage"
            ),
            "cursor": (
                {
                    "source": cursor[0],
                    "next_block": cursor[1],
                    "last_block": cursor[2],
                    "last_block_hash": cursor[3],
                    "updated_at": cursor[4],
                    "last_success_run_id": cursor[5],
                }
                if cursor
                else None
            ),
            "last_coverage": (
                {
                    "source": coverage[0],
                    "started_at": coverage[1],
                    "status": coverage[2],
                    "coverage_scope": coverage[3],
                    "from_block": coverage[4],
                    "to_block": coverage[5],
                    "latest_indexed_block": coverage[6],
                    "event_rows_seen": coverage[7],
                    "event_rows_inserted": coverage[8],
                    "event_tokens_queued": coverage[9],
                    "pending_enrichment_after": coverage[10],
                    "enrichment_complete": bool(coverage[11]),
                    "cursor_before_next_block": coverage[12],
                    "cursor_after_next_block": coverage[13],
                    "gap_reason": coverage[14],
                    "errors": json.loads(coverage[15]),
                }
                if coverage
                else None
            ),
            "pending_enrichment_count": pending_stats[0] or 0,
            "pending_enrichment_unattempted_count": pending_stats[1] or 0,
            "pending_enrichment_oldest_block": pending_stats[2],
            "pending_enrichment_oldest_last_attempt_at": pending_stats[3],
            "pending_enrichment_max_attempts": pending_stats[4] or 0,
            "recent_pool_events": recent,
        }

    def build_summary(self):
        chains = [
            row[0]
            for row in self.connection.execute(
                """
                SELECT chain FROM coverage_runs
                UNION SELECT chain FROM tokens
                UNION SELECT chain FROM pool_events
                UNION SELECT chain FROM source_cursors
                ORDER BY chain
                """
            ).fetchall()
        ]
        summary_chains = {}
        for chain in chains:
            coverage = self.connection.execute(
                """
                SELECT started_at, status, discovery_status, tracking_status,
                       pool_rows_seen, discovered_tokens, observations_written,
                       discovery_observations_written,
                       tracking_observations_written, errors_json, discovery_type,
                       candidate_scope, page_count, oldest_pool_created_at,
                       newest_pool_created_at, coverage_scope, gap_reason,
                       missing_fields_json, watermark_at,
                       admission_eligible, admission_admitted,
                       admission_dropped, admission_policy_version,
                       admission_bucket, admission_inclusion_probability,
                       admission_sample_fraction, admission_frame_complete,
                       admission_block_reason, refresh_eligible_due,
                       refresh_selected, refresh_returned, refresh_usable,
                       refresh_refreshed, refresh_deferred, refresh_errors,
                       refresh_scope, refresh_complete
                FROM coverage_runs
                WHERE chain = ?
                ORDER BY started_at DESC LIMIT 1
                """,
                (chain,),
            ).fetchone()
            provenance = dict(
                self.connection.execute(
                    """
                    SELECT COALESCE(first_valuation_kind, 'unknown'), COUNT(*)
                    FROM tokens WHERE chain = ? GROUP BY first_valuation_kind
                    """,
                    (chain,),
                ).fetchall()
            )
            latest_observation = self.connection.execute(
                "SELECT MAX(observed_at) FROM observations WHERE chain = ?", (chain,)
            ).fetchone()[0]
            last_full_discovery = self.connection.execute(
                """
                SELECT MAX(started_at) FROM coverage_runs
                WHERE chain = ? AND discovery_status = 'success'
                  AND coverage_scope IN (
                    'watermark_closed', 'fixture_snapshot', 'block_range_closed'
                  )
                """,
                (chain,),
            ).fetchone()[0]
            summary_chains[chain] = {
                "coverage": {
                    "last_run_at": coverage[0] if coverage else None,
                    "status": coverage[1] if coverage else "unmeasured",
                    "discovery_status": coverage[2] if coverage else "unmeasured",
                    "tracking_status": coverage[3] if coverage else "unmeasured",
                    "pool_rows_seen": coverage[4] if coverage else 0,
                    "discovered_tokens": coverage[5] if coverage else 0,
                    "observations_written": coverage[6] if coverage else 0,
                    "discovery_observations_written": coverage[7]
                    if coverage
                    else 0,
                    "tracking_observations_written": coverage[8]
                    if coverage
                    else 0,
                    "errors": json.loads(coverage[9]) if coverage else [],
                    "discovery_type": coverage[10] if coverage else "unknown",
                    "candidate_scope": coverage[11] if coverage else "unknown",
                    "page_count": coverage[12] if coverage else 0,
                    "oldest_pool_created_at": coverage[13] if coverage else None,
                    "newest_pool_created_at": coverage[14] if coverage else None,
                    "coverage_scope": coverage[15] if coverage else "unmeasured",
                    "gap_reason": coverage[16] if coverage else None,
                    "missing_fields": json.loads(coverage[17]) if coverage else [],
                    "watermark_at": coverage[18] if coverage else None,
                    "admission_eligible": coverage[19] if coverage else 0,
                    "admission_admitted": coverage[20] if coverage else 0,
                    "admission_dropped": coverage[21] if coverage else 0,
                    "admission_policy_version": coverage[22]
                    if coverage
                    else ADMISSION_POLICY_VERSION,
                    "admission_bucket": coverage[23] if coverage else None,
                    "admission_inclusion_probability": coverage[24]
                    if coverage
                    else 0.0,
                    "admission_sample_fraction": coverage[25]
                    if coverage
                    else 0.0,
                    "admission_frame_complete": bool(coverage[26])
                    if coverage
                    else False,
                    "admission_block_reason": coverage[27]
                    if coverage
                    else "unmeasured",
                    "refresh_eligible_due": coverage[28] if coverage else 0,
                    "refresh_selected": coverage[29] if coverage else 0,
                    "refresh_returned": coverage[30] if coverage else 0,
                    "refresh_usable": coverage[31] if coverage else 0,
                    "refresh_refreshed": coverage[32] if coverage else 0,
                    "refresh_deferred": coverage[33] if coverage else 0,
                    "refresh_errors": coverage[34] if coverage else 0,
                    "refresh_scope": coverage[35]
                    if coverage
                    else "unmeasured",
                    "refresh_complete": bool(coverage[36])
                    if coverage
                    else False,
                },
                "latest_observation_at": latest_observation,
                "last_successful_observation": last_full_discovery,
                "eligible_cohort_count": 0,
                "qualification_status": "pending_history_and_execution_checks",
                "first_seen_high_watch_refresh_status": "pending_bounded_queue",
                "valuation_headlines": {
                    "observed_market_cap": self._valuation_headline(
                        chain, "market_cap"
                    ),
                    "fdv_proxy": self._valuation_headline(chain, "fdv"),
                    "unknown": self._valuation_headline(chain, "unknown"),
                },
                "first_valuation_provenance": provenance,
                "block_discovery": self._block_discovery_summary(chain),
            }

        return {
            "schema_version": 5,
            "generated_at": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "security_status": "unmeasured",
            "executability_status": "unmeasured",
            "qualification_status": "pending_history_and_execution_checks",
            "eligible_cohort_count": 0,
            "main_chain_score_status": "insufficient_sample",
            "main_chain_score_missing_gates": [
                "age_window",
                "hold_duration",
                "security",
                "executability",
                "full_discovery_coverage",
                "admission_frame_complete",
                "complete_refresh_coverage",
                "sampling_uncertainty",
                "first_seen_high_watch_refresh",
                "meme_classification",
                "token_origin",
                "organic_maker_checks",
                "mature_forward_returns",
                "phase2_14d_30d_tracking",
            ],
            "main_chain_score_reason": (
                "Observed reach under the declared sampled-cohort cadence is "
                "separated by provider market cap and FDV proxy; complete "
                "block-event discovery, valuation enrichment, tracking, "
                "sampling uncertainty, origin, meme "
                "classification, security, execution, hold-duration, and "
                "mature-cohort gates remain open."
            ),
            "tracking_scope": (
                "phase1 telemetry through day 7; C3<=14d and C10<=30d "
                "remain unavailable until phase2 tracking is implemented"
            ),
            "chains": summary_chains,
        }

    def write_summary(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        summary = self.build_summary()
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(str(temporary), str(path))
        finally:
            if temporary.exists():
                temporary.unlink()
        return summary
