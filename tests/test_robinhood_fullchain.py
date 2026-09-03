import os
"""Contract tests for RH chain-wide standard pool-event discovery."""

import sys
import tempfile
import unittest
import urllib.parse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
TOOL_DIR = PROJECT_ROOT
sys.path.insert(0, str(SRC_DIR))
os.environ["PYTHONPATH"] = str(SRC_DIR) + os.pathsep + os.environ.get("PYTHONPATH", "")

from meme_cohort_observatory.adapters import (  # noqa: E402
    BlockCursor,
    BlockscoutLogClient,
    PoolEvent,
    RobinhoodPoolEventAdapter,
    SourceBatch,
    SourceError,
    RH_TOKEN_DEPLOYED_TOPIC,
    RH_TOKEN_LAUNCHED_TOPIC,
    RH_V2_PAIR_CREATED_TOPIC,
    RH_V3_POOL_CREATED_TOPIC,
    RH_V4_INITIALIZE_TOPIC,
    decode_robinhood_log,
    default_adapters,
)
from meme_cohort_observatory.cohort import Candidate, collect_once  # noqa: E402
from meme_cohort_observatory.store import CohortStore  # noqa: E402


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 15, 4, 0, tzinfo=UTC)
SOURCE = "blockscout_pool_events"
WETH = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
MEME = "0x1111111111111111111111111111111111111111"
OTHER = "0x2222222222222222222222222222222222222222"
POOL = "0x3333333333333333333333333333333333333333"
UNKNOWN_FACTORY = "0x4444444444444444444444444444444444444444"
PONS_FACTORY = "0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB"
NOXA_FACTORY = "0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB"


def topic_address(address):
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def word_address(address):
    return ("0" * 24) + address.lower().removeprefix("0x")


def word_int(value):
    return "%064x" % value


def raw_log(topic, *, address=UNKNOWN_FACTORY, topics=None, data="0x", index=0):
    return {
        "address": address,
        "topics": [topic] + list(topics or []),
        "data": data,
        "blockNumber": "0x64",
        "blockHash": "0x" + ("ab" * 32),
        "transactionHash": "0x" + ("cd" * 31) + ("%02x" % index),
        "logIndex": hex(index),
        "timeStamp": hex(int(OBSERVED_AT.timestamp())),
    }


def v2_log(index=0, address=UNKNOWN_FACTORY, token0=WETH, token1=MEME):
    return raw_log(
        RH_V2_PAIR_CREATED_TOPIC,
        address=address,
        topics=[topic_address(token0), topic_address(token1)],
        data="0x" + word_address(POOL) + word_int(1),
        index=index,
    )


class FakeJsonClient:
    def __init__(self, responder):
        self.responder = responder
        self.urls = []

    def get_json(self, url):
        self.urls.append(url)
        return self.responder(url)


class FakeLogClient:
    def __init__(self, rows_by_topic=None, failures=None, head=164, hashes=None):
        self.rows_by_topic = rows_by_topic or {}
        self.failures = set(failures or [])
        self.head = head
        self.hashes = hashes or {}
        self.calls = []

    def latest_indexed_block(self):
        return self.head

    def block_by_time(self, timestamp):
        del timestamp
        return 90

    def block_hash(self, height):
        return self.hashes.get(height, "0xhash%04d" % height)

    def get_logs(self, topic, from_block, to_block):
        self.calls.append((topic, from_block, to_block))
        if topic in self.failures:
            raise SourceError("topic unavailable")
        return list(self.rows_by_topic.get(topic, []))


class EmptyEnricher:
    def __init__(self):
        self.requests = []

    def fetch_tokens(self, addresses, observed_at):
        del observed_at
        self.requests.append(list(addresses))
        return SourceBatch([], "success", [], coverage_scope="known_token_refresh")


class OneTokenEnricher:
    def __init__(self):
        self.requests = []

    def fetch_tokens(self, addresses, observed_at):
        self.requests.append(list(addresses))
        token = addresses[0]
        return SourceBatch(
            [
                Candidate(
                    chain="robinhood",
                    token_address=token,
                    pool_address=POOL,
                    source="dexscreener_refresh",
                    observed_at=observed_at,
                    valuation_usd=180_000.0,
                    valuation_kind="market_cap",
                    liquidity_usd=20_000.0,
                    volume_24h_usd=5_000.0,
                    buys_24h=10,
                    sells_24h=4,
                    pool_created_at=observed_at,
                )
            ],
            "success",
            [],
            coverage_scope="known_token_refresh",
        )


class DecodeTests(unittest.TestCase):
    def test_decodes_v2_v3_and_v4_without_trusting_unknown_emitter(self):
        v2 = decode_robinhood_log(RH_V2_PAIR_CREATED_TOPIC, v2_log())
        self.assertEqual("uniswap_v2", v2.protocol)
        self.assertEqual(POOL.lower(), v2.pool_address)
        self.assertEqual("unverified_emitter", v2.venue_status)
        self.assertEqual((MEME.lower(),), v2.candidate_token_addresses)

        v3 = decode_robinhood_log(
            RH_V3_POOL_CREATED_TOPIC,
            raw_log(
                RH_V3_POOL_CREATED_TOPIC,
                topics=[topic_address(WETH), topic_address(MEME), word_int(3000)],
                data="0x" + word_int(60) + word_address(POOL),
            ),
        )
        self.assertEqual("uniswap_v3", v3.protocol)
        self.assertEqual(POOL.lower(), v3.pool_address)

        pool_id = "0x" + ("12" * 32)
        v4 = decode_robinhood_log(
            RH_V4_INITIALIZE_TOPIC,
            raw_log(
                RH_V4_INITIALIZE_TOPIC,
                topics=[pool_id, topic_address("0x" + "00" * 20), topic_address(MEME)],
                data="0x" + word_int(3000) + word_int(60),
            ),
        )
        self.assertEqual(pool_id, v4.pool_id)
        self.assertIsNone(v4.pool_address)

    def test_decodes_pons_and_noxa_launch_events(self):
        deployed = decode_robinhood_log(
            RH_TOKEN_DEPLOYED_TOPIC,
            raw_log(
                RH_TOKEN_DEPLOYED_TOPIC,
                address=PONS_FACTORY,
                topics=[topic_address(MEME), topic_address(OTHER), topic_address(UNKNOWN_FACTORY)],
                data="0x" + word_address(WETH) + word_int(1) + word_int(2),
            ),
        )
        launched = decode_robinhood_log(
            RH_TOKEN_LAUNCHED_TOPIC,
            raw_log(
                RH_TOKEN_LAUNCHED_TOPIC,
                address=NOXA_FACTORY,
                topics=[topic_address(MEME), topic_address(OTHER), topic_address(UNKNOWN_FACTORY)],
                data="0x" + word_address(WETH) + word_address(POOL) + (word_int(0) * 7),
            ),
        )
        self.assertEqual("pons", deployed.protocol)
        self.assertEqual((MEME.lower(),), deployed.candidate_token_addresses)
        self.assertEqual("noxa", launched.protocol)
        self.assertEqual(POOL.lower(), launched.pool_address)


class LogWindowTests(unittest.TestCase):
    def test_notok_empty_payload_fails_closed_but_explicit_no_records_is_empty(self):
        failing = BlockscoutLogClient(
            http_client=FakeJsonClient(
                lambda url: {"status": "0", "message": "NOTOK", "result": []}
            )
        )
        with self.assertRaisesRegex(SourceError, "logs query failed"):
            failing.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 10)
        empty = BlockscoutLogClient(
            http_client=FakeJsonClient(
                lambda url: {
                    "status": "0",
                    "message": "No records found",
                    "result": [],
                }
            )
        )
        self.assertEqual([], empty.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 10))

    def test_large_range_is_proactively_split_before_query(self):
        calls = []

        def responder(url):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            calls.append((int(query["fromBlock"][0]), int(query["toBlock"][0])))
            return {"status": "1", "result": []}

        client = BlockscoutLogClient(
            http_client=FakeJsonClient(responder), max_block_span=5
        )
        self.assertEqual([], client.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 20))
        self.assertEqual([(10, 14), (15, 19), (20, 20)], calls)

    def test_exact_cap_recursively_bisects_and_query_is_topic_only(self):
        calls = []

        def responder(url):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            self.assertNotIn("address", query)
            window = (int(query["fromBlock"][0]), int(query["toBlock"][0]))
            calls.append(window)
            if window == (10, 20):
                return {"status": "1", "result": [{}] * 1000}
            return {"status": "1", "result": [{"window": window}]}

        client = BlockscoutLogClient(http_client=FakeJsonClient(responder))
        rows = client.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 20)
        self.assertEqual(2, len(rows))
        self.assertEqual([(10, 20), (10, 15), (16, 20)], calls)

    def test_generic_source_error_fails_fast_without_recursive_bisection(self):
        calls = []

        def responder(url):
            calls.append(url)
            raise SourceError("public request budget exhausted")

        client = BlockscoutLogClient(
            http_client=FakeJsonClient(responder), max_block_span=20
        )
        with self.assertRaisesRegex(SourceError, "public request budget exhausted"):
            client.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 20)
        self.assertEqual(1, len(calls))

    def test_heavy_window_http_5xx_bisects_down_to_floor_then_succeeds(self):
        calls = []

        def responder(url):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            window = (int(query["fromBlock"][0]), int(query["toBlock"][0]))
            calls.append(window)
            if window[1] - window[0] + 1 > 5:
                raise SourceError("GET failed for %s (HTTP 500)" % url)
            return {"status": "1", "result": [{"window": window}]}

        client = BlockscoutLogClient(
            http_client=FakeJsonClient(responder),
            max_block_span=20,
            min_scan_span=5,
        )
        rows = client.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 29)
        self.assertEqual(4, len(rows))
        self.assertEqual((10, 29), calls[0])
        self.assertTrue(all(hi - lo + 1 <= 5 for lo, hi in calls[1:] if hi - lo + 1 <= 5))

    def test_heavy_window_at_floor_span_fails_closed_without_bisection(self):
        calls = []

        def responder(url):
            calls.append(url)
            raise SourceError("GET failed for %s (timeout)" % url)

        client = BlockscoutLogClient(
            http_client=FakeJsonClient(responder),
            max_block_span=20,
            min_scan_span=20,
        )
        with self.assertRaisesRegex(SourceError, "timeout"):
            client.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 29)
        self.assertEqual(1, len(calls))

    def test_single_block_at_cap_fails_closed(self):
        client = BlockscoutLogClient(
            http_client=FakeJsonClient(
                lambda url: {"status": "1", "result": [{}] * 1000}
            )
        )
        with self.assertRaisesRegex(SourceError, "single_block_log_cap"):
            client.get_logs(RH_V2_PAIR_CREATED_TOPIC, 10, 10)


class AdapterTests(unittest.TestCase):
    def test_deep_backlog_advances_cursor_in_bounded_chunks(self):
        log_client = FakeLogClient(head=50_164)
        adapter = RobinhoodPoolEventAdapter(
            log_client=log_client,
            enricher=EmptyEnricher(),
            confirmation_blocks=64,
            catchup_max_blocks=100,
        )
        batch = adapter.fetch(
            OBSERVED_AT, cursor=BlockCursor("robinhood", SOURCE, 91)
        )
        self.assertEqual("block_range_closed", batch.coverage_scope)
        self.assertEqual(91, batch.block_from)
        self.assertEqual(190, batch.block_to)
        self.assertEqual(191, batch.next_cursor.next_block)
        self.assertEqual(190, batch.next_cursor.last_block)
        self.assertTrue(all(hi <= 190 for _, _, hi in log_client.calls))
        follow_up = adapter.fetch(OBSERVED_AT, cursor=batch.next_cursor)
        self.assertEqual(191, follow_up.block_from)
        self.assertEqual(290, follow_up.block_to)
        self.assertEqual(291, follow_up.next_cursor.next_block)

    def test_closed_empty_window_advances_cursor_without_promoted_fallback(self):
        enricher = EmptyEnricher()
        batch = RobinhoodPoolEventAdapter(
            log_client=FakeLogClient(), enricher=enricher
        ).fetch(OBSERVED_AT, cursor=BlockCursor("robinhood", SOURCE, 91))
        self.assertEqual([], batch.pool_events)
        self.assertEqual([], batch.candidates)
        self.assertEqual("block_range_closed", batch.coverage_scope)
        self.assertEqual(101, batch.next_cursor.next_block)
        self.assertEqual([], enricher.requests)

    def test_dex_unindexed_keeps_raw_event_pending_and_closes_raw_cursor(self):
        enricher = EmptyEnricher()
        batch = RobinhoodPoolEventAdapter(
            log_client=FakeLogClient(
                rows_by_topic={RH_V2_PAIR_CREATED_TOPIC: [v2_log()]}
            ),
            enricher=enricher,
        ).fetch(OBSERVED_AT, cursor=BlockCursor("robinhood", SOURCE, 91))
        self.assertEqual(1, len(batch.pool_events))
        self.assertEqual([MEME.lower()], batch.pending_token_addresses)
        self.assertFalse(batch.enrichment_complete)
        self.assertEqual("block_range_closed", batch.coverage_scope)
        self.assertEqual(101, batch.next_cursor.next_block)

    def test_topic_error_preserves_partial_events_but_never_advances_cursor(self):
        batch = RobinhoodPoolEventAdapter(
            log_client=FakeLogClient(
                rows_by_topic={RH_V2_PAIR_CREATED_TOPIC: [v2_log()]},
                failures={RH_V3_POOL_CREATED_TOPIC},
            ),
            enricher=OneTokenEnricher(),
        ).fetch(OBSERVED_AT, cursor=BlockCursor("robinhood", SOURCE, 91))
        self.assertEqual(1, len(batch.pool_events))
        self.assertEqual("block_gap", batch.coverage_scope)
        self.assertIsNone(batch.next_cursor)

    def test_pending_tokens_are_retried_even_without_new_events(self):
        enricher = OneTokenEnricher()
        batch = RobinhoodPoolEventAdapter(
            log_client=FakeLogClient(), enricher=enricher
        ).fetch(
            OBSERVED_AT,
            cursor=BlockCursor("robinhood", SOURCE, 101, 100, "0xhash0100"),
            pending_token_addresses=[MEME],
        )
        self.assertEqual([MEME.lower()], enricher.requests[0])
        self.assertEqual([], batch.pending_token_addresses)
        self.assertTrue(batch.enrichment_complete)

    def test_enrichment_is_bounded_while_all_unattempted_tokens_stay_pending(self):
        tokens = ["0x%040x" % index for index in range(1, 6)]
        enricher = EmptyEnricher()
        batch = RobinhoodPoolEventAdapter(
            log_client=FakeLogClient(),
            enricher=enricher,
            max_enrichment_tokens=3,
        ).fetch(
            OBSERVED_AT,
            cursor=BlockCursor("robinhood", SOURCE, 101, 100, "0xhash0100"),
            pending_token_addresses=tokens,
        )
        expected = [token.lower() for token in tokens]
        self.assertEqual(expected[:3], enricher.requests[0])
        self.assertEqual(expected[:3], batch.enrichment_attempted_token_addresses)
        self.assertEqual(expected, batch.pending_token_addresses)
        self.assertFalse(batch.enrichment_complete)

    def test_new_event_tokens_take_priority_over_retry_backlog(self):
        old_tokens = ["0x%040x" % index for index in range(1, 5)]
        enricher = EmptyEnricher()
        batch = RobinhoodPoolEventAdapter(
            log_client=FakeLogClient(
                rows_by_topic={RH_V2_PAIR_CREATED_TOPIC: [v2_log()]}
            ),
            enricher=enricher,
            max_enrichment_tokens=3,
        ).fetch(
            OBSERVED_AT,
            cursor=BlockCursor("robinhood", SOURCE, 91),
            pending_token_addresses=old_tokens,
        )
        self.assertEqual(
            [MEME.lower(), old_tokens[0].lower(), old_tokens[1].lower()],
            enricher.requests[0],
        )

    def test_reorg_hash_mismatch_rewinds_window(self):
        log_client = FakeLogClient(hashes={100: "0xchanged"})
        batch = RobinhoodPoolEventAdapter(
            log_client=log_client, enricher=EmptyEnricher(), reorg_rewind_blocks=64
        ).fetch(
            OBSERVED_AT,
            cursor=BlockCursor("robinhood", SOURCE, 101, 100, "0xold"),
        )
        self.assertEqual(37, batch.block_from)
        self.assertEqual(37, batch.rewind_from_block)

    def test_default_robinhood_adapter_is_direct_block_adapter(self):
        adapters = default_adapters(["robinhood"])
        self.assertIsInstance(adapters["robinhood"], RobinhoodPoolEventAdapter)


def pool_event(tx_hash="0xtx1", log_index=1, block_number=100):
    return PoolEvent(
        chain="robinhood",
        source=SOURCE,
        event_type="pair_created",
        protocol="uniswap_v2",
        emitter_address="0x8bceaa40b9acdffedF85adF4fF01F5Ad6517937f",
        token0_address=WETH.lower(),
        token1_address=MEME.lower(),
        pool_address=POOL.lower(),
        block_number=block_number,
        block_hash="0xblock%d" % block_number,
        tx_hash=tx_hash,
        log_index=log_index,
        event_timestamp=OBSERVED_AT,
        venue_status="verified_emitter",
        candidate_token_addresses=(MEME.lower(),),
    )


def cursor(next_block, last_block=None):
    last_block = next_block - 1 if last_block is None else last_block
    return BlockCursor(
        "robinhood", SOURCE, next_block, last_block, "0xblock%d" % last_block
    )


def ingest_kwargs(**overrides):
    values = {
        "run_id": "2026-07-15T04:00:00Z",
        "chain": "robinhood",
        "observed_at": OBSERVED_AT,
        "candidates": [],
        "status": "degraded",
        "errors": [],
        "pool_rows_seen": 1,
        "discovery_type": "onchain_pool_events",
        "candidate_scope": "full_chain_pool_events",
        "page_count": 5,
        "oldest_pool_created_at": None,
        "newest_pool_created_at": None,
        "coverage_scope": "block_range_closed",
        "gap_reason": None,
        "missing_fields": [],
        "pool_events": [pool_event()],
        "block_source": SOURCE,
        "cursor_before": None,
        "next_cursor": cursor(101),
        "block_from": 100,
        "block_to": 100,
        "latest_indexed_block": 100,
        "enrichment_complete": False,
    }
    values.update(overrides)
    return values


class CursorAdapter:
    cursor_source = SOURCE

    def __init__(self, batch):
        self.batch = batch
        self.seen = []

    def fetch(self, observed_at, since=None, cursor=None, pending_token_addresses=None):
        del observed_at, since
        self.seen.append((cursor, list(pending_token_addresses or [])))
        return self.batch


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = CohortStore(self.temp.name)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_closed_range_atomically_persists_event_queue_coverage_and_cursor(self):
        result = self.store.ingest_chain(**ingest_kwargs())
        self.assertEqual(1, result["pool_events_inserted"])
        self.assertEqual(
            ("uniswap_v2", 100, "pending"),
            self.store.connection.execute(
                "SELECT protocol, block_number, enrichment_status FROM pool_events"
            ).fetchone(),
        )
        self.assertEqual([MEME.lower()], self.store.get_pending_pool_event_tokens("robinhood"))
        self.assertEqual(101, self.store.get_source_cursor("robinhood", SOURCE).next_block)

    def test_gap_keeps_partial_raw_event_and_does_not_advance_cursor(self):
        self.store.ingest_chain(**ingest_kwargs())
        before = self.store.get_source_cursor("robinhood", SOURCE)
        self.store.ingest_chain(
            **ingest_kwargs(
                run_id="2026-07-15T04:10:00Z",
                coverage_scope="block_gap",
                gap_reason="single_block_log_cap",
                pool_events=[pool_event("0xtx-gap", 9, 103)],
                cursor_before=before,
                next_cursor=None,
                block_from=101,
                block_to=105,
                latest_indexed_block=105,
            )
        )
        self.assertEqual(2, self.store.connection.execute("SELECT COUNT(*) FROM pool_events").fetchone()[0])
        self.assertEqual(before, self.store.get_source_cursor("robinhood", SOURCE))

    def test_incomplete_reorg_replay_preserves_previous_canonical_events(self):
        self.store.ingest_chain(**ingest_kwargs())
        before = self.store.get_source_cursor("robinhood", SOURCE)

        self.store.ingest_chain(
            **ingest_kwargs(
                run_id="2026-07-15T04:10:00Z",
                status="degraded",
                errors=["topic_scan_error:test"],
                pool_rows_seen=1,
                pool_events=[pool_event("0xpartial", 9, 103)],
                coverage_scope="block_gap",
                gap_reason="topic_scan_error:test",
                cursor_before=before,
                next_cursor=None,
                block_from=37,
                block_to=105,
                latest_indexed_block=105,
                rewind_from_block=37,
                enrichment_complete=False,
                block_scan_status="error",
                block_scan_errors=["topic_scan_error:test"],
            )
        )

        for tx_hash in ("0xtx1", "0xpartial"):
            self.assertEqual(
                (0,),
                self.store.connection.execute(
                    """
                    SELECT orphaned FROM pool_events
                    WHERE chain = 'robinhood' AND tx_hash = ?
                    """,
                    (tx_hash,),
                ).fetchone(),
            )
        self.assertEqual(before, self.store.get_source_cursor("robinhood", SOURCE))
        self.assertEqual(
            ("error", "block_gap", 101, None),
            self.store.connection.execute(
                """
                SELECT status, coverage_scope,
                       cursor_before_next_block, cursor_after_next_block
                FROM block_coverage_runs
                WHERE run_id = '2026-07-15T04:10:00Z'
                  AND chain = 'robinhood'
                  AND source = ?
                """,
                (SOURCE,),
            ).fetchone(),
        )

    def test_closed_empty_range_advances_cursor_and_failure_rolls_back(self):
        self.store.ingest_chain(
            **ingest_kwargs(
                pool_rows_seen=0,
                pool_events=[],
                block_from=100,
                block_to=110,
                latest_indexed_block=110,
                next_cursor=cursor(111, 110),
                enrichment_complete=True,
                status="success",
            )
        )
        self.assertEqual(111, self.store.get_source_cursor("robinhood", SOURCE).next_block)

        other = CohortStore(Path(self.temp.name) / "rollback")
        try:
            with mock.patch.object(other, "_upsert_source_cursor", side_effect=RuntimeError("boom")):
                with self.assertRaises(RuntimeError):
                    other.ingest_chain(**ingest_kwargs())
            for table in ("pool_events", "pool_event_tokens", "block_coverage_runs"):
                self.assertEqual(0, other.connection.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0])
        finally:
            other.close()

    def test_summary_exposes_raw_event_cursor_and_pending_count(self):
        self.store.ingest_chain(**ingest_kwargs())
        block = self.store.build_summary()["chains"]["robinhood"]["block_discovery"]
        self.assertEqual(101, block["cursor"]["next_block"])
        self.assertEqual("block_range_closed", block["last_coverage"]["coverage_scope"])
        self.assertEqual(1, block["pending_enrichment_count"])
        self.assertEqual("0xtx1", block["recent_pool_events"][0]["tx_hash"])

    def test_pending_queue_favors_unattempted_then_oldest_attempt(self):
        second = "0x5555555555555555555555555555555555555555"
        third = "0x6666666666666666666666666666666666666666"
        events = [
            pool_event("0xqueue1", 1, 100),
            replace(pool_event("0xqueue2", 2, 101), candidate_token_addresses=(second,), token1_address=second),
            replace(pool_event("0xqueue3", 3, 102), candidate_token_addresses=(third,), token1_address=third),
        ]
        self.store.ingest_chain(
            **ingest_kwargs(
                pool_rows_seen=3,
                pool_events=events,
                block_to=102,
                latest_indexed_block=102,
                next_cursor=cursor(103, 102),
            )
        )
        self.store.mark_pool_event_tokens_enriched(
            "robinhood", [], OBSERVED_AT, attempted_token_addresses=[MEME]
        )
        self.assertEqual(
            [second, third, MEME.lower()],
            self.store.get_pending_pool_event_tokens("robinhood", limit=3),
        )

    def test_v4_missing_pool_address_stays_null_in_store_and_summary(self):
        event = replace(
            pool_event("0xv4", 7, 100),
            event_type="pool_initialized",
            protocol="uniswap_v4",
            pool_address=None,
            pool_id="0x" + ("12" * 32),
        )
        self.store.ingest_chain(**ingest_kwargs(pool_events=[event]))
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT pool_address FROM pool_events WHERE tx_hash = '0xv4'"
            ).fetchone()[0]
        )
        recent = self.store.build_summary()["chains"]["robinhood"]["block_discovery"]["recent_pool_events"]
        self.assertIsNone(recent[0]["pool_address"])

    def test_rh_admission_stays_blocked_until_event_pair_is_linked(self):
        item = Candidate(
            chain="robinhood",
            token_address=MEME.lower(),
            pool_address=POOL.lower(),
            source="blockscout_pool_events+dexscreener",
            observed_at=OBSERVED_AT,
            valuation_usd=50_000.0,
            valuation_kind="market_cap",
            liquidity_usd=10_000.0,
            volume_24h_usd=1_000.0,
            buys_24h=5,
            sells_24h=2,
            pool_created_at=OBSERVED_AT,
            discovery_type="onchain_pool_events",
            candidate_scope="full_chain_pool_events",
        )
        plan = self.store.plan_admission(
            chain="robinhood",
            candidates=[item],
            observed_at=OBSERVED_AT,
            since=OBSERVED_AT - timedelta(minutes=10),
            discovery_type="onchain_pool_events",
            candidate_scope="full_chain_pool_events",
            coverage_scope="block_range_closed",
            enrichment_complete=True,
        )
        self.assertFalse(plan["frame_complete"])
        self.assertEqual("event_pair_unlinked", plan["block_reason"])

    def test_collect_once_passes_persisted_cursor_and_pending_queue(self):
        self.store.ingest_chain(**ingest_kwargs())
        batch = SourceBatch(
            [],
            "success",
            [],
            discovery_type="onchain_pool_events",
            candidate_scope="full_chain_pool_events",
            coverage_scope="block_range_closed",
            pool_events=[],
            next_cursor=cursor(102, 101),
            enrichment_complete=False,
            pending_token_addresses=[MEME.lower()],
            block_from=101,
            block_to=101,
            latest_indexed_block=101,
        )
        adapter = CursorAdapter(batch)
        collect_once(
            self.store,
            {"robinhood": adapter},
            observed_at=datetime(2026, 7, 15, 4, 10, tzinfo=UTC),
        )
        self.assertEqual(101, adapter.seen[0][0].next_block)
        self.assertEqual([MEME.lower()], adapter.seen[0][1])
        self.assertEqual(102, self.store.get_source_cursor("robinhood", SOURCE).next_block)

    def test_robinhood_raw_discovery_runs_before_other_chains_can_exhaust_budget(self):
        gate = {"exhausted": False, "calls": []}

        class ExhaustingAdapter:
            def fetch(self, observed_at, since=None):
                del observed_at, since
                gate["calls"].append("base")
                gate["exhausted"] = True
                raise SourceError("public request budget exhausted")

        class BudgetAwareCursorAdapter(CursorAdapter):
            def fetch(
                self,
                observed_at,
                since=None,
                cursor=None,
                pending_token_addresses=None,
            ):
                gate["calls"].append("robinhood")
                if gate["exhausted"]:
                    raise SourceError("public request budget exhausted")
                return super().fetch(
                    observed_at,
                    since=since,
                    cursor=cursor,
                    pending_token_addresses=pending_token_addresses,
                )

        rh_batch = SourceBatch(
            [],
            "success",
            [],
            discovery_type="onchain_pool_events",
            candidate_scope="full_chain_pool_events",
            coverage_scope="block_range_closed",
            pool_events=[pool_event()],
            next_cursor=cursor(101),
            enrichment_complete=False,
            pending_token_addresses=[MEME.lower()],
            block_from=100,
            block_to=100,
            latest_indexed_block=100,
        )
        result = collect_once(
            self.store,
            {
                "base": ExhaustingAdapter(),
                "robinhood": BudgetAwareCursorAdapter(rh_batch),
            },
            observed_at=datetime(2026, 7, 15, 4, 20, tzinfo=UTC),
        )

        self.assertEqual(["robinhood", "base"], gate["calls"])
        self.assertEqual(101, self.store.get_source_cursor("robinhood", SOURCE).next_block)
        self.assertEqual("success", result["chains"]["robinhood"]["discovery_status"])
        self.assertEqual("error", result["chains"]["base"]["status"])
        self.assertIsNone(self.store.get_chain_watermark("base"))


if __name__ == "__main__":
    unittest.main()
