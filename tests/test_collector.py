import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
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
    CompositeAdapter,
    DexScreenerAdapter,
    GeckoTerminalAdapter,
    JsonHttpClient,
    RequestBudget,
    SharedRateLimiter,
    SourceBatch,
    SourceError,
)
from meme_cohort_observatory.cohort import Candidate, collect_once  # noqa: E402
from meme_cohort_observatory.runtime_lock import SingleInstanceLock  # noqa: E402
from meme_cohort_observatory.store import CohortStore  # noqa: E402


UTC = timezone.utc


def candidate(
    *,
    chain="base",
    token="0xtoken",
    pool="0xpool",
    valuation=90_000.0,
    kind="market_cap",
    liquidity=50_000.0,
    source="fixture",
    observed_at=None,
    discovery_type="unfiltered_new_pools",
    candidate_scope="unfiltered_new_pools",
    pool_created_at=None,
):
    return Candidate(
        chain=chain,
        token_address=token,
        pool_address=pool,
        source=source,
        observed_at=observed_at or datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
        valuation_usd=valuation,
        valuation_kind=kind,
        liquidity_usd=liquidity,
        volume_24h_usd=10_000.0,
        buys_24h=12,
        sells_24h=5,
        pool_created_at=pool_created_at
        or datetime(2026, 7, 13, 1, 0, tzinfo=UTC),
        discovery_type=discovery_type,
        candidate_scope=candidate_scope,
        token_origin="unverified",
        meme_classification="unclassified",
    )


class StaticAdapter:
    def __init__(self, batch=None, error=None):
        self.batch = batch or SourceBatch([], status="success", errors=[])
        self.error = error

    def fetch(self, observed_at, since=None):
        if self.error:
            raise self.error
        return self.batch


class CapturingAdapter:
    def __init__(self, batches):
        self.batches = list(batches)
        self.since_values = []

    def fetch(self, observed_at, since=None):
        self.since_values.append(since)
        return self.batches.pop(0)


class StaticRefreshAdapter:
    def __init__(self):
        self.requests = []

    def fetch_tokens(self, token_addresses, observed_at):
        self.requests.append(list(token_addresses))
        return SourceBatch(
            [
                candidate(
                    token=token,
                    pool="refresh-" + token,
                    valuation=150_000.0,
                    source="dexscreener_refresh",
                    discovery_type="refresh",
                    candidate_scope="known_token_refresh",
                )
                for token in token_addresses
            ],
            "success",
            [],
            discovery_type="refresh",
            candidate_scope="known_token_refresh",
            coverage_scope="known_token_refresh",
            pool_rows_seen=len(token_addresses),
        )


class ErrorRefreshAdapter:
    def fetch_tokens(self, token_addresses, observed_at):
        del token_addresses, observed_at
        raise SourceError("refresh unavailable")


class NullValuationRefreshAdapter:
    def fetch_tokens(self, token_addresses, observed_at):
        return SourceBatch(
            [
                candidate(
                    token=token,
                    pool="refresh-null-" + token,
                    valuation=None,
                    kind=None,
                    observed_at=observed_at,
                    discovery_type="refresh",
                    candidate_scope="known_token_refresh",
                )
                for token in token_addresses
            ],
            "degraded",
            [],
            discovery_type="refresh",
            candidate_scope="known_token_refresh",
            coverage_scope="known_token_refresh",
            pool_rows_seen=len(token_addresses),
        )


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class AdapterTests(unittest.TestCase):
    @staticmethod
    def gecko_payload(created_at, suffix="a", null_metrics=False):
        return {
            "data": [
                {
                    "id": "base_pool-%s" % suffix,
                    "attributes": {
                        "address": "pool-%s" % suffix,
                        "pool_created_at": created_at,
                        "market_cap_usd": "123456",
                        "fdv_usd": "999999",
                        "reserve_in_usd": None if null_metrics else "80000",
                        "volume_usd": None if null_metrics else {"h24": "30000"},
                        "transactions": None
                        if null_metrics
                        else {"h24": {"buys": 10, "sells": 4}},
                    },
                    "relationships": {
                        "base_token": {"data": {"id": "base_token-%s" % suffix}}
                    },
                }
            ],
            "included": [
                {
                    "id": "base_token-%s" % suffix,
                    "attributes": {"address": "token-%s" % suffix},
                }
            ],
        }

    def test_geckoterminal_paginates_until_watermark_is_closed(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        since = observed_at - timedelta(minutes=10)
        payloads = {
            1: self.gecko_payload("2026-07-14T00:59:00Z", "new"),
            2: self.gecko_payload("2026-07-14T00:49:00Z", "boundary"),
        }
        calls = []

        def opener(request, timeout):
            page = int(request.full_url.rsplit("page=", 1)[-1])
            calls.append(page)
            return FakeResponse(payloads[page])

        batch = GeckoTerminalAdapter(
            "base", "base", timeout=3.5, opener=opener
        ).fetch(observed_at, since=since)

        self.assertEqual([1, 2], calls)
        self.assertEqual(2, batch.page_count)
        self.assertEqual("watermark_closed", batch.coverage_scope)
        self.assertIsNone(batch.gap_reason)
        self.assertEqual("success", batch.status)
        self.assertEqual(2, batch.pool_rows_seen)
        self.assertEqual(
            "2026-07-14T00:49:00Z",
            batch.oldest_pool_created_at.isoformat().replace("+00:00", "Z"),
        )

    def test_geckoterminal_marks_pagination_gap_at_public_max_page(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        since = observed_at - timedelta(minutes=10)
        calls = []

        def opener(request, timeout):
            page = int(request.full_url.rsplit("page=", 1)[-1])
            calls.append(page)
            return FakeResponse(
                self.gecko_payload("2026-07-14T00:59:00Z", str(page))
            )

        batch = GeckoTerminalAdapter("base", "base", opener=opener).fetch(
            observed_at, since=since
        )

        self.assertEqual(list(range(1, 11)), calls)
        self.assertEqual(10, batch.page_count)
        self.assertEqual("degraded", batch.status)
        self.assertEqual("pagination_gap", batch.coverage_scope)
        self.assertEqual("max_pages_before_watermark", batch.gap_reason)

    def test_gecko_page_error_preserves_prior_pages_and_does_not_close_watermark(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        since = observed_at - timedelta(minutes=10)
        sleeps = []
        calls = []

        def opener(request, timeout):
            page = int(request.full_url.rsplit("page=", 1)[-1])
            calls.append(page)
            if page == 1:
                return FakeResponse(
                    self.gecko_payload("2026-07-14T00:59:00Z", "kept")
                )
            raise urllib.error.URLError("page two unavailable")

        adapter = GeckoTerminalAdapter(
            "base",
            "base",
            opener=opener,
            page_delay=0.25,
            sleeper=sleeps.append,
        )
        batch = adapter.fetch(observed_at, since=since)

        self.assertEqual([1, 2], calls)
        self.assertEqual([0.25], sleeps)
        self.assertEqual(["token-kept"], [item.token_address for item in batch.candidates])
        self.assertEqual("degraded", batch.status)
        self.assertEqual("pagination_gap", batch.coverage_scope)
        self.assertEqual("page_fetch_error_before_watermark", batch.gap_reason)
        self.assertEqual(2, batch.page_count)


    def test_geckoterminal_keeps_candidate_when_nested_metrics_are_null(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        batch = GeckoTerminalAdapter("base", "base").parse_payload(
            self.gecko_payload("2026-07-14T00:59:00Z", null_metrics=True),
            observed_at,
        )

        self.assertEqual(1, len(batch.candidates))
        item = batch.candidates[0]
        self.assertIsNone(item.liquidity_usd)
        self.assertIsNone(item.volume_24h_usd)
        self.assertIsNone(item.buys_24h)
        self.assertIsNone(item.sells_24h)
        self.assertEqual("degraded", batch.status)
        self.assertIn("liquidity_usd", batch.missing_fields)
        self.assertIn("volume_24h_usd", batch.missing_fields)
        self.assertIn("transactions_24h", batch.missing_fields)

    def test_shared_gecko_limiter_enforces_thirty_requests_per_minute(self):
        clock = [0.0]
        sleeps = []

        def monotonic():
            return clock[0]

        def sleeper(delay):
            sleeps.append(delay)
            clock[0] += delay

        limiter = SharedRateLimiter(
            min_interval=2.05, monotonic=monotonic, sleeper=sleeper
        )
        limiter.wait()
        clock[0] += 0.5
        limiter.wait()

        self.assertAlmostEqual(1.55, sleeps[0], places=6)

    def test_http_429_honors_bounded_retry_after_once(self):
        calls = []
        sleeps = []

        def opener(request, timeout):
            calls.append((request.full_url, timeout))
            if len(calls) == 1:
                raise urllib.error.HTTPError(
                    request.full_url,
                    429,
                    "rate limited",
                    {"Retry-After": "1.5"},
                    None,
                )
            return FakeResponse({"ok": True})

        client = JsonHttpClient(
            timeout=3.0, opener=opener, max_retries=1, sleeper=sleeps.append
        )
        self.assertEqual(
            {"ok": True}, client.get_json("https://example.invalid/read-only")
        )
        self.assertEqual(2, len(calls))
        self.assertEqual([1.5], sleeps)

    def test_http_retry_also_passes_shared_rate_limiter(self):
        clock = [0.0]
        request_times = []

        def monotonic():
            return clock[0]

        def sleeper(delay):
            clock[0] += delay

        def opener(request, timeout):
            request_times.append(clock[0])
            if len(request_times) == 1:
                raise urllib.error.HTTPError(
                    request.full_url,
                    429,
                    "rate limited",
                    {"Retry-After": "1.5"},
                    None,
                )
            return FakeResponse({"ok": True})

        limiter = SharedRateLimiter(
            min_interval=2.05, monotonic=monotonic, sleeper=sleeper
        )
        client = JsonHttpClient(
            timeout=3.0,
            opener=opener,
            max_retries=1,
            sleeper=sleeper,
            before_attempt=limiter.wait,
        )
        self.assertEqual(
            {"ok": True}, client.get_json("https://example.invalid/gecko")
        )
        self.assertEqual([0.0, 2.05], request_times)

    def test_retry_after_cannot_overrun_whole_run_budget(self):
        clock = [0.0]
        sleeps = []
        budget = RequestBudget(0.1, monotonic=lambda: clock[0])

        def opener(request, timeout):
            raise urllib.error.HTTPError(
                request.full_url,
                429,
                "rate limited",
                {"Retry-After": "10"},
                None,
            )

        client = JsonHttpClient(
            timeout=3.0,
            opener=opener,
            budget=budget,
            sleeper=sleeps.append,
        )
        with self.assertRaises(SourceError):
            client.get_json("https://example.invalid/budget")
        self.assertEqual([], sleeps)

    def test_public_request_budget_caps_timeout_and_exhausts(self):
        clock = [100.0]
        budget = RequestBudget(5.0, monotonic=lambda: clock[0])
        self.assertEqual(5.0, budget.bounded_timeout(8.0))
        clock[0] = 106.0
        with self.assertRaises(SourceError):
            budget.bounded_timeout(1.0)

    def test_geckoterminal_preserves_market_cap_and_fdv_provenance(self):
        payload = {
            "data": [
                {
                    "id": "base_pool-a",
                    "attributes": {
                        "address": "pool-a",
                        "pool_created_at": "2026-07-14T00:00:00Z",
                        "market_cap_usd": "123456",
                        "fdv_usd": "999999",
                        "reserve_in_usd": "80000",
                        "volume_usd": {"h24": "30000"},
                        "transactions": {"h24": {"buys": 10, "sells": 4}},
                    },
                    "relationships": {"base_token": {"data": {"id": "base_token-a"}}},
                },
                {
                    "id": "base_pool-b",
                    "attributes": {
                        "address": "pool-b",
                        "pool_created_at": "2026-07-14T00:00:00Z",
                        "market_cap_usd": None,
                        "fdv_usd": "654321",
                        "reserve_in_usd": "90000",
                        "volume_usd": {"h24": "40000"},
                        "transactions": {"h24": {"buys": 11, "sells": 6}},
                    },
                    "relationships": {"base_token": {"data": {"id": "base_token-b"}}},
                },
            ],
            "included": [
                {"id": "base_token-a", "attributes": {"address": "token-a"}},
                {"id": "base_token-b", "attributes": {"address": "token-b"}},
            ],
        }
        calls = []

        def opener(request, timeout):
            calls.append((request, timeout))
            return FakeResponse(payload)

        adapter = GeckoTerminalAdapter("base", "base", timeout=3.5, opener=opener)
        batch = adapter.fetch(datetime(2026, 7, 14, 1, 0, tzinfo=UTC))

        self.assertEqual(2, len(batch.candidates))
        self.assertEqual(
            (123456.0, "market_cap"),
            (batch.candidates[0].valuation_usd, batch.candidates[0].valuation_kind),
        )
        self.assertEqual(
            (654321.0, "fdv"),
            (batch.candidates[1].valuation_usd, batch.candidates[1].valuation_kind),
        )
        self.assertEqual(3.5, calls[0][1])
        self.assertIn("MemeCohortObservatory", calls[0][0].get_header("User-agent"))

    def test_composite_falls_back_and_marks_degraded(self):
        fallback_candidate = candidate(source="dexscreener")
        adapter = CompositeAdapter(
            [
                StaticAdapter(error=SourceError("gecko unavailable")),
                StaticAdapter(SourceBatch([fallback_candidate], "success", [])),
            ]
        )
        batch = adapter.fetch(datetime(2026, 7, 14, 1, 0, tzinfo=UTC))
        self.assertEqual("degraded", batch.status)
        self.assertEqual([fallback_candidate], batch.candidates)
        self.assertIn("gecko unavailable", batch.errors[0])

    def test_dexscreener_unions_robinhood_profiles_and_boosts(self):
        calls = []
        payloads = {
            "token-profiles": [
                {"chainId": "robinhood", "tokenAddress": "token-profile"},
                {"chainId": "base", "tokenAddress": "not-rh"},
            ],
            "token-boosts": [
                {"chainId": "robinhood", "tokenAddress": "token-boost"},
                {"chainId": "robinhood", "tokenAddress": "token-profile"},
            ],
        }

        def pair(address):
            return [
                {
                    "chainId": "robinhood",
                    "pairAddress": "pool-" + address,
                    "baseToken": {"address": address},
                    "quoteToken": {"address": "usdg"},
                    "marketCap": 200000,
                    "liquidity": {"usd": 80000},
                    "volume": {"h24": 30000},
                    "txns": {"h24": {"buys": 8, "sells": 3}},
                }
            ]

        def opener(request, timeout):
            url = request.full_url
            calls.append(url)
            if "token-profiles" in url:
                return FakeResponse(payloads["token-profiles"])
            if "token-boosts" in url:
                return FakeResponse(payloads["token-boosts"])
            return FakeResponse(pair(url.rsplit("/", 1)[-1]))

        adapter = DexScreenerAdapter(
            "robinhood", "robinhood", timeout=2.0, opener=opener
        )
        batch = adapter.fetch(datetime(2026, 7, 14, 1, 0, tzinfo=UTC))
        self.assertEqual(
            {"token-profile", "token-boost"},
            {item.token_address for item in batch.candidates},
        )
        self.assertTrue(any("token-profiles" in url for url in calls))
        self.assertTrue(any("token-boosts" in url for url in calls))
        self.assertEqual("degraded", batch.status)
        self.assertEqual("promoted_subset", batch.coverage_scope)
        self.assertEqual("promoted_subset", batch.candidate_scope)

    def test_dexscreener_fair_union_does_not_starve_unique_boost(self):
        profiles = [
            {"chainId": "robinhood", "tokenAddress": "profile-%02d" % index}
            for index in range(20)
        ]
        boosts = [{"chainId": "robinhood", "tokenAddress": "unique-boost"}]
        requested = []

        def opener(request, timeout):
            url = request.full_url
            if "token-profiles" in url:
                return FakeResponse(profiles)
            if "token-boosts" in url:
                return FakeResponse(boosts)
            address = url.rsplit("/", 1)[-1]
            requested.append(address)
            return FakeResponse(
                [
                    {
                        "chainId": "robinhood",
                        "pairAddress": "pool-" + address,
                        "baseToken": {"address": address},
                        "marketCap": 200000,
                    }
                ]
            )

        batch = DexScreenerAdapter(
            "robinhood", "robinhood", opener=opener, max_profiles=20
        ).fetch(datetime(2026, 7, 14, 1, 0, tzinfo=UTC))

        self.assertEqual(20, len(batch.candidates))
        self.assertIn("unique-boost", requested)
        self.assertEqual(20, len(set(requested)))

    def test_discovery_semantics_are_explicit_on_candidates(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        gecko = GeckoTerminalAdapter("base", "base").parse_payload(
            self.gecko_payload("2026-07-14T00:59:00Z"), observed_at
        ).candidates[0]
        dex = DexScreenerAdapter("robinhood", "robinhood").parse_pairs(
            [
                {
                    "chainId": "robinhood",
                    "pairAddress": "pool",
                    "baseToken": {"address": "token"},
                    "marketCap": 2_000_000,
                }
            ],
            observed_at,
        ).candidates[0]

        self.assertEqual("unfiltered_new_pools", gecko.discovery_type)
        self.assertEqual("unfiltered_new_pools", gecko.candidate_scope)
        self.assertEqual("promoted_subset", dex.discovery_type)
        self.assertEqual("promoted_subset", dex.candidate_scope)
        for item in (gecko, dex):
            self.assertEqual("unverified", item.token_origin)
            self.assertEqual("unclassified", item.meme_classification)

    def test_dex_refresh_batches_at_thirty_and_selects_deepest_matching_base_pool(self):
        from meme_cohort_observatory import adapters as adapters_module

        self.assertTrue(
            hasattr(adapters_module, "DexScreenerRefreshAdapter"),
            "refresh adapter must exist",
        )
        addresses = ["0xtoken%02d" % index for index in range(31)]
        request_batches = []

        def opener(request, timeout):
            encoded = request.full_url.rsplit("/", 1)[-1]
            requested = urllib.parse.unquote(encoded).split(",")
            request_batches.append(requested)
            pairs = []
            for token in requested:
                pairs.append(
                    {
                        "chainId": "base",
                        "pairAddress": "pool-low-" + token,
                        "baseToken": {"address": token},
                        "marketCap": 110_000,
                        "liquidity": {"usd": 10_000},
                    }
                )
                pairs.append(
                    {
                        "chainId": "base",
                        "pairAddress": "pool-high-" + token,
                        "baseToken": {"address": token},
                        "marketCap": 120_000,
                        "liquidity": {"usd": 90_000},
                    }
                )
                pairs.append(
                    {
                        "chainId": "base",
                        "pairAddress": "pool-zero-" + token,
                        "baseToken": {"address": token},
                        "marketCap": 99_000_000,
                        "liquidity": {"usd": 0},
                    }
                )
            pairs.append(
                {
                    "chainId": "base",
                    "pairAddress": "wrong-base",
                    "baseToken": {"address": "0xother"},
                    "marketCap": 9_000_000,
                    "liquidity": {"usd": 999_000},
                }
            )
            return FakeResponse(pairs)

        refresh = adapters_module.DexScreenerRefreshAdapter(
            "base", "base", opener=opener
        )
        batch = refresh.fetch_tokens(
            addresses, datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        )

        self.assertEqual([30, 1], [len(values) for values in request_batches])
        self.assertEqual(31, len(batch.candidates))
        self.assertEqual([], batch.errors)
        self.assertTrue(
            all(item.pool_address.startswith("pool-high-") for item in batch.candidates)
        )
        self.assertTrue(
            all(item.candidate_scope == "known_token_refresh" for item in batch.candidates)
        )

    def test_dexscreener_does_not_record_other_base_when_requested_token_is_quote(self):
        adapter = DexScreenerAdapter("robinhood", "robinhood")
        payload = [
            {
                "chainId": "robinhood",
                "pairAddress": "pool-wrong",
                "baseToken": {"address": "other-token"},
                "quoteToken": {"address": "requested-token"},
                "marketCap": 9_000_000,
                "liquidity": {"usd": 100_000},
            }
        ]
        batch = adapter.parse_pairs(
            payload,
            datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
            requested_token="requested-token",
        )
        self.assertEqual([], batch.candidates)


class CohortStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp.name)
        self.store = CohortStore(self.state_dir)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_duplicate_token_in_one_run_uses_highest_liquidity_pool(self):
        low = candidate(pool="pool-low", liquidity=10_000.0)
        high = candidate(pool="pool-high", liquidity=90_000.0)
        result = collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch([low, high], "success", []))},
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        self.assertEqual(1, result["chains"]["base"]["observations_written"])
        row = self.store.connection.execute(
            "SELECT pool_address FROM observations"
        ).fetchone()
        self.assertEqual("pool-high", row[0])

    def test_first_seen_above_threshold_uses_scope_specific_non_launch_class(self):
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(token="token-1m", valuation=1_500_000.0),
                            candidate(token="token-3m", valuation=3_500_000.0),
                            candidate(
                                token="token-10m",
                                valuation=12_000_000.0,
                                discovery_type="promoted_subset",
                                candidate_scope="promoted_subset",
                            ),
                        ],
                        "success",
                        [],
                    )
                )
            },
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        tokens = dict(
            self.store.connection.execute(
                "SELECT token_address, first_seen_class FROM tokens"
            ).fetchall()
        )
        events = self.store.connection.execute(
            "SELECT tier FROM events ORDER BY tier"
        ).fetchall()
        self.assertEqual(
            {
                "token-1m": "FIRST_SEEN_HIGH_1M",
                "token-3m": "FIRST_SEEN_HIGH_3M",
                "token-10m": "PROMOTED_HIGH_10M",
            },
            tokens,
        )
        self.assertEqual([], events)

    def test_observed_low_then_normal_threshold_crossings(self):
        times_and_values = [
            (datetime(2026, 7, 14, 1, 0, tzinfo=UTC), 90_000.0),
            (datetime(2026, 7, 14, 2, 0, tzinfo=UTC), 120_000.0),
            (datetime(2026, 7, 14, 3, 0, tzinfo=UTC), 1_200_000.0),
            (datetime(2026, 7, 14, 4, 0, tzinfo=UTC), 3_200_000.0),
            (datetime(2026, 7, 14, 5, 0, tzinfo=UTC), 12_000_000.0),
            (datetime(2026, 7, 14, 6, 0, tzinfo=UTC), 9_000_000.0),
            (datetime(2026, 7, 14, 7, 0, tzinfo=UTC), 12_500_000.0),
        ]
        for observed_at, value in times_and_values:
            collect_once(
                self.store,
                {
                    "base": StaticAdapter(
                        SourceBatch([candidate(valuation=value)], "success", [])
                    )
                },
                observed_at=observed_at,
            )
        events = [
            row[0]
            for row in self.store.connection.execute(
                "SELECT tier FROM events ORDER BY threshold_usd"
            ).fetchall()
        ]
        first_seen_class = self.store.connection.execute(
            "SELECT first_seen_class FROM tokens"
        ).fetchone()[0]
        self.assertEqual(
            [
                "RAW_C100_CROSSING",
                "RAW_C1_CROSSING",
                "RAW_C3_CROSSING",
                "RAW_C10_CROSSING",
            ],
            events,
        )
        observation_times = [
            row[0]
            for row in self.store.connection.execute(
                "SELECT observed_at FROM observations ORDER BY observed_at"
            ).fetchall()
        ]
        self.assertEqual(
            [
                observed_at.isoformat().replace("+00:00", "Z")
                for observed_at, _ in times_and_values
            ],
            observation_times,
        )
        self.assertIsNone(first_seen_class)

    def test_first_run_uses_ten_minute_lookback_then_saved_watermark(self):
        first = SourceBatch(
            [candidate()],
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
            page_count=2,
            pool_rows_seen=1,
        )
        second = SourceBatch(
            [candidate(token="second")],
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
            page_count=1,
            pool_rows_seen=1,
        )
        adapter = CapturingAdapter([first, second])
        first_time = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        second_time = datetime(2026, 7, 14, 2, 10, tzinfo=UTC)

        collect_once(self.store, {"base": adapter}, observed_at=first_time)
        collect_once(self.store, {"base": adapter}, observed_at=second_time)

        self.assertEqual(first_time - timedelta(minutes=10), adapter.since_values[0])
        self.assertEqual(first_time, adapter.since_values[1])
        self.assertEqual(second_time, self.store.get_chain_watermark("base"))

    def test_pagination_gap_does_not_advance_chain_watermark(self):
        gap = SourceBatch(
            [candidate()],
            "degraded",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="pagination_gap",
            gap_reason="max_pages_before_watermark",
            page_count=10,
            pool_rows_seen=10,
        )
        observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        collect_once(
            self.store,
            {"base": StaticAdapter(gap)},
            observed_at=observed_at,
        )
        self.assertIsNone(self.store.get_chain_watermark("base"))

    def test_gap_recovery_reconsiders_tokens_seen_during_incomplete_interval(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time,
        )
        recovered_token = candidate(
            token="gap-recovery-token",
            valuation=90_000.0,
            pool_created_at=first_time + timedelta(minutes=5),
        )
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [recovered_token],
                        "degraded",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="pagination_gap",
                        gap_reason="page_fetch_error_before_watermark",
                    )
                )
            },
            observed_at=first_time + timedelta(minutes=10),
        )
        self.assertEqual(first_time, self.store.get_chain_watermark("base"))

        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [recovered_token],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time + timedelta(minutes=20),
        )

        self.assertEqual(1, result["chains"]["base"]["admission_eligible"])
        self.assertEqual(1, result["chains"]["base"]["admission_admitted"])
        self.assertEqual(
            1,
            self.store.connection.execute(
                "SELECT COUNT(*) FROM tracking_cohort"
            ).fetchone()[0],
        )
        coverage = self.store.build_summary()["chains"]["base"]["coverage"]
        self.assertTrue(coverage["admission_frame_complete"])

    def test_previously_seen_200k_token_cannot_later_fall_into_c100(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        token = "already-high-token"
        first = SourceBatch(
            [
                candidate(
                    token=token,
                    valuation=200_000.0,
                    pool_created_at=first_time - timedelta(minutes=5),
                )
            ],
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
        )
        collect_once(
            self.store,
            {"base": StaticAdapter(first)},
            observed_at=first_time,
        )
        lower_new_pool = SourceBatch(
            [
                candidate(
                    token=token,
                    pool="later-new-pool",
                    valuation=90_000.0,
                    pool_created_at=first_time + timedelta(minutes=5),
                )
            ],
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
        )
        result = collect_once(
            self.store,
            {"base": StaticAdapter(lower_new_pool)},
            observed_at=first_time + timedelta(minutes=10),
        )
        self.assertEqual(0, result["chains"]["base"]["admission_eligible"])
        self.assertEqual(
            0,
            self.store.connection.execute(
                "SELECT COUNT(*) FROM tracking_cohort"
            ).fetchone()[0],
        )

    def test_closing_page_deep_pool_cannot_hide_new_pool_admission(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        token = "multi-pool-token"
        batch = SourceBatch(
            [
                candidate(
                    token=token,
                    pool="new-shallow-pool",
                    liquidity=10_000.0,
                    valuation=90_000.0,
                    pool_created_at=observed_at - timedelta(minutes=5),
                ),
                candidate(
                    token=token,
                    pool="closing-deep-pool",
                    liquidity=500_000.0,
                    valuation=90_000.0,
                    pool_created_at=observed_at - timedelta(minutes=11),
                ),
            ],
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
            pool_rows_seen=2,
        )
        result = collect_once(
            self.store,
            {"base": StaticAdapter(batch)},
            observed_at=observed_at,
        )
        self.assertEqual(1, result["chains"]["base"]["admission_eligible"])
        self.assertEqual(1, result["chains"]["base"]["admission_admitted"])
        self.assertEqual(
            "2026-07-14T00:55:00Z",
            self.store.connection.execute(
                "SELECT pool_created_at FROM tracking_cohort WHERE token_address = ?",
                (token,),
            ).fetchone()[0],
        )

    def test_row_loss_blocks_comparable_admission_and_watermark(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(
                                token="surviving-row",
                                valuation=90_000.0,
                                pool_created_at=observed_at - timedelta(minutes=5),
                            )
                        ],
                        "degraded",
                        ["row 1 missing token or pool address"],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                        pool_rows_seen=2,
                        row_loss_count=1,
                    )
                )
            },
            observed_at=observed_at,
        )

        self.assertEqual(0, result["chains"]["base"]["admission_eligible"])
        self.assertIsNone(self.store.get_chain_watermark("base"))
        coverage = self.store.build_summary()["chains"]["base"]["coverage"]
        self.assertFalse(coverage["admission_frame_complete"])
        self.assertEqual("discovery_row_loss", coverage["admission_block_reason"])

    def test_admitted_tracking_tokens_refresh_with_explicit_deferral(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        second_time = first_time + timedelta(minutes=10)
        initial = [
            candidate(
                token="token-%d" % index,
                valuation=90_000.0,
                pool_created_at=first_time - timedelta(minutes=5),
            )
            for index in range(3)
        ]
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        initial,
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                        pool_rows_seen=3,
                    )
                )
            },
            observed_at=first_time,
        )
        refresh = StaticRefreshAdapter()
        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=second_time,
            refresh_adapters={"base": refresh},
            refresh_cap=2,
        )

        self.assertEqual(1, len(refresh.requests))
        self.assertEqual(2, len(refresh.requests[0]))
        chain_result = result["chains"]["base"]
        self.assertEqual(3, chain_result["refresh_eligible_due"])
        self.assertEqual(2, chain_result["refresh_refreshed"])
        self.assertEqual(1, chain_result["refresh_deferred"])
        self.assertEqual(0, chain_result["refresh_errors"])
        self.assertEqual("degraded", chain_result["status"])
        self.assertEqual("success", chain_result["discovery_status"])
        self.assertEqual("degraded", chain_result["tracking_status"])
        self.assertEqual(0, chain_result["discovered_tokens"])
        self.assertEqual(0, chain_result["discovery_observations_written"])
        self.assertEqual(2, chain_result["tracking_observations_written"])
        coverage = self.store.build_summary()["chains"]["base"]["coverage"]
        self.assertEqual("success", coverage["discovery_status"])
        self.assertEqual("degraded", coverage["tracking_status"])
        self.assertEqual(0, coverage["discovered_tokens"])
        self.assertEqual(0, coverage["discovery_observations_written"])
        self.assertEqual(2, coverage["tracking_observations_written"])
        self.assertEqual(3, coverage["refresh_eligible_due"])
        self.assertEqual(2, coverage["refresh_refreshed"])
        self.assertEqual(1, coverage["refresh_deferred"])
        self.assertEqual(0, coverage["refresh_errors"])
        self.assertEqual(
            "tracking_cohort_anchored_age_tiered_v2_hard_cap_2",
            coverage["refresh_scope"],
        )
        self.assertIn(
            "complete_refresh_coverage",
            self.store.build_summary()["main_chain_score_missing_gates"],
        )

    def test_refresh_error_does_not_mislabel_successful_discovery(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        admitted = candidate(
            token="tracked-token",
            valuation=90_000.0,
            pool_created_at=first_time - timedelta(minutes=5),
        )
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [admitted],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time,
        )

        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time + timedelta(minutes=10),
            refresh_adapters={"base": ErrorRefreshAdapter()},
        )

        chain_result = result["chains"]["base"]
        self.assertEqual("degraded", chain_result["status"])
        self.assertEqual("success", chain_result["discovery_status"])
        self.assertEqual("error", chain_result["tracking_status"])
        coverage = self.store.build_summary()["chains"]["base"]["coverage"]
        self.assertEqual("success", coverage["discovery_status"])
        self.assertEqual("error", coverage["tracking_status"])
        self.assertEqual(0, coverage["discovered_tokens"])
        self.assertEqual(0, coverage["tracking_observations_written"])

    def test_returned_pool_without_valuation_is_not_a_usable_refresh(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(
                                token="null-refresh-token",
                                valuation=90_000.0,
                                pool_created_at=first_time - timedelta(minutes=5),
                            )
                        ],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time,
        )
        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time + timedelta(minutes=10),
            refresh_adapters={"base": NullValuationRefreshAdapter()},
        )

        chain_result = result["chains"]["base"]
        self.assertEqual(1, chain_result["refresh_returned"])
        self.assertEqual(0, chain_result["refresh_usable"])
        self.assertEqual(0, chain_result["refresh_refreshed"])
        self.assertEqual(1, chain_result["refresh_errors"])
        self.assertEqual("error", chain_result["tracking_status"])
        coverage = self.store.build_summary()["chains"]["base"]["coverage"]
        self.assertEqual(1, coverage["refresh_returned"])
        self.assertEqual(0, coverage["refresh_usable"])
        self.assertFalse(coverage["refresh_complete"])

    def test_refresh_schedule_is_admission_anchored_across_tier_boundaries(self):
        admitted_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        token = "anchored-token"
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(
                                token=token,
                                valuation=90_000.0,
                                pool_created_at=admitted_at - timedelta(minutes=5),
                            )
                        ],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=admitted_at,
        )
        delayed = admitted_at + timedelta(minutes=70)
        plan = self.store.get_refresh_plan("base", delayed)
        self.assertEqual([token], plan["token_addresses"])
        self.store.mark_refresh_attempts(
            "base", [token], delayed, usable_token_addresses=[token]
        )
        row = self.store.connection.execute(
            """
            SELECT next_refresh_due_at, next_refresh_slot_minutes,
                   missed_refresh_slots, completed_at
            FROM tracking_cohort WHERE token_address = ?
            """,
            (token,),
        ).fetchone()
        self.assertEqual("2026-07-14T02:30:00Z", row[0])
        self.assertEqual(90, row[1])
        self.assertEqual(5, row[2])
        self.assertIsNone(row[3])

    def test_failed_terminal_lookup_remains_due_until_usable(self):
        admitted_at = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
        token = "terminal-token"
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(
                                token=token,
                                valuation=90_000.0,
                                pool_created_at=admitted_at - timedelta(minutes=5),
                            )
                        ],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=admitted_at,
        )
        late = admitted_at + timedelta(days=8)
        self.assertEqual(
            [token], self.store.get_refresh_plan("base", late)["token_addresses"]
        )
        self.store.mark_refresh_attempts("base", [token], late, [])
        row = self.store.connection.execute(
            """
            SELECT completed_at, next_refresh_due_at FROM tracking_cohort
            WHERE token_address = ?
            """,
            (token,),
        ).fetchone()
        self.assertIsNone(row[0])
        self.assertEqual("2026-07-09T00:10:00Z", row[1])
        retry = late + timedelta(minutes=10)
        self.assertEqual(
            [token], self.store.get_refresh_plan("base", retry)["token_addresses"]
        )
        self.store.mark_refresh_attempts(
            "base", [token], retry, usable_token_addresses=[token]
        )
        row = self.store.connection.execute(
            """
            SELECT completed_at, completion_reason FROM tracking_cohort
            WHERE token_address = ?
            """,
            (token,),
        ).fetchone()
        self.assertEqual("2026-07-09T00:10:00Z", row[0])
        self.assertEqual("phase1_7d_tracking_window_complete", row[1])

    def test_due_tracking_observation_wins_over_overlapping_closing_page_row(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        admitted = candidate(
            token="overlap-token",
            valuation=90_000.0,
            pool_created_at=first_time - timedelta(minutes=5),
        )
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [admitted],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time,
        )

        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(
                                token="overlap-token",
                                pool="closing-page-pool",
                                liquidity=999_000.0,
                                pool_created_at=first_time - timedelta(minutes=5),
                            )
                        ],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time + timedelta(minutes=10),
            refresh_adapters={"base": StaticRefreshAdapter()},
        )

        row = self.store.connection.execute(
            """
            SELECT pool_address, observation_phase FROM observations
            WHERE token_address = 'overlap-token'
            ORDER BY observed_at DESC LIMIT 1
            """
        ).fetchone()
        self.assertEqual(("refresh-overlap-token", "tracking"), row)
        self.assertEqual(0, result["chains"]["base"]["discovery_observations_written"])
        self.assertEqual(1, result["chains"]["base"]["tracking_observations_written"])

    def test_admission_and_token_ingest_rollback_together(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        batch = SourceBatch(
            [
                candidate(
                    token="transactional-token",
                    valuation=90_000.0,
                    pool_created_at=observed_at - timedelta(minutes=5),
                )
            ],
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
        )

        with mock.patch.object(
            self.store,
            "_insert_token",
            side_effect=sqlite3.OperationalError("forced ingest failure"),
        ):
            result = collect_once(
                self.store,
                {"base": StaticAdapter(batch)},
                observed_at=observed_at,
            )

        self.assertEqual("error", result["chains"]["base"]["status"])
        self.assertEqual(
            0,
            self.store.connection.execute(
                "SELECT COUNT(*) FROM tracking_cohort"
            ).fetchone()[0],
        )
        self.assertEqual(
            0,
            self.store.connection.execute("SELECT COUNT(*) FROM tokens").fetchone()[0],
        )
        self.assertEqual(
            0,
            self.store.connection.execute(
                "SELECT COUNT(*) FROM observations"
            ).fetchone()[0],
        )

    def test_refresh_attempt_order_prevents_deferred_token_starvation(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        tokens = [
            candidate(
                token="token-%d" % index,
                valuation=90_000.0,
                pool_created_at=first_time - timedelta(minutes=5),
            )
            for index in range(3)
        ]
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        tokens,
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                        pool_rows_seen=3,
                    )
                )
            },
            observed_at=first_time,
        )
        refresh = StaticRefreshAdapter()
        empty_discovery = StaticAdapter(
            SourceBatch(
                [],
                "success",
                [],
                discovery_type="unfiltered_new_pools",
                candidate_scope="unfiltered_new_pools",
                coverage_scope="watermark_closed",
            )
        )
        collect_once(
            self.store,
            {"base": empty_discovery},
            observed_at=first_time + timedelta(minutes=10),
            refresh_adapters={"base": refresh},
            refresh_cap=2,
        )
        collect_once(
            self.store,
            {"base": empty_discovery},
            observed_at=first_time + timedelta(minutes=20),
            refresh_adapters={"base": refresh},
            refresh_cap=2,
        )

        first_selected = set(refresh.requests[0])
        deferred = ({"token-0", "token-1", "token-2"} - first_selected).pop()
        self.assertIn(deferred, refresh.requests[1])

    def test_gap_and_promoted_discovery_never_enter_comparable_tracking_cohort(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        base_candidate = candidate(
            chain="base",
            token="base-gap",
            valuation=90_000.0,
            pool_created_at=observed_at - timedelta(minutes=5),
        )
        rh_candidate = candidate(
            chain="robinhood",
            token="rh-promoted",
            valuation=90_000.0,
            discovery_type="promoted_subset",
            candidate_scope="promoted_subset",
            pool_created_at=observed_at - timedelta(minutes=5),
        )
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [base_candidate],
                        "degraded",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="pagination_gap",
                        gap_reason="max_pages_before_watermark",
                    )
                ),
                "robinhood": StaticAdapter(
                    SourceBatch(
                        [rh_candidate],
                        "degraded",
                        [],
                        discovery_type="promoted_subset",
                        candidate_scope="promoted_subset",
                        coverage_scope="promoted_subset",
                    )
                ),
            },
            observed_at=observed_at,
        )

        self.assertEqual(
            0,
            self.store.connection.execute(
                "SELECT COUNT(*) FROM tracking_cohort"
            ).fetchone()[0],
        )
        coverage = {
            row[0]: row[1:]
            for row in self.store.connection.execute(
                """
                SELECT chain, admission_eligible, admission_admitted,
                       admission_dropped, admission_block_reason,
                       admission_frame_complete
                FROM coverage_runs ORDER BY chain
                """
            ).fetchall()
        }
        self.assertEqual(
            (0, 0, 0, "discovery_not_comparable", 0), coverage["base"]
        )
        self.assertEqual(
            (0, 0, 0, "discovery_not_comparable", 0), coverage["robinhood"]
        )

    def test_admission_sampling_is_stable_and_reports_probability_and_drops(self):
        observed_at = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        candidates = [
            candidate(
                token="token-%02d" % index,
                valuation=90_000.0,
                pool_created_at=observed_at - timedelta(minutes=5),
            )
            for index in range(8)
        ]
        candidates.extend(
            [
                candidate(
                    token="older-closing-page-row",
                    valuation=90_000.0,
                    pool_created_at=observed_at - timedelta(minutes=11),
                ),
                candidate(
                    token="future-timestamp-row",
                    valuation=90_000.0,
                    pool_created_at=observed_at + timedelta(minutes=1),
                ),
            ]
        )
        batch = SourceBatch(
            candidates,
            "success",
            [],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            coverage_scope="watermark_closed",
            pool_rows_seen=10,
        )
        selections = []
        coverages = []
        for _ in range(2):
            with tempfile.TemporaryDirectory() as temp:
                with CohortStore(temp) as store:
                    collect_once(
                        store,
                        {"base": StaticAdapter(batch)},
                        observed_at=observed_at,
                    )
                    selections.append(
                        [
                            row[0]
                            for row in store.connection.execute(
                                """
                                SELECT token_address FROM tracking_cohort
                                ORDER BY token_address
                                """
                            ).fetchall()
                        ]
                    )
                    coverages.append(
                        store.connection.execute(
                            """
                            SELECT admission_eligible, admission_admitted,
                                   admission_dropped, admission_policy_version,
                                   admission_inclusion_probability,
                                   admission_sample_fraction,
                                   admission_frame_complete, admission_bucket
                            FROM coverage_runs WHERE chain = 'base'
                            """
                        ).fetchone()
                    )

        self.assertEqual(selections[0], selections[1])
        self.assertEqual(5, len(selections[0]))
        self.assertEqual(
            (8, 5, 3, "c100_admission_v1", 0.625, 0.625, 1, "2026-07-14T01:00Z"),
            coverages[0],
        )
        self.assertEqual(coverages[0], coverages[1])

    def test_tracking_due_uses_age_tiered_cadence(self):
        from meme_cohort_observatory.store import tracking_cadence_minutes

        self.assertEqual(10, tracking_cadence_minutes(timedelta(minutes=59)))
        self.assertEqual(30, tracking_cadence_minutes(timedelta(hours=2)))
        self.assertEqual(120, tracking_cadence_minutes(timedelta(hours=7)))
        self.assertEqual(360, tracking_cadence_minutes(timedelta(hours=30)))
        self.assertEqual(720, tracking_cadence_minutes(timedelta(days=4)))
        self.assertIsNone(tracking_cadence_minutes(timedelta(days=8)))

    def test_v2_database_migrates_without_losing_history(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "cohorts.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE observations (
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
                    PRIMARY KEY (chain, token_address, observed_at)
                );
                CREATE TABLE coverage_runs (
                    run_id TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    pool_rows_seen INTEGER NOT NULL,
                    discovered_tokens INTEGER NOT NULL,
                    observations_written INTEGER NOT NULL,
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
                    PRIMARY KEY (run_id, chain)
                );
                """
            )
            rows = [
                ("2026-07-14T00:00:00Z", 90_000.0),
                ("2026-07-14T00:10:00.500000Z", 1_200_000.0),
            ]
            connection.executemany(
                """
                INSERT INTO observations VALUES (
                    'base', 'legacy-token', ?, 'legacy-pool', 'legacy', ?,
                    'market_cap', 50000, 10000, 5, 2,
                    '2026-07-13T23:55:00Z', 'unfiltered_new_pools',
                    'unfiltered_new_pools', 'unverified', 'unclassified'
                )
                """,
                rows,
            )
            connection.execute(
                """
                INSERT INTO coverage_runs VALUES (
                    'legacy-run', 'base', '2026-07-14T00:10:00Z', 'success',
                    2, 1, 2, 'unfiltered_new_pools', 'unfiltered_new_pools',
                    1, '2026-07-13T23:55:00Z', '2026-07-13T23:55:00Z',
                    'watermark_closed', NULL, '[]', '[]',
                    '2026-07-14T00:10:00Z'
                )
                """
            )
            connection.commit()
            connection.close()

            with CohortStore(temp) as migrated:
                observation_columns = {
                    row[1]
                    for row in migrated.connection.execute(
                        "PRAGMA table_info(observations)"
                    ).fetchall()
                }
                coverage_columns = {
                    row[1]
                    for row in migrated.connection.execute(
                        "PRAGMA table_info(coverage_runs)"
                    ).fetchall()
                }
                self.assertIn("observation_phase", observation_columns)
                self.assertIn("admission_eligible", coverage_columns)
                self.assertIn("refresh_usable", coverage_columns)
                self.assertEqual(
                    ("2026-07-14T00:00:00Z", 90_000.0,
                     "2026-07-14T00:10:00.500000Z", 1_200_000.0,
                     1_200_000.0),
                    migrated.connection.execute(
                        """
                        SELECT first_observed_at, first_valuation_usd,
                               current_observed_at, current_valuation_usd,
                               max_valuation_usd
                        FROM valuation_tracks
                        WHERE chain = 'base' AND token_address = 'legacy-token'
                          AND valuation_kind = 'market_cap'
                        """
                    ).fetchone(),
                )
                self.assertEqual(
                    2,
                    migrated.connection.execute(
                        "SELECT COUNT(*) FROM observations"
                    ).fetchone()[0],
                )
                self.assertEqual(5, migrated.build_summary()["schema_version"])

    def test_coverage_splits_pool_token_and_written_counts_with_scope_metadata(self):
        batch = SourceBatch(
            [
                candidate(pool="pool-low", liquidity=10_000.0),
                candidate(pool="pool-high", liquidity=90_000.0),
            ],
            "degraded",
            ["partial field coverage"],
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            page_count=2,
            oldest_pool_created_at=datetime(2026, 7, 13, 1, 0, tzinfo=UTC),
            newest_pool_created_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
            coverage_scope="watermark_closed",
            pool_rows_seen=3,
            missing_fields=["volume_24h_usd"],
        )
        result = collect_once(
            self.store,
            {"base": StaticAdapter(batch)},
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )

        chain_result = result["chains"]["base"]
        self.assertEqual(3, chain_result["pool_rows_seen"])
        self.assertEqual(1, chain_result["discovered_tokens"])
        self.assertEqual(1, chain_result["observations_written"])
        summary_coverage = self.store.build_summary()["chains"]["base"]["coverage"]
        self.assertEqual(2, summary_coverage["page_count"])
        self.assertEqual("watermark_closed", summary_coverage["coverage_scope"])
        self.assertEqual(["volume_24h_usd"], summary_coverage["missing_fields"])
        row = self.store.connection.execute(
            """
            SELECT pool_rows_seen, discovered_tokens, observations_written,
                   coverage_scope, page_count
            FROM coverage_runs WHERE chain = 'base'
            """
        ).fetchone()
        self.assertEqual((3, 1, 1, "watermark_closed", 2), row)

    def test_same_timestamp_different_payload_is_rejected_before_state_changes(self):
        observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch([candidate(valuation=90_000)], "success", []))},
            observed_at=observed_at,
        )
        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch([candidate(valuation=1_200_000)], "success", [])
                )
            },
            observed_at=observed_at,
        )

        token = self.store.connection.execute(
            "SELECT current_valuation_usd FROM tokens"
        ).fetchone()[0]
        observation = self.store.connection.execute(
            "SELECT valuation_usd FROM observations"
        ).fetchone()[0]
        self.assertEqual(90_000.0, token)
        self.assertEqual(90_000.0, observation)
        self.assertEqual(0, self.store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        self.assertEqual("degraded", result["chains"]["base"]["status"])
        self.assertTrue(
            any("same_timestamp_conflict" in error for error in result["chains"]["base"]["errors"])
        )

    def test_out_of_order_history_does_not_regress_current_or_fabricate_crossing(self):
        current_time = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        older_time = current_time - timedelta(hours=1)
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch([candidate(valuation=200_000)], "success", []))},
            observed_at=current_time,
        )
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch([candidate(valuation=90_000)], "success", []))},
            observed_at=older_time,
        )

        current = self.store.connection.execute(
            "SELECT current_observed_at, current_valuation_usd FROM tokens"
        ).fetchone()
        self.assertEqual(
            (current_time.isoformat().replace("+00:00", "Z"), 200_000.0), current
        )
        self.assertEqual(2, self.store.connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0])
        self.assertEqual(0, self.store.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    def test_candidate_semantics_persist_to_token_and_observation(self):
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch([candidate()], "success", []))},
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        token = self.store.connection.execute(
            """
            SELECT discovery_type, candidate_scope, token_origin,
                   meme_classification FROM tokens
            """
        ).fetchone()
        observation = self.store.connection.execute(
            """
            SELECT discovery_type, candidate_scope, token_origin,
                   meme_classification, observation_phase FROM observations
            """
        ).fetchone()
        expected = (
            "unfiltered_new_pools",
            "unfiltered_new_pools",
            "unverified",
            "unclassified",
        )
        self.assertEqual(expected, token)
        self.assertEqual(expected + ("discovery",), observation)

    def test_uppercase_chain_is_lowered_before_evm_address_normalization(self):
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [candidate(chain="BASE", token="0xABC", pool="0xDEF")],
                        "success",
                        [],
                    )
                )
            },
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        self.assertEqual(
            ("base", "0xabc", "0xdef"),
            self.store.connection.execute(
                "SELECT chain, token_address, current_pool_address FROM tokens"
            ).fetchone(),
        )

    def test_one_chain_failure_does_not_rollback_successful_chain(self):
        result = collect_once(
            self.store,
            {
                "base": StaticAdapter(SourceBatch([candidate()], "success", [])),
                "bsc": StaticAdapter(error=SourceError("bsc timeout")),
            },
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        self.assertEqual("success", result["chains"]["base"]["status"])
        self.assertEqual("error", result["chains"]["bsc"]["status"])
        self.assertEqual(
            1,
            self.store.connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
        )
        statuses = dict(
            self.store.connection.execute(
                "SELECT chain, status FROM coverage_runs"
            ).fetchall()
        )
        self.assertEqual({"base": "success", "bsc": "error"}, statuses)

    def test_same_timestamp_run_is_incrementally_idempotent(self):
        observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        adapters = {"base": StaticAdapter(SourceBatch([candidate()], "success", []))}
        collect_once(self.store, adapters, observed_at=observed_at)
        collect_once(self.store, adapters, observed_at=observed_at)
        self.assertEqual(
            1,
            self.store.connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
        )
        self.assertEqual(
            1,
            self.store.connection.execute("SELECT COUNT(*) FROM coverage_runs").fetchone()[0],
        )

    def test_same_address_on_different_chains_is_not_deduplicated(self):
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch([candidate(chain="base", token="0xABC")], "success", [])
                ),
                "bsc": StaticAdapter(
                    SourceBatch([candidate(chain="bsc", token="0xABC")], "success", [])
                ),
            },
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        self.assertEqual(
            [("base", "0xabc"), ("bsc", "0xabc")],
            self.store.connection.execute(
                "SELECT chain, token_address FROM tokens ORDER BY chain"
            ).fetchall(),
        )

    def test_raw_counts_never_become_eligible_without_missing_gates(self):
        low = [candidate(token="token-%02d" % index, valuation=90_000) for index in range(10)]
        high = [
            candidate(token="token-%02d" % index, valuation=1_200_000)
            for index in range(10)
        ]
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch(low, "success", []))},
            observed_at=datetime(2026, 7, 14, 1, 0, tzinfo=UTC),
        )
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch(high, "degraded", ["fallback used"]))},
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        summary = self.store.build_summary()
        self.assertEqual(0, summary["eligible_cohort_count"])
        self.assertEqual(0, summary["chains"]["base"]["eligible_cohort_count"])
        self.assertEqual("degraded", summary["chains"]["base"]["coverage"]["status"])
        self.assertIn("fallback used", summary["chains"]["base"]["coverage"]["errors"])
        self.assertEqual("insufficient_sample", summary["main_chain_score_status"])
        self.assertEqual(
            {"age_window", "hold_duration", "security", "executability"},
            set(summary["main_chain_score_missing_gates"][:4]),
        )

    def test_valuation_provenance_persists_to_sqlite_and_summary(self):
        payload = {
            "data": [
                {
                    "attributes": {
                        "address": "pool-mcap",
                        "market_cap_usd": "123000",
                        "fdv_usd": "999000",
                    },
                    "relationships": {
                        "base_token": {"data": {"id": "base_mcap"}}
                    },
                },
                {
                    "attributes": {
                        "address": "pool-fdv",
                        "market_cap_usd": None,
                        "fdv_usd": "456000",
                    },
                    "relationships": {
                        "base_token": {"data": {"id": "base_fdv"}}
                    },
                },
            ],
            "included": [
                {"id": "base_mcap", "attributes": {"address": "mcap"}},
                {"id": "base_fdv", "attributes": {"address": "fdv"}},
            ],
        }
        observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        parsed = GeckoTerminalAdapter("base", "base").parse_payload(
            payload, observed_at
        )
        collect_once(
            self.store,
            {"base": StaticAdapter(parsed)},
            observed_at=observed_at,
        )
        persisted = dict(
            self.store.connection.execute(
                "SELECT token_address, valuation_kind FROM observations"
            ).fetchall()
        )
        self.assertEqual({"mcap": "market_cap", "fdv": "fdv"}, persisted)
        self.assertEqual(
            {"market_cap": 1, "fdv": 1},
            self.store.build_summary()["chains"]["base"]["first_valuation_provenance"],
        )

    def test_market_cap_and_fdv_summary_headlines_are_fully_separate(self):
        observed_at = datetime(2026, 7, 14, 2, 0, tzinfo=UTC)
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(token="mcap", valuation=1_200_000, kind="market_cap"),
                            candidate(token="fdv", valuation=1_200_000, kind="fdv"),
                        ],
                        "success",
                        [],
                    )
                )
            },
            observed_at=observed_at,
        )
        chain = self.store.build_summary()["chains"]["base"]
        self.assertEqual(
            {"observed_market_cap", "fdv_proxy", "unknown"},
            set(chain["valuation_headlines"]),
        )
        self.assertEqual(1, chain["valuation_headlines"]["observed_market_cap"]["token_count"])
        self.assertEqual(1, chain["valuation_headlines"]["fdv_proxy"]["token_count"])
        self.assertNotIn("raw_threshold_crossings", chain)
        self.assertEqual(
            {
                "coverage",
                "latest_observation_at",
                "last_successful_observation",
                "eligible_cohort_count",
                "qualification_status",
                "first_seen_high_watch_refresh_status",
                "valuation_headlines",
                "first_valuation_provenance",
                "block_discovery",
            },
            set(chain),
        )
        self.assertIn(
            "full_discovery_coverage",
            self.store.build_summary()["main_chain_score_missing_gates"],
        )
        self.assertIn(
            "meme_classification",
            self.store.build_summary()["main_chain_score_missing_gates"],
        )
        self.assertIn(
            "token_origin",
            self.store.build_summary()["main_chain_score_missing_gates"],
        )

    def test_market_cap_and_fdv_tracks_cross_independently_after_kind_switches(self):
        observations = [
            (datetime(2026, 7, 14, 1, 0, tzinfo=UTC), 90_000.0, "fdv"),
            (datetime(2026, 7, 14, 2, 0, tzinfo=UTC), 90_000.0, "market_cap"),
            (datetime(2026, 7, 14, 3, 0, tzinfo=UTC), 120_000.0, "fdv"),
            (datetime(2026, 7, 14, 4, 0, tzinfo=UTC), 120_000.0, "market_cap"),
        ]
        for observed_at, valuation, kind in observations:
            collect_once(
                self.store,
                {
                    "base": StaticAdapter(
                        SourceBatch(
                            [candidate(valuation=valuation, kind=kind)],
                            "success",
                            [],
                        )
                    )
                },
                observed_at=observed_at,
            )

        events = self.store.connection.execute(
            """
            SELECT valuation_kind, tier FROM events
            WHERE tier = 'RAW_C100_CROSSING'
            ORDER BY valuation_kind
            """
        ).fetchall()
        self.assertEqual(
            [("fdv", "RAW_C100_CROSSING"), ("market_cap", "RAW_C100_CROSSING")],
            events,
        )
        headlines = self.store.build_summary()["chains"]["base"]["valuation_headlines"]
        self.assertEqual(1, headlines["fdv_proxy"]["token_count"])
        self.assertEqual(1, headlines["observed_market_cap"]["token_count"])
        self.assertEqual(
            1,
            headlines["fdv_proxy"]["raw_threshold_crossings"]["RAW_C100_CROSSING"],
        )
        self.assertEqual(
            1,
            headlines["observed_market_cap"]["raw_threshold_crossings"]["RAW_C100_CROSSING"],
        )

    def test_one_valuation_track_reaching_10m_does_not_end_other_track(self):
        first_time = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
        token = "dual-track-token"
        collect_once(
            self.store,
            {
                "base": StaticAdapter(
                    SourceBatch(
                        [
                            candidate(
                                token=token,
                                valuation=90_000.0,
                                kind="market_cap",
                                pool_created_at=first_time - timedelta(minutes=5),
                            )
                        ],
                        "success",
                        [],
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        coverage_scope="watermark_closed",
                    )
                )
            },
            observed_at=first_time,
        )
        high_fdv = replace(
            candidate(
                token=token,
                valuation=12_000_000.0,
                kind="fdv",
                discovery_type="refresh",
                candidate_scope="known_token_refresh",
            ),
            observation_phase="tracking",
        )
        self.store.ingest_chain(
            run_id="dual-track-refresh",
            chain="base",
            observed_at=first_time + timedelta(minutes=10),
            candidates=[high_fdv],
            status="success",
            errors=[],
            pool_rows_seen=1,
            discovery_type="refresh",
            candidate_scope="known_token_refresh",
            page_count=1,
            oldest_pool_created_at=None,
            newest_pool_created_at=None,
            coverage_scope="known_token_refresh",
            gap_reason=None,
            missing_fields=[],
        )
        self.assertEqual(
            (None, None),
            self.store.connection.execute(
                """
                SELECT completed_at, completion_reason FROM tracking_cohort
                WHERE token_address = ?
                """,
                (token,),
            ).fetchone(),
        )

    def test_summary_is_atomic_and_truthfully_unmeasured(self):
        collect_once(
            self.store,
            {"base": StaticAdapter(SourceBatch([candidate()], "success", []))},
            observed_at=datetime(2026, 7, 14, 2, 0, tzinfo=UTC),
        )
        summary_path = self.state_dir / "latest_summary.json"
        with mock.patch("meme_cohort_observatory.store.os.replace", wraps=__import__("os").replace) as replace:
            summary = self.store.write_summary(summary_path)
        replace.assert_called_once()
        self.assertTrue(summary_path.exists())
        self.assertFalse((self.state_dir / "latest_summary.json.tmp").exists())
        self.assertEqual("unmeasured", summary["security_status"])
        self.assertEqual("unmeasured", summary["executability_status"])
        self.assertEqual(
            "pending_history_and_execution_checks",
            summary["qualification_status"],
        )
        self.assertEqual("insufficient_sample", summary["main_chain_score_status"])


class RuntimeTests(unittest.TestCase):
    def test_single_instance_lock_is_nonblocking(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "collector.lock"
            first = SingleInstanceLock(path)
            second = SingleInstanceLock(path)
            self.assertTrue(first.acquire())
            self.assertFalse(second.acquire())
            first.release()
            self.assertTrue(second.acquire())
            second.release()

    def test_fixture_cli_smoke(self):
        fixtures = TOOL_DIR / "tests" / "fixtures"
        with tempfile.TemporaryDirectory() as temp:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "meme_cohort_observatory",
                    "collect",
                    "--fixtures",
                    str(fixtures),
                    "--state-dir",
                    temp,
                    "--observed-at",
                    "2026-07-14T02:00:00Z",
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            summary_path = Path(temp) / "latest_summary.json"
            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(
                {"base", "bsc", "robinhood", "solana"},
                set(summary["chains"]),
            )
            with sqlite3.connect(Path(temp) / "cohorts.sqlite3") as connection:
                self.assertGreater(
                    connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
                    0,
                )

    def test_public_cli_wires_known_token_refresh_adapters(self):
        from meme_cohort_observatory import cli as collector_module

        discovery = {"base": object()}
        refresh = {"base": object()}
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(
            collector_module, "default_adapters", return_value=discovery
        ), mock.patch.object(
            collector_module,
            "default_refresh_adapters",
            return_value=refresh,
            create=True,
        ) as refresh_factory, mock.patch.object(
            collector_module,
            "collect_once",
            return_value={"run_id": "fixture", "chains": {}},
        ) as collect:
            exit_code = collector_module.main(
                ["--state-dir", temp, "--network", "base"]
            )

        self.assertEqual(0, exit_code)
        refresh_factory.assert_called_once_with(
            ["base"], timeout=6.0, budget=mock.ANY
        )
        self.assertIs(refresh, collect.call_args.kwargs["refresh_adapters"])

    def test_second_cli_exits_quickly_without_creating_database(self):
        fixtures = TOOL_DIR / "tests" / "fixtures"
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp) / "state"
            lock_path = Path(temp) / "shared.lock"
            holder = SingleInstanceLock(lock_path)
            self.assertTrue(holder.acquire())
            started = time.monotonic()
            try:
                completed = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "meme_cohort_observatory",
                        "collect",
                        "--fixtures",
                        str(fixtures),
                        "--state-dir",
                        str(state_dir),
                        "--lock-file",
                        str(lock_path),
                    ],
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=3,
                )
            finally:
                holder.release()
            self.assertEqual(0, completed.returncode, completed.stderr)
            self.assertLess(time.monotonic() - started, 2.0)
            self.assertEqual(
                {"status": "skipped", "reason": "already_running"},
                json.loads(completed.stdout),
            )
            self.assertFalse((state_dir / "cohorts.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
