"""Normalize observations and coordinate one read-only collection run.

asset-version: v1.6
updated: 2026-07-15
owner_surface: Meme Cohort Observatory
behavior_change: Run Robinhood raw discovery before other chains can exhaust the shared public-request budget; keep the v1.5 block cursor and enrichment boundaries unchanged.
rollback: Restore lexical chain order in collect_once; no external or wallet state is mutated.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional


THRESHOLDS = (
    ("RAW_C100_CROSSING", 100_000.0),
    ("RAW_C1_CROSSING", 1_000_000.0),
    ("RAW_C3_CROSSING", 3_000_000.0),
    ("RAW_C10_CROSSING", 10_000_000.0),
)

EVM_CHAINS = frozenset(("base", "bsc", "robinhood"))


def utc_now():
    return datetime.now(timezone.utc)


def ensure_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def isoformat(value):
    return ensure_utc(value).isoformat().replace("+00:00", "Z")


def canonical_address(chain, address):
    cleaned = (address or "").strip()
    if (chain or "").strip().lower() in EVM_CHAINS:
        return cleaned.lower()
    return cleaned


def first_seen_class_for(valuation_usd, candidate_scope):
    if valuation_usd is None:
        return None
    prefix = (
        "PROMOTED_HIGH"
        if candidate_scope == "promoted_subset"
        else "FIRST_SEEN_HIGH"
    )
    if valuation_usd >= 10_000_000.0:
        return prefix + "_10M"
    if valuation_usd >= 3_000_000.0:
        return prefix + "_3M"
    if valuation_usd >= 1_000_000.0:
        return prefix + "_1M"
    return None


@dataclass(frozen=True)
class Candidate:
    chain: str
    token_address: str
    pool_address: str
    source: str
    observed_at: datetime
    valuation_usd: Optional[float]
    valuation_kind: Optional[str]
    liquidity_usd: Optional[float]
    volume_24h_usd: Optional[float]
    buys_24h: Optional[int]
    sells_24h: Optional[int]
    pool_created_at: Optional[datetime]
    discovery_type: str = "unknown"
    candidate_scope: str = "unknown"
    token_origin: str = "unverified"
    meme_classification: str = "unclassified"
    observation_phase: str = "discovery"

    def normalized(self, observed_at=None):
        chain = self.chain.strip().lower()
        return replace(
            self,
            chain=chain,
            token_address=canonical_address(chain, self.token_address),
            pool_address=canonical_address(chain, self.pool_address),
            observed_at=ensure_utc(observed_at or self.observed_at),
            pool_created_at=(
                ensure_utc(self.pool_created_at) if self.pool_created_at else None
            ),
        )


def deduplicate_candidates(candidates, observed_at):
    """Keep one token observation, preferring due tracking then pool depth."""
    selected = {}
    for raw in candidates:
        item = raw.normalized(observed_at)
        if not item.token_address or not item.pool_address:
            continue
        key = (item.chain, item.token_address)
        previous = selected.get(key)
        item_rank = (
            1 if item.observation_phase == "tracking" else 0,
            item.liquidity_usd if item.liquidity_usd is not None else -1.0,
            item.volume_24h_usd if item.volume_24h_usd is not None else -1.0,
            item.pool_address,
        )
        previous_rank = (
            1 if previous and previous.observation_phase == "tracking" else 0,
            previous.liquidity_usd if previous and previous.liquidity_usd is not None else -1.0,
            previous.volume_24h_usd if previous and previous.volume_24h_usd is not None else -1.0,
            previous.pool_address if previous else "",
        )
        if previous is None or item_rank > previous_rank:
            selected[key] = item
    return list(selected.values())


def collect_once(
    store,
    adapters,
    observed_at=None,
    refresh_adapters=None,
    admission_cap=5,
    refresh_cap=300,
):
    """Collect all configured chains while isolating failures per chain."""
    observed_at = ensure_utc(observed_at or utc_now())
    run_id = isoformat(observed_at)
    result = {"run_id": run_id, "chains": {}}
    refresh_adapters = refresh_adapters or {}

    # RH raw pool-event coverage must run before expensive cross-chain refreshes
    # can consume the shared whole-run request budget. Later-chain failures stay
    # isolated and auditable through the existing per-chain error records.
    chain_order = sorted(adapters, key=lambda chain: (chain != "robinhood", chain))
    for chain in chain_order:
        adapter = adapters[chain]
        cursor_source = getattr(adapter, "cursor_source", None)
        cursor_before = None
        try:
            since = store.get_chain_watermark(chain) or (
                observed_at - timedelta(minutes=10)
            )
            if cursor_source:
                cursor_before = store.get_source_cursor(chain, cursor_source)
                pending_event_tokens = store.get_pending_pool_event_tokens(
                    chain, limit=300
                )
                batch = adapter.fetch(
                    observed_at,
                    since=since,
                    cursor=cursor_before,
                    pending_token_addresses=pending_event_tokens,
                )
            else:
                batch = adapter.fetch(observed_at, since=since)
            candidates = [
                item
                for item in batch.candidates
                if item.chain.strip().lower() == chain.strip().lower()
            ]
            deduplicated = deduplicate_candidates(candidates, observed_at)
            admission = store.plan_admission(
                chain=chain,
                candidates=candidates,
                observed_at=observed_at,
                since=since,
                discovery_type=batch.discovery_type,
                candidate_scope=batch.candidate_scope,
                coverage_scope=batch.coverage_scope,
                row_loss_count=batch.row_loss_count,
                enrichment_complete=batch.enrichment_complete,
                cap=admission_cap,
            )
            refresh_plan = store.get_refresh_plan(
                chain, observed_at, cap=refresh_cap
            )
            refresh_metrics = {
                "eligible_due": refresh_plan["eligible_due"],
                "selected": refresh_plan["selected"],
                "returned": 0,
                "usable": 0,
                "refreshed": 0,
                "deferred": refresh_plan["deferred"],
                "errors": 0,
                "scope": refresh_plan["scope"],
                "complete": True,
            }
            tracking_candidates = []
            refresh_errors = []
            refresh_missing_fields = []
            selected_tokens = refresh_plan["token_addresses"]
            if selected_tokens:
                refresh_adapter = refresh_adapters.get(chain)
                if refresh_adapter is None:
                    refresh_errors.append("tracking_adapter_unavailable")
                else:
                    try:
                        refresh_batch = refresh_adapter.fetch_tokens(
                            selected_tokens, observed_at
                        )
                        refresh_errors.extend(refresh_batch.errors)
                        refresh_missing_fields.extend(refresh_batch.missing_fields)
                        selected_set = set(selected_tokens)
                        tracking_candidates = deduplicate_candidates(
                            [
                                replace(
                                    item,
                                    discovery_type="refresh",
                                    candidate_scope="known_token_refresh",
                                    observation_phase="tracking",
                                )
                                for item in refresh_batch.candidates
                                if item.chain.strip().lower()
                                == chain.strip().lower()
                                and canonical_address(chain, item.token_address)
                                in selected_set
                            ],
                            observed_at,
                        )
                    except Exception as exc:
                        refresh_errors.append(
                            "tracking_fetch_error:%s:%s"
                            % (exc.__class__.__name__, str(exc))
                        )
            returned_tokens = {
                canonical_address(chain, item.token_address)
                for item in tracking_candidates
            }
            usable_tokens = {
                canonical_address(chain, item.token_address)
                for item in tracking_candidates
                if item.valuation_kind in ("market_cap", "fdv")
                and item.valuation_usd is not None
            }
            for token in sorted(returned_tokens - usable_tokens):
                refresh_errors.append("refresh_no_usable_valuation:%s" % token)
            refresh_metrics["returned"] = len(returned_tokens)
            refresh_metrics["usable"] = len(usable_tokens)
            # Backward-compatible field now means a usable valuation refresh,
            # never merely a returned pool row.
            refresh_metrics["refreshed"] = len(usable_tokens)
            refresh_metrics["errors"] = max(
                0, refresh_plan["selected"] - len(usable_tokens)
            )
            refresh_metrics["complete"] = (
                refresh_metrics["deferred"] == 0
                and refresh_metrics["errors"] == 0
                and not refresh_errors
                and not refresh_missing_fields
            )
            if selected_tokens:
                store.mark_refresh_attempts(
                    chain,
                    selected_tokens,
                    observed_at,
                    usable_token_addresses=usable_tokens,
                )
            if refresh_metrics["eligible_due"] == 0:
                tracking_status = "not_due"
            elif (
                refresh_metrics["selected"] > 0
                and refresh_metrics["refreshed"] == 0
                and refresh_metrics["errors"] >= refresh_metrics["selected"]
            ):
                tracking_status = "error"
            elif refresh_metrics["complete"] and not refresh_missing_fields:
                tracking_status = "success"
            else:
                tracking_status = "degraded"
            effective_status = batch.status
            if tracking_status in ("degraded", "error"):
                effective_status = "degraded"
            combined = deduplicate_candidates(
                deduplicated + tracking_candidates, observed_at
            )
            pool_rows_seen = (
                batch.pool_rows_seen
                if batch.pool_rows_seen > 0 or not candidates
                else len(candidates)
            )
            ingest = store.ingest_chain(
                run_id=run_id,
                chain=chain,
                observed_at=observed_at,
                candidates=combined,
                status=effective_status,
                errors=list(batch.errors) + refresh_errors,
                pool_rows_seen=pool_rows_seen,
                discovery_type=batch.discovery_type,
                candidate_scope=batch.candidate_scope,
                page_count=batch.page_count,
                oldest_pool_created_at=batch.oldest_pool_created_at,
                newest_pool_created_at=batch.newest_pool_created_at,
                coverage_scope=batch.coverage_scope,
                gap_reason=batch.gap_reason,
                missing_fields=list(batch.missing_fields)
                + refresh_missing_fields,
                discovered_tokens=len(deduplicated),
                discovery_status=batch.status,
                tracking_status=tracking_status,
                admission=admission,
                refresh=refresh_metrics,
                pool_events=batch.pool_events,
                block_source=cursor_source,
                cursor_before=cursor_before,
                next_cursor=batch.next_cursor,
                block_from=batch.block_from,
                block_to=batch.block_to,
                latest_indexed_block=batch.latest_indexed_block,
                enrichment_complete=batch.enrichment_complete,
                pending_token_addresses=batch.pending_token_addresses,
                enrichment_attempted_token_addresses=(
                    batch.enrichment_attempted_token_addresses
                ),
                rewind_from_block=batch.rewind_from_block,
                block_scan_status=batch.raw_scan_status,
                block_scan_errors=batch.raw_scan_errors,
            )
            result["chains"][chain] = {
                "status": ingest["status"],
                "discovery_status": batch.status,
                "tracking_status": tracking_status,
                "pool_rows_seen": pool_rows_seen,
                "discovered_tokens": len(deduplicated),
                "observations_written": ingest["observations_written"],
                "discovery_observations_written": ingest[
                    "discovery_observations_written"
                ],
                "tracking_observations_written": ingest[
                    "tracking_observations_written"
                ],
                "coverage_scope": batch.coverage_scope,
                "admission_eligible": admission["eligible"],
                "admission_admitted": admission["admitted"],
                "admission_dropped": admission["dropped"],
                "refresh_eligible_due": refresh_metrics["eligible_due"],
                "refresh_selected": refresh_metrics["selected"],
                "refresh_returned": refresh_metrics["returned"],
                "refresh_usable": refresh_metrics["usable"],
                "refresh_refreshed": refresh_metrics["refreshed"],
                "refresh_deferred": refresh_metrics["deferred"],
                "refresh_errors": refresh_metrics["errors"],
                "refresh_scope": refresh_metrics["scope"],
                "block_from": batch.block_from,
                "block_to": batch.block_to,
                "latest_indexed_block": batch.latest_indexed_block,
                "pool_events_seen": len(batch.pool_events),
                "pool_events_inserted": ingest["pool_events_inserted"],
                "pending_event_enrichment": len(batch.pending_token_addresses),
                "errors": ingest["errors"],
            }
        except Exception as exc:  # one source/network cannot abort other networks
            message = "%s: %s" % (exc.__class__.__name__, str(exc))
            store.record_coverage_error(run_id, chain, observed_at, message)
            if cursor_source:
                store.record_block_coverage_error(
                    run_id,
                    chain,
                    cursor_source,
                    observed_at,
                    cursor_before,
                    message,
                )
            result["chains"][chain] = {
                "status": "error",
                "discovery_status": "error",
                "tracking_status": "not_run_source_error",
                "pool_rows_seen": 0,
                "discovered_tokens": 0,
                "observations_written": 0,
                "discovery_observations_written": 0,
                "tracking_observations_written": 0,
                "coverage_scope": "error",
                "admission_eligible": 0,
                "admission_admitted": 0,
                "admission_dropped": 0,
                "refresh_eligible_due": 0,
                "refresh_selected": 0,
                "refresh_returned": 0,
                "refresh_usable": 0,
                "refresh_refreshed": 0,
                "refresh_deferred": 0,
                "refresh_errors": 0,
                "refresh_scope": "tracking_not_run_source_error",
                "errors": [message],
            }

    store.write_summary(store.state_dir / "latest_summary.json")
    return result
