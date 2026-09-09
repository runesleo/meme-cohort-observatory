"""Robicop shadow-only evidence normalization and replay helpers.

This module is deliberately non-authoritative. It can preserve adverse wallet
intelligence beside MCO's canonical cohort/lifecycle state, but it cannot admit,
exclude, promote, rank, or otherwise mutate a token. Robicop itself documents
that absence of a record is not clearance; this module makes that constraint an
explicit invariant.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .cohort import canonical_address

ROBICOP_VERDICTS = frozenset(("clear", "caution", "avoid"))
BAD_CANONICAL_OUTCOMES = frozenset(("failed", "deteriorated", "rugged", "dead"))
GOOD_CANONICAL_OUTCOMES = frozenset(("survived", "improved", "active"))


def _utc_iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        parsed = value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class RobicopBookedWallet:
    address: str
    classification: str | None = None
    coin_count: int | None = None
    proof_tx: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "classification": self.classification,
            "coin_count": self.coin_count,
            "proof_tx": self.proof_tx,
        }


@dataclass(frozen=True)
class RobicopShadowObservation:
    chain: str
    token_address: str
    observed_at: str | None
    source_status: str
    raw_verdict: str | None
    shadow_signal: str
    booked_wallets: tuple[RobicopBookedWallet, ...]
    reason: str | None = None
    note: str | None = None
    source_error: str | None = None
    source: str = "robicop_check_coin"

    @property
    def booked_wallet_count(self) -> int:
        return len(self.booked_wallets)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "mco_robicop_shadow_observation.v1",
            "mode": "shadow_only",
            "source": self.source,
            "source_status": self.source_status,
            "chain": self.chain,
            "token_address": self.token_address,
            "observed_at": self.observed_at,
            "raw_verdict": self.raw_verdict,
            "shadow_signal": self.shadow_signal,
            "booked_wallet_count": self.booked_wallet_count,
            "booked_wallets": [wallet.to_dict() for wallet in self.booked_wallets],
            "reason": self.reason,
            "note": self.note,
            "source_error": self.source_error,
            "semantics": {
                "canonical_mutation": False,
                "admission_effect": "none",
                "promotion_effect": "none",
                "trade_authorization": False,
                "absence_is_clearance": False,
                "buy_signal": False,
            },
        }


def _normalize_wallet(chain: str, raw: Any) -> RobicopBookedWallet | None:
    row = _mapping(raw)
    address = _string(_first(row, "address", "wallet", "wallet_address", "actor"))
    if not address:
        return None
    normalized = canonical_address(chain, address)
    return RobicopBookedWallet(
        address=normalized,
        classification=_string(_first(row, "classification", "class", "tag", "offence", "behavior")),
        coin_count=_int(_first(row, "coin_count", "coins", "coinCount", "count")),
        proof_tx=_string(_first(row, "proof_tx", "proof", "transaction", "tx", "tx_hash")),
    )


def normalize_robicop_check_coin(
    payload: Mapping[str, Any] | None,
    *,
    chain: str,
    token_address: str,
    observed_at: datetime | str | None = None,
    source_error: str | None = None,
) -> RobicopShadowObservation:
    """Normalize a Robicop ``check_coin`` result without granting it authority.

    ``payload=None`` or an explicit ``source_error`` is SOURCE_UNAVAILABLE. A
    malformed/internally contradictory payload is SOURCE_INCONSISTENT. The
    ``clear`` verdict maps only to NO_ADVERSE_RECORD; it never becomes SAFE.
    """

    normalized_chain = chain.strip().lower()
    normalized_token = canonical_address(normalized_chain, token_address)
    observed = _utc_iso(observed_at)

    if payload is None or source_error:
        return RobicopShadowObservation(
            chain=normalized_chain,
            token_address=normalized_token,
            observed_at=observed,
            source_status="SOURCE_UNAVAILABLE",
            raw_verdict=None,
            shadow_signal="UNKNOWN",
            booked_wallets=(),
            source_error=_string(source_error) or "robicop response unavailable",
        )

    row = _mapping(payload)
    raw_verdict = (_string(_first(row, "verdict", "status", "risk")) or "").lower()
    wallets_raw = _first(row, "booked_wallets", "wallets", "bookings", "actors")
    wallets = tuple(
        wallet
        for wallet in (_normalize_wallet(normalized_chain, item) for item in _sequence(wallets_raw))
        if wallet is not None
    )
    reason = _string(_first(row, "reason", "summary", "message"))
    note = _string(_first(row, "note", "disclaimer", "coverage_note"))

    if raw_verdict not in ROBICOP_VERDICTS:
        return RobicopShadowObservation(
            chain=normalized_chain,
            token_address=normalized_token,
            observed_at=observed,
            source_status="SOURCE_INCONSISTENT",
            raw_verdict=raw_verdict or None,
            shadow_signal="UNKNOWN",
            booked_wallets=wallets,
            reason=reason,
            note=note,
            source_error="unsupported or missing Robicop verdict",
        )

    if raw_verdict == "clear" and wallets:
        return RobicopShadowObservation(
            chain=normalized_chain,
            token_address=normalized_token,
            observed_at=observed,
            source_status="SOURCE_INCONSISTENT",
            raw_verdict=raw_verdict,
            shadow_signal="UNKNOWN",
            booked_wallets=wallets,
            reason=reason,
            note=note,
            source_error="clear verdict conflicts with booked-wallet evidence",
        )

    signal = {
        "avoid": "ADVERSE_EVIDENCE",
        "caution": "CAUTION_EVIDENCE",
        "clear": "NO_ADVERSE_RECORD",
    }[raw_verdict]
    return RobicopShadowObservation(
        chain=normalized_chain,
        token_address=normalized_token,
        observed_at=observed,
        source_status="PASS",
        raw_verdict=raw_verdict,
        shadow_signal=signal,
        booked_wallets=wallets,
        reason=reason,
        note=note,
    )


def compare_shadow_to_canonical(
    observation: RobicopShadowObservation,
    *,
    canonical_outcome: str,
    case_id: str | None = None,
) -> dict[str, Any]:
    """Compare shadow evidence to a later canonical outcome for evaluation only."""

    outcome = canonical_outcome.strip().lower()
    signal = observation.shadow_signal
    if observation.source_status != "PASS":
        replay_class = "NOT_EVALUABLE"
    elif signal == "ADVERSE_EVIDENCE" and outcome in BAD_CANONICAL_OUTCOMES:
        replay_class = "SUPPORTED_BY_LATER_OUTCOME"
    elif signal == "ADVERSE_EVIDENCE" and outcome in GOOD_CANONICAL_OUTCOMES:
        replay_class = "ADVERSE_SIGNAL_NOT_CONFIRMED"
    elif signal == "CAUTION_EVIDENCE" and outcome in BAD_CANONICAL_OUTCOMES:
        replay_class = "CAUTION_SUPPORTED_BY_LATER_OUTCOME"
    elif signal == "CAUTION_EVIDENCE" and outcome in GOOD_CANONICAL_OUTCOMES:
        replay_class = "CAUTION_NOT_CONFIRMED"
    elif signal == "NO_ADVERSE_RECORD" and outcome in BAD_CANONICAL_OUTCOMES:
        replay_class = "NO_RECORD_DID_NOT_CLEAR"
    elif signal == "NO_ADVERSE_RECORD" and outcome in GOOD_CANONICAL_OUTCOMES:
        replay_class = "NON_DISCONFIRMING_ONLY"
    else:
        replay_class = "OUTCOME_UNCLASSIFIED"

    return {
        "schema": "mco_robicop_shadow_replay_case.v1",
        "case_id": case_id,
        "chain": observation.chain,
        "token_address": observation.token_address,
        "shadow_signal": signal,
        "source_status": observation.source_status,
        "raw_verdict": observation.raw_verdict,
        "canonical_outcome": outcome,
        "replay_class": replay_class,
        "would_mutate_canonical": False,
        "would_promote": False,
        "would_authorize_trade": False,
        "absence_is_clearance": False,
        "booked_wallet_count": observation.booked_wallet_count,
    }


def replay_golden_cases(cases: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Replay deterministic semantic fixtures and summarize comparison classes."""

    results = []
    for raw in cases:
        case = _mapping(raw)
        observation = normalize_robicop_check_coin(
            _mapping(case.get("robicop")) if case.get("robicop") is not None else None,
            chain=_string(case.get("chain")) or "robinhood",
            token_address=_string(case.get("token_address")) or "",
            observed_at=_string(case.get("observed_at")),
            source_error=_string(case.get("source_error")),
        )
        results.append(
            compare_shadow_to_canonical(
                observation,
                canonical_outcome=_string(case.get("canonical_outcome")) or "unknown",
                case_id=_string(case.get("case_id")),
            )
        )

    counts: dict[str, int] = {}
    for item in results:
        counts[item["replay_class"]] = counts.get(item["replay_class"], 0) + 1

    return {
        "schema": "mco_robicop_shadow_replay.v1",
        "mode": "shadow_only",
        "case_count": len(results),
        "class_counts": dict(sorted(counts.items())),
        "cases": results,
        "invariants": {
            "canonical_mutations": 0,
            "promotions": 0,
            "trade_authorizations": 0,
            "clear_verdict_means_safe": False,
        },
    }
