import json
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from meme_cohort_observatory.robicop_shadow import (  # noqa: E402
    compare_shadow_to_canonical,
    normalize_robicop_check_coin,
    replay_golden_cases,
)


TOKEN = "0x1111111111111111111111111111111111111111"
WALLET = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
PROOF = "0x" + ("ab" * 32)


class RobicopNormalizationTests(unittest.TestCase):
    def test_avoid_preserves_adverse_evidence_without_authority(self):
        observation = normalize_robicop_check_coin(
            {
                "verdict": "avoid",
                "reason": "booked wallet present",
                "booked_wallets": [
                    {
                        "address": WALLET,
                        "classification": "rugger",
                        "coins": 4,
                        "proof": PROOF,
                    }
                ],
            },
            chain="robinhood",
            token_address=TOKEN,
            observed_at="2026-09-09T00:00:00Z",
        )
        payload = observation.to_dict()
        self.assertEqual("PASS", observation.source_status)
        self.assertEqual("avoid", observation.raw_verdict)
        self.assertEqual("ADVERSE_EVIDENCE", observation.shadow_signal)
        self.assertEqual(WALLET.lower(), observation.booked_wallets[0].address)
        self.assertEqual("rugger", observation.booked_wallets[0].classification)
        self.assertEqual(4, observation.booked_wallets[0].coin_count)
        self.assertFalse(payload["semantics"]["canonical_mutation"])
        self.assertFalse(payload["semantics"]["trade_authorization"])
        self.assertFalse(payload["semantics"]["buy_signal"])

    def test_clear_is_no_adverse_record_never_safe(self):
        observation = normalize_robicop_check_coin(
            {"verdict": "clear", "booked_wallets": []},
            chain="robinhood",
            token_address=TOKEN,
        )
        payload = observation.to_dict()
        self.assertEqual("NO_ADVERSE_RECORD", observation.shadow_signal)
        self.assertFalse(payload["semantics"]["absence_is_clearance"])
        self.assertEqual("none", payload["semantics"]["promotion_effect"])

    def test_unavailable_is_unknown_not_clear(self):
        observation = normalize_robicop_check_coin(
            None,
            chain="robinhood",
            token_address=TOKEN,
            source_error="timeout",
        )
        self.assertEqual("SOURCE_UNAVAILABLE", observation.source_status)
        self.assertEqual("UNKNOWN", observation.shadow_signal)
        self.assertIsNone(observation.raw_verdict)

    def test_clear_with_booked_wallet_fails_closed_as_inconsistent(self):
        observation = normalize_robicop_check_coin(
            {
                "verdict": "clear",
                "booked_wallets": [{"address": WALLET, "classification": "rugger"}],
            },
            chain="robinhood",
            token_address=TOKEN,
        )
        self.assertEqual("SOURCE_INCONSISTENT", observation.source_status)
        self.assertEqual("UNKNOWN", observation.shadow_signal)
        self.assertIn("conflicts", observation.source_error)

    def test_unknown_verdict_fails_closed(self):
        observation = normalize_robicop_check_coin(
            {"verdict": "safe"},
            chain="robinhood",
            token_address=TOKEN,
        )
        self.assertEqual("SOURCE_INCONSISTENT", observation.source_status)
        self.assertEqual("UNKNOWN", observation.shadow_signal)


class RobicopReplayTests(unittest.TestCase):
    def test_no_record_then_failure_encodes_no_clearance(self):
        observation = normalize_robicop_check_coin(
            {"verdict": "clear", "booked_wallets": []},
            chain="robinhood",
            token_address=TOKEN,
        )
        replay = compare_shadow_to_canonical(
            observation,
            canonical_outcome="failed",
            case_id="no-clearance",
        )
        self.assertEqual("NO_RECORD_DID_NOT_CLEAR", replay["replay_class"])
        self.assertFalse(replay["would_mutate_canonical"])
        self.assertFalse(replay["would_promote"])
        self.assertFalse(replay["would_authorize_trade"])
        self.assertFalse(replay["absence_is_clearance"])

    def test_adverse_then_failure_is_supported_but_still_shadow_only(self):
        observation = normalize_robicop_check_coin(
            {
                "verdict": "avoid",
                "booked_wallets": [{"address": WALLET, "proof_tx": PROOF}],
            },
            chain="robinhood",
            token_address=TOKEN,
        )
        replay = compare_shadow_to_canonical(observation, canonical_outcome="rugged")
        self.assertEqual("SUPPORTED_BY_LATER_OUTCOME", replay["replay_class"])
        self.assertFalse(replay["would_mutate_canonical"])

    def test_golden_fixture_replays_all_semantic_cases(self):
        fixture_path = PROJECT_ROOT / "tests" / "fixtures" / "robicop_shadow_golden.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        report = replay_golden_cases(fixture["cases"])

        self.assertEqual("mco_robicop_shadow_replay.v1", report["schema"])
        self.assertEqual("shadow_only", report["mode"])
        self.assertEqual(7, report["case_count"])
        self.assertEqual(0, report["invariants"]["canonical_mutations"])
        self.assertEqual(0, report["invariants"]["promotions"])
        self.assertEqual(0, report["invariants"]["trade_authorizations"])
        self.assertFalse(report["invariants"]["clear_verdict_means_safe"])

        by_id = {row["case_id"]: row for row in report["cases"]}
        self.assertEqual(
            "SUPPORTED_BY_LATER_OUTCOME",
            by_id["avoid_then_failed"]["replay_class"],
        )
        self.assertEqual(
            "ADVERSE_SIGNAL_NOT_CONFIRMED",
            by_id["avoid_then_active"]["replay_class"],
        )
        self.assertEqual(
            "CAUTION_SUPPORTED_BY_LATER_OUTCOME",
            by_id["caution_then_deteriorated"]["replay_class"],
        )
        self.assertEqual(
            "NO_RECORD_DID_NOT_CLEAR",
            by_id["clear_then_failed_proves_no_clearance"]["replay_class"],
        )
        self.assertEqual(
            "NON_DISCONFIRMING_ONLY",
            by_id["clear_then_active_non_disconfirming"]["replay_class"],
        )
        self.assertEqual("NOT_EVALUABLE", by_id["source_unavailable"]["replay_class"])
        self.assertEqual(
            "NOT_EVALUABLE",
            by_id["clear_with_wallet_is_inconsistent"]["replay_class"],
        )


if __name__ == "__main__":
    unittest.main()
