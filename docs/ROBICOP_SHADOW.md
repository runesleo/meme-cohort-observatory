# Robicop shadow-only evaluation

## Purpose

MCO remains a coverage-aware cohort/lifecycle research tool, not a risk scanner or trade gate. This layer exists only to measure whether an external Robinhood Chain wallet-intelligence signal would have added useful adverse evidence to already-recorded cases.

Robicop's public contract is read-only and disconfirming: `check_coin` returns `clear`, `caution`, or `avoid` with booked-wallet evidence, while its own documentation explicitly says that an empty record is **not** clearance. Source: `https://robicop.com/`.

## Hard invariants

The shadow layer cannot:

- change cohort admission or exclusion;
- change lifecycle observations or threshold crossings;
- promote a token or create a BUY/WATCH label;
- authorize, size, sign, or place a trade;
- convert `clear` or an empty record into `safe`;
- convert source failure into `clear`.

Normalized semantics are therefore deliberately asymmetric:

| Robicop result | MCO shadow signal | Canonical effect |
|---|---|---|
| `avoid` | `ADVERSE_EVIDENCE` | none |
| `caution` | `CAUTION_EVIDENCE` | none |
| `clear` with no booked wallets | `NO_ADVERSE_RECORD` | none |
| unavailable / malformed / contradictory | `UNKNOWN` | none |

A contradictory payload such as `clear` plus booked-wallet evidence fails closed as `SOURCE_INCONSISTENT`.

## Golden-case replay

`tests/fixtures/robicop_shadow_golden.json` contains synthetic semantic fixtures. The addresses are intentionally fixtures; they are not allegations about real wallets or tokens.

Run:

```bash
python scripts/replay_robicop_shadow.py
```

The replay compares a captured shadow observation to a later canonical outcome. It measures agreement/disagreement classes without rewriting history. The key regression case is `clear_then_failed_proves_no_clearance`: a later failure after a `clear`/empty Robicop record must remain `NO_RECORD_DID_NOT_CLEAR`, never a false claim that the token had been safe.

## Live integration boundary

This PR does not add an MCP client, background poller, scheduler, or production dependency. A future runtime may capture the read-only `check_coin` response from `https://robicop.com/mcp` and feed that JSON into `normalize_robicop_check_coin`; the resulting shadow record must remain outside canonical cohort state until separate evidence demonstrates a reason to change that architecture.
