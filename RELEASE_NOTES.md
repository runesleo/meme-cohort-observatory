# Meme Cohort Observatory v0.1.0

The first public release of a coverage-aware, replayable cohort research CLI for newly observable token pools.

## What ships

- Public new-pool collection for Solana, BSC, Base, and Robinhood Chain.
- Comparable-frame gating and bounded cohort admission.
- Admission-anchored 10m / 1h / 1d / 7d follow-up.
- Separate market-cap and FDV lifecycle tracks.
- Distinct ever-observed threshold counts and explicit below-to-above transition events.
- Missing-refresh accounting, request-budget evidence, pagination/reorg provenance, SQLite state, JSON/JSONL export, and per-token inspection.
- Five CLI commands: `collect`, `report`, `export`, `inspect-token`, and `doctor`.

## Verified

- 108 automated tests pass.
- Public-surface audit passes.
- A wheel builds and installs in a clean virtual environment.
- The offline fixture path completes collection, report generation, export, and doctor checks.
- Live keyless read-only smoke tests succeeded on BSC and Base; a Solana rate-limit event degraded honestly instead of admitting an incomplete cohort.

## Privacy and safety

MCO uses public read-only sources and local state. It requires no account, API key, browser cookie, wallet, signature, or trading permission. The package contains no wallet or trade execution surface.

Local state may contain public token and pool addresses. Do not mix private labels or research notes into a state directory you intend to share.

## Important limits

MCO is research infrastructure, not a meme alpha scanner, token risk score, or buy signal. It does not reconstruct intraperiod ATHs, guarantee complete chain coverage, or claim exact-time survival estimates. See the README and `docs/COMPETITIVENESS.md` for the narrow product claim.

## Breaking changes

None. This is the initial public release.
