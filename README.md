# Meme Cohort Observatory

Track newly observable token pools from first sighting to 7-day outcomes, with reproducible cohort sampling and explicit coverage limits.

```text
chain      status    coverage          complete  seen  admit  drop  mcap>=1m
---------  --------  ----------------  --------  ----  -----  ----  --------
base       success   fixture_snapshot  no        1     0      0     0
bsc        success   fixture_snapshot  no        1     0      0     0
robinhood  degraded  promoted_subset   no        1     0      0     0
solana     success   fixture_snapshot  no        1     0      0     0
```

The important result above is not a token score. It is that the fixture run is **not a comparable cohort frame**, so the tool refuses to turn four observations into a success-rate claim.

## What it does

- Discovers public new-pool observations on Solana, BSC, Base, and Robinhood Chain.
- Keeps market-cap and FDV telemetry on separate tracks.
- Records watermark, pagination, request-budget, reorg, and enrichment provenance.
- Samples bounded cohorts with an explicit inclusion probability.
- Follows admitted tokens on an admission-anchored 10-minute to 7-day schedule.
- Reports observed `$100K → $1M/$3M/$10M` crossings without inventing intraperiod ATHs.
- Fails closed when coverage is not a valid denominator.

It does **not** connect a wallet, sign, swap, place orders, classify every token as a meme, or turn discovery into a buy signal.

## 60-second offline demo

Python 3.10+ is the only runtime requirement.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'

mco collect       --fixtures tests/fixtures       --state-dir /tmp/mco-demo       --observed-at 2026-07-14T02:00:00Z

mco report --state-dir /tmp/mco-demo --format markdown
mco export --state-dir /tmp/mco-demo --format jsonl | head
mco doctor --state-dir /tmp/mco-demo --json
```

Without installing, use:

```bash
PYTHONPATH=src python -m meme_cohort_observatory collect       --fixtures tests/fixtures       --state-dir /tmp/mco-demo       --observed-at 2026-07-14T02:00:00Z
```

## Live public collection

```bash
mco collect --network solana --network bsc --state-dir ./state
```

Public collection is GET-only and bounded by per-request and whole-run budgets. A source failure degrades only the affected chain and is retained in coverage evidence.

## Outputs

- `cohorts.sqlite3` — tokens, observations, cohorts, valuation tracks, crossings, source coverage, pool events, and cursors.
- `latest_summary.json` — atomic low-noise state for reports or downstream tools.
- `mco report` — deterministic human-readable coverage and outcome summary.
- `mco export` — reproducible JSON or JSONL rows.

## Research boundaries

A pool event proves only that a matching public event was observed. It does not prove safe liquidity, token origin, organic demand, executability, or future return. Provider-reported values are telemetry, not independently verified circulating-supply calculations.

See [docs/EXTRACTION.md](docs/EXTRACTION.md) for provenance and the public/private cut.
