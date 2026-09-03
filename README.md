# Meme Cohort Observatory

[中文](README.zh.md)

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Alpha-orange)

**Preserve the cohort denominator when public DEX data becomes incomplete.**

Meme Cohort Observatory (MCO) is a coverage-aware, replayable research CLI for newly observable token pools. It records how a cohort was discovered, which tokens were admitted, what follow-up observations were missed, and which lifecycle claims the evidence can actually support.

It is not a meme alpha scanner, risk score, buy signal, wallet, or trading agent.

```text
chain      status    coverage          complete  seen  admit  drop  mcap_C1evt  fdv_C1evt
---------  --------  ----------------  --------  ----  -----  ----  ----------  ---------
base       success   fixture_snapshot  no        1     0      0     0           0
bsc        success   fixture_snapshot  no        1     0      0     0           0
robinhood  degraded  promoted_subset   no        1     0      0     0           0
solana     success   fixture_snapshot  no        1     0      0     0           0
```

The useful result above is the refusal: this fixture is not a comparable discovery frame, so MCO does not turn four observations into a success-rate claim.

## What you get

- Public new-pool collection for Solana, BSC, Base, and Robinhood Chain.
- Explicit watermark, pagination, request-budget, reorg, and enrichment provenance.
- Bounded cohort admission with recorded inclusion probability and sample fraction.
- Admission-anchored follow-up from 10 minutes to 7 days.
- Separate market-cap and FDV tracks.
- Separate `ever observed >= threshold` counts and explicit same-track below-to-above transitions.
- Missing-refresh accounting instead of silently treating missing data as failure or survival.
- Deterministic SQLite state, JSON/JSONL export, human-readable reports, and per-token inspection.
- Offline fixtures and replayable tests.

## How it works

```text
public pool sources
        │
        ▼
coverage-aware discovery ──► watermark / pagination / reorg evidence
        │
        ▼
comparable-frame gate ─────► incomplete frames cannot admit a cohort
        │
        ▼
bounded cohort admission ──► inclusion probability + sample fraction
        │
        ▼
anchored follow-up ─────────► 10m / 1h / 1d / 7d observations + lag
        │
        ▼
lifecycle report ───────────► denominator, missing slots, thresholds, limits
```

A current snapshot answers “what is readable now.” MCO answers “what population did we start with, how complete was discovery, and where did follow-up evidence disappear?”

## Setup

```bash
git clone https://github.com/runesleo/meme-cohort-observatory.git
cd meme-cohort-observatory
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

MCO is not published to PyPI in v0.1.0. Install it from the repository.

## Requirements and privacy

- Python 3.10 or newer.
- No account, API key, browser cookie, wallet, signature, or private RPC is required for the included paths.
- Live collection sends GET-only or read-only RPC requests to configured public data sources.
- Local state may contain public token addresses, pool addresses, valuations, and timestamps. Keep the state directory private if you add your own labels or research notes.
- The package has no wallet or trade execution surface.

## Quick start

### Offline, deterministic demo

```bash
mco collect \
  --fixtures tests/fixtures \
  --state-dir /tmp/mco-demo \
  --observed-at 2026-07-14T02:00:00Z

mco report --state-dir /tmp/mco-demo --format markdown
mco export --state-dir /tmp/mco-demo --format jsonl | head
mco doctor --state-dir /tmp/mco-demo --json
```

Without installation:

```bash
PYTHONPATH=src python -m meme_cohort_observatory collect \
  --fixtures tests/fixtures \
  --state-dir /tmp/mco-demo \
  --observed-at 2026-07-14T02:00:00Z
```

### Live public collection

```bash
mco collect --network solana --network bsc --state-dir ./state
mco report --state-dir ./state --format table
```

First-time RPC bootstrap can take tens of seconds. Public requests are bounded by per-request and whole-run budgets. A source failure degrades only the affected chain and remains visible in coverage evidence.

### Inspect one recorded token

```bash
mco inspect-token bsc 0x... --state-dir ./state --format markdown
```

The command reads local state only. It does not query a wallet or place a trade.

## Outputs

- `cohorts.sqlite3` — tokens, observations, cohorts, valuation tracks, threshold events, source coverage, pool events, and cursors.
- `latest_summary.json` — atomic low-noise state for reports or downstream tools.
- `mco report` — coverage plus comparable-cohort lifecycle analytics.
- `mco export` — deterministic JSON or JSONL rows.
- `mco inspect-token` — one token's observations, checkpoints, valuation tracks, transitions, and missing-refresh context.

## Verified

The v0.1.0 release candidate was verified locally with:

- 108 automated tests.
- A public-surface audit with zero private absolute paths, embedded secrets, wallet/trade imports, or wallet/trade calls.
- A clean wheel build and installation in a fresh virtual environment.
- Successful `collect`, `report`, `export`, `inspect-token`, and `doctor` command discovery and fixture execution.
- Live keyless read-only smoke tests on BSC and Base; a Solana HTTP 429 correctly produced degraded coverage and admitted no comparable cohort.

One fixed forward BSC cohort illustrates the product job. The first five tokens were admitted chronologically before a later comparison. At the later current-snapshot read, all five addresses returned a pair row, only one had a positive-liquidity pair, and none exposed a usable current valuation. MCO still retained the original five admissions and ten missed anchored refresh slots. This is evidence of provenance and missingness handling, not a profitability claim.

See [Stage A](docs/STAGE_A.md), [Stage B](docs/STAGE_B.md), and [the competitiveness review](docs/COMPETITIVENESS.md) for reproducible acceptance details.

## Known limitations (v0.1.0)

- MCO does not classify every discovered token as a meme.
- Public-source coverage varies by chain and provider; custom AMMs, private bonding curves, and unindexed pools can remain outside the frame.
- Checkpoints use the first recorded observation at or after the target age and expose lag; they are observability measurements, not exact-time survival estimates.
- Thresholds are observed provider values, not reconstructed intraperiod ATHs.
- Market-cap and FDV values depend on provider methodology and are not independently reconstructed circulating-supply estimates.
- The current sample reports inclusion probabilities and missingness but does not claim a formal confidence interval.
- No dashboard, hosted API, MCP server, PyPI package, wallet, or trading integration is included.

## Roadmap

Near term:

- Improve 1-day and 7-day follow-up coverage without hiding missed observations.
- Add stronger uncertainty summaries for sampled cohorts.
- Publish more fixed, non-cherry-picked cohort case studies.

Only with demonstrated user demand:

- Add another public-source adapter.
- Extract a narrower reusable Python library interface.
- Add an agent-facing wrapper without adding execution authority.

## Research boundaries

A pool event proves only that a matching public event was observed. It does not prove safe liquidity, token origin, organic demand, executability, or future return. Discovery is not a recommendation to buy.

See [docs/EXTRACTION.md](docs/EXTRACTION.md) for provenance and the public/private cut.

## About the author

*Leo ([@runes_leo](https://x.com/runes_leo)) — AI × Crypto independent builder. Trading on [Polymarket](https://polymarket.com/?via=runes-leo&r=runesleo&utm_source=github&utm_content=meme-cohort-observatory), building data and content pipelines with Claude Code and Codex.*

*[leolabs.me](https://leolabs.me) — writing · community · open-source tools · indie projects · all platforms.*

*[X Subscription](https://x.com/runes_leo/creator-subscriptions/subscribe) — paid content weekly, or just buy me a coffee 😁*

*Learn in public, Build in public.*

## License

MIT. See [LICENSE](LICENSE).
