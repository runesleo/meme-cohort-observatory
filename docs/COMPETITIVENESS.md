# Competitiveness verdict

## Verdict: `PUBLIC_CANDIDATE`

MCO is a public candidate **only with a narrow positioning**:

> A coverage-aware, replayable cohort research CLI for newly observable token pools. It helps researchers answer whether a lifecycle statistic has a trustworthy denominator — not which meme coin to buy.

It should **not** ship as another meme scanner, AI score, risk dashboard, alert bot, survival dashboard, or MCP wrapper. Those surfaces are already crowded and several public tools are stronger there.

## Why this survives the cut

### 1. Raw discovery is commodity; denominator integrity is not

DexScreener and GeckoTerminal already expose rich current/new-pool data. A wrapper around those APIs is not a product. MCO's retained value starts after ingestion: comparable-frame admission, inclusion probability, explicit coverage gaps, anchored follow-up and missed-slot accounting.

### 2. Survival analytics is also commodity

Dune has Pump.fun launch/graduation/survival work, and CoinGecko analyzed 18.67M Pump.fun tokens. MCO cannot win on dataset size or “X% survive N days.”

### 3. Fixed forward case proves a different job

The first five chronological BSC cohort members were fixed before the comparison. At the later current-snapshot benchmark, DexScreener still returned one pair row for all five, but only one token had a positive-liquidity pair and none exposed a usable current valuation. A current-snapshot workflow therefore cannot reconstruct the original cohort result from that read alone. MCO retained all five admissions, original values and ten missed anchored refresh slots.

This proves **provenance/missingness value**, not predictive alpha.

### 4. Historical state proves the semantics matter at scale

The existing public-data state contains 698 comparable cohort members. On Base, MCAP “ever observed >= $1M” is 6/429 tracked, while only 2 objects have an explicit same-track below-to-above $1M transition. Treating those as the same statistic would be false precision. The state also contains 2,518 Base missed refresh slots, so lifecycle results must carry observation quality.

## Where MCO loses

- Robinhood Memescan and other scanners have better dashboard/AI/social/holder/report UX.
- Risk scanners have contract/holder/honeypot enrichment MCO intentionally lacks.
- Dune/CoinGecko have much larger datasets and easier charts.
- MCO currently has weak 7d coverage and no external-user proof.

## Public v0.1 positioning

Homepage sentence:

> Track new token-pool cohorts across chains without silently losing the denominator when APIs rate-limit, pools disappear, valuations switch between market cap and FDV, or follow-up observations are missed.

Three demos only:

1. `mco collect` — bounded public collection with coverage status.
2. `mco report` — comparable cohort + lifecycle + missingness.
3. `mco inspect-token` — replay one token's recorded path.

No scoring. No BUY/WATCH labels. No wallet. No Telegram. No Agent wrapper in v0.1.

## Release gate

`PUBLIC_CANDIDATE` does **not** mean public release is already approved. Before user confirmation, local prep may continue only for:

- license/provenance audit;
- README/demo cleanup;
- package metadata;
- release checklist;
- GitHub description/topics draft;
- one privacy-safe case-study output.

Creating a public GitHub repo, adding a public remote, pushing, releasing or posting remains an explicit user gate.
