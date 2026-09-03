# Local release preparation

Status: `PUBLIC_CANDIDATE / NOT PUBLISHED`

This file is a gate checklist. Completing it locally does not authorize creating a public repository or pushing code.

## Positioning

**Meme Cohort Observatory** is a coverage-aware, replayable cohort research CLI for newly observable token pools. Its job is to preserve cohort denominators and observation provenance through API gaps, disappearing liquidity and long follow-up windows.

Do not market it as:

- an AI meme scanner;
- a BUY/WATCH signal engine;
- a rug checker;
- a wallet/trading agent;
- a generic MCP wrapper;
- a promise of predictive alpha.

## Local package gate

- [x] Python package under `src/`
- [x] console script `mco`
- [x] no runtime dependencies
- [x] fixture-only offline demo
- [x] 107 tests
- [x] public-surface audit
- [x] clean-clone install/CLI/test acceptance
- [x] README starts with user outcome
- [x] lifecycle report and `inspect-token`
- [x] MCAP/FDV track separation
- [x] coverage/missing-slot semantics
- [x] no wallet/trade surface

## License / provenance gate

Current package declares MIT and contains an MIT `LICENSE` with `Copyright (c) 2026 Leo Labs`.

Reviewed source history for the extracted original path is local monorepo history from 2026-07-14 onward. A repository grep found no third-party copyright/SPDX/copied-from markers in the tracked extraction surface. This is evidence of provenance hygiene, **not a legal authorship opinion**. Before first public push, Leo must confirm that the extracted implementation is owned by him/Leo Labs or otherwise licensed for MIT publication.

- [x] no third-party license marker found in reviewed source surface
- [x] source commit history recorded
- [ ] Leo ownership/license confirmation before public push

## Privacy gate

- [x] no user-specific absolute home path in public surface
- [x] no embedded credentials/API keys
- [x] no Telegram token/chat route
- [x] no wallet/position/private ledger
- [x] competitiveness artifacts use aggregate fixed-case output rather than publishing selected CAs
- [x] no remote configured

## Public metadata draft

GitHub description:

> Coverage-aware cross-chain launch cohort telemetry with reproducible sampling, lifecycle tracking, and explicit missing-data semantics.

Suggested topics:

`onchain`, `crypto-data`, `dex`, `memecoin`, `cohort-analysis`, `data-quality`, `reproducible-research`, `python`

Release title:

`v0.1.0 — Cohorts before scores`

Release summary:

> Track new token-pool cohorts across Solana, BSC, Base and Robinhood Chain without silently losing the denominator when sources rate-limit, liquidity disappears, valuations switch between market cap and FDV, or follow-up observations are missed. v0.1.0 is GET-only, wallet-free and includes deterministic replay, coverage-aware reports and per-token lifecycle inspection.

## External-action gate

The following remain **not authorized** until Leo explicitly approves them:

- create/change GitHub repository visibility;
- add a public remote;
- push any branch or tag;
- publish a GitHub release;
- publish an X/TG/article announcement;
- deploy a hosted API/dashboard;
- submit it to a hackathon.
