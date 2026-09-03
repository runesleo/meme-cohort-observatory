# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-09-03

### Added

- Coverage-aware public new-pool collection for Solana, BSC, Base, and Robinhood Chain.
- Reproducible cohort admission with explicit inclusion probability and sample fraction.
- Admission-anchored lifecycle tracking from 10 minutes to 7 days.
- Separate market-cap and FDV tracks, ever-observed thresholds, and explicit transition events.
- Missing-refresh accounting, reorg-aware cursors, bounded request budgets, and fail-closed coverage semantics.
- `mco collect`, `report`, `export`, `inspect-token`, and `doctor` commands.
- Deterministic offline fixtures, 108 automated tests, and public-surface audit tooling.

### Security

- No wallet connection, transaction signing, order placement, private keys, browser cookies, or embedded credentials.

[Unreleased]: https://github.com/runesleo/meme-cohort-observatory/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/runesleo/meme-cohort-observatory/releases/tag/v0.1.0
