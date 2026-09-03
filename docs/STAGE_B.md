# Stage B · Lifecycle analytics

Status: `LOCAL FUNCTIONAL / NOT PUBLIC READY`

Implementation commit: `4d6250c17cdf0d66117dc50c1831f2118832c8a3`

Stage B turns the collector database into a research product rather than adding another discovery source.

## What changed

- `mco report` now adds comparable-cohort lifecycle analytics from SQLite.
- `mco inspect-token <chain> <address>` returns one recorded token's observations, valuation tracks, checkpoints and threshold events.
- 10m / 1h / 1d / 7d checkpoints use the **first recorded observation at-or-after the target age** and always expose lag. They are observability checkpoints, not exact-time survival estimates.
- Market cap and FDV remain separate denominators.
- `ever_observed_ge` is separate from an explicit same-track below→above `transition_event`. This avoids fabricating a crossing time when a valuation track first appears already above a threshold.
- Inclusion probability, sample fraction and missed refresh slots are exposed. No formal confidence interval is claimed.

## Deterministic acceptance

The two-token fixture proves the semantics without cherry-picking a historical winner:

- cohort n = 2;
- one MCAP track explicitly transitions from below $1M to above $1M;
- observed transition age = 65 minutes from pool creation;
- 10m checkpoint median lag = 5 minutes;
- 7d remains unobserved rather than fabricated.

Full repository validation: **107/107 tests PASS**, public-surface audit PASS, clean-clone editable install + console CLI PASS.

## Real historical readback

The pre-existing canonical research state contains **698 comparable cohort members**: Base 613 and BSC 85.

Base MCAP illustrates why Stage B separates two concepts:

- ever observed >= $1M: **6/429 tracked**;
- explicit same-track crossing events: **2**.

Those are not interchangeable. The historical state also shows **2518 Base missed refresh slots**, so the report exposes incomplete lifecycle coverage instead of claiming a clean success rate.

## Current forward cohort

The BSC cohort opened naturally during the 2026-09-03 keyless smoke now has **10** members. Its first follow-up had source degradation and produced **10** missed slots; the lifecycle report retains those gaps. This is useful product behavior, not evidence of alpha.

## Gate

Do not add MCP/Agent/hackathon packaging yet. Stage B is functionally real, but public readiness still needs a bounded competitor/usefulness review and at least one external-user or reproducible case proving that cohort sampling + coverage provenance + lifecycle history is materially better than a simple new-pool script.
