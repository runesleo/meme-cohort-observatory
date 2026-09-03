# Launch Cohort Report

Generated: 2026-09-03T12:04:41.722782Z
Qualification: `pending_history_and_execution_checks`
Security: `unmeasured`
Executability: `unmeasured`
Main-chain score: `insufficient_sample`

| Chain | Status | Coverage | Comparable frame | Discovered | Admitted | Dropped | Sample fraction | Missing refresh | MCAP ≥$1M | MCAP ≥$3M | MCAP ≥$10M |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| base | success | fixture_snapshot | no | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| bsc | success | fixture_snapshot | no | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| robinhood | degraded | promoted_subset | no | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| solana | success | fixture_snapshot | no | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## Interpretation limits

- Crossings are observed provider values, not reconstructed intraperiod ATHs.
- Market cap and FDV remain separate tracks.
- Discovery is not a meme classification, security verdict, or buy signal.
- Incomplete coverage is not a valid denominator for comparable cohort rates.

## Coverage caveats

- `base`: status=success; comparable_frame=False; gap=UNKNOWN.
- `bsc`: status=success; comparable_frame=False; gap=UNKNOWN.
- `robinhood`: status=degraded; comparable_frame=False; gap=discovery_endpoint_is_promoted_only.
- `solana`: status=success; comparable_frame=False; gap=UNKNOWN.
