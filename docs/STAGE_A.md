# Stage A acceptance

Status: **local complete; not public**.

The extraction is an independent package rather than a copy of the surrounding internal monorepo. The verified local surface is:

```text
public adapters → normalized observations → coverage provenance
→ sampled cohort admission → anchored follow-up → SQLite
→ deterministic report/export
```

## Verified

- Source baseline: 101 tests.
- Extracted repository: 105 tests.
- Offline `collect → report → export → doctor`: PASS in 0.789 seconds.
- Clean local clone: editable installation, `mco` console script, audit, and full tests PASS.
- Public-surface audit: zero private absolute paths, embedded secrets, wallet/trade imports, or execution calls.
- Keyless live smoke: BSC and Base completed successfully. Solana hit an HTTP 429 pagination gap and remained explicitly degraded rather than becoming a comparable cohort.
- A real Base smoke exposed a missing `RequestBudget.check` method. The extracted project now has a fail-closed implementation and regression tests.

## Still not claimed

- Discovery is not a meme classification, safety verdict, or buy signal.
- Observed provider values are not reconstructed intraperiod ATHs.
- Fixture success is not evidence of trading expectancy.
- Two successful source smokes are not evidence of durable production coverage.
- No external user has yet validated the README or installation flow.

## External gates

No remote is configured. Creating a public repository, pushing, releasing, deploying, registering an account, or submitting a hackathon remains separately gated.

Machine-readable evidence is under `artifacts/stage-a-demo/`.
