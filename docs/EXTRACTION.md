# Extraction provenance and boundaries

This local Stage A repository was extracted from the user-owned cross-chain cohort collector under the Research DD workspace. It intentionally starts a new repository history rather than publishing the surrounding monorepo.

## Source snapshot

| Logical source | SHA-256 |
|---|---|
| `README.md` | `e160fe22f725c87e0e3d3016614a5d96aa85d47b6db59b4936c30f133e89cdb9` |
| `adapters.py` | `58aa748ac9b7cd235847030b005d3d54673c8f363558cd2d9fa2e72b192298b2` |
| `cohort.py` | `5432967d517e03b5d64c3bc74d3a4ec3e7389e1c0e37446820576c7cfea1463d` |
| `collector.py` | `0f98e195e1a652c652a1050a85d693fcf0e193f8d66714c34fda1a056f991cdb` |
| `store.py` | `8e77b4b48a23c1de436d06af50404a98bd52b6557bbc72c273b9fc0a045c0163` |
| `runtime_lock.py` | `434c358903d294a12c0d86cd60a98b4a4650890a805f6e9e7fe01a0d8f0a0147` |
| `tests/test_collector.py` | `41ebf1973c44252c381eed2ebbe74ce59e586ef439ef053d1418df3662a5e549` |
| `tests/test_robinhood_fullchain.py` | `e8630b643023471b083b0e4ea3e36aac675d474958092809e15d2d14267df80b` |
| `tests/test_rpc_log_client.py` | `b20bf0d629afcbdebdab6f1059029b0ab4dcc7bfccae5ef196fe33dd253c12ba` |

## Included

Public GET-only adapters, normalized observations, cohort admission, lifecycle scheduling, SQLite state, coverage semantics, reorg-aware event provenance, fixtures, tests, deterministic reports, exports, and local safety audits.

## Excluded

LaunchAgent installation, local absolute paths, Telegram delivery, credentials, private KOL lists, focus boards, real paper/live ledgers, wallet or trade execution, and any strategy-specific entry/exit/position-sizing parameters.

## License audit

The extracted runtime uses Python standard-library modules only. No vendored third-party source was copied. Public API fixtures remain test data and are not claimed as proprietary market data. The local repo uses MIT for the newly packaged user-owned code; publication remains a separate approval gate.
