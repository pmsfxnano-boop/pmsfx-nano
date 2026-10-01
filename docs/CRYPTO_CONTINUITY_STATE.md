# Crypto Backend Continuity State

Last verified: 2026-10-01 02:03 UTC.

Canonical scope: Crypto only. Active branches: main, gorila-crypto-binance-parallel, gorila-crypto-cleanroom.

Infrastructure:
- Render workspace tea-dal284740ujc7399jvc0
- Binance Oregon srv-dau3kqqd0e5s73e44rf0
- Binance Frankfurt srv-dau6fjqd0e5s73eet9r0
- Cleanroom Oregon srv-dau169flk1mc73d9phhg
- Postgres dpg-dama7gqjnfac73ecks0g-a

Legacy separation:
- gorila_argentum schema physically retired.
- Prior public legacy tables physically retired.
- Audit retirement_id 507c0604ced44509b30a91133c7e34d0, executed 2026-10-01T00:23:44.738866+00:00.
- Legacy Render services still exist only as decommissioned resources because service deletion was not exposed by the available Render API.

Backend hardening:
- Transactional/idempotent Crypto batch ledger writes.
- Runtime buffering at 250 events or 50ms.
- Stale capture-session reconciliation before single-writer claim.
- /api/crypto/health is liveness; /api/crypto/readiness is strict data readiness.
- PIT horizon cap is anchored to received/information-availability time plus preregistered tolerance.
- Autonomous research runner with 7-day maturity gate, exact-ledger fingerprinting, Quality Gate, 6 directed pairs x 6 horizons, OOS persistence, and research-run idempotency.

- Pre-QG verification at 2026-10-01T02:03:27Z: current active session had 3,032 BTCUSDT rows, 1,695 ETHUSDT rows, and 408 SOLUSDT rows; event-type totals 4,248 bookTicker / 904 trade; p99 transport latency by symbol 143.8 / 199.2 / 62.8 ms; future events=0, future received timestamps=0, negative latency=0, receive-time reversals=0, persisted gaps=0, duplicate event keys=0.

Current blockers:
1. The production runtime is currently deployed at commit d6b1e7d2b32e88f0f5ea788e7743d1c7bd9fe2e0.
2. The exact-current-head CI result is not independently verified in this environment; do not claim CI green without a run record.
3. The active prospective session is b1b79084-fd1d-48bd-89a8-c8b8598fa889, started 2026-10-01T02:02:18.729046+00:00 UTC, with runtime run e83621e1-59dd-4df0-b279-ef15199acde0.
4. The active session currently shows all six preregistered Binance channels, zero duplicate event keys, and zero persisted data gaps at the latest verification.
5. The per-symbol feed watchdog was hardened because a prior cohort demonstrated an ETH-specific stall while BTC/SOL remained active; the deployed d6b1e7... runtime now evaluates required-symbol freshness and restarts the feed without ending the cohort.
6. A test-only fixture correction was committed after deployment (7b2ee441...), so the deployed production code and the branch test suite differ only in that regression fixture; do not redeploy for the test-only change during the current cohort.
7. Production PIT/walk-forward/OOS evidence cannot exist until this new complete 7-day prospective cohort passes Quality Gate.
8. Frankfurt remains not considered clean-live until separately verified.
Automatic promotion remains disabled.

