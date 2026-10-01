# Crypto Backend Continuity State

Last verified: 2026-10-01 UTC.

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

Current blockers:
1. Current Binance branch head dccbd5bb750eeeb3cd87d02ec879f477cb90bd06 contains the corrected health/readiness test.
2. CI must pass on the exact current head before Render promotion.
3. Oregon had a stale STARTING capture session; hardened runtime must reconcile it after deployment.
4. Frankfurt still has a queued/update-in-progress older deployment and is not yet considered clean-live.
5. Production PIT/walk-forward/OOS evidence cannot exist until a complete 7-day prospective cohort passes Quality Gate.

Exact continuation:
1. Verify CI for current Binance head.
2. Deploy Oregon Binance and verify new session, runtime RUNNING, fresh BTCUSDT/ETHUSDT/SOLUSDT events, zero gaps, and creation of crypto_research_runs.
3. Synchronize the verified head to main and cleanroom.
4. Reconcile Frankfurt only after Oregon is stable.
5. At cohort maturity, verify all 36 PIT/walk-forward/OOS reports in Postgres.

This file is the durable engineering handoff. Always re-verify GitHub, Render and Postgres before changing anything. Never infer live/OOS state from chat context alone.
