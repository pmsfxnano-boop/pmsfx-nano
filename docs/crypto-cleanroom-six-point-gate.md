# Crypto Cleanroom — six-point engineering gate

Status date: 2026-09-30 UTC  
Branch: `gorila-crypto-binance-parallel`  
Base: `gorila-crypto-cleanroom`

This document records only verified engineering state. A blocked external dependency is
not converted into a PASS.

## 1. Controlled Binance capture

**Status: PASS_DURABLE_STORAGE_LINK; LIVE_CAPTURE_EXTERNAL_451_BLOCKED**

The isolated Render service `gorila-crypto-cleanroom-binance-capture` was successfully
updated by Blueprint sync from `render-crypto-binance.yaml`. The deploy is live and the
runtime heartbeat explicitly reports `backend=postgres`.

The Blueprint creates `DATABASE_URL` for the isolated Binance service from the existing
Render Postgres resource `pmsf-nano-db`; no database secret is written into source
control or injected as plaintext.

Post-deploy evidence:
- Blueprint-triggered deploy `dep-dau5sijncjis73asrt30` reached `live`.
- The process started successfully and exposed the health endpoint.
- The runtime emitted a durable-storage heartbeat with `backend=postgres`.
- There is no `GORILA_CAPTURE_BLOCKED` durable-storage error in the post-sync evidence.

The storage-link blocker is therefore resolved.

## 2. Binance invariants

**Status: PASS_CODE_AND_REST_SANITY; LIVE_WSS_BLOCKED_BY_PROVIDER_451**

The adapter has deterministic tests for:
- explicit symbol identity;
- trade normalization;
- `bookTicker` receive-time semantics;
- depth `U/u` continuity;
- stale/duplicate handling;
- snapshot + buffered-depth bootstrap;
- hard gap detection;
- resynchronization after a gap.

The isolated Render runtime starts with the Binance provider path, but its WebSocket
handshake is rejected with HTTP status 451 and Binance's response states that the service is
unavailable from the restricted location under its eligibility terms.

Therefore:
- the Binance code path is running;
- durable Postgres is reachable;
- live Binance WebSocket capture is **not** currently established from this Render
  region;
- no WebSocket ledger evidence is promoted to OOS evidence.

Independent live Binance Spot REST sanity checks from earlier work remain valid evidence for
market availability, but are not a substitute for persisted WebSocket sequence evidence.

## 3. Kraken timestamp anomaly

**Status: DIAGNOSTIC_CONFIRMED; ROOT_CAUSE_UNRESOLVED**

Render heartbeat evidence shows received time continuing to advance while provider event
time lag increased materially. Example, for BTC/USD:
- 21:54:21 receive vs 21:44:50.985 event: ~9.5 minutes lag.
- 22:04:23.641 receive vs 21:45:19.781 event: ~19.1 minutes lag.

The event clock itself advanced by only about 28.8 seconds during that ten-minute receive
interval. The diagnostic classifies this pattern as
`PROVIDER_OR_TRANSPORT_BACKLOG`.

An independent Binance server/trade clock remained near current time in the same overall
period, so the observed phenomenon is not evidence of a universal local clock failure.

The remaining distinction is venue WebSocket source timestamp generation versus upstream
buffering/transport. The adapter path itself preserves the received provider timestamp
without artificial subtraction, and local receive time is independently captured.

## 4. Kraken book integrity

**Status: PASS_ISOLATED_IMPLEMENTATION; LIVE_KRAKEN_BRANCH_UNTOUCHED**

The isolated branch now:
- treats Kraken `book` as L2;
- preserves raw bids/asks;
- keeps derived L1 separate;
- obtains instrument precision;
- reconstructs the local book;
- verifies the provider CRC32 checksum over the required top levels;
- records expected/computed checksum and precision;
- marks unverified/failing rows explicitly;
- forces connection resynchronization on checksum failure;
- parses Kraken wire prices/quantities as Decimal rather than binary floats before
  checksum reconstruction;
- requires a fresh book snapshot on every WebSocket connection before accepting
  incremental updates.

Checksum logic, decimal-preservation behavior, adapter integration, snapshot-before-update
ordering, and the pre-snapshot rejection path have deterministic tests. The latest branch
code state was CI-green. The live Kraken service continues to run from the original
cleanroom branch and has not been changed by this work.

## 5. Cross-venue audit

**Status: PASS_GUARD; LIVE_COMPARISON_BLOCKED_UNTIL_UNIT_ALIGNMENT**

The audit now requires explicit:
- base asset;
- quote currency;
- receive-time alignment;
- explicit quote conversion when quotes differ.

Consequently, BTC/USD versus BTCUSDT is blocked as an economic comparison until an
explicit USD/USDT conversion series is supplied. No assumption that USD = USDT is made.

## 6. Prospective ledger → Quality Gate → PIT/OOS → friction/stress

**Status: PIPELINE_HARDENED; LIVE_OOS_BLOCKED**

The research path now hard-stops OOS when:
- quality status is not PASS;
- the replay slice is too short;
- the quality report fingerprint does not match the intended replay.

The existing OOS engine includes temporal walk-forward folds, purge/embargo, probabilistic
baselines, explicit transaction costs/slippage, placebo testing, temporal stability,
stress scenarios and persisted run/fold/OOS/lineage evidence.

No model promotion or execution is enabled.

The current Render heartbeat shows existing Postgres rows for symbols such as BTC/USD,
ETH/USD and SOL/USD. Those rows are not sufficient evidence of a Binance prospective
ledger because they include prior shared-database observations and the new Binance
WebSocket session is currently blocked by HTTP 451. The current quality/OOS gate therefore
remains closed rather than treating shared or historical rows as fresh Binance OOS data.

The current Render Postgres instance is available, but its free-plan expiry is
2026-10-18 and therefore must be treated as an operational continuity deadline for the
ledger.

## Overall disposition

The durable-storage blocker has been resolved and verified in the live Render service.
The remaining external capture blocker is the Binance WebSocket HTTP 451 location
restriction from the current Render region.

The remaining empirical blocker is sufficient **valid prospective** Binance data that passes
the quality and replay gates. Neither blocker is bypassed with synthetic or inferred
evidence.

## Latest hardening delta

The latest operational change adds two verified points:
1. The isolated Binance Blueprint was synced against the existing Postgres resource and the
   live service now reports `backend=postgres`.
2. The live Binance WebSocket handshake is explicitly observed as HTTP 451 from Render's
   current region; this is recorded as an external capture blocker rather than converted
   into a data-quality pass.

Current Kraken Render evidence continues to show durable Postgres ingestion while provider
event time trails receive time by many minutes. That backlog remains unresolved at the
venue/transport layer and is not converted into a quality pass.
