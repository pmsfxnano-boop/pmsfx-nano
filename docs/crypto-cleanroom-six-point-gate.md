# Crypto Cleanroom — six-point engineering gate

Status date: 2026-09-30 UTC  
Branch: `gorila-crypto-binance-parallel`  
Base: `gorila-crypto-cleanroom`

This document records only verified engineering state. A blocked external dependency is
not converted into a PASS.

## 1. Controlled Binance capture

**Status: PASS_DURABLE_STORAGE_LINK; LIVE_CAPTURE_CONNECTED; FRESHNESS_GATE_BLOCKED**

The Frankfurt replacement service `gorila-crypto-cleanroom-binance-frankfurt-capture`
successfully reaches the existing Oregon Render Postgres through the external TLS endpoint;
the database secret is not written into source control.

The Render runtime has also reached a successful Binance WebSocket connection from
Frankfurt. The deployment history retains earlier failed location/network attempts, but
the current connection is no longer blocked at the WebSocket handshake layer.

Post-deploy evidence:
- Frankfurt runtime instance `srv-dau6fjqd0e5s73eet9r0` reached a running state with
  Postgres-backed ingestion.
- The runtime starts and records connection lifecycle events.
- The shared Postgres database is reachable from Frankfurt through the configured external
  hostname with TLS required by the application.

The storage-link blocker is therefore resolved.

## 2. Binance invariants

**Status: PASS_CODE_AND_REST_SANITY; LIVE_WSS_CONNECTED; EVENT_FRESHNESS_UNACCEPTABLE**

The adapter has deterministic tests for:
- explicit symbol identity;
- trade normalization;
- `bookTicker` receive-time semantics;
- depth `U/u` continuity;
- stale/duplicate handling;
- snapshot + buffered-depth bootstrap;
- hard gap detection;
- resynchronization after a gap.

The Frankfurt runtime now establishes a Binance WebSocket connection using the
market-data-only endpoint `data-stream.binance.vision`. This removes the previous 451
handshake blocker for the current Frankfurt service, but it does **not** establish an
acceptable capture yet.

Observed live evidence from the current session shows:
- `bookTicker` events continue to arrive with near-current provider and receive times;
- `trade` and `depthUpdate` events were initially arriving materially behind current
  receive time, indicating an ingestion/backpressure bottleneck rather than an absent
  socket;
- the application was opening short-lived Postgres connections on the hot event path,
  and the latest branch hardening replaces this with a reusable writer connection and
  throttles source-health persistence.

Therefore:
- WebSocket connectivity is **established** from Frankfurt;
- fresh, low-latency trade/depth capture is still **not** established;
- no Binance WebSocket slice is promoted to OOS evidence;
- the next gate is a sustained freshness/integrity run after the hot-path changes deploy.

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

The current Render heartbeat has historically exposed shared Postgres rows across venues;
provider-scoped heartbeat, quality, gap, source-health and prospective-status reads are now
implemented so Binance telemetry cannot be contaminated by Kraken rows. The current
quality/OOS gate remains closed until Binance trade/book/depth freshness and sequence
integrity are sustained over the required observation window.

The current Render Postgres instance is available, but its free-plan expiry is
2026-10-18 and therefore must be treated as an operational continuity deadline for the
ledger.

## Overall disposition

Durable storage and Frankfurt WebSocket connectivity are established. The active empirical
blocker is now **ingestion freshness/throughput for trade and depth**, followed by the full
quality/replay gate. No blocker is bypassed with synthetic or inferred evidence.

## Latest hardening delta

The latest operational changes add four verified engineering points:
1. Frankfurt uses the Binance market-data-only WebSocket/REST endpoints.
2. Cross-region Postgres access is performed through the external endpoint with TLS rather
   than Render's region-local private hostname.
3. Provider-scoped telemetry prevents Kraken observations from contaminating Binance
   heartbeat/quality/gap/source-health views.
4. The hot ingestion path now reuses a durable writer connection, throttles source-health
   writes, and fails the HTTP health check when the ingest worker is dead.

Current Kraken Render evidence continues to show durable Postgres ingestion while provider
event time trails receive time by many minutes. That backlog remains unresolved at the
venue/transport layer and is not converted into a quality pass.
