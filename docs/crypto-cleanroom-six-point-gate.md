# Crypto Cleanroom — six-point engineering gate

Status date: 2026-09-30 UTC  
Branch: `gorila-crypto-binance-parallel`  
Base: `gorila-crypto-cleanroom`

This document records only verified engineering state. A blocked external dependency is
not converted into a PASS.

## 1. Controlled Binance capture

**Status: BLOCKED_DURABLE_STORAGE_LINK**

The isolated Render service is live on the Binance branch with ingestion enabled, but it
fails closed because no durable `DATABASE_URL` is available to the service. The Render
configuration contains a Blueprint service-reference definition for the database; the
hosted connector does not expose a safe Blueprint-sync operation. No database secret is
written into source control or injected as plaintext.

Safety result: no ephemeral accumulation and no hidden fallback to SQLite.

## 2. Binance invariants

**Status: PASS_CODE_AND_REST_SANITY; LIVE_WSS_LEDGER_PENDING**

The adapter has deterministic tests for:
- explicit symbol identity;
- trade normalization;
- `bookTicker` receive-time semantics;
- depth `U/u` continuity;
- stale/duplicate handling;
- snapshot + buffered-depth bootstrap;
- hard gap detection;
- resynchronization after a gap.

Independent live Binance Spot checks returned BTCUSDT and ETHUSDT as `TRADING`.
A live BTCUSDT REST depth snapshot returned a monotone order book around
83688 USDT/BTC, while recent trade IDs increased through 6723287258.
The latest Binance Spot server time observed was 2026-09-29T23:38:44.789Z and the latest
sampled trade timestamp was 2026-09-29T23:38:44.687Z.

The REST observations are sanity evidence, not a substitute for persisted WebSocket
sequence evidence.

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

Checksum logic, decimal-preservation behavior, and adapter integration have deterministic
tests. The live Kraken service
continues to run from the original cleanroom branch and has not been changed by this
work.

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

**Status: PIPELINE_HARDENED; LIVE_OOS_BLOCKED_PENDING_PROSPECTIVE_DATA**

The research path now hard-stops OOS when:
- quality status is not PASS;
- the replay slice is too short;
- the quality report fingerprint does not match the intended replay.

The existing OOS engine includes temporal walk-forward folds, purge/embargo, probabilistic
baselines, explicit transaction costs/slippage, placebo testing, temporal stability,
stress scenarios and persisted run/fold/OOS/lineage evidence.

No model promotion or execution is enabled.

The live OOS stage cannot truthfully be evaluated until the Binance durable-capture link is
configured and a sufficiently long prospective ledger exists.

## Overall disposition

The six-point engineering work is implemented to the maximum safe extent available in the
current environment. The two hard external blockers are:

1. durable Render database reference for the isolated Binance service;
2. enough valid prospective rows to run the empirical OOS gate.

Neither blocker is bypassed with synthetic or inferred evidence.


## Latest hardening delta

Commit chain after the CRC32-format correction adds two non-negotiable integrity controls:
1. Kraken JSON numeric tokens are decoded with `Decimal`, preventing IEEE-754 conversion
   from altering checksum-relevant price/quantity values.
2. Every Kraken connection clears local state and refuses a book update until a fresh
   snapshot has been observed, eliminating stale-book carryover across reconnects.

These controls do not resolve the observed historical timestamp backlog; that remains a
prospective forensic question requiring raw live frames.
