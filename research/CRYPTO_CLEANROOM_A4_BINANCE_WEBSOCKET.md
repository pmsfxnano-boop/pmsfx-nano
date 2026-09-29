# Gorila Crypto Cleanroom — A4 Binance WebSocket

Date: 2026-09-29
Status: IMPLEMENTED
Branch: gorila-crypto-cleanroom

## Verified upstream contract

The current Binance Spot documentation exposes:
- raw trade streams with trade ID, event time and trade time;
- bookTicker streams with best bid/ask and order-book update ID;
- diff-depth streams at 100ms or 1000ms containing first/final update IDs U/u;
- the REST order-book snapshot at /api/v3/depth with lastUpdateId and up to
  5000 levels.

Binance's documented local-book procedure is snapshot + buffered WebSocket
updates, discard updates at or below the snapshot ID, require the first retained
event to bridge the next update ID, and resynchronize on any subsequent gap.
The cleanroom implementation follows that sequence rule.

## Implemented

Added:
- gorila_crypto/binance.py
- tests/test_binance_adapter.py

The adapter supports:
- BTCUSDT/ETHUSDT/SOLUSDT-style symbol configuration;
- combined streams;
- trade;
- bookTicker;
- diff-depth at 100ms or 1000ms;
- local receive timestamps including nanosecond precision;
- provider event/trade time preservation where Binance supplies it;
- explicit sequence IDs;
- REST depth snapshot retrieval;
- strict local order-book bootstrap;
- hard gap detection;
- explicit RESYNC_REQUIRED state;
- bounded reconnect backoff;
- planned connection rotation before the provider's 24-hour connection boundary.

## Important semantic decision

Binance bookTicker payloads expose an update ID but not the market event timestamp
fields used by the trade/depth streams. The adapter therefore does NOT fabricate
a provider event timestamp for bookTicker. It marks these events
TRANSPORT_TIME_ONLY and explicitly records RECEIVE_TIME_ONLY semantics.

This prevents a fast transport timestamp from being mistaken later for a
point-in-time provider event timestamp.

## What is deliberately NOT activated

A4 does not start a background worker and does not make forecasts.

The adapter is a transport module only. Persistence and the 24/7 worker are
introduced only after the event-ledger/replay contracts are completed.

## Validation

The repository contains offline tests for:
- deterministic stream construction;
- trade normalization;
- combined message handling;
- subscription acknowledgements;
- depth application;
- stale-update rejection;
- hard sequence-gap detection;
- strict snapshot bridging;
- invalid payload rejection;
- coordinator resynchronization.

The execution environment used for this work could not resolve github.com from the
container, so a local pytest run could not be performed here. The test suite is
therefore committed but its passing result is not claimed.

## Sources

Binance Spot WebSocket Market Streams:
https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

Binance Spot Market Data REST:
https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints

## Next gate

A5 — persistent event ledger + deterministic replay.
