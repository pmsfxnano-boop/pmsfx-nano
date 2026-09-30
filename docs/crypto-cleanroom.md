# Crypto Cleanroom — Operational Contract

The active empirical study is the preregistered Binance Spot cohort
`crypto-binance-spot-prospective-v2`.

## Fixed universe

Symbols: BTCUSDT, ETHUSDT, SOLUSDT.

Streams: trade and bookTicker only.

Instrument identities are fixed as Spot BTC/USDT, ETH/USDT and SOL/USDT.
Depth is not part of the first empirical cohort.

## Evidence requirements

The cohort is not eligible for OOS evaluation until it has:

- 7 complete UTC calendar days;
- at least 100,000 trade events per symbol;
- no invalid or future-dated observations;
- no required sequence gaps;
- p99 transport latency <= 5 seconds;
- per-symbol trade coverage and per-symbol duration;
- an immutable protocol hash and capture-session identity.

Walk-forward validation uses 5 s purge and 5 s embargo. Economic evaluation is net of
the preregistered 1+1 bps base friction and 2+2 / 4+4 bps stress scenarios.

Promotion remains blocked unless the probabilistic, temporal, placebo/multiple-testing,
economic, DSR and CPCV/PBO research gates all pass.

## Runtime safety

Only one active capture session may own the study. Session acquisition is idempotent
for the same protocol identity, and conflicting active cohorts are rejected.

Runtime leases are heartbeated and stale runs are marked `ABORTED_STALE`.

Transport connectivity and market-data freshness are separate signals. A connected
WebSocket is never treated as evidence that a market event is current.

## Current state

The active capture is running from the isolated Frankfurt Binance service against the
durable Postgres ledger. The cohort is still in data-accumulation / quality-gated state;
no OOS result or trading permission exists.
