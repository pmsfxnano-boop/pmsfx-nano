# Gorila Crypto Cleanroom — A9 Prospective Ingestion Runtime

Date: 2026-09-29
Status: IMPLEMENTED / NOT AUTO-ACTIVATED
Branch: gorila-crypto-cleanroom

## Objective

A9 converts the proven Binance transport boundary into a persistence-only
prospective data engine. The purpose is to accumulate a point-in-time research
ledger that can later support genuine OOS validation.

## Runtime contract

`ProspectiveCryptoIngestor`:
- consumes the existing BinanceSpotMarketAdapter;
- persists every normalized market event into `crypto_events`;
- records connection lifecycle into `crypto_connection_events`;
- records in-connection depth sequence gaps into `crypto_data_gaps`;
- updates per-source freshness and transport health;
- persists the runtime lifecycle into `crypto_runtime_runs`.

It does not:
- compute forecasts;
- create trade signals;
- execute orders;
- promote models.

## Sequence integrity

Depth updates use their explicit U/u update range for continuity checks.
A gap is recorded only inside one connection epoch. A reconnect boundary resets
continuity state so the runtime does not invent a missing interval merely because
the provider stream resumed at a later sequence.

Trade IDs are preserved in the immutable ledger but are not treated as proof of
contiguity by this runtime. This avoids elevating an unverified assumption into a
data-loss claim.

## Freshness

Each persisted observation is assessed against event time and receive time.
Transport success and observation freshness remain separate dimensions.

Stored health fields include:
- event age;
- transport age;
- LIVE / DELAYED / STALE / INVALID_TIMESTAMP classification;
- last event and receive timestamps.

## Failure behavior

Malformed provider data remains the adapter's responsibility and fails safely.
Runtime errors are persisted as degraded connection/runtime state.
Missing market events are never synthesized.

## 24/7 operation

The adapter already provides bounded reconnect backoff and controlled connection
rotation. A9 provides the durable sink around that transport.

The runtime is intentionally not wired into `gorila_crypto.app` yet. This keeps
the cleanroom demonstrably free of an automatically started network worker until
the ingestion path has passed CI and a controlled staging/prospective run.

## Research safety

Every event retains provider/event time, local receive time, source, sequence
metadata, payload and canonical ledger identity. This makes the prospective feed
replayable without asking the live provider to reconstruct history.

No historical backfill is silently mixed into the prospective window.
No forecast result is emitted from the ingestion layer.
No production deployment is performed by A9.

## Evidence gate

The next empirical step is not model promotion. It is accumulation of a sufficiently
long prospective ledger, monitoring of connection/gap/freshness quality, and then a
frozen replay manifest feeding the A8 validation framework.

Only after the ledger demonstrates sufficient completeness can true market OOS
evidence be generated.
