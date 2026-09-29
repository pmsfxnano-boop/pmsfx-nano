# Prospective Crypto Study Protocol — Pre-Registered

Date: 2026-09-29
Branch: gorila-crypto-cleanroom
Domain: Binance Spot public market data

## 1. Purpose

Accumulate a genuinely prospective, point-in-time ledger and determine whether
the A7 forecast family retains information value after strict OOS validation,
temporal stability tests, and explicit transaction-cost/slippage assumptions.

Nothing in this protocol authorizes production trading or model promotion.

## 2. Fixed capture universe

Symbols:
- BTCUSDT
- ETHUSDT
- SOLUSDT

Streams:
- trade
- bookTicker

Depth is deliberately excluded from the first capture tranche to keep the raw
ledger compact and to preserve a clean trade-event research baseline. The depth
adapter and sequence machinery remain available for a later microstructure tranche.

## 3. Required prospective duration

Minimum study window: 7 complete UTC calendar days after the first successful
capture timestamp.

Minimum trade observations:
- 100,000 trade events per symbol.

The study does not substitute historical backfill to satisfy the prospective
window. Backfilled or replayed data may only be used in separately labelled
historical experiments.

## 4. Data-quality gate

The complete prospective replay must satisfy:
- zero invalid timestamps;
- zero future-dated events relative to the frozen replay cutoff;
- zero receive-time reversals inside an ingest connection epoch;
- zero required-source gaps;
- p99 event-to-receive transport latency <= 5 seconds;
- each symbol independently satisfies the minimum row and duration thresholds.

The A11 quality report and its deterministic hash are part of the final evidence.

## 5. Point-in-time rule

For every forecast decision:
- feature event time <= decision event time;
- feature receive time <= decision receive time;
- labels may only use information observed after the decision;
- event-time order is never substituted for receive-time causality;
- feature-set hash and all source event IDs are persisted.

Any failed PIT lineage audit invalidates the corresponding OOS result.

## 6. OOS design

Validation is purged walk-forward only.

Default research design:
- non-overlapping OOS windows;
- fixed 5-second purge;
- fixed 5-second embargo;
- ridge-logistic model family;
- training-only standardization;
- fixed probability thresholds 0.55 / 0.45 with abstention between them;
- P=0.50 and training-prevalence baselines.

At least three OOS folds are required. The study target is four or more folds
from the 7-day window.

## 7. Probabilistic acceptance

Reported for the aggregate and each fold:
- Brier score;
- log loss;
- AUC when defined;
- ECE-10.

Promotion eligibility requires beating both declared baselines on Brier and log
loss in at least 67% of folds.

## 8. Economic acceptance

Every OOS observation is evaluated net of round-trip costs and slippage.

Pre-registered cost scenarios:
- base: 1 bps cost + 1 bps slippage;
- stress-1: 2 bps cost + 2 bps slippage;
- stress-2: 4 bps cost + 4 bps slippage.

The model must retain positive net economics under every declared stress scenario.

No threshold, cost assumption, or stress scenario may be tuned after seeing the OOS results.

## 9. Temporal degradation

Fold chronology is split into early and late halves.

The late period must:
- pass the same 67% fold baseline/economic consistency requirement;
- increase average log loss by no more than 10% relative to the early half;
- increase Brier score by no more than 0.02;
- reduce mean net bps by no more than 2 bps.

This temporal gate is evaluated before any final admissibility decision.

## 10. Placebo and robustness

A block-preserving label placebo is mandatory.

Placebo evidence is diagnostic and does not replace multiple-testing correction or
independent replication.

Additional null synthetic tests must remain non-promotable.

## 11. Evidence chain

Final empirical evidence must bind:
- source and symbol universe;
- prospective start/end times;
- replay fingerprint;
- data-quality report hash;
- feature-set version/hash;
- fold definitions;
- model specification hashes;
- OOS probabilities and labels;
- exact cost/slippage scenario;
- temporal stability report;
- placebo configuration and seed.

## 12. Promotion rule

Production remains blocked unless every required gate passes. A positive result
from one metric, one symbol, one fold, or one economic assumption is insufficient.

Automatic promotion remains false by design.

## 13. Infrastructure note

The current cleanroom capture service is an experimental prospective collector.
It uses SQLite on the Render filesystem and is not yet considered the final archival
ledger because that filesystem is not a durable database layer across service restarts.

Therefore the study cannot be declared complete until the capture layer has a
durable persistence strategy and the full prospective window survives that strategy.
Until then, captured data is treated as an experimental staging ledger.
