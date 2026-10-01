# Gorila Crypto Cleanroom — A7 Forecast Integration

Date: 2026-09-29
Status: IMPLEMENTED / BLOCKED FOR PROMOTION
Branch: gorila-crypto-cleanroom

## Purpose

A7 connects the replayable market information set to an explicit forecast contract
without importing the legacy Argentina forecast stack.

The forecast is an additional scientific layer. It does not replace the
Opportunity Engine and it does not become active merely because a model can
produce a numerical probability.

## Forecast target semantics

The current target definition is:

P(SIGNED_TARGET_RETURN_BPS_POSITIVE)

where the target return is measured in the leader-event direction over an explicit
future horizon.

This is deliberately NOT labelled P(UP). The semantics are tied to the
leader/target opportunity context and cannot be confused with an absolute
directional probability.

## PIT feature contract

gorila_crypto.forecast.build_detection_features creates a versioned detection
snapshot using only information satisfying both:
- provider/event time is no later than the detection event;
- receive time is no later than the detection receive time.

Current feature set crypto_detection_v1 contains:
- leader return in bps;
- absolute leader return;
- leader direction;
- leader transport latency;
- target lookback return;
- target absolute lookback return;
- target information age;
- target market age.

Future reaction, convergence, MFE, MAE and other post-detection quantities are
not allowed in this feature set.

Every feature snapshot stores:
- decision event time;
- decision receive time;
- source event IDs;
- deterministic feature-set SHA-256.

## Model contract

ForecastModelSpec supports coefficient-based scoring, but the scoring gate
requires all of the following:
- validated model;
- OOS status PASS;
- economic status PASS;
- explicit point-in-time status;
- stress-test status PASS.

Otherwise scoring returns:
BLOCKED_NO_VALIDATED_MODEL

and no probability is emitted.

## Current state

No Crypto model has yet earned those gates.

Therefore A7 is deliberately operating with:
- forecast status BLOCKED_NO_VALIDATED_MODEL;
- automatic promotion FALSE;
- execution FALSE.

The cleanroom health endpoint exposes this state explicitly so the UI cannot
mistake an experimental numerical artifact for a production forecast.

## Persistence

Forecast shadow data is isolated in:
- crypto_forecast_shadow
- crypto_forecast_outcomes

Each shadow forecast binds to:
- replay fingerprint;
- model id/version;
- leader event identity;
- feature-set hash;
- exact target semantics and horizon.

Outcome storage includes explicit transaction cost and slippage fields so
economic evaluation cannot be separated from the eventual forecast evidence.

## Why no automatic model fitting is included yet

A fitted model without a sufficiently long live/replay history would create an
apparent result without a defensible OOS sample.

A7 therefore establishes the contract and evidence boundary first. Model fitting,
walk-forward validation, purging/embargo, placebo testing, stress testing and
cost/slippage evaluation remain research gates for the next validation phase.

## Safety

No Argentina model or US-equity model is imported.
No existing G2/PIT/Block R evidence is modified.
No forecast worker is started automatically.
No execution endpoint is added.

## Next gate

A8 — prospective data accumulation and full OOS/economic validation:
data sufficiency -> walk-forward -> baselines -> leakage audit -> placebo ->
cost/slippage -> stress -> stability across regimes -> promotion decision.
