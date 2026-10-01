# Gorila Crypto Cleanroom — A8 OOS and Economic Validation

Date: 2026-09-29
Status: IMPLEMENTED AS RESEARCH FRAMEWORK / NO PRODUCTION PROMOTION
Branch: gorila-crypto-cleanroom

## Objective

A8 establishes the validation boundary that a Crypto forecast must cross before
it can become admissible. The framework is deliberately stricter than a simple
train/test split.

## Information-set rule

Features are timestamped by decision receive time. That timestamp is the primary
walk-forward information boundary.

Labels are timestamped by the time the future outcome became observed. A training
observation is purged whenever its label observation reaches the OOS boundary.
An explicit embargo gap separates the end of usable training information from the
start of each OOS window.

## Walk-forward

Each fold:
1. selects a strictly earlier training information set;
2. fits a ridge-regularized logistic model using training observations only;
3. standardizes features using training statistics only;
4. evaluates probabilities on a future OOS block;
5. never refits using OOS observations.

Overlapping OOS windows are rejected by configuration validation.

## Probabilistic baselines

Every fold reports the model alongside:
- fixed P=0.50 baseline;
- training-prevalence baseline.

Reported metrics:
- Brier score;
- log loss;
- AUC when both classes exist;
- ten-bin expected calibration error.

## Economic evaluation

A separate, fixed policy layer converts probability into a signed decision.
The long and short probability thresholds are fixed before evaluation. Probabilities between them abstain; neither threshold is optimized on OOS.

Every economic result explicitly includes:
- gross signed return;
- round-trip transaction costs;
- slippage;
- net mean bps;
- cumulative net bps;
- traded fraction;
- null synthetic benchmark behavior;

Stress scenarios can apply return haircuts and independent cost/slippage
multipliers. A result that survives only under optimistic costs is not considered
economically validated.

## Placebo

A block-preserving label permutation is used against fixed OOS predictions.
The resulting p-value is a diagnostic against temporal dependence, not a substitute
for nested model-selection correction, multiple-testing correction, or an independent
economic replication.

## Stability

OOS evidence is partitioned by target symbol and horizon. Aggregated performance
alone is insufficient because an apparent aggregate relation can be concentrated in
one symbol or one horizon.

## Promotion gate

A8 promotion eligibility requires, at minimum:
- multiple non-overlapping OOS folds;
- sufficient OOS observations;
- improvement over fixed baselines in both Brier and log loss;
- positive net economics after costs/slippage;
- placebo p-value at the configured threshold;
- positive net economics under every declared stress scenario;
- no-promotion behavior on a deterministic null benchmark;
- sufficient observations for every evaluated symbol and horizon.

This gate is a research gate only. It does not itself flip the production
`ForecastModelSpec` admissibility flags.

## Important limitation

A8 currently validates a model family through deterministic ridge-logistic fitting.
It does not claim that the Crypto market contains exploitable alpha. Real evidence
requires a sufficiently long prospective ledger replay and an independently
frozen experiment configuration.

## Reproducibility

Each validation run is intended to bind to:
- replay fingerprint;
- feature-set version and hash;
- model family/version;
- fold configuration;
- economic policy;
- stress scenarios;
- random seed for placebo generation.

The next persistence layer records fold-level and observation-level evidence so a
result can be reconstructed without relying on terminal output.

## Production status

`automatic_promotion = false`.
`execution = false`.
No Argentina, US-equity, or legacy forecast is used by the Crypto validator.
