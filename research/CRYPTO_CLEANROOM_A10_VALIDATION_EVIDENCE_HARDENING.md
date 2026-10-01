# Gorila Crypto Cleanroom — A10 Validation Evidence Hardening

Date: 2026-09-29
Status: IMPLEMENTED / RESEARCH-ONLY
Branch: gorila-crypto-cleanroom

## Objective

A10 strengthens the boundary between a numerical OOS result and evidence that is
worthy of model consideration.

## Fold consistency

Aggregate performance is no longer sufficient for the promotion gate.

Each walk-forward fold must be evaluated against both:
- fixed P=0.50 baseline;
- training-prevalence baseline.

A configurable minimum fraction of folds must beat both baselines on Brier and
log loss, and the same minimum fraction must have positive net economic mean after
the declared costs and slippage.

Default minimum fold pass fraction: 67%.

## Exact OOS lineage

Each persisted OOS observation is now linked to:
- the exact feature-set SHA-256;
- the exact source event IDs used to build the PIT snapshot.

This lineage lives in `crypto_validation_lineage`, keyed by run/fold/row.
The validation run already binds the evidence to the replay fingerprint and
experiment configuration.

## Consequence

An aggregate positive result with one or more systematically failed folds is no
longer sufficient for automatic research promotion eligibility.

Likewise, an OOS probability without its underlying feature/source lineage is not
considered fully reproducible evidence.

## Current status

This remains a research gate. It does not activate a Crypto forecast, order
execution, or automatic promotion.

The next empirical requirement remains prospective market history of sufficient
duration and integrity to populate the A8 framework with genuinely future data.
