# Gorila Crypto Cleanroom — A11 Prospective Data Quality Gate

Date: 2026-09-29
Status: IMPLEMENTED / RESEARCH-ONLY
Branch: gorila-crypto-cleanroom

## Objective

Before empirical OOS is trusted, the replay itself must demonstrate that the
prospective feed is sufficiently complete and temporally coherent.

## Gate dimensions

`evaluate_replay_quality` checks:
- minimum rows per symbol;
- minimum event-time duration per symbol;
- invalid timestamp count;
- future-dated events relative to an explicit reference time;
- receive-time reversals within an ingest connection epoch;
- required-source sequence-gap count;
- p99 transport latency.

The configuration is explicit and persisted indirectly through the deterministic
quality-report fingerprint and replay fingerprint.

## Conservative principle

The quality gate does not try to repair missing data. It classifies the replay as
PASS or FAIL and leaves the underlying ledger unchanged.

Depth gaps can be used as a hard exclusion when depth is a required event type.
Trade IDs are not assumed to be contiguous by this gate.

## Persistence

Quality reports are stored in `crypto_quality_reports` with:
- replay fingerprint;
- PASS/FAIL status;
- deterministic report hash;
- canonical report JSON.

This makes the data-quality decision itself part of the research evidence.

## Relationship to A8

A8 remains the OOS/economic validator.
A11 is the upstream admissibility check for the empirical replay slice.

Recommended empirical sequence:
prospective ingestion → quality gate → frozen replay manifest → A8 walk-forward →
economic/stress evidence.

## Production status

No automatic quality-worker or forecast-worker is started by A11.
No production deployment is performed.
No trading or model promotion decision is emitted.
