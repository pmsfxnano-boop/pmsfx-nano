# Gorila Crypto Cleanroom — A5 Event Ledger + Deterministic Replay

Date: 2026-09-29
Status: IMPLEMENTED
Branch: gorila-crypto-cleanroom

## Purpose

A live market model is not research-grade until the exact information set available
to the system can be reconstructed. A5 therefore separates:
- market event time;
- provider time when supplied;
- local receive time;
- immutable ingestion order;
- deterministic market-time ordering.

## Ledger

crypto_events now contains:
- monotonic ledger_seq;
- deterministic event_key;
- event_id;
- symbol and event type;
- event_time and received_time;
- provider_time;
- source;
- sequence_start / sequence_end;
- payload_hash;
- complete canonical payload_json;
- quality;
- metadata;
- recorded_at.

The complete payload is retained so replay does not depend on the live provider.

Duplicate delivery is idempotent using provider identity. When an existing
provider identity arrives with a different payload hash, the storage layer fails
closed with LedgerIntegrityError instead of silently accepting a conflict.

## Replay modes

ingest order reconstructs the information arrival sequence. This is the default
for point-in-time research.

event_time order reconstructs market chronology. It is explicitly NOT treated as
a proxy for what the strategy knew at that time.

Received-time cutoffs can therefore define a genuine point-in-time information
set, while event-time ordering can be used separately for market chronology.

## Reproducibility

Every replay produces:
- row count;
- first/last ledger sequence;
- replay policy;
- SHA-256 fingerprint of the exact canonical rows.

Replay manifests can be persisted in crypto_replay_manifests.

This creates an auditable binding between:
data slice -> replay policy -> fingerprint -> later model evidence.

## Binance alignment

Binance Spot WebSocket exposes event time and sequence/update identifiers on the
trade and depth streams. Diff-depth carries U/u, while trade carries trade ID t;
these fields are retained by the cleanroom adapter and bound into the ledger
identity.

BookTicker exposes updateId and bid/ask fields, but does not expose the same
event-time field used by trade/depth. The adapter therefore does not manufacture
provider event time for bookTicker.

## Test coverage added

A5 tests cover payload persistence, duplicate idempotence, ingest-order
reconstruction, explicit event-time ordering, received-time point-in-time
cutoff, stable replay fingerprints, deterministic reducer state, replay manifest
construction, conflicting provider identity fail-closed behavior, and isolated
replay-manifest persistence.

The repository environment available for this run could not execute the committed
pytest suite because the container could not resolve github.com. No test-pass
claim is made without execution evidence.

## Safety

A5 remains on the cleanroom branch. No live worker starts and no Render production
deployment occurs.

## Next gate

A6 — Lead/Lag + Opportunity Clock in SHADOW mode, based only on the replayable
event ledger. No forecast promotion or execution is permitted at A6.
