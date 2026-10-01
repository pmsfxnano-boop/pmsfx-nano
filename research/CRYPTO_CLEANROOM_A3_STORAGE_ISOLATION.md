# Gorila Crypto Cleanroom — A3 Storage Isolation

Date: 2026-09-29
Status: COMPLETE
Branch: gorila-crypto-cleanroom

## Objective

Create an explicit Crypto persistence boundary before any live market adapter is
allowed to write data.

## Storage contract

Crypto does not instantiate or import the legacy Argentina Store.

Instead it uses the CryptoStore in gorila_crypto/storage.py.

PostgreSQL:
- requires GORILA_CRYPTO_DATABASE_URL;
- creates and uses only the GORILA_CRYPTO_DB_SCHEMA namespace, defaulting to
  gorila_crypto;
- never consumes the legacy DATABASE_URL or GORILA_DB_SCHEMA variables.

SQLite:
- uses GORILA_CRYPTO_SQLITE_PATH;
- creates only crypto_* tables.

## Tables introduced

- crypto_events
- crypto_connection_events
- crypto_data_gaps
- crypto_runtime_runs
- crypto_source_health

The event table already preserves the fields required for later ingestion and
replay: event time, receive time, provider time, source, sequence range,
payload hash, quality and metadata.

No Argentina daily tables are reused.

## Test gate

Focused A3 tests verify:
- no legacy table names are created;
- events persist only in crypto_events;
- legacy DATABASE_URL does not control CryptoStore;
- required Crypto tables exist.

## Safety

A3 is confined to the cleanroom branch. Production Argentina code and databases
are untouched.

## Next gate

A4 — Binance WebSocket adapter with explicit sequence handling and resynchronization.
