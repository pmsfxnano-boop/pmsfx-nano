# Cryptonita Crypto Backend Continuity State

Last verified: 2026-10-01.

## Canonical scope

Crypto only. The canonical market capture boundary is Binance Spot on the Oregon service, backed by the existing durable PostgreSQL ledger.

## Operational architecture

- Hot market cache with monotonic cursor.
- Durable immutable event ledger.
- Session-local bounded evidence spool for transient storage outages.
- Freshness and reconnect supervision per symbol.
- Quality Gate, PIT/OOS and research evidence are fail-closed.
- Automatic promotion and execution remain disabled.

## Current infrastructure

- Render workspace: tea-dal284740ujc7399jvc0
- Canonical Binance service: srv-dau3kqqd0e5s73e44rf0
- Durable Postgres: dpg-dama7gqjnfac73ecks0g-a

## Continuity rule

The canonical capture session is never stitched across an outage. Each prospective cohort remains immutable and must independently satisfy the complete quality and OOS contract.
