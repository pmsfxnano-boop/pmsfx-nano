# Gorila Crypto Cleanroom — A2 Runtime Isolation

Date: 2026-09-29
Status: COMPLETE
Branch: gorila-crypto-cleanroom

## Objective

Create a minimal Crypto application boundary that cannot accidentally execute the
Argentina or US-equity runtime.

## Implemented

Added a domain-neutral core:
- gorila_core/__init__.py
- gorila_core/market_freshness.py

The freshness implementation is byte-for-byte identical to the existing Gorila
freshness implementation at this stage. Event age remains independent from
transport age, with LIVE/DELAYED/STALE/INVALID_TIMESTAMP states.

Added the isolated Crypto package:
- gorila_crypto/__init__.py
- gorila_crypto/config.py
- gorila_crypto/app.py

The Crypto application:
- exposes only / and /api/crypto/* routes;
- has no Argentina or US configuration;
- has zero market network adapters at A2;
- has zero background workers at A2;
- disables FastAPI docs/OpenAPI routes;
- imports only the domain-neutral core and its own configuration.

Default cleanroom symbols are BTCUSDT, ETHUSDT and SOLUSDT, configured only through
GORILA_CRYPTO_SYMBOLS.

Added architecture guardrails:
- tests/test_crypto_cleanroom_architecture.py
- .github/workflows/gorila-crypto-cleanroom.yml

The guardrails reject forbidden legacy imports, legacy provider/cohort references,
non-Crypto routes and background-worker bootstrap.

## Verification

A focused local execution of the A2 architecture suite passed:

5 passed in 0.29s

The initial route test exposed FastAPI default /docs, /redoc and /openapi.json
routes. Those routes were then disabled and the complete focused suite passed.

The GitHub workflow is now present for future cleanroom pushes and pull requests.
The available workflow-status query did not return a push run for this branch, so
GitHub CI completion is not claimed here.

## Branch isolation

A2 changes exist only on gorila-crypto-cleanroom. Production remains on
gorila-argentum-v0-hardening and no Render deployment was triggered.

## Next gate

A3 — storage isolation, followed by A4 Binance WebSocket ingestion.
