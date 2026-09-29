# Gorila Crypto Cleanroom — Baseline Freeze

Date: 2026-09-29
Purpose: immutable engineering baseline before any Argentina/US runtime decoupling or Crypto-domain construction.

## Baseline source
- Repository: pmsfxnano-boop/pmsfx-nano
- Baseline branch: gorila-argentum-v0-hardening
- Cleanroom branch: gorila-crypto-cleanroom
- Baseline commit: 35fc16c8f92672941c5abcb23ceafa6ea059e0f2
- Baseline commit message: Add regression tests for forecast probability semantics
- Production Render service: gorila-argentum-research
- Production service ID: srv-dargqovpn0mc73cgi320
- Last verified live deploy before cleanroom work: dep-datul9ou01pc73ajnq50
- Production URL: https://gorila-argentum-research.onrender.com

## Scientific state that must not be modified by cleanroom work
- G2 frozen artifact/model identity and PIT evidence
- walk-forward/OOS contracts
- calibration contracts and automatic-promotion gates
- HJB/controller logic
- Block R prospective holdout
- existing shadow/promotion/audit semantics
- universal market-freshness semantics already proven in production
- existing production regression suite

## Cleanroom objective
The Crypto domain will be constructed as an isolated research/runtime domain. Argentina and US-equity logic will first be disabled from the Crypto runtime and frontend, then removed from the Crypto service perimeter only after dependency and regression checks pass.

## Non-negotiable separation rules
1. No Crypto code may call BCRA, BYMA, ArgentinaDatos, Rava, IOL, Yahoo, or Twelve Data.
2. No Crypto predictor may consume Argentina or US-equity features by implicit import, shared mutable cache, or shared symbol registry.
3. The existing freshness/event-time semantics are infrastructure, not domain logic, and must survive unchanged.
4. The Crypto event path must preserve event_time, received_time, provenance, sequence/update identity where available, and quality status.
5. No new data source becomes a model feature merely because it is available. It must pass replay, PIT, OOS, cost/slippage, and stress gates.
6. Production is not modified as part of this freeze step.

## Phase gates
- A0 baseline freeze: COMPLETE when this branch exists at the exact verified baseline commit and this document is persisted.
- A1 dependency inventory: enumerate all active Argentina/US imports, routes, tasks, writes, config, and frontend calls.
- A2 runtime isolation: Crypto service starts without Argentina/US network calls or background tasks.
- A3 storage isolation: Crypto event tables/namespaces are explicit and provenance-safe.
- A4 WebSocket ingestion: Binance event stream with reconnect, sequence integrity, and freshness checks.
- A5 event ledger/replay: deterministic reconstruction of what was known and when.
- A6 lead/lag and opportunity clock: shadow only.
- A7 forecast integration: only after evidence.
- A8 OOS/economic validation: no promotion on in-sample results.

## Rollback rule
The original Argentina production branch remains untouched. Any cleanroom failure must roll back by deployment/branch selection, never by destructive edits to the Argentina baseline.
