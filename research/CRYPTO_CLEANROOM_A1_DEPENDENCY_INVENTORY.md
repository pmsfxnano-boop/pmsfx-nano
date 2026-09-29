# Gorila Crypto Cleanroom — A1 Dependency Inventory

Date: 2026-09-29
Status: COMPLETE
Scope: repository/runtime dependency mapping before any destructive decoupling.

## 1. Active production boundary verified
- Gorila Render service: gorila-argentum-research
- Production branch: gorila-argentum-v0-hardening
- Build: pip install -r requirements-gorila.txt
- Start: uvicorn gorila_argentum.app:app --host 0.0.0.0 --port $PORT
- Crypto cleanroom is a separate safety boundary. No production service is changed by A1.

## 2. Critical finding
The current Gorila application is heavily Argentina-specific at the orchestration layer. The cleanroom must NOT attempt to hide that coupling behind flags. The correct move is to create a Crypto runtime boundary that does not import or schedule Argentina paths.
US-equity functionality exists in a separate legacy/sibling application entrypoint (main.py) centered on Tiingo IEX and the AAPL/MSFT/NVDA/TSLA cohort. It is not imported by gorila_argentum.app and is therefore not an active dependency of the Gorila Argentina Render service. It remains outside the Crypto runtime and must not be imported into it.

## 3. Classification

### A. KEEP AS SHARED INFRASTRUCTURE CANDIDATES

gorila_argentum/market_freshness.py
- Generic event-time/receive-time freshness engine.
- LIVE / DELAYED / STALE / INVALID_TIMESTAMP.
- Must become universal Data Fabric infrastructure.
- Do not delete or rewrite during the domain split.

gorila_argentum/storage.py
- Generic persistence primitives plus shadow/promotion/runtime tables.
- Current observations table is useful legacy persistence but is NOT sufficient as the final Crypto event ledger.
- Crypto should receive explicit event/sequence/order-book tables rather than forcing microstructure events into daily observation semantics.

gorila_argentum/security.py
- Runtime/internal key validation; domain-neutral.

gorila_argentum/calibration.py
gorila_argentum/drift.py
gorila_argentum/shadow.py
gorila_argentum/audit.py
gorila_argentum/control.py
gorila_argentum/promotion.py
- Potential shared research-control infrastructure.
- Do not import into Crypto blindly.
- Each will be contract-reviewed before reuse because current evidence/model identifiers contain Argentina-era assumptions.

gorila_argentum/regime.py
gorila_argentum/features.py
gorila_argentum/coupling.py
- Potentially reusable mathematical infrastructure, but no reuse until feature/schema assumptions are audited.

## 4. ARGENTINA-SPECIFIC RUNTIME — EXCLUDE FROM CRYPTO

gorila_argentum/app.py
- Direct coupling to Argentina session clock and BYMA calendar.
- BCRA / ArgentinaDatos macro loops.
- BYMA live loop.
- Twelve Data Argentine live fallback.
- Argentina signal snapshot loop.
- Argentina cross-sectional endpoint.
- BCRA endpoint.
- Argentine live quote endpoint.
- G2 endpoints.
- Argentina signal matrix.
- Argentina chart/terminal orchestration.
- Argentina-specific E2E/self-tests.
- Argentina source-health semantics.
- Imports the mixed source module, bcra_macro, cross_sectional_live and the AR signal engine.
- For Crypto, replace orchestration with a clean domain application boundary rather than surgically flagging dozens of branches.

gorila_argentum/config.py
- Default AR symbol universe.
- BCRA URLs.
- ArgentinaDatos URLs.
- BYMA URLs.
- BYMA SSL/history settings.
- Twelve Data credentials/interval.
- AR core symbols.
- Crypto must use a separate domain configuration contract.

gorila_argentum/sources.py
- Mixed source module containing ArgentinaDatos, BCRA, Twelve Data, BYMA, Rava, and Yahoo adapters.
- Crypto must never import this mixed module.
- It must eventually be split by domain.

gorila_argentum/ingest.py
- Argentina daily ingestion: BYMA, Rava, Twelve Data, BCRA/ArgentinaDatos, canonical reconciliation, AR health policy.
- Do not run from Crypto runtime.

gorila_argentum/bcra_macro.py
- Argentina-only macro ingestion/trader snapshot.

gorila_argentum/canonical_data.py
- Argentina daily canonical fabric.
- America/Argentina/Buenos_Aires session timezone.
- BYMADATA/Rava/TwelveData source ranking.
- Yahoo exclusion.
- canonical_daily and quarantine tables.
- daily close semantics.
- Legacy AR research only; not the Crypto event fabric.

gorila_argentum/signal_engine.py
- CORE_SYMBOLS = GGAL, BMA, YPFD, PAMP, TGSU2, CEPU.
- Current score/forecast contract is tied to the AR snapshot architecture.
- Crypto receives a separate signal/forecast domain after Opportunity Engine validation.

gorila_argentum/cross_sectional_live.py
- Current fixed-pooled-logit V2 model is daily AR.
- Six AR symbols, canonical daily dataset, 5-day horizon, relative-outperformance semantics.
- Do not use as Crypto model.

scripts/gorila_runtime_tick.py
- Current autonomous tick calls AR batch ingestion, AR canonical daily learning, AR 5-day learner, AR recalibration, AR V2 evidence sync and AR promotion/evidence flow.
- Crypto must not call this runtime tick.
- A new Crypto autonomous runtime will be created later.

gorila_argentum/dashboard_terminal.py
- Current terminal is AR: AR universe matrix, BYMA session, BCRA display, AR signal detail, V2 relative alpha and AR endpoint polling.
- UI will be replaced by a Crypto terminal after backend cleanroom stability.

## 5. US-EQUITY / TIINGO LEGACY — EXCLUDE FROM CRYPTO

main.py
- Separate PMSF-X service entrypoint.
- Tiingo IEX HTTP and WebSocket paths.
- AAPL/MSFT/NVDA/TSLA collector cohort.
- US Eastern session logic.
- US online cohort and V0 OFI research.
- This file is not imported by gorila_argentum.app.
- Preserve as legacy research; do not import into Crypto.

quant/v0_ofi.py
- Separate legacy US-equity research module used by the PMSF-X path.
- Not a Crypto dependency.

## 6. Frontend calls that must disappear from Crypto UI
- /api/gorila/signal-matrix
- /api/gorila/cross-sectional
- /api/gorila/signal/{ticker}
- /api/gorila/bcra
- /api/gorila/live/{ticker}
- /api/gorila/chart/{ticker} in its current AR implementation
- /api/gorila/control and AR health payloads
- /api/gorila/terminal/{ticker}
- AR session and source-fabric display
Crypto UI will call only Crypto-domain APIs after the new backend contract exists.

## 7. Data-store separation requirement
The generic observations table contains symbol, field, value, event_time, received_time, source, latency_ms, quality and metadata. This is useful provenance, but not enough for order-book deltas, trade IDs, update IDs, sequence gaps, local book state, reconnect/resync events or opportunity lifecycle.
Crypto therefore requires explicit schemas/tables for the event ledger and microstructure. Do not overload canonical_daily.

## 8. A1 dependency decisions
- Argentina network calls: REMOVE FROM CRYPTO RUNTIME.
- US/Tiingo network calls: REMOVE FROM CRYPTO RUNTIME.
- Argentina background tasks: REMOVE FROM CRYPTO RUNTIME.
- US background tasks: REMOVE FROM CRYPTO RUNTIME.
- AR/US frontend surfaces: REMOVE FROM CRYPTO UI.
- AR/US model artifacts: PRESERVE AS LEGACY / ROLLBACK.
- Freshness Core: KEEP and generalize.
- Generic persistence/security/audit primitives: KEEP behind explicit interfaces.
- G2/PIT/controller/Block R: PRESERVE FROZEN; do not mutate as part of Crypto cleanroom.
- Autonomous runtime: REBUILD FOR CRYPTO; do not reuse AR runtime tick.

## 9. Next stage
A2 — Runtime isolation:
1. Introduce the Crypto app entrypoint.
2. Introduce domain-neutral configuration.
3. Guarantee zero AR/US network calls and zero AR/US background tasks in the Crypto process.
4. Add architectural tests that fail on forbidden imports/routes.
5. Keep the AR production branch untouched.
6. No deletion of legacy files until A2 tests prove the cleanroom is isolated.
