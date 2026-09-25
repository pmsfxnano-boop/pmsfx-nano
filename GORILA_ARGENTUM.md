# Gorila Argentum V0

Independent Argentina-focused market intelligence engine.

Current batch:
- L00 foundation: FastAPI + storage + health.
- L01 data fabric: ArgentinaDatos FX/EMBI+, BCRA adapter, optional Twelve Data, explicit BYMA state.
- L02 fast ingestion: concurrent batch execution, deduplication, source health and latency.
- L04 first coupling matrix: persisted structural-correlation snapshots.

Runtime:
uvicorn gorila_argentum.app:app --host 0.0.0.0 --port $PORT

Batch worker:
python -m gorila_argentum.worker

Production persistence:
set DATABASE_URL to PostgreSQL. SQLite fallback is development-only.

Data access policy:
uses documented/public endpoints and configured vendor APIs; does not bypass authentication, paywalls or contractual controls.
