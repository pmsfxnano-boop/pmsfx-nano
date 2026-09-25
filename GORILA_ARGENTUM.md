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


## Evidence checkpoint — 2026-09-25

### Real Argentine V0 OOS
Using 5 years of public daily history for the six core symbols, chronological 80/20 split:
- GGAL: accuracy 0.5542, Brier 0.24317, Brier skill +0.01188.
- BMA: accuracy 0.5333, Brier 0.24774, Brier skill +0.00464.
- YPFD: accuracy 0.4958, Brier 0.25672, Brier skill -0.03147.
- PAMP: accuracy 0.5500, Brier 0.27041, Brier skill -0.08163.
- TGSU2: accuracy 0.4458, Brier 0.27216, Brier skill -0.10365.
- CEPU: accuracy 0.5625, Brier 0.25205, Brier skill -0.01839.

These are research measurements, not a production signal.

### Coupling experiment
The first direct coupling augmentation used point-in-time peer pressure + FX return + EMBI delta. On 1,135 samples per symbol with 908 train / 227 test, the advanced vector increased test Brier for all six core symbols relative to the baseline vector:
- GGAL +0.00265
- BMA +0.00068
- YPFD +0.00099
- PAMP +0.00239
- TGSU2 +0.00065
- CEPU +0.00375

Therefore the coupling matrix is not promoted into the directional predictor V0.

### Engineering decision
The coupling engine is retained as a structural state engine. The next research path is:
coupling level -> coupling change -> synchronisation regime -> regime-conditioned predictor.

The CI is configured with pipefail, so research-script failures cannot be masked by tee.
