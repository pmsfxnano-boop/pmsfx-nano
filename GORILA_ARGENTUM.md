# Gorila Argentum V0

Independent Argentina-focused market intelligence engine.

Current batch:
- L00 foundation: FastAPI + storage + health.
- L01 data fabric: ArgentinaDatos FX/EMBI+, BCRA adapter, optional Twelve Data, explicit BYMA state.
- L02 fast ingestion: concurrent batch execution, deduplication, source health and latency.
- L04 coupling matrix: persisted dynamic structural-correlation state.

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

### Coupling experiment: direct predictor
The first direct coupling augmentation used point-in-time peer pressure + FX return + EMBI delta. On 1,135 samples per symbol with 908 train / 227 test, the advanced vector increased test Brier for all six core symbols relative to the baseline vector:
- GGAL +0.00265
- BMA +0.00068
- YPFD +0.00099
- PAMP +0.00239
- TGSU2 +0.00065
- CEPU +0.00375

Decision: do not use this coupling vector in the directional predictor V0.

### Coupling experiment: structural-regime features
A second point-in-time OOS test added five rolling coupling-state features to the same baseline predictor. It also degraded performance on all six symbols:
- GGAL: delta accuracy -0.07240, delta Brier +0.09505
- BMA: delta accuracy -0.02262, delta Brier +0.07007
- YPFD: delta accuracy +0.00000, delta Brier +0.05268
- PAMP: delta accuracy -0.02262, delta Brier +0.02642
- TGSU2: delta accuracy -0.00452, delta Brier +0.03950
- CEPU: delta accuracy -0.02715, delta Brier +0.02013

Decision: structural coupling is not promoted into the V0 predictor.

### Coupling experiment: gating the existing predictor
A third point-in-time OOS test left the baseline predictor unchanged and only partitioned its test behavior by coupling-score terciles learned from the training period.

The high-versus-low coupling-state Brier difference was:
- GGAL: +0.01589
- BMA: -0.00860
- YPFD: -0.00059
- PAMP: -0.01663
- TGSU2: -0.01452
- CEPU: -0.01225

The sign is mixed across assets, and the high-sync test subsets were small (29–41 observations per symbol). This is not enough evidence for a universal confidence gate, signal suppression rule, or threshold.

Decision: coupling remains diagnostic/state information only. It is not part of the V0 directional predictor or its signal-gating logic.

### Current engineering position
The experiments have now answered the first coupling question: coupling is observable and operational, but the tested formulations do not add robust OOS predictive value.

The next work should therefore move away from forcing coupling into prediction and focus on the components that can be measured directly against the final objective: stronger point-in-time feature construction, purged walk-forward validation, multi-horizon evaluation, calibration, and realistic execution-aware metrics. Coupling can remain in the command center as context and as a future research variable.

The CI uses pipefail, all four research artifacts are uploaded, and the latest Gorila Argentum CI run completed successfully.
