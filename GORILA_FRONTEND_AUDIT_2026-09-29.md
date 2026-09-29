# Gorila Argentum — Frontend / UI Audit
Date: 2026-09-29
Branch: gorila-argentum-v0-hardening

## Findings

### 1. Root serving path
The public Gorila service serves exactly one UI at `/`: `gorila_argentum/dashboard_terminal.py`.
This is imported by `gorila_argentum/app.py` as `DASHBOARD_HTML`.

### 2. Legacy interfaces
`gorila_argentum/dashboard.py` is the PMSF-X main service dashboard used by `main.py`; it is not the Gorila root.
`static/index.html` is not mounted by the Gorila FastAPI service.
These files are therefore decoupled from Gorila rather than deleted, avoiding accidental damage to the separate PMSF-X service.

### 3. Critical blank/static failure
The Gorila matrix renderer called `toFixed(1)` on `signal_score` without guarding `null`.
When a symbol was `NO_DATA`, that exception could abort matrix rendering and leave the interface apparently static/blank.
This is now fail-safe: unavailable scores render as `—` and never abort the UI.

### 4. Existing chart data was not surfaced
The backend already produced chart rows inside `/api/gorila/terminal/{ticker}`, selecting `close_1m`, `close_5m`, or daily `close`.
The active Gorila frontend did not consume those rows, so the page had no real market chart.
This is now replaced by a dedicated `/api/gorila/chart/{ticker}` contract.

### 5. New professional chart behavior
The terminal now provides:
- free selection of the six supported Argentina equities;
- 1D / 5D / 1M / 3M / 6M / 1Y ranges;
- real backend series with explicit effective resolution;
- responsive canvas rendering;
- adaptive price scale;
- grid + time axis;
- area fill + high-contrast line;
- last-price marker;
- crosshair and hover tooltip;
- live change percentage;
- coverage and update metadata;
- automatic chart refresh without replacing the selected instrument.

### 6. Architecture rule
The chart is visualization only. It does not mutate G2, PIT calibration, promotion gates, controller limits, or Block R.
All research/execution safety flags remain unchanged.

## Verification
- `pytest`: green on chart/frontend contract commit.
- Gorila Hardening CI: green.
- Learning Integrity: green.
- A dedicated production E2E check now verifies both the HTML chart contract and `/api/gorila/chart/GGAL?timeframe=1D`.
- Render service: `gorila-argentum-research`, branch `gorila-argentum-v0-hardening`.

## Remaining operational gate
Render had been serving commit `86b5a197...` despite the branch being correct. A manual rollout was therefore triggered for the chart commit so the public service can be verified against the exact frontend artifact.

## 7. Screenshot data-status audit — 2026-09-29

The screenshots show several different conditions that were being collapsed into the same NO DATA/warning language.

### Confirmed real conditions
- NO_FORECAST is real in the active Gorila Argentina snapshot path. The snapshot builder currently constructs the state with forecast=None; there is no validated forecast bound to this local snapshot.
- MODEL_NOT_VALIDATED is therefore also real for this snapshot. It does not mean the whole research program or frozen G2 artifact is invalid; it means the active Argentina snapshot has no validated forecast/evaluation bound to it.
- PROMOTION BLOCKED remains a real independent research gate and was not changed.
- The V2 cross-sectional numbers can remain present while the local forecast is unavailable because they come from a separate canonical-daily relative-alpha surface.

### Confirmed misleading/over-broad conditions
- DATA_NOT_FRESH was being emitted when there was no forecast at all. This conflated no forecast with stale data. The flag is now only emitted when a forecast/evidence snapshot exists and its freshness is actually below threshold.
- NO LIVE FEED was being shown after the market closed whenever the live cache was empty. The live API now returns MARKET_CLOSED in that situation, so the UI does not misclassify a closed session as a missing feed.
- NO DATA was an ambiguous UI label because the real market series/chart existed. The terminal now renders FORECAST UNAVAILABLE for this condition and NOT SCORED · FORECAST UNAVAILABLE in the validation panel. A historical chart close is used as a non-live price fallback in the detail view when available.

### Drift / PSI finding
The screenshot's PSI = 8.3 with DRIFT_ALERT is not accepted as an operational model-drift gate in the current terminal. The active signal path was reading latest_drift(symbol, field=close), while the drift implementation computes PSI on the supplied numeric level series. Raw price levels are non-stationary and are not a valid stand-alone model-distribution drift contract for this gate.

The current branch contains no active call site that writes new save_drift() snapshots for this live Argentina signal path. Persisted raw-close snapshots are therefore treated as legacy/unbound telemetry rather than allowed to halt the research runtime.

Operational drift snapshots are now accepted only when they carry a versioned methodology binding and a stationary feature/prediction field. Legacy raw-close snapshots are ignored for the circuit breaker and reported as unavailable/degraded rather than as a fresh DRIFT_ALERT.

### Safety consequence
This does not weaken promotion. A missing/unbound drift monitor does not become permission to act: the signal engine marks drift as UNAVAILABLE and requires an eligible drift state (OK/WARN/WATCH) for an actionable research signal. Promotion remains independently blocked until its existing research evidence gates pass.

### Current UI/data interpretation
The visual design, colors, chart, and layout are intentionally unchanged. Only semantic status labels and data-state plumbing were corrected.
