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
