"""Production entrypoint for Gorila Argentum.

The production service uses the mature PMSF-X application as its quantitative
engine and adds the Argentina data fabric, durable research controls and the
Gorila control surface on the same FastAPI instance.
"""

from __future__ import annotations

import asyncio
import os
import time

import httpx
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import Header, HTTPException

from main import (
    app,
    get_cached_bars,
    market_session_state,
    normalize_ticker,
    outcome_summary,
    persistence_summary,
    quote as advanced_quote,
    state as advanced_state,
    stream_status,
)
from .audit import build_audit_state
from .config import settings
from .control import build_control_state
from .coupling import current_coupling_state
from .dashboard import HTML as DASHBOARD_HTML
from .drift import rolling_drift
from .features import build_features
from .promotion import CURRENT_BATCH10_EVIDENCE, evaluate_promotion
from .regime import classify_regime
from .security import require_runtime_tick_key, require_internal_key
from .shadow import compute_shadow_outcome, validate_shadow_prediction
from .state import build_market_state
from .storage import Store
from .sources import argentina_datos_fx, argentina_datos_risk, bcra_fx
from scripts.gorila_runtime_tick import run_tick as run_runtime_tick

# The public service uses a single process. The cache keeps the latency-critical
# UI path independent of the expensive historical/multi-horizon research call.
_FORECAST_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_FORECAST_LOCK = asyncio.Lock()
_MACRO_TASK: asyncio.Task | None = None
_MACRO_STATE: dict[str, Any] = {
    "status": "STARTING",
    "updated_at": None,
    "last_result": None,
}
_MACRO_INTERVAL_SECONDS = max(
    60,
    int(__import__("os").getenv("GORILA_MACRO_INTERVAL_SECONDS", "300")),
)

# The mature PMSF-X Render service already owns the verified Tiingo connection.
# Gorila can read that research engine when its own Tiingo secret is not present.
_UPSTREAM_ENGINE_URL = os.getenv(
    "GORILA_UPSTREAM_ENGINE_URL",
    "https://pmsfx-nano.onrender.com",
).rstrip("/")
_UPSTREAM_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_UPSTREAM_LOCK = asyncio.Lock()
_UPSTREAM_CACHE_SECONDS = 10.0


async def _upstream_state(symbol: str, *, force: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    cached = _UPSTREAM_CACHE.get(symbol)
    if cached and not force and now - cached[0] < _UPSTREAM_CACHE_SECONDS:
        return dict(cached[1])

    async with _UPSTREAM_LOCK:
        now = time.monotonic()
        cached = _UPSTREAM_CACHE.get(symbol)
        if cached and not force and now - cached[0] < _UPSTREAM_CACHE_SECONDS:
            return dict(cached[1])

        async with httpx.AsyncClient(timeout=12.0) as client:
            response = await client.get(
                f"{_UPSTREAM_ENGINE_URL}/api/state/{symbol}",
                headers={"User-Agent": "Gorila-Argentum/1.0"},
            )
        if response.status_code >= 400:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"upstream_engine_http_{response.status_code}",
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("upstream_engine_invalid_payload")
        print(
            "GORILA_ENGINE_BRIDGE",
            {
                "symbol": symbol,
                "source": "PMSF_X",
                "forecast_status": payload.get("forecast_status"),
                "forecast_present": payload.get("forecast") is not None,
            },
            flush=True,
        )
        _UPSTREAM_CACHE[symbol] = (time.monotonic(), dict(payload))
        return payload


def _upstream_forecast_payload(symbol: str, state: dict[str, Any]) -> dict[str, Any]:
    result = dict(state)
    result["engine_source"] = "upstream_pmsf_x"
    result["engine_url"] = _UPSTREAM_ENGINE_URL
    result["symbol"] = symbol
    result.setdefault(
        "forecast_status",
        "READY" if result.get("forecast") is not None else "NO_FORECAST",
    )
    return result


def _upstream_quote_payload(symbol: str, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "upstream_pmsf_x",
        "received_at": state.get("received_at"),
        "symbol": symbol,
        "quote": {
            "last": state.get("last"),
            "bidPrice": state.get("bid"),
            "askPrice": state.get("ask"),
            "bidSize": state.get("bid_size"),
            "askSize": state.get("ask_size"),
            "quoteTimestamp": state.get("quote_timestamp"),
            "timestamp": state.get("quote_timestamp"),
        },
    }


async def _gorila_forecast(symbol: str, *, force: bool = False) -> dict[str, Any]:
    if os.getenv("TIINGO_API_KEY", "").strip():
        return await gorila_forecast(symbol, force=force)
    state = await _upstream_state(symbol, force=force)
    return _upstream_forecast_payload(symbol, state)


async def _gorila_quote(symbol: str) -> dict[str, Any]:
    if os.getenv("TIINGO_API_KEY", "").strip():
        return await advanced_quote(symbol)
    state = await _upstream_state(symbol)
    return _upstream_quote_payload(symbol, state)



def _run_macro_ingest() -> dict[str, Any]:
    store = Store()
    store.init()
    funcs = [argentina_datos_fx, argentina_datos_risk, bcra_fx]
    results = []
    with ThreadPoolExecutor(max_workers=len(funcs)) as executor:
        futures = [executor.submit(fn) for fn in funcs]
        for future in futures:
            results.append(future.result())

    rows_inserted = 0
    for result in results:
        if result.rows:
            inserted = store.insert_observations(result.rows)
            rows_inserted += inserted
            store.upsert_health(
                result.source,
                "HEALTHY",
                rows=len(result.rows),
                latency_ms=result.latency_ms,
                success=True,
            )
        else:
            store.upsert_health(
                result.source,
                "DEGRADED",
                last_error=result.error,
                rows=0,
                latency_ms=result.latency_ms,
                success=False,
            )

    payload = {
        "status": "COMPLETED" if all(r.rows for r in results) else "DEGRADED",
        "rows_inserted": rows_inserted,
        "results": [
            {
                "source": r.source,
                "rows": len(r.rows),
                "error": r.error,
                "latency_ms": round(float(r.latency_ms or 0), 2),
            }
            for r in results
        ],
    }
    return payload


async def _macro_loop() -> None:
    while True:
        started = time.perf_counter()
        try:
            result = await asyncio.to_thread(_run_macro_ingest)
            _MACRO_STATE.update(
                {
                    "status": result.get("status", "UNKNOWN"),
                    "updated_at": time.time(),
                    "last_result": result,
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                }
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _MACRO_STATE.update(
                {
                    "status": "ERROR",
                    "updated_at": time.time(),
                    "last_result": {
                        "status": "ERROR",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                }
            )
        await asyncio.sleep(_MACRO_INTERVAL_SECONDS)


@app.on_event("startup")
async def gorila_runtime_startup() -> None:
    global _MACRO_TASK
    Store().init()
    if _MACRO_TASK is None or _MACRO_TASK.done():
        _MACRO_TASK = asyncio.create_task(
            _macro_loop(),
            name="gorila-argentina-macro-loop",
        )


@app.on_event("shutdown")
async def gorila_runtime_shutdown() -> None:
    global _MACRO_TASK
    if _MACRO_TASK is not None:
        _MACRO_TASK.cancel()
        try:
            await _MACRO_TASK
        except asyncio.CancelledError:
            pass
    _MACRO_TASK = None


@app.get("/", include_in_schema=False)
def gorila_root():
    return DASHBOARD_HTML


@app.get("/api/gorila/health")
def gorila_health():
    tiingo_configured = bool(os.getenv("TIINGO_API_KEY", "").strip())
    upstream_available = bool(_UPSTREAM_ENGINE_URL)
    engine_ready = tiingo_configured or upstream_available
    return {
        "service": "gorila-argentum",
        "mode": "RESEARCH",
        "trading_execution": False,
        "automatic_promotion": False,
        "primary_market_data": {
            "provider": "TIINGO",
            "configured": tiingo_configured,
            "upstream_engine": "PMSF-X" if upstream_available else None,
            "source": "LOCAL_TIINGO" if tiingo_configured else ("UPSTREAM_PMSF_X" if upstream_available else "NONE"),
            "status": "READY" if engine_ready else "MISSING_FEED",
        },
        "database": persistence_summary(),
        "market_stream": stream_status(),
        "market_session": market_session_state(),
        "macro_ingest": {
            **_MACRO_STATE,
            "updated_at": (
                __import__("datetime").datetime.fromtimestamp(
                    _MACRO_STATE["updated_at"],
                    tz=__import__("datetime").timezone.utc,
                ).isoformat()
                if _MACRO_STATE.get("updated_at")
                else None
            ),
        },
        "sources": Store().health(),
    }


@app.get("/api/gorila/market")
def gorila_market():
    return build_market_state()


@app.get("/api/gorila/sources")
def gorila_sources():
    return {
        "macro_runtime": _MACRO_STATE,
        "sources": Store().health(),
    }


@app.get("/api/gorila/forecast/{ticker}")
async def gorila_forecast(ticker: str, force: bool = False):
    symbol = normalize_ticker(ticker)
    now = time.monotonic()
    cached = _FORECAST_CACHE.get(symbol)
    if cached and not force and now - cached[0] < 30.0:
        result = dict(cached[1])
        result["cache"] = {"hit": True, "age_seconds": round(now - cached[0], 3)}
        return result

    async with _FORECAST_LOCK:
        now = time.monotonic()
        cached = _FORECAST_CACHE.get(symbol)
        if cached and not force and now - cached[0] < 30.0:
            result = dict(cached[1])
            result["cache"] = {"hit": True, "age_seconds": round(now - cached[0], 3)}
            return result

        if os.getenv("TIINGO_API_KEY", "").strip():
            result = await advanced_state(symbol)
        else:
            result = _upstream_forecast_payload(
                symbol,
                await _upstream_state(symbol, force=force),
            )
        _FORECAST_CACHE[symbol] = (time.monotonic(), dict(result))
        result["cache"] = {"hit": False, "age_seconds": 0.0}
        return result


@app.get("/api/gorila/terminal/{ticker}")
async def gorila_terminal(ticker: str):
    symbol = normalize_ticker(ticker)
    quote_task = asyncio.create_task(_gorila_quote(symbol))
    forecast_task = asyncio.create_task(_gorila_forecast(symbol))
    macro_task = asyncio.to_thread(build_market_state)

    quote_result = None
    quote_error = None
    try:
        quote_result = await quote_task
    except Exception as exc:
        quote_error = f"{type(exc).__name__}: {exc}"

    try:
        forecast = await forecast_task
    except Exception as exc:
        forecast = {
            "symbol": symbol,
            "forecast_status": "ERROR",
            "forecast": None,
            "error": f"{type(exc).__name__}: {exc}",
        }

    macro = await macro_task
    bars = get_cached_bars(symbol) or []
    chart = [
        {
            "time": row.get("date") or row.get("timestamp"),
            "close": row.get("close"),
        }
        for row in bars[-180:]
        if row.get("close") is not None
    ]
    return {
        "service": "gorila-argentum",
        "symbol": symbol,
        "quote": quote_result,
        "quote_error": quote_error,
        "forecast": forecast,
        "macro": macro,
        "chart": chart,
        "stream": stream_status(),
        "session": market_session_state(),
    }


# ---------------------------------------------------------------------------
# Compatibility/observability surface used by the research control panel.
# ---------------------------------------------------------------------------

@app.get("/api/state/live")
def legacy_live_state():
    return build_market_state()


@app.get("/api/features/{symbol}")
def legacy_features(symbol: str):
    return build_features(symbol)


@app.get("/api/regime/{symbol}")
def legacy_regime(symbol: str):
    from math import log

    store = Store()
    store.init()
    series = store.recent_series(symbol, "close", limit=40)
    returns_bps = [
        10000.0 * log(b / a)
        for (_, a), (_, b) in zip(series, series[1:])
        if a > 0 and b > 0
    ]
    fx_series = store.recent_series("USD_MEP", "sell", limit=2)
    risk_series = store.recent_series("EMBI_ARG", "embi_bps", limit=2)
    fx_stress = None
    risk_delta = None
    if len(fx_series) == 2 and fx_series[-2][1] != 0:
        fx_stress = (fx_series[-1][1] / fx_series[-2][1]) - 1.0
    if len(risk_series) == 2:
        risk_delta = risk_series[-1][1] - risk_series[-2][1]
    return classify_regime(
        returns_bps,
        fx_stress=fx_stress,
        risk_delta_bps=risk_delta,
    )


@app.get("/api/drift")
def drift_summary(symbol: str | None = None, field: str | None = None, limit: int = 100):
    store = Store()
    store.init()
    return {"items": store.latest_drift(symbol=symbol, field=field, limit=limit)}


@app.get("/api/coupling")
def coupling():
    pairs = [
        ("USD_MEP", "sell", "USD_CCL", "sell"),
        ("USD_BLUE", "sell", "USD_MEP", "sell"),
        ("USD_MEP", "sell", "EMBI_ARG", "embi_bps"),
        ("USD_CCL", "sell", "EMBI_ARG", "embi_bps"),
        ("USD_MEP", "sell", "USD_BCRA", "reference"),
    ]
    return current_coupling_state(pairs)


@app.get("/api/control")
def control():
    return build_control_state()


@app.get("/api/audit")
def audit():
    return build_audit_state()


@app.get("/api/runtime/runs")
def runtime_runs(kind: str | None = None, limit: int = 20):
    store = Store()
    store.init()
    return {
        "items": store.latest_runtime_run(
            kind=kind,
            limit=max(1, min(100, int(limit))),
        )
    }


@app.post("/api/runtime/tick")
def runtime_tick(
    x_gorila_runtime_key: str | None = Header(
        default=None,
        alias="X-Gorila-Runtime-Key",
    ),
):
    require_runtime_tick_key(x_gorila_runtime_key)
    try:
        return run_runtime_tick()
    except RuntimeError as exc:
        if str(exc) == "durable_storage_required":
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise


@app.post("/api/shadow/prediction")
def create_shadow_prediction(
    symbol: str,
    probability_up: float,
    horizon_seconds: int,
    entry_price: float,
    regime: str = "UNKNOWN",
    feature_hash: str = "",
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    # Shadow creation is a write operation. It remains strictly research-gated:
    # durable Postgres is mandatory and the circuit breaker must be NORMAL.
    require_internal_key(x_gorila_internal_key)
    store = Store()
    store.init()
    control_state = build_control_state(store)
    runtime = control_state.get("runtime") or {}
    if not store.pg or runtime.get("circuit_breaker") != "NORMAL":
        reasons = runtime.get("circuit_breaker_reasons") or ["RESEARCH_CIRCUIT_BREAKER_HALTED"]
        raise HTTPException(
            status_code=409,
            detail={
                "code": "RESEARCH_CIRCUIT_BREAKER_HALTED",
                "reasons": reasons,
            },
        )
    validated = validate_shadow_prediction(
        symbol=symbol,
        probability_up=probability_up,
        horizon_seconds=horizon_seconds,
        entry_price=entry_price,
    )
    return store.save_shadow_prediction(
        validated["symbol"],
        "Gorila-Advanced",
        validated["probability_up"],
        validated["horizon_seconds"],
        regime,
        validated["entry_price"],
        feature_hash=feature_hash,
        metadata={"source": "gorila_production_shadow"},
    )

@app.get("/api/shadow")
def shadow(limit: int = 50, symbol: str | None = None, status: str | None = None):
    store = Store()
    store.init()
    return {
        "items": store.latest_shadow(
            symbol=symbol,
            status=status,
            limit=limit,
        )
    }


@app.get("/api/shadow/summary")
def shadow_summary():
    store = Store()
    store.init()
    return store.shadow_summary()


@app.get("/api/promotion")
def promotion():
    store = Store()
    store.init()
    return {
        "current_evaluation": evaluate_promotion(CURRENT_BATCH10_EVIDENCE),
        "latest_decision": store.latest_promotion_decision(),
    }


@app.get("/api/learning")
def learning(symbol: str | None = None, limit: int = 20):
    store = Store()
    store.init()
    return {"items": store.latest_learning(symbol=symbol, limit=limit)}


@app.get("/api/recalibration")
def recalibration(limit: int = 10):
    store = Store()
    store.init()
    return {"items": store.latest_calibration(limit=limit)}
