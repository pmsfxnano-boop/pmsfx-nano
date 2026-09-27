"""Production entrypoint for Gorila Argentum.

The production service uses the mature PMSF-X application as its quantitative
engine and adds the Argentina data fabric, durable research controls and the
Gorila control surface on the same FastAPI instance.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import httpx
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from datetime import datetime, time as dt_time, timezone
from zoneinfo import ZoneInfo

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
from .dashboard_terminal import HTML as DASHBOARD_HTML
from .drift import rolling_drift
from .features import build_features
from .promotion import evaluate_live_promotion
from .regime import classify_regime
from .security import require_runtime_tick_key, require_internal_key
from .shadow import compute_shadow_outcome, validate_shadow_prediction
from .signal_engine import CORE_SYMBOLS as SIGNAL_SYMBOLS, build_matrix, build_signal
from .cross_sectional_live import score_universe as score_cross_sectional
from .state import build_market_state
from .storage import Store
from .sources import argentina_datos_fx, argentina_datos_risk, bcra_fx, yahoo_chart_intraday, twelve_data_intraday, twelve_data_live_quote
from .bcra_macro import bcra_macro_cycle, build_bcra_trader_snapshot
from scripts.gorila_runtime_tick import run_tick as run_runtime_tick, run_autonomous_tick
from quant.db import connection as quant_connection

# The public service uses a single process. The autonomous runtime loop is
# intentionally part of this process so research continues without a cron.
# The cache keeps the latency-critical UI path independent of the expensive
# historical/multi-horizon research call.
_FORECAST_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_FORECAST_LOCK = asyncio.Lock()
_MACRO_TASK: asyncio.Task | None = None
_MACRO_STATE: dict[str, Any] = {
    "status": "STARTING",
    "updated_at": None,
    "last_result": None,
}
_MACRO_INTERVAL_SECONDS = max(
    300,
    int(settings.macro_interval_seconds),
)

_AUTONOMOUS_INTERVAL_SECONDS = max(
    300,
    int(os.getenv("GORILA_AUTONOMOUS_INTERVAL_SECONDS", "300")),
)
_AUTONOMOUS_START_DELAY_SECONDS = max(
    10,
    int(os.getenv("GORILA_AUTONOMOUS_START_DELAY_SECONDS", "20")),
)
_AUTONOMOUS_TASK: asyncio.Task | None = None
_ARG_LIVE_TASK: asyncio.Task | None = None
_ARG_LIVE_INTERVAL_SECONDS = max(15, int(os.getenv("GORILA_LIVE_INTERVAL_SECONDS", "30")))
_ARG_LIVE_CACHE: dict[str, dict[str, Any]] = {}
_ARG_LIVE_STATE: dict[str, Any] = {"status":"STARTING","updated_at":None,"last_cycle_ms":None,"updated_symbols":0,"errors":[]}
_SIGNAL_MATRIX_CACHE: tuple[float, dict[str, Any]] | None = None
_SIGNAL_MATRIX_LOCK = asyncio.Lock()
_SIGNAL_MATRIX_CACHE_SECONDS = max(5.0, float(os.getenv("GORILA_MATRIX_CACHE_SECONDS", "15")))
_CROSS_SECTIONAL_CACHE: tuple[float, dict[str, Any]] | None = None
_CROSS_SECTIONAL_LOCK = asyncio.Lock()
_CROSS_SECTIONAL_CACHE_SECONDS = max(30.0, float(os.getenv("GORILA_CROSS_SECTIONAL_CACHE_SECONDS", "60")))
_BCRA_SNAPSHOT_CACHE: tuple[float, dict[str, Any]] | None = None
_BCRA_SNAPSHOT_CACHE_SECONDS = 60.0
_AUTONOMOUS_STATE: dict[str, Any] = {
    "status": "STARTING",
    "updated_at": None,
    "last_result": None,
    "interval_seconds": _AUTONOMOUS_INTERVAL_SECONDS,
}

# The mature PMSF-X Render service already owns the verified Tiingo connection.
# Gorila can read that research engine when its own Tiingo secret is not present.
_UPSTREAM_ENGINE_URL = os.getenv(
    "GORILA_UPSTREAM_ENGINE_URL",
    "https://pmsfx-nano.onrender.com",
).rstrip("/")
_UPSTREAM_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_UPSTREAM_LOCK = asyncio.Lock()
_UPSTREAM_CACHE_SECONDS = 10.0

ARGENTINA_TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")
ARGENTINA_SESSION_OPEN = dt_time(11, 0)
ARGENTINA_SESSION_CLOSE = dt_time(17, 0)


def argentina_session_state(now: datetime | None = None) -> dict[str, Any]:
    current = (now or datetime.now(timezone.utc)).astimezone(ARGENTINA_TIMEZONE)
    weekday = current.weekday()
    open_now = weekday < 5 and ARGENTINA_SESSION_OPEN <= current.time() <= ARGENTINA_SESSION_CLOSE
    return {
        "timezone": "America/Argentina/Buenos_Aires",
        "local_time": current.isoformat(),
        "weekday": weekday,
        "open": open_now,
        "regular_window": {
            "open": ARGENTINA_SESSION_OPEN.isoformat(),
            "close": ARGENTINA_SESSION_CLOSE.isoformat(),
        },
        "calendar_source": "BYMA",
    }



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


def _latest_persisted_engine_state(symbol: str) -> dict[str, Any] | None:
    sql = """
    SELECT
        created_at, symbol, last, bid, ask, spread_bps, microprice,
        model_id, status, direction, p_up, p_down, confidence,
        horizon_seconds, validated, gatillazo, data_source,
        quote_timestamp, evaluation
    FROM forecasts
    WHERE symbol = %(symbol)s
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    """
    try:
        with quant_connection() as conn:
            if conn is None:
                return None
            with conn.cursor() as cur:
                cur.execute(sql, {"symbol": symbol})
                row = cur.fetchone()
        if row is None:
            return None
        created_at = row[0]
        age_seconds = max(
            0.0,
            (time.time() - created_at.timestamp())
            if hasattr(created_at, "timestamp")
            else 0.0,
        )
        probability_up = float(row[10]) if row[10] is not None else None
        probability_down = float(row[11]) if row[11] is not None else (
            1.0 - probability_up if probability_up is not None else None
        )
        forecast = None
        if probability_up is not None:
            forecast = {
                "direction": row[9] or (
                    "UP" if probability_up >= 0.55
                    else ("DOWN" if probability_up <= 0.45 else "NEUTRAL")
                ),
                "raw_probability_up": probability_up,
                "raw_probability_down": probability_down,
                "confidence_raw": float(row[12]) if row[12] is not None else abs(probability_up - 0.5) * 2.0,
                "model_id": row[7],
                "status": row[8],
                "validated": bool(row[14]),
                "calibrated": False,
                "horizon_seconds": int(row[13]) if row[13] is not None else None,
            }
        return {
            "symbol": row[1],
            "last": row[2],
            "bid": row[3],
            "ask": row[4],
            "spread_bps": row[5],
            "microprice": row[6],
            "data_source": row[16],
            "quote_timestamp": row[17].isoformat() if row[17] is not None else None,
            "received_at": created_at.isoformat(),
            "forecast": forecast,
            "forecast_status": row[8],
            "gatillazo": row[15],
            "evaluation": row[18] or {},
            "model": {
                "id": row[7],
                "status": row[8],
                "validated": bool(row[14]),
            },
            "engine_source": "shared_postgres_pmsf_x",
            "engine_freshness": {
                "created_at": created_at.isoformat(),
                "age_seconds": round(age_seconds, 1),
                "stale": age_seconds > 3600.0,
            },
        }
    except Exception as exc:
        print(
            "GORILA_PERSISTED_ENGINE_READ_ERROR",
            {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"},
            flush=True,
        )
        return None


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

    persisted = _latest_persisted_engine_state(symbol)
    if persisted is not None:
        return persisted

    state = await _upstream_state(symbol, force=force)
    return _upstream_forecast_payload(symbol, state)


def _latest_persisted_engine_chart(symbol: str, limit: int = 180) -> list[dict[str, Any]]:
    sql = """
    SELECT created_at, last
    FROM forecasts
    WHERE symbol = %(symbol)s
      AND last IS NOT NULL
    ORDER BY created_at DESC, id DESC
    LIMIT %(limit)s
    """
    try:
        with quant_connection() as conn:
            if conn is None:
                return []
            with conn.cursor() as cur:
                cur.execute(sql, {"symbol": symbol, "limit": max(1, min(500, int(limit)))})
                rows = cur.fetchall()
        return [
            {"time": row[0].isoformat(), "close": float(row[1])}
            for row in reversed(rows)
            if row[1] is not None
        ]
    except Exception as exc:
        print(
            "GORILA_PERSISTED_CHART_READ_ERROR",
            {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"},
            flush=True,
        )
        return []


async def _gorila_quote(symbol: str) -> dict[str, Any]:
    if os.getenv("TIINGO_API_KEY", "").strip():
        return await advanced_quote(symbol)
    persisted = _latest_persisted_engine_state(symbol)
    if persisted is not None:
        return _upstream_quote_payload(symbol, persisted)
    state = await _upstream_state(symbol)
    return _upstream_quote_payload(symbol, state)



def _run_macro_ingest() -> dict[str, Any]:
    store = Store()
    store.init()
    funcs = [argentina_datos_fx, argentina_datos_risk, bcra_fx, bcra_macro_cycle]
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


async def _argentina_live_loop() -> None:
    """Maintain a bounded intraday cache without blocking the API process.

    Priority: configured Twelve Data intraday feed. Yahoo is opt-in only because
    repeated anonymous polling was observed to return HTTP 429 from Render.
    When BYMA is closed the loop does not hit any vendor and reports CLOSED.
    """
    await asyncio.sleep(5)
    while True:
        started = time.perf_counter()
        errors: list[dict[str, str]] = []
        updated = 0
        rows_to_store: list[dict[str, Any]] = []
        session = argentina_session_state()
        provider = "twelve_data" if settings.twelve_data_api_key else (
            "yahoo_fallback" if os.getenv("GORILA_ALLOW_YAHOO_LIVE", "0").strip().lower() in {"1","true","yes"} else "none"
        )
        if not session["open"]:
            _ARG_LIVE_STATE.update({
                "status": "MARKET_CLOSED",
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": 0,
                "errors": [],
                "interval_seconds": _ARG_LIVE_INTERVAL_SECONDS,
                "provider": provider,
                "symbols": list(settings.core_symbols),
            })
            await asyncio.sleep(_ARG_LIVE_INTERVAL_SECONDS)
            continue
        if provider == "none":
            _ARG_LIVE_STATE.update({
                "status": "NO_PROFESSIONAL_FEED",
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": 0,
                "errors": [{"symbol":"*","error":"TWELVE_DATA_API_KEY_MISSING_AND_YAHOO_FALLBACK_DISABLED"}],
                "interval_seconds": _ARG_LIVE_INTERVAL_SECONDS,
                "provider": provider,
                "symbols": list(settings.core_symbols),
            })
            await asyncio.sleep(_ARG_LIVE_INTERVAL_SECONDS)
            continue
        try:
            for symbol in settings.core_symbols:
                result = await asyncio.to_thread(
                    twelve_data_live_quote if provider == "twelve_data" else yahoo_chart_intraday,
                    symbol,
                    "1min",
                )
                if not result.rows:
                    errors.append({"symbol": symbol, "error": result.error or "NO_ROWS"})
                    continue
                latest = max(result.rows, key=lambda row: str(row.get("event_time") or ""))
                _ARG_LIVE_CACHE[symbol] = {
                    "symbol": symbol,
                    "last": float(latest["value"]),
                    "quote_timestamp": str(latest.get("event_time")),
                    "received_at": str(latest.get("received_time")),
                    "source": result.source,
                    "latency_ms": round(float(result.latency_ms or 0), 2),
                    "updated_epoch": time.time(),
                }
                rows_to_store.append(latest)
                updated += 1
            if rows_to_store:
                try:
                    await asyncio.to_thread(Store().insert_observations, rows_to_store)
                except Exception as exc:
                    errors.append({"symbol":"*","error":f"persist:{type(exc).__name__}: {exc}"})
            _ARG_LIVE_STATE.update({
                "status": "HEALTHY" if updated == len(settings.core_symbols) else "DEGRADED",
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": updated,
                "errors": errors[-8:],
                "interval_seconds": _ARG_LIVE_INTERVAL_SECONDS,
                "provider": provider,
                "symbols": list(settings.core_symbols),
            })
            print("GORILA_ARG_LIVE_CYCLE", _ARG_LIVE_STATE.copy(), flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _ARG_LIVE_STATE.update({
                "status":"ERROR",
                "updated_at":time.time(),
                "last_cycle_ms":round((time.perf_counter()-started)*1000,2),
                "updated_symbols":updated,
                "errors":[{"symbol":"*","error":f"{type(exc).__name__}: {exc}"}],
                "interval_seconds":_ARG_LIVE_INTERVAL_SECONDS,
                "provider":provider,
                "symbols":list(settings.core_symbols),
            })
        await asyncio.sleep(_ARG_LIVE_INTERVAL_SECONDS)


async def _autonomous_loop() -> None:
    await asyncio.sleep(_AUTONOMOUS_START_DELAY_SECONDS)
    while True:
        started = time.perf_counter()
        try:
            result = await asyncio.to_thread(
                run_autonomous_tick,
                kind="autonomous",
            )
            _AUTONOMOUS_STATE.update(
                {
                    "status": result.get("status", "UNKNOWN"),
                    "updated_at": time.time(),
                    "last_result": result,
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                }
            )
            print(
                "GORILA_AUTONOMOUS_CYCLE",
                {
                    "run_id": result.get("run_id"),
                    "status": result.get("status"),
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "shadow_predictions": (result.get("audit") or {}).get("shadow", {}).get("predictions"),
                    "shadow_settled": (result.get("audit") or {}).get("shadow", {}).get("settled"),
                    "pmsfx_shadow": result.get("pmsfx_shadow"),
                },
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _AUTONOMOUS_STATE.update(
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
        retry_seconds = 15 if _AUTONOMOUS_STATE.get("status") in {"SKIPPED_ALREADY_RUNNING", "SKIPPED_ALREADY_CLAIMED"} else _AUTONOMOUS_INTERVAL_SECONDS
        await asyncio.sleep(retry_seconds)


async def _macro_loop() -> None:
    print("GORILA_MACRO_LOOP_ENTERED", {"interval_seconds": _MACRO_INTERVAL_SECONDS}, flush=True)
    while True:
        started = time.perf_counter()
        try:
            result = await asyncio.to_thread(_run_macro_ingest)
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            _MACRO_STATE.update(
                {
                    "status": result.get("status", "UNKNOWN"),
                    "updated_at": time.time(),
                    "last_result": result,
                    "latency_ms": latency_ms,
                }
            )
            print(
                "GORILA_MACRO_CYCLE",
                {
                    "status": result.get("status", "UNKNOWN"),
                    "latency_ms": latency_ms,
                    "rows_inserted": result.get("rows_inserted", 0),
                    "results": result.get("results", []),
                },
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            _MACRO_STATE.update(
                {
                    "status": "ERROR",
                    "updated_at": time.time(),
                    "last_result": {
                        "status": "ERROR",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    "latency_ms": latency_ms,
                }
            )
            print(
                "GORILA_MACRO_CYCLE_ERROR",
                {
                    "error": f"{type(exc).__name__}: {exc}",
                    "latency_ms": latency_ms,
                },
                flush=True,
            )
        await asyncio.sleep(_MACRO_INTERVAL_SECONDS)


async def _production_self_test() -> None:
    if os.getenv("GORILA_SELF_TEST", "").strip().lower() not in {"1", "true", "yes"}:
        return
    await asyncio.sleep(60)
    started = time.perf_counter()
    base = f"http://127.0.0.1:{os.getenv('PORT', '10000')}"
    results: dict[str, Any] = {}
    timeout = httpx.Timeout(180.0, connect=5.0)
    async with httpx.AsyncClient(base_url=base, timeout=timeout) as client:
        async def probe(name: str, path: str) -> dict[str, Any]:
            t0 = time.perf_counter()
            try:
                response = await client.get(path, headers={"User-Agent": "Gorila-Production-SelfTest/1.0"})
                payload: Any
                try:
                    payload = response.json()
                except Exception:
                    payload = None
                return {
                    "ok": response.status_code == 200,
                    "status_code": response.status_code,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "payload": payload,
                }
            except Exception as exc:
                return {
                    "ok": False,
                    "status_code": None,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "error": f"{type(exc).__name__}: {exc}",
                }

        results["health"] = await probe("health", "/api/gorila/health")
        results["market"] = await probe("market", "/api/gorila/market")
        results["cross_sectional"] = await probe("cross_sectional", "/api/gorila/cross-sectional")
        results["terminal"] = await probe("terminal", "/api/gorila/terminal/AAPL")
        results["control"] = await probe("control", "/api/gorila/control")

    health_payload = results["health"].get("payload") or {}
    market_payload = results["market"].get("payload") or {}
    cross_payload = results["cross_sectional"].get("payload") or {}
    terminal_payload = results["terminal"].get("payload") or {}
    control_payload = results["control"].get("payload") or {}
    forecast = terminal_payload.get("forecast") or {}
    runtime_items = (control_payload.get("runtime") or {}).get("items") or []

    results["health_contract"] = {
        "ok": results["health"]["ok"]
        and health_payload.get("service") == "gorila-argentum"
        and health_payload.get("mode") == "RESEARCH"
        and health_payload.get("trading_execution") is False
        and health_payload.get("automatic_promotion") is False,
    }
    results["market_contract"] = {
        "ok": results["market"]["ok"]
        and "fx" in market_payload
        and "risk" in market_payload
        and "official" in (market_payload.get("fx") or {})
        and "embi_bps" in (market_payload.get("risk") or {}),
        "status": market_payload.get("status"),
    }
    results["cross_contract"] = {
        "ok": results["cross_sectional"]["ok"]
        and cross_payload.get("status") in {"READY", "INSUFFICIENT_DATA"},
        "status": cross_payload.get("status"),
        "reason": cross_payload.get("reason"),
        "missing_symbols": cross_payload.get("missing_symbols"),
        "observations": cross_payload.get("observations"),
        "training_rows": cross_payload.get("training_rows"),
        "skipped_anchors": cross_payload.get("skipped_anchors"),
        "latest_dates": cross_payload.get("latest_dates"),
        "coverage": cross_payload.get("coverage"),
    }
    results["terminal_contract"] = {
        "ok": results["terminal"]["ok"]
        and terminal_payload.get("service") == "gorila-argentum"
        and terminal_payload.get("symbol") == "AAPL"
        and "quote" in terminal_payload
        and "forecast" in terminal_payload
        and "macro" in terminal_payload
        and "chart" in terminal_payload
        and forecast.get("forecast_status") not in {"ERROR", "BLOCKED_DATA_HEALTH"}
        and forecast.get("forecast") is not None,
        "forecast_status": forecast.get("forecast_status"),
        "engine_source": forecast.get("engine_source"),
        "chart_points": len(terminal_payload.get("chart") or []),
    }
    latest = runtime_items[0] if runtime_items else {}
    results["control_contract"] = {
        "ok": results["control"]["ok"]
        and "health" in control_payload
        and "promotion" in control_payload
        and "shadow" in control_payload
        and "runtime" in control_payload,
        "latest_runtime_kind": latest.get("kind"),
        "latest_runtime_status": latest.get("status"),
    }
    # Route-level HTTP pass is separate from scientific readiness. The cross-sectional
    # endpoint may legitimately return INSUFFICIENT_DATA; that is not an HTTP failure.
    results["all_http_ok"] = all(
        bool(results[name].get("ok"))
        for name in ("health", "market", "cross_sectional", "terminal", "control")
    )
    results["all_contracts_ok"] = all(
        bool(results[name].get("ok"))
        for name in (
            "health_contract",
            "market_contract",
            "cross_contract",
            "terminal_contract",
            "control_contract",
        )
    )
    results["all_ok"] = results["all_http_ok"] and results["all_contracts_ok"]
    results["latency_ms"] = round((time.perf_counter() - started) * 1000, 2)

    # Do not print full payloads; emit a compact, auditable numerical summary.
    summary = {
        "http_status": {
            name: results[name].get("status_code")
            for name in ("health", "market", "cross_sectional", "terminal", "control")
        },
        "contracts": {
            "health": results["health_contract"],
            "market": results["market_contract"],
            "cross_sectional": results["cross_contract"],
            "terminal": results["terminal_contract"],
            "control": results["control_contract"],
        },
        "all_http_ok": results["all_http_ok"],
        "all_contracts_ok": results["all_contracts_ok"],
        "all_ok": results["all_ok"],
        "latency_ms": results["latency_ms"],
    }
    print("GORILA_PRODUCTION_HTTP_E2E", json.dumps(summary, sort_keys=True, default=str), flush=True)


@app.on_event("startup")
async def gorila_runtime_startup() -> None:
    global _MACRO_TASK, _AUTONOMOUS_TASK, _ARG_LIVE_TASK
    Store().init()
    print("GORILA_ARG_FEED_CONFIG", {"twelve_data_configured": bool(settings.twelve_data_api_key), "yahoo_fallback_enabled": os.getenv("GORILA_ALLOW_YAHOO_LIVE","0").strip().lower() in {"1","true","yes"}, "symbols": list(settings.core_symbols)}, flush=True)
    if _MACRO_TASK is None or _MACRO_TASK.done():
        _MACRO_TASK = asyncio.create_task(
            _macro_loop(),
            name="gorila-argentina-macro-loop",
        )
    if _AUTONOMOUS_TASK is None or _AUTONOMOUS_TASK.done():
        _AUTONOMOUS_TASK = asyncio.create_task(
            _autonomous_loop(),
            name="gorila-autonomous-runtime-loop",
        )
    if _ARG_LIVE_TASK is None or _ARG_LIVE_TASK.done():
        _ARG_LIVE_TASK = asyncio.create_task(
            _argentina_live_loop(),
            name="gorila-argentina-live-loop",
        )
    if os.getenv("GORILA_SELF_TEST", "").strip().lower() in {"1", "true", "yes"}:
        asyncio.create_task(_production_self_test(), name="gorila-production-self-test")


@app.on_event("shutdown")
async def gorila_runtime_shutdown() -> None:
    global _MACRO_TASK, _AUTONOMOUS_TASK, _ARG_LIVE_TASK
    for task in (_MACRO_TASK, _AUTONOMOUS_TASK):
        if task is not None:
            task.cancel()
    for task in (_MACRO_TASK, _AUTONOMOUS_TASK):
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass
    if _ARG_LIVE_TASK is not None:
        _ARG_LIVE_TASK.cancel()
        try:
            await _ARG_LIVE_TASK
        except asyncio.CancelledError:
            pass
    _MACRO_TASK = None
    _AUTONOMOUS_TASK = None
    _ARG_LIVE_TASK = None


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
        "autonomous_cycle": {
            "enabled": True,
            "interval_seconds": _AUTONOMOUS_INTERVAL_SECONDS,
            "start_delay_seconds": _AUTONOMOUS_START_DELAY_SECONDS,
        },
        "primary_market_data": {
            "provider": "TIINGO",
            "configured": tiingo_configured,
            "upstream_engine": "PMSF-X" if upstream_available else None,
            "source": (
                "LOCAL_TIINGO"
                if tiingo_configured
                else "SHARED_POSTGRES_FORECAST"
                if persistence_summary().get("forecast_count")
                else "UPSTREAM_PMSF_X"
                if upstream_available
                else "NONE"
            ),
            "status": (
                "READY"
                if tiingo_configured
                else "READY_LAST_FORECAST"
                if persistence_summary().get("forecast_count")
                else "READY_BRIDGE"
                if upstream_available
                else "MISSING_FEED"
            ),
        },
        "database": persistence_summary(),
        "market_stream": stream_status(),
        "market_session": market_session_state(),
        "autonomous_runtime": {
            **_AUTONOMOUS_STATE,
            "updated_at": (
                __import__("datetime").datetime.fromtimestamp(
                    _AUTONOMOUS_STATE["updated_at"],
                    tz=__import__("datetime").timezone.utc,
                ).isoformat()
                if _AUTONOMOUS_STATE.get("updated_at")
                else None
            ),
        },
        "argentina_live": {
            **_ARG_LIVE_STATE,
            "updated_at": (
                __import__("datetime").datetime.fromtimestamp(_ARG_LIVE_STATE["updated_at"], tz=__import__("datetime").timezone.utc).isoformat()
                if _ARG_LIVE_STATE.get("updated_at") else None
            ),
        },
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


@app.get("/api/gorila/control")
def gorila_control_snapshot():
    store = Store()
    store.init()
    return {
        "health": gorila_health(),
        "promotion": {
            "current_evaluation": evaluate_live_promotion(store),
            "latest_decision": store.latest_promotion_decision(),
        },
        "shadow": store.shadow_summary(),
        "runtime": {
            "items": store.latest_runtime_run(kind=None, limit=1),
        },
    }


@app.get("/api/gorila/bcra")
def gorila_bcra_snapshot():
    global _BCRA_SNAPSHOT_CACHE
    now_mono = time.monotonic()
    if _BCRA_SNAPSHOT_CACHE and now_mono - _BCRA_SNAPSHOT_CACHE[0] < _BCRA_SNAPSHOT_CACHE_SECONDS:
        return {**_BCRA_SNAPSHOT_CACHE[1], "cache": {"hit": True, "age_seconds": round(now_mono - _BCRA_SNAPSHOT_CACHE[0], 3)}}
    store = Store()
    store.init()
    snapshot = {**build_bcra_trader_snapshot(store), "runtime": {"status": _MACRO_STATE.get("status"), "updated_at": _MACRO_STATE.get("updated_at"), "latency_ms": _MACRO_STATE.get("latency_ms")}}
    _BCRA_SNAPSHOT_CACHE = (time.monotonic(), dict(snapshot))
    return {**snapshot, "cache": {"hit": False, "age_seconds": 0.0}}


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


@app.get("/api/gorila/cross-sectional")
async def gorila_cross_sectional():
    global _CROSS_SECTIONAL_CACHE
    now = time.monotonic()
    if _CROSS_SECTIONAL_CACHE and now - _CROSS_SECTIONAL_CACHE[0] < _CROSS_SECTIONAL_CACHE_SECONDS:
        return {**_CROSS_SECTIONAL_CACHE[1], "cache": {"hit": True, "age_seconds": round(now - _CROSS_SECTIONAL_CACHE[0], 3)}}
    async with _CROSS_SECTIONAL_LOCK:
        now = time.monotonic()
        if _CROSS_SECTIONAL_CACHE and now - _CROSS_SECTIONAL_CACHE[0] < _CROSS_SECTIONAL_CACHE_SECONDS:
            return {**_CROSS_SECTIONAL_CACHE[1], "cache": {"hit": True, "age_seconds": round(now - _CROSS_SECTIONAL_CACHE[0], 3)}}
        store = Store()
        store.init()
        result = score_cross_sectional(store=store)
        _CROSS_SECTIONAL_CACHE = (time.monotonic(), dict(result))
        return {**result, "cache": {"hit": False, "age_seconds": 0.0}}


@app.get("/api/gorila/live/{ticker}")
def gorila_live_quote(ticker: str):
    symbol = normalize_ticker(ticker)
    snapshot = _ARG_LIVE_CACHE.get(symbol)
    if snapshot is None:
        return {"symbol":symbol,"status":"NO_LIVE_CACHE","quote":None,"session":argentina_session_state(),"runtime":dict(_ARG_LIVE_STATE),"research_only":True,"no_execution_authority":True}
    age = max(0.0, time.time() - float(snapshot.get("updated_epoch") or time.time()))
    return {
        "symbol": symbol,
        "status": "LIVE" if age <= _ARG_LIVE_INTERVAL_SECONDS * 2.5 else "STALE",
        "quote": {"last": snapshot.get("last"), "quoteTimestamp": snapshot.get("quote_timestamp"), "timestamp": snapshot.get("quote_timestamp"), "source": snapshot.get("source")},
        "age_seconds": round(age,2),
        "received_at": snapshot.get("received_at"),
        "latency_ms": snapshot.get("latency_ms"),
        "session": argentina_session_state(),
        "runtime": dict(_ARG_LIVE_STATE),
        "research_only": True,
        "no_execution_authority": True,
    }

@app.get("/api/gorila/signal/{ticker}")
async def gorila_signal(ticker: str):
    """Return the auditable research signal for one Argentine symbol."""
    symbol = normalize_ticker(ticker)
    if symbol not in SIGNAL_SYMBOLS:
        raise HTTPException(status_code=404, detail="ARGENTUM_SYMBOL_NOT_IN_UNIVERSE")
    state = await _gorila_forecast(symbol)
    live = _ARG_LIVE_CACHE.get(symbol)
    if live is not None:
        live_age = max(0.0, time.time() - float(live.get("updated_epoch") or time.time()))
        state = {**state, "last": live.get("last", state.get("last")), "quote_timestamp": live.get("quote_timestamp", state.get("quote_timestamp")), "data_source": live.get("source") or state.get("data_source"), "market_freshness": {"age_seconds": live_age}}
    store = Store()
    store.init()
    drift_rows = store.latest_drift(symbol=symbol, field="close", limit=1)
    shadow = store.shadow_summary()
    series = store.recent_series(symbol, "close_1m", limit=240)
    if not series:
        series = store.recent_series(symbol, "close_5m", limit=240)
    if not series:
        series = store.recent_series(symbol, "close", limit=240)
    signal = build_signal(
        symbol=symbol,
        state=state,
        price_series=series,
        drift=drift_rows[0] if drift_rows else None,
        shadow_summary=shadow,
    )
    signal["session"] = market_session_state()
    signal["source_health"] = [
        row for row in store.health()
        if row.get("source") in {
            "BYMA/MarketData",
            f"YahooChart/{symbol}.BA",
            f"YahooChartLive/{symbol}.BA",
            f"EODHD/{symbol}.BA/5m",
        }
    ]
    return signal


@app.get("/api/gorila/signal-matrix")
async def gorila_signal_matrix():
    global _SIGNAL_MATRIX_CACHE
    now = time.monotonic()
    if _SIGNAL_MATRIX_CACHE and now - _SIGNAL_MATRIX_CACHE[0] < _SIGNAL_MATRIX_CACHE_SECONDS:
        return {**_SIGNAL_MATRIX_CACHE[1], "cache": {"hit": True, "age_seconds": round(now - _SIGNAL_MATRIX_CACHE[0], 3)}}
    async with _SIGNAL_MATRIX_LOCK:
        now = time.monotonic()
        if _SIGNAL_MATRIX_CACHE and now - _SIGNAL_MATRIX_CACHE[0] < _SIGNAL_MATRIX_CACHE_SECONDS:
            return {**_SIGNAL_MATRIX_CACHE[1], "cache": {"hit": True, "age_seconds": round(now - _SIGNAL_MATRIX_CACHE[0], 3)}}
    """Return the complete Argentine research signal matrix."""
    async def build_one(symbol: str):
        try:
            state = await _gorila_forecast(symbol)
            live = _ARG_LIVE_CACHE.get(symbol)
            if live is not None:
                live_age = max(0.0, time.time() - float(live.get("updated_epoch") or time.time()))
                state = {**state, "last": live.get("last", state.get("last")), "quote_timestamp": live.get("quote_timestamp", state.get("quote_timestamp")), "data_source": live.get("source") or state.get("data_source"), "market_freshness": {"age_seconds": live_age}}
            store = Store()
            store.init()
            drift_rows = store.latest_drift(symbol=symbol, field="close", limit=1)
            series = store.recent_series(symbol, "close_5m", limit=240)
            if not series:
                series = store.recent_series(symbol, "close", limit=240)
            return build_signal(
                symbol=symbol,
                state=state,
                price_series=series,
                drift=drift_rows[0] if drift_rows else None,
                shadow_summary=store.shadow_summary(),
            )
        except Exception as exc:
            return {
                "symbol": symbol,
                "signal": "NEUTRAL",
                "status": "NO_DATA",
                "actionable": False,
                "signal_score": 0.0,
                "error": f"{type(exc).__name__}: {exc}",
                "research_only": True,
                "no_execution_authority": True,
            }

    items = await asyncio.gather(*(build_one(symbol) for symbol in SIGNAL_SYMBOLS))
    result = build_matrix(items)
    result["session"] = market_session_state()
    result["argentina_session"] = argentina_session_state()
    result["engine"] = {
        "forecast_cache_seconds": 30.0,
        "upstream_cache_seconds": _UPSTREAM_CACHE_SECONDS,
        "matrix_cache_seconds": _SIGNAL_MATRIX_CACHE_SECONDS,
        "cross_sectional_cache_seconds": _CROSS_SECTIONAL_CACHE_SECONDS,
        "universe": list(SIGNAL_SYMBOLS),
    }
    _SIGNAL_MATRIX_CACHE = (time.monotonic(), dict(result))
    return {**result, "cache": {"hit": False, "age_seconds": 0.0}}


@app.get("/api/gorila/terminal/{ticker}")
async def gorila_terminal(ticker: str):
    symbol = normalize_ticker(ticker)
    local_tiingo = bool(os.getenv("TIINGO_API_KEY", "").strip())
    forecast_task = asyncio.create_task(_gorila_forecast(symbol))
    quote_task = asyncio.create_task(advanced_quote(symbol)) if local_tiingo else None
    macro_task = asyncio.to_thread(build_market_state)

    quote_result = None
    quote_error = None
    try:
        forecast = await forecast_task
        if not local_tiingo:
            quote_result = _upstream_quote_payload(symbol, forecast)
    except Exception as exc:
        forecast = {
            "symbol": symbol,
            "forecast_status": "ERROR",
            "forecast": None,
            "error": f"{type(exc).__name__}: {exc}",
        }

    if quote_task is not None:
        try:
            quote_result = await quote_task
        except Exception as exc:
            quote_error = f"{type(exc).__name__}: {exc}"

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
    if not chart and not local_tiingo:
        chart = _latest_persisted_engine_chart(symbol, limit=180)

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
