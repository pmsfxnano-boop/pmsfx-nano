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

from fastapi import FastAPI, Header, HTTPException

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
from .sources import argentina_datos_fx, argentina_datos_risk, bcra_fx, twelve_data_intraday, twelve_data_live_quote, byma_live_panel
from .bcra_macro import bcra_macro_cycle, build_bcra_trader_snapshot
from scripts.gorila_runtime_tick import run_tick as run_runtime_tick, run_autonomous_tick
from quant.db import persistence_summary

# The public service uses a single process. The autonomous runtime loop is
# intentionally part of this process so research continues without a cron.
# The cache keeps the latency-critical UI path independent of the expensive
# historical/multi-horizon research call.
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
_ARG_SIGNAL_TASK: asyncio.Task | None = None
_ARG_LIVE_INTERVAL_SECONDS = max(15, int(os.getenv("GORILA_LIVE_INTERVAL_SECONDS", "30")))
_ARG_SIGNAL_INTERVAL_SECONDS = max(5, int(os.getenv("GORILA_SIGNAL_INTERVAL_SECONDS", "10")))
_ARG_SIGNAL_STATE: dict[str, Any] = {"status":"STARTING","updated_at":None,"last_cycle_ms":None,"updated_symbols":0,"errors":[]}
_DB_INIT_TASK: asyncio.Task | None = None
_ARG_E2E_TASK: asyncio.Task | None = None
_DB_STATE: dict[str, Any] = {"status":"STARTING","ready":False,"error":None,"updated_at":None}
_ARG_LIVE_CACHE: dict[str, dict[str, Any]] = {}
_ARG_SIGNAL_SNAPSHOTS: dict[str, dict[str, Any]] = {}
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

app = FastAPI(title="Gorila Argentum", version="1.0")
ARGENTINA_TIMEZONE = ZoneInfo("America/Argentina/Buenos_Aires")
ARGENTINA_ALIASES = {"GGAL":"GGAL","BMA":"BMA","YPFD":"YPFD","PAMP":"PAMP","TGSU2":"TGSU2","CEPU":"CEPU"}

def normalize_ticker(value: str) -> str:
    return str(value).strip().upper()

def market_session_state(now: datetime | None = None) -> dict[str, Any]:
    return argentina_session_state(now)

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

    Priority: BYMADATA Open Access (unauthenticated public BYMA feed). It returns
    the whole leading-equity panel in one request, so the loop avoids six separate
    vendor calls and respects the public endpoint's rate-limit guidance. Twelve Data
    remains a secondary fallback when explicitly configured. Yahoo is opt-in only.
    When BYMA is closed the loop does not hit any vendor and reports CLOSED.
    """
    await asyncio.sleep(5)
    while True:
        started = time.perf_counter()
        errors: list[dict[str, str]] = []
        updated = 0
        rows_to_store: list[dict[str, Any]] = []
        session = argentina_session_state()
        provider = "byma_open_access"
        fallback_provider = "twelve_data" if settings.twelve_data_api_key else "none"
        if not session["open"]:
            _ARG_LIVE_STATE.update({
                "status": "MARKET_CLOSED",
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": 0,
                "errors": [],
                "interval_seconds": _ARG_LIVE_INTERVAL_SECONDS,
                "provider": provider if not errors else (provider if updated else fallback_provider),
                "symbols": list(settings.core_symbols),
            })
            await asyncio.sleep(_ARG_LIVE_INTERVAL_SECONDS)
            continue
        try:
            result = await asyncio.to_thread(byma_live_panel)
            if result.rows:
                for latest in result.rows:
                    symbol = str(latest.get("symbol") or "").upper()
                    if symbol not in settings.core_symbols:
                        continue
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
            if not result.rows:
                errors.append({"symbol":"*","error": result.error or "BYMA_NO_ROWS"})
                if fallback_provider != "none":
                    for symbol in settings.core_symbols:
                        fallback = await asyncio.to_thread(
                            twelve_data_live_quote,
                            symbol,
                            "1min",
                        )
                        if not fallback.rows:
                            errors.append({"symbol":symbol,"error":fallback.error or "FALLBACK_NO_ROWS"})
                            continue
                        latest = max(fallback.rows, key=lambda row: str(row.get("event_time") or ""))
                        _ARG_LIVE_CACHE[symbol] = {
                            "symbol": symbol,
                            "last": float(latest["value"]),
                            "quote_timestamp": str(latest.get("event_time")),
                            "received_at": str(latest.get("received_time")),
                            "source": fallback.source,
                            "latency_ms": round(float(fallback.latency_ms or 0), 2),
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


async def _init_store_background() -> None:
    started = time.perf_counter()
    try:
        await asyncio.to_thread(Store().init)
        _DB_STATE.update({
            "status": "READY",
            "ready": True,
            "error": None,
            "updated_at": time.time(),
            "latency_ms": round((time.perf_counter()-started)*1000,2),
        })
        print("GORILA_DB_INIT", _DB_STATE.copy(), flush=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _DB_STATE.update({
            "status": "ERROR",
            "ready": False,
            "error": f"{type(exc).__name__}: {exc}",
            "updated_at": time.time(),
            "latency_ms": round((time.perf_counter()-started)*1000,2),
        })
        print("GORILA_DB_INIT_ERROR", _DB_STATE.copy(), flush=True)

def _build_argentina_signal_snapshot(symbol: str) -> dict[str, Any]:
    store = Store(); store.init()
    state = {
        "symbol": symbol,
        "forecast": None,
        "forecast_status": "NO_LOCAL_ARGENTINA_FORECAST",
        "engine_source": "argentina_local_snapshot",
        "evaluation": {},
        "engine_freshness": {"age_seconds": None, "stale": True},
    }
    live = _ARG_LIVE_CACHE.get(symbol)
    if live is not None:
        live_age = max(0.0, time.time() - float(live.get("updated_epoch") or time.time()))
        state = {**state, "last": live.get("last", state.get("last")),
                 "quote_timestamp": live.get("quote_timestamp", state.get("quote_timestamp")),
                 "data_source": live.get("source") or state.get("data_source"),
                 "market_freshness": {"age_seconds": live_age}}
    series = store.recent_series(symbol, "close_1m", limit=240) or store.recent_series(symbol, "close_5m", limit=240) or store.recent_series(symbol, "close", limit=240)
    drift_rows = store.latest_drift(symbol=symbol, field="close", limit=1)
    signal = build_signal(symbol=symbol, state=state, price_series=series,
                          drift=drift_rows[0] if drift_rows else None,
                          shadow_summary=store.shadow_summary())
    signal["session"] = market_session_state()
    signal["snapshot_source"] = "argentina_local_snapshot"
    signal["snapshot_runtime"] = {"worker_interval_seconds": _ARG_SIGNAL_INTERVAL_SECONDS,
                                  "generated_at": datetime.now(timezone.utc).isoformat()}
    return signal

async def _argentina_signal_snapshot_loop() -> None:
    await asyncio.sleep(2)
    print("GORILA_ARG_SIGNAL_LOOP_ENTERED", {"interval_seconds": _ARG_SIGNAL_INTERVAL_SECONDS}, flush=True)
    while True:
        started = time.perf_counter(); updated = 0; errors = []
        try:
            results = await asyncio.gather(
                *[asyncio.to_thread(_build_argentina_signal_snapshot, symbol) for symbol in SIGNAL_SYMBOLS],
                return_exceptions=True,
            )
            for symbol, result in zip(SIGNAL_SYMBOLS, results):
                if isinstance(result, Exception):
                    errors.append({"symbol": symbol, "error": f"{type(result).__name__}: {result}"})
                    _ARG_SIGNAL_SNAPSHOTS.pop(symbol, None)
                    continue
                _ARG_SIGNAL_SNAPSHOTS[symbol] = result
                updated += 1
            _ARG_SIGNAL_STATE.update({
                "status": "HEALTHY" if updated == len(SIGNAL_SYMBOLS) and not errors else "DEGRADED",
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": updated,
                "errors": errors[-8:],
                "interval_seconds": _ARG_SIGNAL_INTERVAL_SECONDS,
                "universe": list(SIGNAL_SYMBOLS),
            })
            print("GORILA_ARG_SIGNAL_CYCLE", _ARG_SIGNAL_STATE.copy(), flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _ARG_SIGNAL_STATE.update({
                "status": "ERROR", "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": updated,
                "errors": [{"symbol":"*","error":f"{type(exc).__name__}: {exc}"}],
                "interval_seconds": _ARG_SIGNAL_INTERVAL_SECONDS, "universe": list(SIGNAL_SYMBOLS),
            })
        sleep_seconds = _ARG_SIGNAL_INTERVAL_SECONDS if argentina_session_state().get("open") else 60
        await asyncio.sleep(sleep_seconds)

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


async def _argentina_e2e_self_test() -> None:
    if os.getenv("GORILA_SELF_TEST", "").strip().lower() not in {"1", "true", "yes"}:
        return
    await asyncio.sleep(30)
    base = f"http://127.0.0.1:{os.getenv('PORT', '10000')}"
    paths = {
        "health": "/api/gorila/health",
        "matrix": "/api/gorila/signal-matrix",
        "signal_ggal": "/api/gorila/signal/GGAL",
        "signal_cepu": "/api/gorila/signal/CEPU",
        "live_cepu": "/api/gorila/live/CEPU",
        "bcra": "/api/gorila/bcra",
    }
    results = {}
    timeout = httpx.Timeout(20.0, connect=3.0)
    async with httpx.AsyncClient(base_url=base, timeout=timeout) as client:
        for name, path in paths.items():
            t0 = time.perf_counter()
            try:
                r = await client.get(path, headers={"User-Agent":"Gorila-Argentina-E2E/1.0"})
                try:
                    payload = r.json()
                except Exception:
                    payload = None
                results[name] = {
                    "status": r.status_code,
                    "latency_ms": round((time.perf_counter()-t0)*1000,2),
                    "json": payload is not None,
                }
            except Exception as exc:
                results[name] = {
                    "status": None,
                    "latency_ms": round((time.perf_counter()-t0)*1000,2),
                    "json": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
    summary = {
        "all_http_200": all(v.get("status")==200 for v in results.values()),
        "results": results,
        "matrix_snapshot_status": _ARG_SIGNAL_STATE.get("status"),
        "snapshot_updated_symbols": _ARG_SIGNAL_STATE.get("updated_symbols"),
        "snapshot_errors": _ARG_SIGNAL_STATE.get("errors"),
    }
    print("GORILA_ARG_E2E_SELFTEST", json.dumps(summary, sort_keys=True, default=str), flush=True)

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
    global _MACRO_TASK, _AUTONOMOUS_TASK, _ARG_LIVE_TASK, _ARG_SIGNAL_TASK, _DB_INIT_TASK, _ARG_E2E_TASK
    if _DB_INIT_TASK is None or _DB_INIT_TASK.done():
        _DB_INIT_TASK = asyncio.create_task(_init_store_background(), name="gorila-db-init")
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
    if _ARG_SIGNAL_TASK is None or _ARG_SIGNAL_TASK.done():
        _ARG_SIGNAL_TASK = asyncio.create_task(
            _argentina_signal_snapshot_loop(),
            name="gorila-argentina-signal-snapshot-loop",
        )
    if _ARG_E2E_TASK is None or _ARG_E2E_TASK.done():
        _ARG_E2E_TASK = asyncio.create_task(_argentina_e2e_self_test(), name="gorila-argentina-e2e-self-test")


@app.on_event("shutdown")
async def gorila_runtime_shutdown() -> None:
    global _MACRO_TASK, _AUTONOMOUS_TASK, _ARG_LIVE_TASK, _ARG_SIGNAL_TASK, _DB_INIT_TASK, _ARG_E2E_TASK
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
    if _ARG_SIGNAL_TASK is not None:
        _ARG_SIGNAL_TASK.cancel()
        try:
            await _ARG_SIGNAL_TASK
        except asyncio.CancelledError:
            pass
    if _DB_INIT_TASK is not None:
        _DB_INIT_TASK.cancel()
        try:
            await _DB_INIT_TASK
        except asyncio.CancelledError:
            pass
    if _ARG_E2E_TASK is not None:
        _ARG_E2E_TASK.cancel()
        try:
            await _ARG_E2E_TASK
        except asyncio.CancelledError:
            pass
    _MACRO_TASK = None
    _AUTONOMOUS_TASK = None
    _ARG_LIVE_TASK = None
    _ARG_SIGNAL_TASK = None
    _ARG_E2E_TASK = None


@app.head("/", include_in_schema=False)
def gorila_root_head():
    from fastapi.responses import Response
    return Response(status_code=200, headers={"Cache-Control":"no-store"})

@app.get("/", include_in_schema=False)
def gorila_root():
    # The control surface is dynamic and must never be served from a browser/proxy cache.
    # This prevents an older dashboard contract from masking the current production API.
    from fastapi.responses import HTMLResponse
    return HTMLResponse(
        content=DASHBOARD_HTML.body,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0, s-maxage=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/gorila/health")
def gorila_health():
    live = dict(_ARG_LIVE_STATE)
    snap = dict(_ARG_SIGNAL_STATE)
    snapshot_ready = snap.get("updated_symbols", 0) >= len(SIGNAL_SYMBOLS)
    db = persistence_summary() if _DB_STATE.get("ready") else {"ready": False, "status": _DB_STATE.get("status")}
    return {
        "service": "gorila-argentum",
        "mode": "RESEARCH",
        "trading_execution": False,
        "automatic_promotion": False,
        "primary_market_data": {
            "provider": "ARGENTINA",
            "configured": bool(settings.twelve_data_api_key),
            "source": "ARGENTINA_LIVE_FEED" if live.get("status") == "HEALTHY" else "ARGENTINA_SIGNAL_SNAPSHOT",
            "status": "READY_LIVE" if live.get("status") == "HEALTHY" else "READY_SNAPSHOT" if snapshot_ready else "DEGRADED",
        },
        "database": db,
        "market_session": market_session_state(),
        "autonomous_runtime": dict(_AUTONOMOUS_STATE),
        "argentina_live": live,
        "argentina_signals": snap,
        "macro_ingest": dict(_MACRO_STATE),
        "db_init": dict(_DB_STATE),
        "sources": Store().health() if _DB_STATE.get("ready") else [],
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
def gorila_forecast(ticker: str):
    symbol = normalize_ticker(ticker)
    if symbol not in SIGNAL_SYMBOLS:
        raise HTTPException(status_code=404, detail="ARGENTUM_SYMBOL_NOT_IN_UNIVERSE")
    if not _DB_STATE.get("ready"):
        return {
            "symbol": symbol, "signal": "NEUTRAL", "status": "NO_DATA",
            "actionable": False, "signal_score": None,
            "probability": {"up": None, "down": None, "confidence": None},
            "market": {"last": None, "data_grade": "UNKNOWN", "source": None},
            "error_code": "DATABASE_INITIALIZING",
            "research_only": True, "no_execution_authority": True,
            "session": argentina_session_state(),
        }
    signal = _ARG_SIGNAL_SNAPSHOTS.get(symbol)
    if signal is None:
        return {"symbol": symbol, "forecast_status": "NO_DATA", "forecast": None, "source": "ARGENTINA_SNAPSHOT"}
    p = signal.get("probability") or {}
    return {
        "symbol": symbol,
        "forecast_status": signal.get("status"),
        "forecast": {
            "direction": signal.get("signal"),
            "raw_probability_up": p.get("up"),
            "raw_probability_down": p.get("down"),
            "confidence_raw": p.get("confidence"),
            "validated": bool((signal.get("validation") or {}).get("validated")),
            "model_id": (signal.get("snapshot") or {}).get("source", "argentina_snapshot"),
        },
        "source": "ARGENTINA_SNAPSHOT",
        "snapshot": signal.get("snapshot"),
    }

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
def gorila_signal(ticker: str):
    """Read one precomputed Argentina snapshot; never execute forecast work in request time."""
    symbol = normalize_ticker(ticker)
    if symbol not in SIGNAL_SYMBOLS:
        raise HTTPException(status_code=404, detail="ARGENTUM_SYMBOL_NOT_IN_UNIVERSE")
    if not _DB_STATE.get("ready"):
        signal = _ARG_SIGNAL_SNAPSHOTS.get(symbol)
    else:
        signal = _ARG_SIGNAL_SNAPSHOTS.get(symbol)
    if signal is None:
        return {
            "symbol": symbol, "signal": "NEUTRAL", "status": "NO_DATA",
            "actionable": False, "signal_score": None,
            "probability": {"up": None, "down": None, "confidence": None},
            "market": {"last": None, "data_grade": "UNKNOWN", "source": None},
            "error_code": "SNAPSHOT_NOT_READY",
            "research_only": True, "no_execution_authority": True,
            "session": argentina_session_state(),
        }
    signal["session"] = market_session_state()
    return signal

@app.get("/api/gorila/signal-matrix")
def gorila_signal_matrix():
    """Read the current Argentina snapshot matrix; no forecast computation in request time."""
    items = [
        _ARG_SIGNAL_SNAPSHOTS.get(symbol) or {
            "symbol": symbol, "signal": "NEUTRAL", "status": "NO_DATA",
            "signal_score": None, "probability": {"up": None, "down": None, "confidence": None},
            "market": {"last": None, "data_grade": "UNKNOWN", "source": None},
            "research_only": True, "no_execution_authority": True,
        }
        for symbol in SIGNAL_SYMBOLS
    ]
    result = build_matrix(items)
    result["session"] = market_session_state()
    result["argentina_session"] = argentina_session_state()
    result["snapshot_runtime"] = dict(_ARG_SIGNAL_STATE)
    result["engine"] = {
        "request_path": "memory_signal_snapshot",
        "snapshot_interval_seconds": _ARG_SIGNAL_INTERVAL_SECONDS,
        "matrix_cache_seconds": _SIGNAL_MATRIX_CACHE_SECONDS,
        "universe": list(SIGNAL_SYMBOLS),
    }
    return result


@app.get("/api/gorila/terminal/{ticker}")
def gorila_terminal(ticker: str):
    symbol = normalize_ticker(ticker)
    if symbol not in SIGNAL_SYMBOLS:
        raise HTTPException(status_code=404, detail="ARGENTUM_SYMBOL_NOT_IN_UNIVERSE")
    store = Store()
    store.init()
    signal = _ARG_SIGNAL_SNAPSHOTS.get(symbol)
    live = _ARG_LIVE_CACHE.get(symbol)
    macro = build_market_state()
    chart_rows = store.recent_series(symbol, "close_1m", limit=180) or store.recent_series(symbol, "close_5m", limit=180) or store.recent_series(symbol, "close", limit=180)
    chart = [{"time": row[0], "close": row[1]} for row in chart_rows]
    return {
        "service": "gorila-argentum",
        "symbol": symbol,
        "quote": {
            "last": (live or {}).get("last") if live else (signal or {}).get("market", {}).get("last"),
            "quoteTimestamp": (live or {}).get("quote_timestamp") if live else (signal or {}).get("market", {}).get("quote_timestamp"),
            "source": (live or {}).get("source") if live else (signal or {}).get("market", {}).get("source"),
        } if (live or signal) else None,
        "quote_error": None,
        "forecast": signal or {"symbol": symbol, "status":"NO_DATA", "forecast":None},
        "macro": macro,
        "chart": chart,
        "stream": dict(_ARG_LIVE_STATE),
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
