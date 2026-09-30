"""Production entrypoint for Gorila Argentum.

The production service uses the mature PMSF-X application as its quantitative
engine and adds the Argentina data fabric, durable research controls and the
Gorila control surface on the same FastAPI instance.
"""

from __future__ import annotations

import asyncio
import logging
import json
import os
import time
from pathlib import Path

import httpx
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from datetime import datetime, time as dt_time, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Header, HTTPException

from .audit import build_audit_state
from .config import settings
from .market_freshness import assess_observation, aggregate_status, choose_fresher_observation
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
from .sources import argentina_datos_fx, argentina_datos_risk, bcra_fx, twelve_data_intraday, twelve_data_live_quote, byma_live_panel, byma_historical_daily, rava_public_historical_daily
from .bcra_macro import bcra_macro_cycle, build_bcra_trader_snapshot
from scripts.gorila_runtime_tick import run_tick as run_runtime_tick, run_autonomous_tick
from quant.db import persistence_summary

_LOGGER = logging.getLogger("gorila-argentum")

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
_MACRO_SOURCE_REFRESH_SECONDS = {
    "ArgentinaDatos/FX": max(900, int(os.getenv("GORILA_MACRO_FX_REFRESH_SECONDS", "3600"))),
    "ArgentinaDatos/EMBI+": max(3600, int(os.getenv("GORILA_MACRO_RISK_REFRESH_SECONDS", "21600"))),
    "BCRA/FX": max(900, int(os.getenv("GORILA_BCRA_FX_REFRESH_SECONDS", "3600"))),
    "BCRA/MonetaryV4": max(3600, int(os.getenv("GORILA_BCRA_MONETARY_REFRESH_SECONDS", "21600"))),
}

_AUTONOMOUS_INTERVAL_SECONDS = max(
    300,
    int(os.getenv("GORILA_AUTONOMOUS_INTERVAL_SECONDS", "300")),
)
_AUTONOMOUS_START_DELAY_SECONDS = max(
    10,
    int(os.getenv("GORILA_AUTONOMOUS_START_DELAY_SECONDS", "20")),
)
_AUTONOMOUS_TASK: asyncio.Task | None = None
_AUTONOMOUS_WATCHDOG_TASK: asyncio.Task | None = None
_ARG_LIVE_TASK: asyncio.Task | None = None
_ARG_SIGNAL_TASK: asyncio.Task | None = None
_ARG_LIVE_INTERVAL_SECONDS = max(15, int(os.getenv("GORILA_LIVE_INTERVAL_SECONDS", "30")))
_ARG_SIGNAL_INTERVAL_SECONDS = max(5, int(os.getenv("GORILA_SIGNAL_INTERVAL_SECONDS", "10")))
_ARG_SIGNAL_STATE: dict[str, Any] = {"status":"STARTING","updated_at":None,"last_cycle_ms":None,"updated_symbols":0,"errors":[]}
_DB_INIT_TASK: asyncio.Task | None = None
_ARG_E2E_TASK: asyncio.Task | None = None
_PRODUCTION_E2E_TASK: asyncio.Task | None = None
_DB_STATE: dict[str, Any] = {"status":"STARTING","ready":False,"error":None,"updated_at":None}
_ARG_LIVE_CACHE: dict[str, dict[str, Any]] = {}
_ARG_LIVE_FALLBACK_CACHE: dict[str, dict[str, Any]] = {}
_ARG_LIVE_FALLBACK_LAST_ATTEMPT: dict[str, float] = {}
_ARG_LIVE_FALLBACK_MIN_INTERVAL_SECONDS = max(30.0, float(os.getenv("GORILA_LIVE_FALLBACK_MIN_INTERVAL_SECONDS", "60")))
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
    "started_at": None,
    "completed_at": None,
    "running": False,
    "latency_ms": None,
    "watchdog_status": "STARTING",
    "watchdog_updated_at": None,
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



def _macro_source_due(store: Store, source: str) -> tuple[bool, str]:
    interval = int(_MACRO_SOURCE_REFRESH_SECONDS.get(source, _MACRO_INTERVAL_SECONDS))
    health_by_source = {str(row.get("source")): row for row in store.health()}
    state = health_by_source.get(source)
    if not state or not state.get("last_success_at"):
        return True, "NO_SUCCESS_YET"
    stamp = state["last_success_at"]
    try:
        if isinstance(stamp, datetime):
            dt = stamp
        else:
            dt = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age = max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return True, "INVALID_LAST_SUCCESS_AT"
    if age >= interval:
        return True, f"STALE_{round(age)}S"
    return False, f"FRESH_{round(age)}S"


def _run_macro_ingest() -> dict[str, Any]:
    store = Store()
    store.init()
    source_functions = (
        ("ArgentinaDatos/FX", argentina_datos_fx),
        ("ArgentinaDatos/EMBI+", argentina_datos_risk),
        ("BCRA/FX", bcra_fx),
        ("BCRA/MonetaryV4", bcra_macro_cycle),
    )
    results = []
    skipped = []
    for source, fn in source_functions:
        due, reason = _macro_source_due(store, source)
        if due:
            results.append(fn())
        else:
            skipped.append({"source": source, "reason": reason, "refresh_interval_seconds": _MACRO_SOURCE_REFRESH_SECONDS[source]})

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

    return {
        "status": "COMPLETED" if all(r.rows for r in results) else ("IDLE" if not results else "DEGRADED"),
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
        "skipped": skipped,
        "refresh_intervals_seconds": _MACRO_SOURCE_REFRESH_SECONDS,
        "daily_equity_history": {
            "owner": "autonomous_runtime",
            "refresh_policy": "THROTTLED_IN_INGEST_RUN_BATCH",
        },
    }


async def _argentina_live_loop() -> None:
    """Maintain a point-in-time intraday cache without blocking the API.

    BYMADATA transport success is tracked separately from observation freshness.
    A row with an old trade timestamp is never labeled LIVE merely because the
    HTTP request completed recently.
    """
    await asyncio.sleep(5)
    while True:
        started = time.perf_counter()
        errors: list[dict[str, str]] = []
        updated = 0
        rows_to_store: list[dict[str, Any]] = []
        assessments: list[dict[str, Any]] = []
        session = argentina_session_state()
        provider = "byma_open_access"
        fallback_provider = "twelve_data" if settings.twelve_data_api_key else "none"
        if not session["open"]:
            _ARG_LIVE_STATE.update({
                "status": "MARKET_CLOSED",
                "transport_status": "IDLE",
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": 0,
                "live_symbols": 0,
                "delayed_symbols": 0,
                "stale_symbols": 0,
                "errors": [],
                "interval_seconds": _ARG_LIVE_INTERVAL_SECONDS,
                "provider": provider,
                "symbols": list(settings.core_symbols),
            })
            await asyncio.sleep(_ARG_LIVE_INTERVAL_SECONDS)
            continue
        try:
            result = await asyncio.to_thread(byma_live_panel)
            primary_by_symbol: dict[str, dict[str, Any]] = {}
            for latest in result.rows or []:
                symbol = str(latest.get("symbol") or "").upper()
                if symbol not in settings.core_symbols:
                    continue
                current = primary_by_symbol.get(symbol)
                if current is None or str(latest.get("event_time") or "") > str(current.get("event_time") or ""):
                    primary_by_symbol[symbol] = latest

            fallback_results = []
            fallback_rows_by_symbol: dict[str, dict[str, Any]] = {}
            if fallback_provider != "none":
                stale_or_missing = []
                now_epoch = time.time()
                for symbol in settings.core_symbols:
                    primary = primary_by_symbol.get(symbol)
                    primary_is_live = False
                    if primary is not None:
                        primary_is_live = (
                            assess_observation(
                                primary.get("event_time"),
                                primary.get("received_time"),
                            )["status"] == "LIVE"
                        )
                    if primary_is_live:
                        continue

                    cached_secondary = _ARG_LIVE_FALLBACK_CACHE.get(symbol)
                    if cached_secondary is not None:
                        cached_freshness = assess_observation(
                            cached_secondary.get("event_time"),
                            cached_secondary.get("received_time"),
                        )
                        if cached_freshness["status"] in {"LIVE", "DELAYED"}:
                            fallback_rows_by_symbol[symbol] = cached_secondary

                    last_attempt = _ARG_LIVE_FALLBACK_LAST_ATTEMPT.get(symbol, 0.0)
                    if now_epoch - last_attempt >= _ARG_LIVE_FALLBACK_MIN_INTERVAL_SECONDS:
                        stale_or_missing.append(symbol)

                if stale_or_missing:
                    fallback_results = list(
                        await asyncio.gather(
                            *[
                                asyncio.to_thread(
                                    twelve_data_live_quote,
                                    symbol,
                                    "1min",
                                )
                                for symbol in stale_or_missing
                            ]
                        )
                    )
                    now_epoch = time.time()
                    for symbol, fallback in zip(stale_or_missing, fallback_results):
                        _ARG_LIVE_FALLBACK_LAST_ATTEMPT[symbol] = now_epoch
                        if fallback.rows:
                            latest = max(
                                fallback.rows,
                                key=lambda row: str(row.get("event_time") or ""),
                            )
                            latest_symbol = str(latest.get("symbol") or symbol).upper()
                            _ARG_LIVE_FALLBACK_CACHE[latest_symbol] = latest
                            fallback_rows_by_symbol[latest_symbol] = latest
                        else:
                            errors.append({
                                "symbol": symbol,
                                "error": fallback.error or "FALLBACK_NO_ROWS",
                            })

            now_utc = datetime.now(timezone.utc)
            selected_sources: set[str] = set()
            for symbol in settings.core_symbols:
                primary = primary_by_symbol.get(symbol)
                secondary = fallback_rows_by_symbol.get(symbol)
                latest, freshness, selected_role = choose_fresher_observation(
                    primary,
                    secondary,
                    now=now_utc,
                )
                if latest is None or freshness is None:
                    errors.append({"symbol": symbol, "error": result.error or "NO_VALID_LIVE_OBSERVATION"})
                    continue

                source = str(latest.get("source") or result.source)
                selected_sources.add(source)
                latest["quality"] = freshness["status"]
                metadata = dict(latest.get("metadata") or {})
                metadata["freshness"] = freshness
                metadata["selection"] = {
                    "selected_role": selected_role,
                    "primary_source": result.source,
                    "secondary_source": next(
                        (
                            item.source
                            for item in fallback_results
                            if item.rows and str(item.rows[0].get("source") or "") != result.source
                        ),
                        None,
                    ),
                }
                latest["metadata"] = metadata
                source_result = result
                for fallback in fallback_results:
                    if fallback.source == source:
                        source_result = fallback
                        break
                _ARG_LIVE_CACHE[symbol] = {
                    "symbol": symbol,
                    "last": float(latest["value"]),
                    "quote_timestamp": str(latest.get("event_time")),
                    "received_at": str(latest.get("received_time")),
                    "source": source,
                    "latency_ms": round(float(source_result.latency_ms or 0), 2),
                    "updated_epoch": time.time(),
                    "event_quality": freshness,
                }
                rows_to_store.append(latest)
                assessments.append(freshness)
                updated += 1

            if not result.rows:
                errors.append({"symbol":"*","error": result.error or "BYMA_NO_ROWS"})

            aggregate = aggregate_status(assessments)
            if rows_to_store:
                try:
                    store = Store()
                    await asyncio.to_thread(store.insert_observations, rows_to_store)
                    source_results = [result, *fallback_results]
                    seen_sources = set()
                    for source_result in source_results:
                        if source_result.source in seen_sources:
                            continue
                        seen_sources.add(source_result.source)
                        source_rows = [
                            row for row in (source_result.rows or [])
                            if str(row.get("symbol") or "").upper() in settings.core_symbols
                        ]
                        source_assessments = [
                            assess_observation(row.get("event_time"), row.get("received_time"))
                            for row in source_rows
                        ]
                        source_status = aggregate_status(source_assessments)["status"]
                        await asyncio.to_thread(
                            store.upsert_health,
                            source_result.source,
                            source_status,
                            source_result.error,
                            len(source_rows),
                            float(source_result.latency_ms or 0),
                            bool(source_rows),
                        )
                except Exception as exc:
                    errors.append({"symbol":"*","error":f"persist:{type(exc).__name__}: {exc}"})
            transport_status = "HEALTHY" if result.rows or any(f.rows for f in fallback_results) else "DEGRADED"
            cycle_status = aggregate["status"] if updated == len(settings.core_symbols) else "DEGRADED"
            if len(selected_sources) == 1:
                provider_name = next(iter(selected_sources))
            elif len(selected_sources) > 1:
                provider_name = "mixed"
            else:
                provider_name = fallback_provider if fallback_provider != "none" else provider
            _ARG_LIVE_STATE.update({
                "status": cycle_status,
                "transport_status": transport_status,
                "updated_at": time.time(),
                "last_cycle_ms": round((time.perf_counter()-started)*1000,2),
                "updated_symbols": updated,
                "live_symbols": aggregate["live_symbols"],
                "delayed_symbols": aggregate["delayed_symbols"],
                "stale_symbols": aggregate["stale_symbols"],
                "invalid_timestamp_symbols": aggregate["invalid_timestamp_symbols"],
                "median_event_age_seconds": aggregate["median_event_age_seconds"],
                "max_event_age_seconds": aggregate["max_event_age_seconds"],
                "errors": errors[-8:],
                "interval_seconds": _ARG_LIVE_INTERVAL_SECONDS,
                "provider": provider_name,
                "symbols": list(settings.core_symbols),
            })
            print("GORILA_ARG_LIVE_CYCLE", _ARG_LIVE_STATE.copy(), flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _ARG_LIVE_STATE.update({
                "status":"ERROR",
                "transport_status":"ERROR",
                "updated_at":time.time(),
                "last_cycle_ms":round((time.perf_counter()-started)*1000,2),
                "updated_symbols":updated,
                "errors":[{"symbol":"*","error":f"{type(exc).__name__}: {exc}"}],
                "interval_seconds":_ARG_LIVE_INTERVAL_SECONDS,
                "provider":provider,
                "symbols":list(settings.core_symbols),
            })
        await asyncio.sleep(_ARG_LIVE_INTERVAL_SECONDS)


def _repair_canonical_history_if_needed() -> dict[str, Any]:
    """Rebuild the model-facing daily fabric when legacy quarantine emptied it."""
    if os.getenv("GORILA_CANONICAL_REPAIR_ON_STARTUP", "1").strip().lower() not in {"1", "true", "yes"}:
        return {"status": "DISABLED"}

    from .canonical_data import canonical_daily_series, reconcile_all

    store = Store()
    store.init()
    counts = {
        symbol: len(canonical_daily_series(store, symbol, "close", limit=2500))
        for symbol in settings.core_symbols
    }
    threshold = max(65 + 5 + 1, int(os.getenv("GORILA_CANONICAL_REPAIR_MIN_ROWS", "100")))
    if counts and min(counts.values()) >= threshold:
        return {"status": "HEALTHY", "counts": counts, "threshold": threshold}

    repaired = reconcile_all(
        store,
        settings.core_symbols,
        field="close",
        limit_sessions=2500,
    )
    post_counts = {
        symbol: len(canonical_daily_series(store, symbol, "close", limit=2500))
        for symbol in settings.core_symbols
    }
    return {
        "status": "REBUILT",
        "before": counts,
        "after": post_counts,
        "threshold": threshold,
        "symbols": len(repaired),
    }


async def _init_store_background() -> None:
    started = time.perf_counter()
    try:
        canonical_repair = await asyncio.to_thread(_repair_canonical_history_if_needed)
        _LOGGER.info("GORILA_CANONICAL_REPAIR %s", json.dumps(canonical_repair, sort_keys=True, default=str))
        _DB_STATE.update({
            "status": "READY",
            "ready": True,
            "error": None,
            "updated_at": time.time(),
            "latency_ms": round((time.perf_counter()-started)*1000,2),
        })
        _LOGGER.info("GORILA_DB_STATE %s", json.dumps(_DB_STATE.copy(), sort_keys=True, default=str))
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
        _LOGGER.error("GORILA_DB_STATE_ERROR %s", json.dumps(_DB_STATE.copy(), sort_keys=True, default=str))

def _build_argentina_signal_snapshot(symbol: str) -> dict[str, Any]:
    store = Store(); store.init()
    state = {
        "symbol": symbol,
        "forecast": None,
        "forecast_status": "NO_LOCAL_ARGENTINA_FORECAST",
        "engine_source": "argentina_local_snapshot",
        "evaluation": {},
        "engine_freshness": {"age_seconds": None, "stale": True},
        "market_session_open": bool(argentina_session_state().get("open")),
    }
    live = _ARG_LIVE_CACHE.get(symbol)
    if live is not None:
        freshness = assess_observation(live.get("quote_timestamp"), live.get("received_at"))
        state = {**state, "last": live.get("last", state.get("last")),
                 "quote_timestamp": live.get("quote_timestamp", state.get("quote_timestamp")),
                 "data_source": live.get("source") or state.get("data_source"),
                 "market_freshness": {
                     "age_seconds": freshness.get("event_age_seconds"),
                     "event_age_seconds": freshness.get("event_age_seconds"),
                     "transport_age_seconds": freshness.get("transport_age_seconds"),
                     "status": freshness.get("status"),
                 }}
    series = store.recent_series(symbol, "close_1m", limit=240) or store.recent_series(symbol, "close_5m", limit=240) or store.recent_series(symbol, "close", limit=240)
    # Do not bind persisted raw-close drift snapshots to the live research signal.
    # Price levels are non-stationary, and the legacy drift ledger is not a current
    # stationary feature-drift contract. Until a point-in-time stationary monitor
    # is bound here, surface the drift monitor as unavailable rather than emitting
    # a stale/ambiguous DRIFT_ALERT.
    signal = build_signal(symbol=symbol, state=state, price_series=series,
                          drift=None,
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

async def _autonomous_watchdog_loop() -> None:
    """Watchdog for the long-lived autonomous task without creating overlapping ticks."""
    global _AUTONOMOUS_TASK
    while True:
        try:
            now = time.time()
            task = _AUTONOMOUS_TASK
            running = bool(_AUTONOMOUS_STATE.get("running"))
            started_at = _AUTONOMOUS_STATE.get("started_at")
            completed_at = _AUTONOMOUS_STATE.get("completed_at")

            if task is not None and task.done() and not task.cancelled():
                print(
                    "GORILA_AUTONOMOUS_WATCHDOG_RESTART",
                    {"reason": "TASK_TERMINATED_UNEXPECTEDLY"},
                    flush=True,
                )
                _AUTONOMOUS_STATE.update({
                    "status": "RESTARTING",
                    "running": False,
                    "watchdog_status": "RESTARTING",
                    "watchdog_updated_at": now,
                })
                _AUTONOMOUS_TASK = asyncio.create_task(
                    _autonomous_loop(),
                    name="gorila-autonomous-runtime-loop-restarted",
                )
            else:
                warning = False
                age_seconds = None
                if running and started_at:
                    age_seconds = max(0.0, now - float(started_at))
                    warning = age_seconds > max(60.0, 2.0 * _AUTONOMOUS_INTERVAL_SECONDS)
                elif completed_at:
                    age_seconds = max(0.0, now - float(completed_at))
                    warning = age_seconds > max(120.0, 2.5 * _AUTONOMOUS_INTERVAL_SECONDS)

                _AUTONOMOUS_STATE.update({
                    "watchdog_status": "STALE" if warning else "HEALTHY",
                    "watchdog_updated_at": now,
                    "watchdog_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
                })
                if warning:
                    print(
                        "GORILA_AUTONOMOUS_WATCHDOG_STALE",
                        {
                            "running": running,
                            "age_seconds": round(age_seconds or 0.0, 3),
                            "interval_seconds": _AUTONOMOUS_INTERVAL_SECONDS,
                        },
                        flush=True,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _AUTONOMOUS_STATE.update({
                "watchdog_status": "ERROR",
                "watchdog_updated_at": time.time(),
                "watchdog_error": f"{type(exc).__name__}: {exc}",
            })
            print(
                "GORILA_AUTONOMOUS_WATCHDOG_ERROR",
                {"error": f"{type(exc).__name__}: {exc}"},
                flush=True,
            )
        await asyncio.sleep(30.0)


async def _autonomous_loop() -> None:
    await asyncio.sleep(_AUTONOMOUS_START_DELAY_SECONDS)
    print("GORILA_AUTONOMOUS_LOOP_ENTERED", {"interval_seconds": _AUTONOMOUS_INTERVAL_SECONDS}, flush=True)
    while True:
        started = time.perf_counter()
        _AUTONOMOUS_STATE.update({
            "status": _AUTONOMOUS_STATE.get("status") if _AUTONOMOUS_STATE.get("status") not in {"STARTING", "RESTARTING"} else "RUNNING",
            "running": True,
            "started_at": time.time(),
            "watchdog_status": "HEALTHY",
        })
        try:
            print("GORILA_AUTONOMOUS_TICK_STARTED", flush=True)
            result = await asyncio.to_thread(
                run_autonomous_tick,
                kind="autonomous",
            )
            _AUTONOMOUS_STATE.update(
                {
                    "status": result.get("status", "UNKNOWN"),
                    "updated_at": time.time(),
                    "completed_at": time.time(),
                    "last_result": result,
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "running": False,
                }
            )
            print(
                "GORILA_AUTONOMOUS_CYCLE",
                {
                    "run_id": result.get("run_id"),
                    "status": result.get("status"),
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "timings": result.get("timings"),
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
                    "completed_at": time.time(),
                    "running": False,
                    "last_result": {
                        "status": "ERROR",
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                }
            )
            print(
                "GORILA_AUTONOMOUS_CYCLE_ERROR",
                {
                    "status": "ERROR",
                    "error": f"{type(exc).__name__}: {exc}",
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                },
                flush=True,
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
            _LOGGER.info(
                "GORILA_MACRO_RESULT %s",
                json.dumps(
                    {
                        "status": result.get("status", "UNKNOWN"),
                        "latency_ms": latency_ms,
                        "rows_inserted": result.get("rows_inserted", 0),
                        "results": result.get("results", []),
                        "canonical_daily": result.get("canonical_daily", []),
                    },
                    sort_keys=True,
                    default=str,
                ),
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
            _LOGGER.error(
                "GORILA_MACRO_RESULT_ERROR %s",
                json.dumps(
                    {
                        "error": f"{type(exc).__name__}: {exc}",
                        "latency_ms": latency_ms,
                    },
                    sort_keys=True,
                ),
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
    byma_history_raw_rows: list[dict[str, Any]] = []
    history_t0 = time.perf_counter()
    try:
        history_result = await asyncio.to_thread(byma_historical_daily, "GGAL")
        byma_history_raw_rows = list(history_result.rows)
        results["byma_history"] = {
            "ok": bool(history_result.rows),
            "rows": len(history_result.rows),
            "source": history_result.source,
            "latency_ms": round(float(history_result.latency_ms or 0), 2),
            "error": history_result.error,
            "last_event_time": history_result.rows[-1].get("event_time") if history_result.rows else None,
            "age_probe_ms": round((time.perf_counter() - history_t0) * 1000, 2),
        }
    except Exception as exc:
        results["byma_history"] = {
            "ok": False,
            "rows": 0,
            "source": "BYMADATA/GGAL/historical",
            "latency_ms": round((time.perf_counter() - history_t0) * 1000, 2),
            "error": f"{type(exc).__name__}: {exc}",
        }

    if os.getenv("GORILA_RAVA_PUBLIC_ENABLED", "true").strip().lower() in {"1", "true", "yes"}:
        rava_t0 = time.perf_counter()
        try:
            rava_result = await asyncio.to_thread(
                rava_public_historical_daily,
                "GGAL",
                limit_rows=400,
            )
            results["rava_history"] = {
                "ok": bool(rava_result.rows),
                "rows": len(rava_result.rows),
                "source": rava_result.source,
                "latency_ms": round(float(rava_result.latency_ms or 0), 2),
                "error": rava_result.error,
                "last_event_time": rava_result.rows[-1].get("event_time") if rava_result.rows else None,
            }

            byma_rows = byma_history_raw_rows
            rava_rows = rava_result.rows or []
            byma_by_session = {
                str(row.get("event_time") or "")[:10]: float(row.get("value"))
                for row in byma_rows
                if row.get("event_time") and row.get("value") is not None
            }
            rava_by_session = {
                str(row.get("event_time") or "")[:10]: float(row.get("value"))
                for row in rava_rows
                if row.get("event_time") and row.get("value") is not None
            }
            overlap = sorted(set(byma_by_session) & set(rava_by_session))
            latest_overlap = overlap[-1] if overlap else None
            spread = None
            if latest_overlap is not None:
                a = byma_by_session[latest_overlap]
                b = rava_by_session[latest_overlap]
                median = (a + b) / 2.0
                spread = abs(a - b) / median if median > 0 else None
            results["rava_byma_agreement"] = {
                "status": "OK" if spread is not None and spread <= 0.0025 else (
                    "NO_OVERLAP" if spread is None else "DISCREPANCY"
                ),
                "overlap_sessions": len(overlap),
                "latest_overlap_session": latest_overlap,
                "relative_spread": spread,
                "threshold": 0.0025,
            }
        except Exception as exc:
            results["rava_history"] = {
                "ok": False,
                "rows": 0,
                "source": "RavaPublic/GGAL",
                "latency_ms": round((time.perf_counter() - rava_t0) * 1000, 2),
                "error": f"{type(exc).__name__}: {exc}",
            }
            results["rava_byma_agreement"] = {
                "status": "SOURCE_ERROR",
                "overlap_sessions": 0,
                "latest_overlap_session": None,
                "relative_spread": None,
                "threshold": 0.0025,
            }

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
    http_probe_names = tuple(paths.keys())
    summary = {
        "all_http_200": all((results.get(name) or {}).get("status") == 200 for name in http_probe_names),
        "byma_history_ok": bool((results.get("byma_history") or {}).get("ok")),
        "rava_history_ok": bool((results.get("rava_history") or {}).get("ok")) if "rava_history" in results else None,
        "history_sources_ok": bool((results.get("byma_history") or {}).get("ok"))
        and (bool((results.get("rava_history") or {}).get("ok")) if "rava_history" in results else True)
        and (
            (results.get("rava_byma_agreement") or {}).get("status") == "OK"
            if "rava_history" in results
            else True
        ),
        "results": results,
        "matrix_snapshot_status": _ARG_SIGNAL_STATE.get("status"),
        "snapshot_updated_symbols": _ARG_SIGNAL_STATE.get("updated_symbols"),
        "snapshot_errors": _ARG_SIGNAL_STATE.get("errors"),
    }
    summary["ok"] = bool(summary["all_http_200"] and summary["history_sources_ok"] and summary["matrix_snapshot_status"] == "HEALTHY")
    _LOGGER.info("GORILA_ARG_E2E_RESULT %s", json.dumps(summary, sort_keys=True, default=str))

async def _production_self_test() -> None:
    enabled = os.getenv("GORILA_SELF_TEST", "true").strip().lower() in {"1", "true", "yes"}
    print("GORILA_PRODUCTION_SELFTEST_CONFIG", {"enabled": enabled}, flush=True)
    if not enabled:
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
        results["terminal"] = await probe("terminal", "/api/gorila/terminal/GGAL")
        results["control"] = await probe("control", "/api/gorila/control")
        results["g2_h10"] = await probe("g2_h10", "/api/gorila/g2-h10/status")
        results["g2_h10_score"] = await probe("g2_h10_score", "/api/gorila/g2-h10/score")

    health_payload = results["health"].get("payload") or {}
    market_payload = results["market"].get("payload") or {}
    cross_payload = results["cross_sectional"].get("payload") or {}
    terminal_payload = results["terminal"].get("payload") or {}
    control_payload = results["control"].get("payload") or {}
    g2_payload = results["g2_h10"].get("payload") or {}
    g2_score_payload = results["g2_h10_score"].get("payload") or {}
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
        and terminal_payload.get("symbol") == "GGAL"
        and "quote" in terminal_payload
        and "forecast" in terminal_payload
        and "macro" in terminal_payload
        and "chart" in terminal_payload
        and forecast.get("forecast_status") in {"READY", "NO_DATA"}
        and (
            forecast.get("forecast_status") == "NO_DATA"
            or forecast.get("forecast") is not None
        ),
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

    autonomous = (health_payload.get("autonomous_runtime") or {})
    results["autonomous_contract"] = {
        "ok": results["health"]["ok"]
        and autonomous.get("status") == "COMPLETED"
        and autonomous.get("interval_seconds", 0) >= 300
        and bool(autonomous.get("last_result")),
        "status": autonomous.get("status"),
        "interval_seconds": autonomous.get("interval_seconds"),
    }
    results["g2_h10_score_contract"] = {
        "ok": results["g2_h10_score"]["ok"]
        and g2_score_payload.get("model_id") == "G2_PIT_FIXED_C0.25_H10"
        and g2_score_payload.get("research_only") is True
        and g2_score_payload.get("no_execution_authority") is True
        and g2_score_payload.get("runtime_serving") == "DISABLED"
        and g2_score_payload.get("status") in {"ARTIFACT_UNAVAILABLE", "READY", "INSUFFICIENT_DATA"},
        "status": g2_score_payload.get("status"),
    }

    results["g2_h10_contract"] = {
        "ok": results["g2_h10"]["ok"]
        and g2_payload.get("status") == "REGISTERED"
        and g2_payload.get("model_id") == "G2_PIT_FIXED_C0.25_H10"
        and g2_payload.get("promotion") == "BLOCKED"
        and g2_payload.get("runtime_serving") in {
            "DISABLED_UNTIL_EXACT_PACKAGE_VERIFIED",
            "DISABLED_UNTIL_EXACT_PACKAGE_VERIFIED_AND_SCORER_REPRODUCED",
        }
        and g2_payload.get("research_only") is True
        and g2_payload.get("no_execution_authority") is True,
        "status": g2_payload.get("status"),
        "model_id": g2_payload.get("model_id"),
        "runtime_serving": g2_payload.get("runtime_serving"),
    }
    # Route-level HTTP pass is separate from scientific readiness. The cross-sectional
    # endpoint may legitimately return INSUFFICIENT_DATA; that is not an HTTP failure.
    results["all_http_ok"] = all(
        bool(results[name].get("ok"))
        for name in ("health", "market", "cross_sectional", "terminal", "control", "g2_h10", "g2_h10_score")
    )
    results["all_contracts_ok"] = all(
        bool(results[name].get("ok"))
        for name in (
            "health_contract",
            "market_contract",
            "cross_contract",
            "terminal_contract",
            "control_contract",
            "autonomous_contract",
            "g2_h10_contract",
            "g2_h10_score_contract",
        )
    )
    results["all_ok"] = results["all_http_ok"] and results["all_contracts_ok"]
    results["latency_ms"] = round((time.perf_counter() - started) * 1000, 2)

    # Do not print full payloads; emit a compact, auditable numerical summary.
    summary = {
        "http_status": {
            name: results[name].get("status_code")
            for name in ("health", "market", "cross_sectional", "terminal", "control", "g2_h10", "g2_h10_score")
        },
        "contracts": {
            "health": results["health_contract"],
            "market": results["market_contract"],
            "cross_sectional": results["cross_contract"],
            "terminal": results["terminal_contract"],
            "control": results["control_contract"],
            "autonomous": results["autonomous_contract"],
            "g2_h10": results["g2_h10_contract"],
            "g2_h10_score": results["g2_h10_score_contract"],
        },
        "all_http_ok": results["all_http_ok"],
        "all_contracts_ok": results["all_contracts_ok"],
        "all_ok": results["all_ok"],
        "latency_ms": results["latency_ms"],
    }
    print("GORILA_PRODUCTION_HTTP_E2E", json.dumps(summary, sort_keys=True, default=str), flush=True)


@app.on_event("startup")
async def gorila_runtime_startup() -> None:
    global _MACRO_TASK, _AUTONOMOUS_TASK, _AUTONOMOUS_WATCHDOG_TASK, _ARG_LIVE_TASK, _ARG_SIGNAL_TASK, _DB_INIT_TASK, _ARG_E2E_TASK, _PRODUCTION_E2E_TASK
    if _DB_INIT_TASK is None or _DB_INIT_TASK.done():
        _DB_INIT_TASK = asyncio.create_task(_init_store_background(), name="gorila-db-init")
    print("GORILA_ARG_FEED_CONFIG", {"byma_open_access": True, "twelve_data_configured": bool(settings.twelve_data_api_key), "yahoo_fallback_enabled": False, "symbols": list(settings.core_symbols)}, flush=True)
    print("GORILA_PRODUCTION_SELFTEST_CONFIG", {"enabled": os.getenv("GORILA_SELF_TEST", "true").strip().lower() in {"1", "true", "yes"}}, flush=True)
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
    if _AUTONOMOUS_WATCHDOG_TASK is None or _AUTONOMOUS_WATCHDOG_TASK.done():
        _AUTONOMOUS_WATCHDOG_TASK = asyncio.create_task(
            _autonomous_watchdog_loop(),
            name="gorila-autonomous-watchdog-loop",
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
    if _PRODUCTION_E2E_TASK is None or _PRODUCTION_E2E_TASK.done():
        _PRODUCTION_E2E_TASK = asyncio.create_task(_production_self_test(), name="gorila-production-http-e2e")


@app.on_event("shutdown")
async def gorila_runtime_shutdown() -> None:
    global _MACRO_TASK, _AUTONOMOUS_TASK, _AUTONOMOUS_WATCHDOG_TASK, _ARG_LIVE_TASK, _ARG_SIGNAL_TASK, _DB_INIT_TASK, _ARG_E2E_TASK, _PRODUCTION_E2E_TASK
    if _AUTONOMOUS_WATCHDOG_TASK is not None:
        _AUTONOMOUS_WATCHDOG_TASK.cancel()
        try:
            await _AUTONOMOUS_WATCHDOG_TASK
        except asyncio.CancelledError:
            pass

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
    if _PRODUCTION_E2E_TASK is not None:
        _PRODUCTION_E2E_TASK.cancel()
        try:
            await _PRODUCTION_E2E_TASK
        except asyncio.CancelledError:
            pass
    _MACRO_TASK = None
    _AUTONOMOUS_TASK = None
    _AUTONOMOUS_WATCHDOG_TASK = None
    _ARG_LIVE_TASK = None
    _ARG_SIGNAL_TASK = None
    _ARG_E2E_TASK = None
    _PRODUCTION_E2E_TASK = None


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


RUNTIME_CONTRACT_VERSION = "gorila-runtime-2026-09-29"

_UI_HIDDEN_SOURCE_EXACT = {"BYMADATA/MarketData"}
_UI_HIDDEN_SOURCE_PREFIXES = ("YahooChart",)


def _visible_source_health(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Hide retired/secondary vendors from the public Gorila data-fabric panel."""
    visible = []
    for row in rows or []:
        source = str(row.get("source") or "")
        if source in _UI_HIDDEN_SOURCE_EXACT or any(source.startswith(prefix) for prefix in _UI_HIDDEN_SOURCE_PREFIXES):
            continue
        visible.append(row)
    return visible


def build_identity() -> dict[str, Any]:
    """Expose immutable deployment identity for production E2E verification."""
    return {
        "schema_version": 1,
        "runtime_contract": RUNTIME_CONTRACT_VERSION,
        "render": bool(os.getenv("RENDER")),
        "commit": os.getenv("RENDER_GIT_COMMIT"),
        "branch": os.getenv("RENDER_GIT_BRANCH"),
        "repo": os.getenv("RENDER_GIT_REPO_SLUG"),
        "external_url": os.getenv("RENDER_EXTERNAL_URL"),
    }

@app.get("/api/gorila/build")
def gorila_build():
    return build_identity()

@app.get("/api/gorila/health")
def gorila_health():
    live = dict(_ARG_LIVE_STATE)
    snap = dict(_ARG_SIGNAL_STATE)
    snapshot_ready = snap.get("updated_symbols", 0) >= len(SIGNAL_SYMBOLS)
    db = persistence_summary() if _DB_STATE.get("ready") else {"ready": False, "status": _DB_STATE.get("status")}
    return {
        "service": "gorila-argentum",
        "build": build_identity(),
        "mode": "RESEARCH",
        "trading_execution": False,
        "automatic_promotion": False,
        "primary_market_data": {
            "provider": "BYMADATA_OPEN_ACCESS",
            "configured": True,
            "source": "BYMADATA_OPEN_ACCESS" if live.get("updated_symbols", 0) else "ARGENTINA_SIGNAL_SNAPSHOT",
            "status": (
                "READY_LIVE" if live.get("status") == "HEALTHY" and live.get("live_symbols", 0) == len(SIGNAL_SYMBOLS)
                else "READY_DELAYED" if live.get("status") in {"DELAYED", "STALE"}
                else "READY_SNAPSHOT" if snapshot_ready else "DEGRADED"
            ),
            "transport_status": live.get("transport_status"),
            "live_symbols": live.get("live_symbols", 0),
            "delayed_symbols": live.get("delayed_symbols", 0),
            "stale_symbols": live.get("stale_symbols", 0),
        },
        "database": db,
        "market_session": market_session_state(),
        "autonomous_runtime": dict(_AUTONOMOUS_STATE),
        "argentina_live": live,
        "argentina_signals": snap,
        "macro_ingest": dict(_MACRO_STATE),
        "db_init": dict(_DB_STATE),
        "sources": _visible_source_health(Store().health()) if _DB_STATE.get("ready") else [],
    }

@app.get("/api/gorila/market")
def gorila_market():
    return build_market_state()


@app.get("/api/gorila/g2-h10/status")
def gorila_g2_h10_status():
    manifest_path = Path(__file__).resolve().parents[1] / "research" / "g2_h10_frozen_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "UNAVAILABLE",
            "model_id": "G2_PIT_FIXED_C0.25_H10",
            "promotion": "BLOCKED",
            "runtime_serving": "DISABLED",
            "error": f"{type(exc).__name__}: {exc}",
            "research_only": True,
            "no_execution_authority": True,
        }
    artifact_path = os.getenv(
        "GORILA_G2_ARTIFACT_PATH",
        manifest.get("source_artifact") or "",
    )
    artifact_present = bool(artifact_path and Path(artifact_path).exists())
    return {
        "status": "REGISTERED",
        **manifest,
        "artifact_path": artifact_path or None,
        "artifact_present": artifact_present,
        "scorer_module": "gorila_argentum.g2_frozen_scorer",
        "scorer_integration": "RESEARCH_ONLY_IMPLEMENTED",
        "research_only": True,
        "no_execution_authority": True,
        "runtime_serving": manifest.get(
            "runtime_serving",
            "DISABLED_UNTIL_EXACT_PACKAGE_VERIFIED_AND_SCORER_REPRODUCED",
        ),
    }


@app.get("/api/gorila/g2-h10/score")
def gorila_g2_h10_score():
    from gorila_argentum.g2_frozen_scorer import G2FrozenModel, load_g2_frozen_model_from_env, score_g2_h10

    try:
        manifest_path = Path(__file__).resolve().parents[1] / "research" / "g2_h10_frozen_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "UNAVAILABLE",
            "model_id": "G2_PIT_FIXED_C0.25_H10",
            "reason": "G2_MANIFEST_UNAVAILABLE",
            "error": f"{type(exc).__name__}: {exc}",
            "research_only": True,
            "no_execution_authority": True,
        }

    artifact_path = os.getenv("GORILA_G2_ARTIFACT_PATH") or manifest.get("source_artifact") or ""
    if not artifact_path or not Path(artifact_path).exists():
        return {
            "status": "ARTIFACT_UNAVAILABLE",
            "model_id": manifest.get("model_id", "G2_PIT_FIXED_C0.25_H10"),
            "model_package_sha256": manifest.get("model_package_sha256"),
            "artifact_path": artifact_path or None,
            "runtime_serving": "DISABLED",
            "research_only": True,
            "no_execution_authority": True,
        }

    try:
        model = load_g2_frozen_model_from_env()
        store = Store()
        store.init()
        result = score_g2_h10(store, model)
        return {
            **result,
            "research_only": True,
            "no_execution_authority": True,
            "runtime_serving": "DISABLED",
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "model_id": manifest.get("model_id", "G2_PIT_FIXED_C0.25_H10"),
            "error": f"{type(exc).__name__}: {exc}",
            "runtime_serving": "DISABLED",
            "research_only": True,
            "no_execution_authority": True,
        }


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
    session = argentina_session_state()
    snapshot = _ARG_LIVE_CACHE.get(symbol)
    if snapshot is None:
        status = "MARKET_CLOSED" if not session.get("open") else "NO_LIVE_CACHE"
        return {
            "symbol": symbol,
            "status": status,
            "quote": None,
            "age_seconds": None,
            "event_age_seconds": None,
            "transport_age_seconds": None,
            "is_live": False,
            "session": session,
            "runtime": dict(_ARG_LIVE_STATE),
            "research_only": True,
            "no_execution_authority": True,
        }
    freshness = assess_observation(snapshot.get("quote_timestamp"), snapshot.get("received_at"))
    if not session.get("open"):
        status = "MARKET_CLOSED"
    else:
        status = str(freshness.get("status") or "INVALID_TIMESTAMP")
    return {
        "symbol": symbol,
        "status": status,
        "quote": {
            "last": snapshot.get("last"),
            "quoteTimestamp": snapshot.get("quote_timestamp"),
            "timestamp": snapshot.get("quote_timestamp"),
            "source": snapshot.get("source"),
        },
        # Backward-compatible alias: age_seconds now means market-event age,
        # not HTTP/cache receipt age.
        "age_seconds": freshness.get("event_age_seconds"),
        "event_age_seconds": freshness.get("event_age_seconds"),
        "transport_age_seconds": freshness.get("transport_age_seconds"),
        "is_live": status == "LIVE",
        "freshness": freshness,
        "received_at": snapshot.get("received_at"),
        "latency_ms": snapshot.get("latency_ms"),
        "session": session,
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


@app.get("/api/gorila/chart/{ticker}")
def gorila_chart(
    ticker: str,
    timeframe: str = "1D",
    limit: int | None = None,
):
    """Low-latency market chart surface for the single Gorila terminal UI."""
    symbol = normalize_ticker(ticker)
    if symbol not in SIGNAL_SYMBOLS:
        raise HTTPException(status_code=404, detail="ARGENTUM_SYMBOL_NOT_IN_UNIVERSE")

    presets = {
        "1D": ("close_1m", 390, "1m"),
        "5D": ("close_5m", 390, "5m"),
        "1M": ("close", 30, "1d"),
        "3M": ("close", 90, "1d"),
        "6M": ("close", 180, "1d"),
        "1Y": ("close", 252, "1d"),
    }
    key = str(timeframe or "1D").upper()
    if key not in presets:
        raise HTTPException(status_code=400, detail={"code": "INVALID_CHART_TIMEFRAME", "allowed": sorted(presets)})

    field, default_limit, resolution = presets[key]
    requested_limit = default_limit if limit is None else max(30, min(int(limit), 1000))
    store = Store()
    store.init()

    selected_field = field
    rows = store.recent_series(symbol, field, limit=requested_limit)

    # Fall back only when the requested intraday fabric has no observations.
    # The response identifies the effective resolution so the UI never implies
    # unavailable precision.
    if not rows and field in {"close_1m", "close_5m"}:
        fallback_limit = 90 if key in {"1D", "5D"} else requested_limit
        rows = store.recent_series(symbol, "close", limit=fallback_limit)
        selected_field = "close"
        resolution = "1d"
    # Keep the chart aligned with the canonical latest live observation. The
    # persistent series may lag one write cycle behind the in-memory cache.
    live = _ARG_LIVE_CACHE.get(symbol)
    if live and selected_field == "close_1m" and live.get("quote_timestamp"):
        live_point = (str(live["quote_timestamp"]), float(live["last"]))
        if not rows or str(rows[-1][0]) < live_point[0]:
            rows = [*rows, live_point][-requested_limit:]

    status = "READY" if rows else "NO_DATA"
    if rows and selected_field in {"close_1m", "close_5m"}:
        latest_freshness = assess_observation(rows[-1][0])
    else:
        latest_freshness = {
            "status": "HISTORICAL",
            "event_age_seconds": assess_observation(rows[-1][0]).get("event_age_seconds"),
            "transport_age_seconds": None,
        } if rows else {"status":"INVALID_TIMESTAMP","event_age_seconds":None,"transport_age_seconds":None}

    first = float(rows[0][1]) if rows else None
    last = float(rows[-1][1]) if rows else None
    change = (last - first) if rows and first is not None and last is not None else None
    change_pct = (change / first) if rows and first not in (None, 0) and change is not None else None

    return {
        "service": "gorila-argentum",
        "symbol": symbol,
        "timeframe": key,
        "status": status,
        "field": selected_field,
        "resolution": resolution,
        "requested_limit": requested_limit,
        "rows": [{"time": row[0], "close": row[1]} for row in rows],
        "bars": len(rows),
        "coverage": {
            "start": rows[0][0] if rows else None,
            "end": rows[-1][0] if rows else None,
        },
        "change": change,
        "change_pct": change_pct,
        "last": last,
        "research_only": True,
        "no_execution_authority": True,
        "freshness": latest_freshness,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

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
    signal_payload = signal or {"symbol": symbol, "status": "NO_DATA", "probability": {"up": None, "down": None}}
    probability = signal_payload.get("probability") or {}
    forecast_status = "READY" if probability.get("up") is not None else "NO_DATA"
    forecast_payload = {
        "forecast_status": forecast_status,
        "forecast": {
            "p_up": probability.get("up"),
            "p_down": probability.get("down"),
            "confidence": probability.get("confidence"),
            "direction": signal_payload.get("signal"),
        } if forecast_status == "READY" else None,
        "signal_status": signal_payload.get("status"),
        "validated": (signal_payload.get("validation") or {}).get("validated"),
        "research_only": True,
        "no_execution_authority": True,
    }
    return {
        "service": "gorila-argentum",
        "symbol": symbol,
        "quote": {
            "last": (live or {}).get("last") if live else (signal or {}).get("market", {}).get("last"),
            "quoteTimestamp": (live or {}).get("quote_timestamp") if live else (signal or {}).get("market", {}).get("quote_timestamp"),
            "source": (live or {}).get("source") if live else (signal or {}).get("market", {}).get("source"),
        } if (live or signal) else None,
        "quote_error": None,
        "forecast": forecast_payload,
        "signal": signal_payload,
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
