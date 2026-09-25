import asyncio
import os
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response, JSONResponse

from quant.specialists.flow import run_flow_specialist
from quant.online import observe_online
from quant.specialists.historical import (
    get_historical_forecast,
    get_historical_bars,
    get_cached_bars,
)
from quant.cross_asset import get_cross_asset_forecast
from quant.horizon_consensus import build_horizon_consensus
from quant.multihorizon_meta import validate_multi_horizon_meta
from quant.cross_asset_validation import validate_cross_asset_symbol
from quant.db import (
    get_outcome,
    init_db,
    repair_probabilistic_outcomes,
    outcome_summary,
    eligible_outcomes,
    pending_due_forecasts,
    persistence_summary,
    record_backtest,
    record_forecast,
    record_model_registry,
    record_outcome,
    active_research_run,
    recover_incomplete_research_runs,
    create_research_run,
    update_research_run,
    get_research_run,
    latest_research_run,
    record_online_cohort_sample,
    load_online_cohort_samples,
    online_cohort_summary,
)
from quant.data_health import assess_quote
from quant.execution_costs import estimate_execution_cost
from quant.model_health import assess_model_health
from quant.model_registry import build_registry_record
from quant.meta import combine_specialists
from quant.trigger import evaluate_trigger
from quant.outcome import resolve_forecast
from quant.online import observe_online, evaluate_temporal_cohort, ENGINE as ONLINE_ENGINE
from quant.market_stream import get_quote as get_stream_quote, start_stream, stop_stream, stream_status
from quant.v0_ofi import run_v0_research

APP_DIR = Path(__file__).resolve().parent
TIINGO_IEX_URL = "https://api.tiingo.com/iex"
TIINGO_EQUITY_URL = "https://api.tiingo.com/tiingo/equity/intraday"

TICKER_ALIASES = {
    "APPLE": "AAPL",
    "MICROSOFT": "MSFT",
    "MICROSOF": "MSFT",
    "NVIDIA": "NVDA",
    "NDIVIA": "NVDA",
    "TESLA": "TSLA",
}

app = FastAPI(title="PMSF-X Nano", version="0.4.0")
DB_READY = False
OUTCOME_COLLECTOR_TASK = None
OUTCOME_RESOLVER_TASK = None
OUTCOME_FORECAST_TASK = None
COLLECTOR_SYMBOLS = ("AAPL", "MSFT", "NVDA", "TSLA")
OUTCOME_RESOLVER_INTERVAL_SECONDS = 60.0
OUTCOME_FORECAST_INTERVAL_SECONDS = 600.0
ONLINE_COHORT_INTERVAL_SECONDS = 5.0
RESEARCH_PROCESS_POOL = ProcessPoolExecutor(
    max_workers=1,
    mp_context=mp.get_context("spawn"),
)
US_EASTERN = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
MARKET_LAST_FORECAST = time(15, 59, 30)
MARKET_CLOSE = time(16, 0)


def is_us_equity_session(now: datetime | None = None) -> bool:
    current = (now or datetime.now(timezone.utc)).astimezone(US_EASTERN)
    if current.weekday() >= 5:
        return False
    return MARKET_OPEN <= current.time() <= MARKET_LAST_FORECAST


def market_session_state(now: datetime | None = None) -> dict[str, object]:
    current = (now or datetime.now(timezone.utc)).astimezone(US_EASTERN)
    return {
        "timezone": "America/New_York",
        "local_time": current.isoformat(),
        "weekday": current.weekday(),
        "open": is_us_equity_session(current),
        "forecast_window": {
            "open": MARKET_OPEN.isoformat(),
            "last_forecast": MARKET_LAST_FORECAST.isoformat(),
            "close": MARKET_CLOSE.isoformat(),
        },
    }


def normalize_ticker(value: str) -> str:
    symbol = value.strip().upper()
    return TICKER_ALIASES.get(symbol, symbol)


def tiingo_headers() -> dict:
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")
    return {"Authorization": f"Token {token}", "Content-Type": "application/json"}


def first_record(data):
    if isinstance(data, list):
        return data[0] if data else None
    if isinstance(data, dict):
        return data
    return None


def _snapshot_fields(source: str, quote_data: dict) -> dict:
    if source == "tiingo_iex_tops":
        return {
            "last": quote_data.get("last"),
            "bid": quote_data.get("bidPrice"),
            "ask": quote_data.get("askPrice"),
            "bid_size": quote_data.get("bidSize") or 0,
            "ask_size": quote_data.get("askSize") or 0,
        }
    return {
        "last": quote_data.get("tngoLast") if quote_data.get("tngoLast") is not None else quote_data.get("last"),
        "bid": quote_data.get("lqBidPrice"),
        "ask": quote_data.get("lqAskPrice"),
        "bid_size": quote_data.get("lqBidSize") or 0,
        "ask_size": quote_data.get("lqAskSize") or 0,
    }


@app.on_event("startup")
async def initialize_persistence():
    global DB_READY
    try:
        DB_READY = bool(init_db())
        if DB_READY:
            repaired = repair_probabilistic_outcomes()
            recovered = recover_incomplete_research_runs()
            for _symbol in COLLECTOR_SYMBOLS:
                restored = load_online_cohort_samples(_symbol, limit=600)
                if restored:
                    ONLINE_ENGINE.restore(_symbol, restored)
                    print("PMSF-X ONLINE COHORT RESTORED:", {"symbol": _symbol, "samples": len(restored)})
            print("PMSF-X OUTCOME ELIGIBILITY REPAIR:", {"rows_updated": repaired})
            print(
                "PMSF-X RESEARCH STALE RECOVERY:",
                {"rows_recovered": recovered},
            )
            audit_rows = eligible_outcomes(limit=100)
            audit_losses = [row["brier_loss"] for row in audit_rows if row["brier_loss"] is not None]
            print(
                "PMSF-X OUTCOME QUALITY AUDIT:",
                {
                    "eligible_count": len(audit_rows),
                    "mean_brier": round(sum(audit_losses) / len(audit_losses), 6) if audit_losses else None,
                    "eligible_rows": [
                        {
                            "outcome_id": row["id"],
                            "forecast_id": row["forecast_id"],
                            "symbol": row["symbol"],
                            "p_up": row["forecast_p_up"],
                            "direction": row["forecast_direction"],
                            "realized_direction": row["realized_direction"],
                            "return_bps": row["realized_return_bps"],
                            "brier": row["brier_loss"],
                            "elapsed_s": row["actual_elapsed_seconds"],
                            "horizon_s": row["target_horizon_seconds"],
                        }
                        for row in audit_rows
                    ],
                },
            )
        print("PMSF-X DB:", "READY" if DB_READY else "NOT_CONFIGURED")
    except Exception as exc:
        DB_READY = False
        print(f"PMSF-X DB ERROR: {type(exc).__name__}: {exc}")


@app.on_event("startup")
async def research_validation_runner():
    if os.getenv("PMSFX_RESEARCH_BOOT", "0") != "1":
        return
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        print("PMSF-X RESEARCH RUNNER: TIINGO_API_KEY missing")
        return

    symbols = tuple(
        item.strip().upper()
        for item in os.getenv("PMSFX_RESEARCH_SYMBOLS", "AAPL").split(",")
        if item.strip()
    )
    print(
        "PMSF-X RESEARCH RUNNER: STARTED",
        {"symbols": symbols, "mode": "non_blocking_one_shot"},
    )

    async def run():
        for symbol in symbols:
            try:
                historical = await asyncio.wait_for(
                    get_historical_forecast(symbol, token, evaluate=False),
                    timeout=60.0,
                )
                rows = get_cached_bars(symbol)
                if not rows:
                    print(
                        "PMSF-X RESEARCH RUNNER: NO_BARS",
                        {"symbol": symbol},
                    )
                    continue
                validation = await asyncio.wait_for(
                    asyncio.to_thread(
                        validate_multi_horizon_meta,
                        rows,
                        symbol=symbol,
                    ),
                    timeout=600.0,
                )
                print(
                    "PMSF-X RESEARCH RUNNER: RESULT",
                    {
                        "symbol": symbol,
                        "status": validation.get("status"),
                        "validated": validation.get("validated"),
                        "sample_counts": validation.get("sample_counts"),
                        "outer_folds": validation.get("outer_fold_count"),
                        "usable_folds": validation.get("usable_fold_count"),
                        "oos_count": validation.get("oos_count"),
                        "metrics": validation.get("metrics"),
                        "primary_300s_metrics": validation.get("primary_300s_metrics"),
                        "delta_brier_vs_300s": validation.get("delta_brier_vs_300s"),
                        "bootstrap": validation.get("bootstrap"),
                        "gate": validation.get("gate"),
                    },
                )
            except Exception as exc:
                print(
                    "PMSF-X RESEARCH RUNNER: ERROR",
                    {
                        "symbol": symbol,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
        print("PMSF-X RESEARCH RUNNER: FINISHED", {"symbols": symbols})

    asyncio.create_task(run())


@app.on_event("startup")
async def tiingo_startup_check():
    if os.getenv("PMSFX_TIINGO_SELFTEST") != "1":
        print("PMSF-X SELFTEST AAPL: SKIPPED (set PMSFX_TIINGO_SELFTEST=1 to enable)")
        return
    if not os.getenv("TIINGO_API_KEY"):
        print("PMSF-X SELFTEST AAPL: TIINGO_API_KEY missing")
        return

    await asyncio.sleep(2.0)
    stream_quote = get_stream_quote("AAPL")
    if stream_quote:
        print(
            "PMSF-X SELFTEST AAPL:",
            {
                "source": "tiingo_iex_websocket",
                "last": stream_quote.get("last"),
                "bid": stream_quote.get("bidPrice"),
                "ask": stream_quote.get("askPrice"),
                "quote_timestamp": stream_quote.get("quoteTimestamp"),
            },
        )
    else:
        print("PMSF-X SELFTEST AAPL: WS CACHE NOT_READY")



async def online_cohort_loop():
    """Capture high-frequency WS samples for the online-flow research cohort."""
    print("PMSF-X ONLINE COHORT: LOOP_ENTERED", {"interval_s": ONLINE_COHORT_INTERVAL_SECONDS})
    while True:
        if not DB_READY or not os.getenv("TIINGO_API_KEY") or not is_us_equity_session():
            await asyncio.sleep(ONLINE_COHORT_INTERVAL_SECONDS)
            continue
        iteration_stats = {"quotes": 0, "observations": 0, "new_samples": 0, "db_saved": 0, "missing_quotes": 0, "errors": 0}
        for symbol in COLLECTOR_SYMBOLS:
            try:
                q = get_stream_quote(symbol)
                if not q:
                    iteration_stats["missing_quotes"] += 1
                    continue
                iteration_stats["quotes"] += 1
                bid = q.get("bidPrice")
                ask = q.get("askPrice")
                last = q.get("last")
                if bid is None or ask is None:
                    continue
                mid = (float(bid) + float(ask)) / 2.0
                spread_bps = (float(ask) - float(bid)) / mid * 10000.0 if mid > 0 else None
                result = observe_online(
                    symbol=symbol,
                    last=last if last is not None else mid,
                    bid=bid,
                    ask=ask,
                    bid_size=q.get("bidSize") or 0,
                    ask_size=q.get("askSize") or 0,
                    spread_bps=spread_bps,
                    microprice=mid,
                )
                iteration_stats["observations"] += 1
                for sample in result.get("newly_resolved", []):
                    iteration_stats["new_samples"] += 1
                    from datetime import datetime as _dt
                    event_time = _dt.fromtimestamp(float(sample["event_time"]), tz=timezone.utc)
                    label_end_time = _dt.fromtimestamp(float(sample["label_end_time"]), tz=timezone.utc)
                    sample["event_time"] = event_time
                    sample["label_end_time"] = label_end_time
                    if record_online_cohort_sample(sample):
                        iteration_stats["db_saved"] += 1
            except Exception as exc:
                iteration_stats["errors"] += 1
                print("PMSF-X ONLINE COHORT ERROR:", {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
        print("PMSF-X ONLINE COHORT HEARTBEAT:", iteration_stats)
        await asyncio.sleep(ONLINE_COHORT_INTERVAL_SECONDS)


@app.get("/api/research/v0-ofi/{ticker}")
async def v0_ofi_research(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")
    try:
        # V0 measurement must use a fresh point-in-time historical pull.
        # Do not silently fall back to the live-bar cache, which may contain
        # only a small recent window and would invalidate the OOS sample size.
        source = "tiingo_history_force_refresh"
        rows = await asyncio.wait_for(
            get_historical_bars(symbol, token, force_refresh=True),
            timeout=120.0,
        )
        if not rows:
            raise HTTPException(status_code=503, detail="Historical bars unavailable")
        result = await asyncio.to_thread(run_v0_research, rows, symbol=symbol)
        result["history_source"] = source
        print("PMSF-X V0 OFI RESEARCH RESULT:", {"symbol": symbol, "bars": result.get("bars_used"), "status": result.get("status"), "summary": result.get("validation_summary")})
        return result
    except HTTPException:
        raise
    except Exception as exc:
        print("PMSF-X V0 OFI RESEARCH ERROR:", {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


@app.get("/api/research/online-cohort")
def online_cohort_status():
    summary = online_cohort_summary()
    payload = {"service": "pmsfx-nano", "cohort": summary, "symbols": {}}
    for symbol in COLLECTOR_SYMBOLS:
        rows = load_online_cohort_samples(symbol, limit=600)
        payload["symbols"][symbol] = evaluate_temporal_cohort(rows)
    return payload


@app.get("/api/research/online-cohort/probe")
def online_cohort_probe():
    summary = online_cohort_summary()
    stream = stream_status()
    payload = {
        "service": "pmsfx-nano",
        "status": "FAIL",
        "database_ready": bool(summary.get("ready")),
        "stream_status": stream.get("status"),
        "stream_symbols": stream.get("cached_symbols"),
        "cohort_count": int(summary.get("count") or 0) if summary.get("ready") else 0,
        "cohort_up": int(summary.get("up") or 0) if summary.get("ready") else 0,
        "cohort_down": int(summary.get("down") or 0) if summary.get("ready") else 0,
        "first_event": summary.get("first_event"),
        "last_label": summary.get("last_label"),
    }
    healthy = (
        payload["database_ready"]
        and payload["stream_status"] == "CONNECTED"
        and payload["cohort_count"] > 0
        and payload["cohort_up"] > 0
        and payload["cohort_down"] > 0
    )
    payload["status"] = "PASS" if healthy else "FAIL"
    return JSONResponse(status_code=200 if healthy else 503, content=payload)

async def outcome_resolver_loop():
    while True:
        if not DB_READY or not os.getenv("TIINGO_API_KEY"):
            await asyncio.sleep(OUTCOME_RESOLVER_INTERVAL_SECONDS)
            continue
        try:
            resolved = await asyncio.wait_for(resolve_outcomes(limit=1), timeout=30.0)
            print(
                "PMSF-X OUTCOME RESOLVER:",
                {
                    "resolved": resolved.get("resolved"),
                    "errors": resolved.get("errors"),
                    "outcome_count": outcome_summary().get("outcome_count"),
                    "binary_eligible_count": outcome_summary().get("binary_eligible_count"),
                    "eligibility_breakdown": outcome_summary().get("eligibility_breakdown"),
                },
            )
            if any("429" in str(item.get("error") or item.get("reason")) for item in resolved.get("error_details", [])):
                await asyncio.sleep(300.0)
        except Exception as exc:
            print(
                "PMSF-X OUTCOME RESOLVER ERROR:",
                {"error": f"{type(exc).__name__}: {exc}"},
            )
        await asyncio.sleep(OUTCOME_RESOLVER_INTERVAL_SECONDS)


async def collect_forecast_once(symbol: str):
    print("PMSF-X OUTCOME FORECAST COLLECTOR: ATTEMPT", {"symbol": symbol})
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(lambda: asyncio.run(state(symbol))),
            timeout=180.0,
        )
        print(
            "PMSF-X OUTCOME FORECAST COLLECTOR:",
            {
                "symbol": symbol,
                "forecast_id": payload.get("forecast_id"),
                "forecast_status": payload.get("forecast_status"),
            },
        )
        return payload
    except Exception as exc:
        print(
            "PMSF-X OUTCOME FORECAST COLLECTOR ERROR:",
            {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"},
        )
        return None


async def outcome_forecast_loop():
    index = 0
    print("PMSF-X OUTCOME FORECAST COLLECTOR: LOOP_ENTERED")
    while True:
        if not DB_READY or not os.getenv("TIINGO_API_KEY"):
            await asyncio.sleep(OUTCOME_FORECAST_INTERVAL_SECONDS)
            continue

        if not is_us_equity_session():
            await asyncio.sleep(OUTCOME_FORECAST_INTERVAL_SECONDS)
            continue

        symbol = COLLECTOR_SYMBOLS[index % len(COLLECTOR_SYMBOLS)]
        index += 1
        await collect_forecast_once(symbol)
        await asyncio.sleep(OUTCOME_FORECAST_INTERVAL_SECONDS)


@app.on_event("startup")
async def tiingo_market_stream_startup():
    if os.getenv("TIINGO_API_KEY"):
        start_stream()

@app.on_event("startup")
async def outcome_collector_startup():
    global OUTCOME_COLLECTOR_TASK, OUTCOME_RESOLVER_TASK, OUTCOME_FORECAST_TASK
    if os.getenv("PMSFX_OUTCOME_COLLECTOR") != "1":
        return
    OUTCOME_RESOLVER_TASK = asyncio.create_task(outcome_resolver_loop())
    OUTCOME_FORECAST_TASK = asyncio.create_task(outcome_forecast_loop())
    if os.getenv("PMSFX_ONLINE_COHORT", "1") != "0":
        asyncio.create_task(online_cohort_loop())
    OUTCOME_COLLECTOR_TASK = OUTCOME_FORECAST_TASK
    print(
        "PMSF-X OUTCOME COLLECTOR: STARTED",
        {
            "resolver_interval_seconds": OUTCOME_RESOLVER_INTERVAL_SECONDS,
            "forecast_interval_seconds": OUTCOME_FORECAST_INTERVAL_SECONDS,
            "symbols": COLLECTOR_SYMBOLS,
            "target_internal_request_budget_per_hour": 6,
            "market_data_path": "Tiingo consolidated WebSocket + REST history cache",
            "task_mode": "independent_background_tasks",
        },
    )


@app.on_event("shutdown")
async def research_process_pool_shutdown():
    RESEARCH_PROCESS_POOL.shutdown(
        wait=False,
        cancel_futures=True,
    )
    print("PMSF-X RESEARCH PROCESS POOL: STOPPED")


@app.on_event("shutdown")
async def outcome_collector_shutdown():
    global OUTCOME_COLLECTOR_TASK, OUTCOME_RESOLVER_TASK, OUTCOME_FORECAST_TASK

    for task in (OUTCOME_RESOLVER_TASK, OUTCOME_FORECAST_TASK):
        if task is not None:
            task.cancel()

    for task in (OUTCOME_RESOLVER_TASK, OUTCOME_FORECAST_TASK):
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass

    OUTCOME_RESOLVER_TASK = None
    OUTCOME_FORECAST_TASK = None
    OUTCOME_COLLECTOR_TASK = None
    print("PMSF-X OUTCOME COLLECTOR: STOPPED")
    stop_stream()


@app.head("/", include_in_schema=False)
def dashboard_head():
    return Response(status_code=200)


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(
        APP_DIR / "static" / "index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/health")
def health():
    tiingo_configured = bool(os.getenv("TIINGO_API_KEY"))
    status = "ok" if tiingo_configured and DB_READY else "degraded"
    return {
        "status": status,
        "service": "pmsfx-nano",
        "version": "0.4.0",
        "tiingo_configured": tiingo_configured,
        "db_ready": DB_READY,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "market_stream": stream_status(),
        "market_session": market_session_state(),
    }


@app.get("/api/persistence")
def persistence():
    summary = persistence_summary()
    return {
        "service": "pmsfx-nano",
        "version": "0.4.0",
        **summary,
    }


@app.get("/api/outcome/summary")
def outcome_summary_endpoint():
    return {
        "service": "pmsfx-nano",
        "version": "0.4.0",
        **outcome_summary(),
    }


@app.get("/api/outcome/eligible")
def eligible_outcomes_endpoint(limit: int = 100):
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
    rows = eligible_outcomes(limit=limit)
    losses = [row["brier_loss"] for row in rows if row["brier_loss"] is not None]
    directional = [
        row["prediction_correct"]
        for row in rows
        if row["forecast_direction"] in ("UP", "DOWN")
        and row["realized_direction"] in ("UP", "DOWN")
        and row["prediction_correct"] is not None
    ]
    return {
        "service": "pmsfx-nano",
        "count": len(rows),
        "mean_brier": round(sum(losses) / len(losses), 6) if losses else None,
        "directional_accuracy": round(sum(1 for x in directional if x) / len(directional), 4) if directional else None,
        "rows": rows,
    }


@app.post("/api/outcome/resolve")
async def resolve_outcomes(limit: int = 20):
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")

    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")

    due = pending_due_forecasts(limit=limit)
    resolved = []
    errors = []

    for forecast in due:
        try:
            outcome = await resolve_forecast(forecast, token)
            if outcome.get("status") != "RESOLVED":
                errors.append(outcome)
                continue
            outcome_id = record_outcome(outcome)
            outcome["outcome_id"] = outcome_id
            resolved.append(outcome)
            print(
                "PMSF-X OUTCOME SAVED:",
                {
                    "outcome_id": outcome_id,
                    "forecast_id": outcome["forecast_id"],
                    "symbol": outcome["symbol"],
                    "realized_return_bps": outcome["realized_return_bps"],
                    "prediction_correct": outcome["prediction_correct"],
                    "binary_eligible": outcome["binary_eligible"],
                    "eligibility_reason": (outcome.get("metadata") or {}).get("eligibility_reason"),
                    "actual_elapsed_seconds": outcome["actual_elapsed_seconds"],
                },
            )
        except Exception as exc:
            error = {
                "forecast_id": forecast["id"],
                "symbol": forecast["symbol"],
                "status": "ERROR",
                "error": f"{type(exc).__name__}: {exc}",
            }
            errors.append(error)
            print(f"PMSF-X OUTCOME ERROR: {error}")

    return {
        "service": "pmsfx-nano",
        "attempted": len(due),
        "resolved": len(resolved),
        "errors": len(errors),
        "outcomes": resolved,
        "error_details": errors,
    }


@app.get("/api/outcome/{forecast_id}")
def outcome_by_forecast(forecast_id: int):
    result = get_outcome(forecast_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Outcome not found")
    return result


@app.get("/api/quote/{ticker}")
async def quote(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")

    stream_quote = get_stream_quote(symbol)
    if (
        stream_quote
        and stream_quote.get("last") is not None
        and stream_quote.get("bidPrice") is not None
        and stream_quote.get("askPrice") is not None
    ):
        received_at = stream_quote.get("received_at") or datetime.now(timezone.utc).isoformat()
        return {
            "source": "tiingo_iex_websocket",
            "received_at": received_at,
            "symbol": symbol,
            "quote": stream_quote,
        }

    headers = tiingo_headers()
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{TIINGO_IEX_URL}/{symbol}", headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail="Tiingo IEX request failed")

        iex = first_record(response.json()) or {}
        has_iex_tops = iex.get("bidPrice") is not None and iex.get("askPrice") is not None
        if has_iex_tops:
            return {
                "source": "tiingo_iex_tops",
                "received_at": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "quote": iex,
            }

        response = await client.get(f"{TIINGO_EQUITY_URL}/{symbol}", headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail="Tiingo equity intraday request failed")

        equity = first_record(response.json())
        if not equity:
            raise HTTPException(status_code=404, detail="No Tiingo quote returned")

        return {
            "source": "tiingo_equity_intraday",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "quote": equity,
        }

@app.get("/api/state/{ticker}")
async def state(ticker: str):
    symbol = normalize_ticker(ticker)
    print("PMSF-X FORECAST PIPELINE: STATE_START", {"symbol": symbol})
    result = await quote(symbol)
    print("PMSF-X FORECAST PIPELINE: QUOTE_READY", {"symbol": symbol, "source": result.get("source")})
    q = result["quote"]

    fields = _snapshot_fields(result["source"], q)
    last = fields["last"]
    bid = fields["bid"]
    ask = fields["ask"]
    bid_size = fields["bid_size"]
    ask_size = fields["ask_size"]
    feed_label = "Tiingo IEX WebSocket" if result["source"] == "tiingo_iex_websocket" else ("Tiingo IEX TOPS" if result["source"] == "tiingo_iex_tops" else "Tiingo consolidated · liquidity reference")

    spread_bps = None
    microprice = None
    if bid is not None and ask is not None and bid > 0:
        mid = (bid + ask) / 2
        spread_bps = round((ask - bid) / mid * 10000, 3)
        if bid_size + ask_size > 0:
            microprice = round((ask * bid_size + bid * ask_size) / (bid_size + ask_size), 6)

    data_health = assess_quote(
        symbol=symbol,
        last=last,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        quote_timestamp=q.get("quoteTimestamp") or q.get("timestamp"),
        received_at=result["received_at"],
    )
    healthy = data_health["status"] == "HEALTHY"

    if not healthy:
        return {
            "symbol": symbol,
            "last": last,
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "spread_bps": spread_bps,
            "microprice": microprice,
            "data_health": data_health,
            "data_source": result["source"],
            "feed_label": feed_label,
            "quote_timestamp": q.get("quoteTimestamp") or q.get("timestamp"),
            "received_at": result["received_at"],
            "forecast": None,
            "forecast_status": "BLOCKED_DATA_HEALTH",
            "gatillazo": "BLOCKED_DATA_HEALTH",
            "specialists": {},
            "model": {"id": None, "status": "BLOCKED_DATA_HEALTH"},
            "evaluation": {},
            "rule": "NO DATA HEALTH -> NO FORECAST -> NO GATILLAZO",
            "forecast_id": None,
        }

    flow = run_flow_specialist(
        last=last,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        spread_bps=spread_bps,
        microprice=microprice,
    )

    online = observe_online(
        symbol=symbol,
        last=last,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        spread_bps=spread_bps,
        microprice=microprice,
    )

    token = os.getenv("TIINGO_API_KEY")
    print("PMSF-X FORECAST PIPELINE: HISTORICAL_START", {"symbol": symbol})
    historical = (
        await get_historical_forecast(symbol, token, evaluate=False)
        if token and healthy
        else {"status": "HISTORICAL_SKIPPED", "forecast": None, "evaluation": {}, "model_id": "historical-logit-v1"}
    )
    print(
        "PMSF-X FORECAST PIPELINE: HISTORICAL_READY",
        {
            "symbol": symbol,
            "status": historical.get("status"),
            "bars": historical.get("bars"),
            "error": historical.get("error"),
        },
    )

    # Cross-asset lead/lag remains research-only until its independent OOS
    # validation gate passes. Keep it out of the latency-critical live path.
    target_rows = get_cached_bars(symbol)
    cross_asset = {
        "status": "RESEARCH_DECOUPLED",
        "model_id": "cross-asset-leadlag-v1",
        "forecast": None,
        "evaluation": {},
        "context": None,
        "reason": "RESEARCH_ONLY_LIVE_PATH_EXCLUDED",
    }
    print(
        "PMSF-X FORECAST PIPELINE: CROSS_ASSET_READY",
        {
            "symbol": symbol,
            "status": cross_asset.get("status"),
            "samples": 0,
            "validated": False,
        },
    )

    multi_horizon = historical.get("multi_horizon") or {}
    horizon_consensus = build_horizon_consensus(
        multi_horizon,
        cross_asset=cross_asset,
    )
    print(
        "PMSF-X FORECAST PIPELINE: HORIZON_CONSENSUS_READY",
        {
            "symbol": symbol,
            "status": horizon_consensus.get("status"),
            "horizon_count": horizon_consensus.get("horizon_count"),
            "confluence_index": horizon_consensus.get("confluence_index"),
            "production_eligible": horizon_consensus.get("production_eligible"),
        },
    )

    # Historical price model is primary until live flow and cross-asset
    # specialists have their own validation evidence.
    hist_forecast = historical.get("forecast")
    flow_forecast = online.get("forecast")
    cross_forecast = cross_asset.get("forecast")
    meta = combine_specialists(
        historical=hist_forecast,
        flow=flow_forecast,
        historical_evaluation=historical.get("evaluation", {}),
        cross_asset=cross_forecast,
        cross_asset_evaluation=cross_asset.get("evaluation", {}),
    )

    if meta["status"] == "READY":
        p_up = meta["probability_up"]
        model_id = "meta-adaptive-v1"
        selected_status = "VALIDATED" if bool(historical.get("evaluation", {}).get("validated")) else "EXPERIMENTAL"
        selected_validated = bool(historical.get("evaluation", {}).get("validated"))
        horizons = [x.get("horizon_seconds") for x in (hist_forecast, flow_forecast) if x and x.get("horizon_seconds")]
        horizon_seconds = min(horizons) if horizons else None
    elif hist_forecast:
        p_up = hist_forecast["raw_probability_up"]
        model_id = hist_forecast["model_id"]
        selected_status = historical.get("status", "EXPERIMENTAL")
        selected_validated = bool(hist_forecast.get("validated"))
        horizon_seconds = hist_forecast["horizon_seconds"]
    elif flow_forecast:
        p_up = flow_forecast["raw_probability_up"]
        model_id = flow_forecast["model_id"]
        selected_status = online.get("status", "EXPERIMENTAL")
        selected_validated = bool(flow_forecast.get("validated"))
        horizon_seconds = flow_forecast["horizon_seconds"]
    else:
        p_up = None
        model_id = None
        selected_status = online.get("status", "WARMUP")
        selected_validated = False
        horizon_seconds = None

    forecast = None
    if p_up is not None:
        p_up = max(0.0, min(1.0, float(p_up)))
        p_down = 1.0 - p_up
        confidence = abs(p_up - 0.5) * 2.0
        direction = "UP" if p_up >= 0.55 else ("DOWN" if p_down >= 0.55 else "NEUTRAL")
        forecast = {
            "direction": direction,
            "raw_probability_up": round(p_up, 4),
            "raw_probability_down": round(p_down, 4),
            "confidence_raw": round(confidence, 4),
            "model_id": model_id,
            "status": selected_status,
            "validated": selected_validated,
            "calibrated": False,
            "horizon_seconds": horizon_seconds,
            "components": {
                "historical": historical.get("status"),
                "flow": online.get("status"),
            },
        }

    execution_cost = estimate_execution_cost(
        bid=bid, ask=ask,
        mid=((bid + ask) / 2.0) if bid is not None and ask is not None else None,
        order_notional=0.0, adv_notional=1.0,
        commission_bps=0.0, slippage_bps=1.0, latency_bps=0.5,
        max_spread_bps=5.0,
    )
    execution_gate = {
        "status": "BLOCKED_EXPECTED_MOVE_REQUIRED",
        "executable_quote": execution_cost.executable,
        "estimated_cost_bps": round(execution_cost.total_bps, 4),
        "spread_bps": round(execution_cost.spread_bps, 4),
        "slippage_bps": execution_cost.slippage_bps,
        "latency_bps": execution_cost.latency_bps,
        "impact_bps": None,
        "reason": "EXPECTED_MOVE_AND_REAL_ADV_REQUIRED_FOR_EXECUTION_GATE",
    }

    armed = bool(
        forecast
        and forecast["validated"]
        and forecast["confidence_raw"] >= 0.20
        and spread_bps is not None
        and spread_bps <= 5.0
        and forecast["direction"] != "NEUTRAL"
        and execution_gate["status"] == "PASS"
    )

    evaluation = historical.get("evaluation", {}) or online.get("evaluation", {}) or {}
    if multi_horizon:
        evaluation = dict(evaluation)
        evaluation["multi_horizon"] = multi_horizon
    if horizon_consensus:
        evaluation = dict(evaluation)
        evaluation["horizon_consensus"] = horizon_consensus
    forecast_status = forecast["status"] if forecast else selected_status
    model_health = assess_model_health(data_health=data_health, evaluation=evaluation, forecast=forecast)
    registry_record = build_registry_record(model_id=model_id or "none", version="v1", status="ACTIVE" if not model_health["safe_mode"] else "SAFE_MODE", evaluation=evaluation)
    trigger = evaluate_trigger(
        probability_up=forecast.get("raw_probability_up") if forecast else None,
        agreement=meta.get("agreement", 0.0),
        confidence=forecast.get("confidence_raw", 0.0) if forecast else 0.0,
        model_health=model_health,
        regime=evaluation.get("regime"),
        spread_bps=spread_bps,
        expected_move_bps=None,
        estimated_cost_bps=execution_gate.get("estimated_cost_bps"),
        persistence_count=1,
        cooldown_active=False,
    )
    if model_health["safe_mode"]:
        armed = False
        gatillazo_status = "SAFE_MODE"
    else:
        gatillazo_status = trigger["status"]
    response_payload = {
        "symbol": symbol,
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread_bps": spread_bps,
        "microprice": microprice,
        "execution_costs": execution_gate,
        "regime": evaluation.get("regime"),
        "model_health": model_health,
        "model_registry": registry_record,
        "meta": meta,
        "trigger": trigger,
        "data_health": data_health,
        "data_source": result["source"],
        "feed_label": feed_label,
        "quote_timestamp": q.get("quoteTimestamp") or q.get("timestamp"),
        "received_at": result["received_at"],
        "forecast": forecast,
        "forecast_status": forecast_status,
        "gatillazo": gatillazo_status,
        "specialists": {
            "flow": flow,
            "cross_asset": cross_asset,
        },
        "model": {
            "id": forecast["model_id"] if forecast else None,
            "status": forecast_status,
            "historical_id": historical.get("model_id"),
            "flow_id": online.get("forecast", {}).get("model_id") if online.get("forecast") else "flow-baseline-v0",
            "resolved_flow_samples": online["evaluation"]["sample_count"],
            "pending_flow_samples": online.get("pending_samples", 0),
            "historical_bars": historical.get("bars"),
        },
        "evaluation": evaluation,
        "multi_horizon": multi_horizon,
        "horizon_consensus": horizon_consensus,
        "cross_asset": {
            "model_id": cross_asset.get("model_id"),
            "status": cross_asset.get("status"),
            "evaluation": cross_asset.get("evaluation") or {},
            "context": cross_asset.get("context"),
        },
        "rule": "NO DATA HEALTH OR MODEL HEALTH -> NO FORECAST -> NO GATILLAZO",
    }
    try:
        forecast_id = record_forecast(response_payload)
        print(
            "PMSF-X FORECAST SAVED:",
            {"forecast_id": forecast_id, "symbol": symbol, "model_id": model_id},
        )
    except Exception as exc:
        print(f"PMSF-X FORECAST LOG ERROR: {type(exc).__name__}: {exc}")
        forecast_id = None
    response_payload["forecast_id"] = forecast_id
    try:
        registry_id = record_model_registry(registry_record)
    except Exception as exc:
        print(f"PMSF-X MODEL REGISTRY ERROR: {type(exc).__name__}: {exc}")
        registry_id = None
    response_payload["model_registry_id"] = registry_id
    return response_payload

async def _run_multihorizon_research_job(run_id: str, symbol: str):
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        update_research_run(
            run_id,
            status="ERROR",
            error="TIINGO_API_KEY is not configured",
            finished_at=datetime.now(timezone.utc),
        )
        return

    started_at = datetime.now(timezone.utc)
    update_research_run(
        run_id,
        status="RUNNING",
        started_at=started_at,
    )
    print(
        "PMSF-X MULTIHORIZON RESEARCH JOB START:",
        {"run_id": run_id, "symbol": symbol},
    )

    try:
        # Research runs must not invoke the live forecast path: that path
        # rebuilds multiple horizons and can block the request coroutine.
        # Reuse the proven bar cache when available; otherwise fetch only bars.
        rows = get_cached_bars(symbol)
        history_source = "cache"
        if not rows:
            history_source = "tiingo_history"
            rows = await asyncio.wait_for(
                get_historical_bars(symbol, token),
                timeout=30.0,
            )
        if not rows:
            raise RuntimeError("Historical bars unavailable")

        print(
            "PMSF-X MULTIHORIZON DATA READY:",
            {
                "run_id": run_id,
                "symbol": symbol,
                "target_rows": len(rows),
                "history_source": history_source,
            },
        )

        loop = asyncio.get_running_loop()
        validation = await asyncio.wait_for(
            loop.run_in_executor(
                RESEARCH_PROCESS_POOL,
                partial(
                    validate_multi_horizon_meta,
                    rows,
                    symbol=symbol,
                ),
            ),
            timeout=600.0,
        )

        research_backtest_id = record_backtest(
            symbol,
            {
                "model_id": "multihorizon-meta-research-v1",
                "lookback_days": None,
                "bars": len(rows),
                "evaluation": validation,
            },
        )

        finished_at = datetime.now(timezone.utc)
        result = {
            "service": "pmsfx-nano",
            "symbol": symbol,
            "historical_model_id": "historical-bars-only-v1",
            "historical_bars": len(rows),
            "research_run_id": run_id,
            "research_backtest_id": research_backtest_id,
            "validation": validation,
            "history_source": history_source,
        }
        update_research_run(
            run_id,
            status="COMPLETED",
            result=result,
            finished_at=finished_at,
        )
        print(
            "PMSF-X MULTIHORIZON RESEARCH JOB COMPLETE:",
            {
                "run_id": run_id,
                "symbol": symbol,
                "status": validation.get("status"),
                "validated": validation.get("validated"),
                "usable_folds": validation.get("usable_fold_count"),
                "oos_count": validation.get("oos_count"),
                "brier_skill": (validation.get("metrics") or {}).get("brier_skill"),
                "delta_brier_vs_300s": validation.get("delta_brier_vs_300s"),
                "research_backtest_id": research_backtest_id,
            },
        )
    except asyncio.TimeoutError:
        finished_at = datetime.now(timezone.utc)
        error = "MULTIHORIZON_RESEARCH_TIMEOUT"
        update_research_run(
            run_id,
            status="ERROR",
            error=error,
            finished_at=finished_at,
        )
        print(
            "PMSF-X MULTIHORIZON RESEARCH JOB ERROR:",
            {
                "run_id": run_id,
                "symbol": symbol,
                "error": error,
            },
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        error = f"{type(exc).__name__}: {exc}"
        update_research_run(
            run_id,
            status="ERROR",
            error=error,
            finished_at=finished_at,
        )
        print(
            "PMSF-X MULTIHORIZON RESEARCH JOB ERROR:",
            {"run_id": run_id, "symbol": symbol, "error": error},
        )


async def _run_cross_asset_research_job(run_id: str, symbol: str):
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        update_research_run(
            run_id,
            status="ERROR",
            error="TIINGO_API_KEY is not configured",
            finished_at=datetime.now(timezone.utc),
        )
        return

    started_at = datetime.now(timezone.utc)
    update_research_run(run_id, status="RUNNING", started_at=started_at)
    print(
        "PMSF-X CROSS-ASSET RESEARCH JOB START:",
        {"run_id": run_id, "symbol": symbol},
    )

    try:
        historical = await asyncio.wait_for(
            get_historical_forecast(
                symbol,
                token,
                evaluate=False,
                include_rows=True,
            ),
            timeout=60.0,
        )
        target_rows = historical.get("__bars_rows") or []
        print(
            "PMSF-X CROSS-ASSET TARGET HISTORY:",
            {
                "symbol": symbol,
                "status": historical.get("status"),
                "bars": historical.get("bars"),
                "rows": len(target_rows),
                "fetch_meta": historical.get("__research_fetch_meta"),
            },
        )
        if not target_rows:
            raise RuntimeError("Historical target bars unavailable")

        peer_rows: dict[str, list[dict[str, Any]]] = {}
        peer_symbols = {
            "AAPL": ("MSFT", "NVDA"),
            "MSFT": ("AAPL", "NVDA"),
            "NVDA": ("AAPL", "MSFT"),
            "TSLA": ("AAPL", "NVDA"),
        }.get(symbol, ())
        for peer_symbol in peer_symbols:
            try:
                peer_result = await asyncio.wait_for(
                    get_historical_forecast(
                        peer_symbol,
                        token,
                        evaluate=False,
                        include_rows=True,
                    ),
                    timeout=60.0,
                )
                peer_bars = peer_result.get("__bars_rows") or []
                print(
                    "PMSF-X CROSS-ASSET PEER HISTORY:",
                    {
                        "symbol": symbol,
                        "peer": peer_symbol,
                        "status": peer_result.get("status"),
                        "bars": peer_result.get("bars"),
                        "rows": len(peer_bars),
                        "fetch_meta": peer_result.get("__research_fetch_meta"),
                    },
                )
                if peer_bars:
                    peer_rows[peer_symbol] = peer_bars
            except Exception as peer_exc:
                print(
                    "PMSF-X CROSS-ASSET PEER PRELOAD ERROR:",
                    {
                        "symbol": symbol,
                        "peer": peer_symbol,
                        "error": f"{type(peer_exc).__name__}: {peer_exc}",
                    },
                )

        loop = asyncio.get_running_loop()
        validation = await asyncio.wait_for(
            loop.run_in_executor(
                RESEARCH_PROCESS_POOL,
                partial(
                    validate_cross_asset_symbol,
                    symbol,
                    token,
                    target_rows,
                    peer_rows,
                ),
            ),
            timeout=900.0,
        )

        research_backtest_id = record_backtest(
            symbol,
            {
                "model_id": "cross-asset-leadlag-research-v1",
                "lookback_days": historical.get("lookback_days"),
                "bars": historical.get("bars"),
                "evaluation": validation,
            },
        )

        finished_at = datetime.now(timezone.utc)
        result = {
            "service": "pmsfx-nano",
            "symbol": symbol,
            "historical_model_id": historical.get("model_id"),
            "historical_bars": historical.get("bars"),
            "research_run_id": run_id,
            "research_backtest_id": research_backtest_id,
            "validation": validation,
        }
        update_research_run(
            run_id,
            status="COMPLETED",
            result=result,
            finished_at=finished_at,
        )
        print(
            "PMSF-X CROSS-ASSET RESEARCH JOB COMPLETE:",
            {
                "run_id": run_id,
                "symbol": symbol,
                "status": validation.get("status"),
                "validated": validation.get("validated"),
                "usable_folds": validation.get("usable_fold_count"),
                "oos_count": validation.get("oos_count"),
                "brier_skill": (validation.get("metrics") or {}).get("brier_skill"),
                "research_backtest_id": research_backtest_id,
            },
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        error = f"{type(exc).__name__}: {exc}"
        update_research_run(
            run_id,
            status="ERROR",
            error=error,
            finished_at=finished_at,
        )
        print(
            "PMSF-X CROSS-ASSET RESEARCH JOB ERROR:",
            {"run_id": run_id, "symbol": symbol, "error": error},
        )


@app.post("/api/research/cross-asset/{ticker}", status_code=202)
async def start_cross_asset_research(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")
    if symbol not in ("AAPL", "MSFT", "NVDA", "TSLA"):
        raise HTTPException(status_code=400, detail="Unsupported cross-asset ticker")
    if not os.getenv("TIINGO_API_KEY"):
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")
    if not DB_READY:
        raise HTTPException(status_code=503, detail="Database is not ready")

    model_id = "cross-asset-leadlag-research-v1"
    existing = active_research_run(symbol, model_id=model_id)
    if existing is not None:
        print(
            "PMSF-X CROSS-ASSET RESEARCH DEDUP:",
            {
                "symbol": symbol,
                "run_id": existing.get("run_id"),
                "status": existing.get("status"),
            },
        )
        return {
            "service": "pmsfx-nano",
            "symbol": symbol,
            "run_id": existing["run_id"],
            "status": existing["status"],
            "poll": f"/api/research/run/{existing['run_id']}",
            "validation_type": "cross_asset_walk_forward_purged_embargoed",
            "deduplicated": True,
        }

    run_id = create_research_run(symbol, model_id=model_id)
    if not run_id:
        raise HTTPException(status_code=503, detail="Unable to create research run")

    asyncio.create_task(_run_cross_asset_research_job(run_id, symbol))
    return {
        "service": "pmsfx-nano",
        "symbol": symbol,
        "run_id": run_id,
        "status": "QUEUED",
        "poll": f"/api/research/run/{run_id}",
        "validation_type": "cross_asset_walk_forward_purged_embargoed",
        "deduplicated": False,
    }


@app.post("/api/research/multihorizon/{ticker}", status_code=202)
async def start_multihorizon_research(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")
    if not os.getenv("TIINGO_API_KEY"):
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")
    if not DB_READY:
        raise HTTPException(status_code=503, detail="Database is not ready")

    existing = active_research_run(symbol)
    if existing is not None:
        print(
            "PMSF-X MULTIHORIZON RESEARCH DEDUP:",
            {
                "symbol": symbol,
                "run_id": existing.get("run_id"),
                "status": existing.get("status"),
            },
        )
        return {
            "service": "pmsfx-nano",
            "symbol": symbol,
            "run_id": existing["run_id"],
            "status": existing["status"],
            "poll": f"/api/research/run/{existing['run_id']}",
            "validation_type": "cross_fitted_walk_forward_purged_embargoed",
            "deduplicated": True,
        }

    run_id = create_research_run(symbol)
    if not run_id:
        raise HTTPException(status_code=503, detail="Unable to create research run")

    asyncio.create_task(_run_multihorizon_research_job(run_id, symbol))
    return {
        "service": "pmsfx-nano",
        "symbol": symbol,
        "run_id": run_id,
        "status": "QUEUED",
        "poll": f"/api/research/run/{run_id}",
        "validation_type": "cross_fitted_walk_forward_purged_embargoed",
        "deduplicated": False,
    }


@app.get("/api/research/run/{run_id}")
def research_run_status(run_id: str):
    result = get_research_run(run_id)
    if result is None:
        print(
            "PMSF-X RESEARCH STATUS:",
            {"run_id": run_id, "status": "NOT_FOUND"},
        )
        raise HTTPException(status_code=404, detail="Research run not found")

    print(
        "PMSF-X RESEARCH STATUS:",
        {
            "run_id": run_id,
            "status": result.get("status"),
            "error": result.get("error"),
        },
    )
    return {
        "service": "pmsfx-nano",
        **result,
    }


@app.get("/api/research/cross-asset/{ticker}")
def latest_cross_asset_research(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")
    result = latest_research_run(symbol)
    if result is None or result.get("model_id") != "cross-asset-leadlag-research-v1":
        raise HTTPException(status_code=404, detail="No cross-asset research run found")
    return {
        "service": "pmsfx-nano",
        "symbol": symbol,
        **result,
    }


@app.get("/api/research/multihorizon/{ticker}")
def latest_multihorizon_research(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")
    result = latest_research_run(symbol)
    if result is None:
        raise HTTPException(status_code=404, detail="No research run found")
    return {
        "service": "pmsfx-nano",
        "symbol": symbol,
        **result,
    }


@app.get("/api/backtest/{ticker}")
async def backtest(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")

    result = await get_historical_forecast(symbol, token)
    evaluation = result.get("evaluation") or {}
    target_rows = get_cached_bars(symbol)
    cross_asset_backtest = {}
    if target_rows:
        try:
            cross_asset_backtest = await asyncio.to_thread(
                lambda: asyncio.run(
                    get_cross_asset_forecast(
                        symbol,
                        token,
                        target_rows=target_rows,
                        evaluate=True,
                    )
                )
            )
        except Exception as exc:
            cross_asset_backtest = {
                "status": "ERROR",
                "model_id": "cross-asset-leadlag-v1",
                "forecast": None,
                "evaluation": {},
                "context": None,
                "error": f"{type(exc).__name__}: {exc}",
            }
    multi_horizon = result.get("multi_horizon") or {}
    horizon_consensus = build_horizon_consensus(
        multi_horizon,
        cross_asset=cross_asset_backtest,
    )
    payload = {
        "symbol": symbol,
        "model_id": result.get("model_id"),
        "lookback_days": result.get("lookback_days"),
        "bars": result.get("bars"),
        "evaluation": evaluation,
        "multi_horizon": multi_horizon,
        "horizon_consensus": horizon_consensus,
        "cross_asset": {
            "model_id": cross_asset_backtest.get("model_id"),
            "status": cross_asset_backtest.get("status"),
            "evaluation": cross_asset_backtest.get("evaluation") or {},
        },
    }
    try:
        run_id = record_backtest(symbol, payload)
        print(
            "PMSF-X BACKTEST SAVED:",
            {"backtest_id": run_id, "symbol": symbol, "model_id": result.get("model_id")},
        )
    except Exception as exc:
        print(f"PMSF-X BACKTEST LOG ERROR: {type(exc).__name__}: {exc}")
        run_id = None

    return {
        "backtest_id": run_id,
        "symbol": symbol,
        "model_id": result.get("model_id"),
        "lookback_days": result.get("lookback_days"),
        "bars": result.get("bars"),
        "metrics_oos": evaluation,
        "multi_horizon": multi_horizon,
        "horizon_consensus": horizon_consensus,
        "cross_asset_backtest": cross_asset_backtest,
        "forecast_status": result.get("status"),
        "note": "Chronological holdout: first 80% train, final 20% OOS test. No future rows are used for fitting the OOS model.",
    }