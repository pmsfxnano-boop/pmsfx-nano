import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response

from quant.specialists.flow import run_flow_specialist
from quant.online import observe_online
from quant.specialists.historical import get_historical_forecast
from quant.db import (
    get_outcome,
    init_db,
    outcome_summary,
    pending_due_forecasts,
    persistence_summary,
    record_backtest,
    record_forecast,
    record_model_registry,
    record_outcome,
)
from quant.data_health import assess_quote
from quant.execution_costs import estimate_execution_cost
from quant.model_health import assess_model_health
from quant.model_registry import build_registry_record
from quant.meta import combine_specialists
from quant.trigger import evaluate_trigger
from quant.outcome import resolve_forecast

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
COLLECTOR_SYMBOLS = ("AAPL", "MSFT", "NVDA", "TSLA")
OUTCOME_RESOLVER_INTERVAL_SECONDS = 60.0
OUTCOME_FORECAST_INTERVAL_SECONDS = 300.0


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
        print("PMSF-X DB:", "READY" if DB_READY else "NOT_CONFIGURED")
    except Exception as exc:
        DB_READY = False
        print(f"PMSF-X DB ERROR: {type(exc).__name__}: {exc}")


@app.on_event("startup")
async def tiingo_startup_check():
    if not os.getenv("TIINGO_API_KEY"):
        print("PMSF-X SELFTEST AAPL: TIINGO_API_KEY missing")
        return

    try:
        headers = tiingo_headers()
        async with httpx.AsyncClient(timeout=10.0) as client:
            iex_response = await client.get(f"{TIINGO_IEX_URL}/AAPL", headers=headers)
            iex = first_record(iex_response.json()) or {} if iex_response.status_code < 400 else {}
            has_tops = iex.get("bidPrice") is not None and iex.get("askPrice") is not None

            if has_tops:
                source = "tiingo_iex_tops"
                fields = _snapshot_fields(source, iex)
            else:
                equity_response = await client.get(f"{TIINGO_EQUITY_URL}/AAPL", headers=headers)
                equity = first_record(equity_response.json()) or {} if equity_response.status_code < 400 else {}
                source = "tiingo_equity_intraday"
                fields = _snapshot_fields(source, equity)

            print(
                "PMSF-X SELFTEST AAPL:",
                {
                    "iex_status": iex_response.status_code,
                    "source": source,
                    "last": fields["last"],
                    "bid": fields["bid"],
                    "ask": fields["ask"],
                    "bid_size": fields["bid_size"],
                    "ask_size": fields["ask_size"],
                },
            )
    except Exception as exc:
        print(f"PMSF-X SELFTEST AAPL ERROR: {type(exc).__name__}: {exc}")


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


async def outcome_forecast_loop():
    index = 0
    while True:
        if not DB_READY or not os.getenv("TIINGO_API_KEY"):
            await asyncio.sleep(OUTCOME_FORECAST_INTERVAL_SECONDS)
            continue

        symbol = COLLECTOR_SYMBOLS[index % len(COLLECTOR_SYMBOLS)]
        index += 1
        try:
            payload = await asyncio.wait_for(state(symbol), timeout=45.0)
            print(
                "PMSF-X OUTCOME FORECAST COLLECTOR:",
                {
                    "symbol": symbol,
                    "forecast_id": payload.get("forecast_id"),
                    "forecast_status": payload.get("forecast_status"),
                },
            )
        except Exception as exc:
            print(
                "PMSF-X OUTCOME FORECAST COLLECTOR ERROR:",
                {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"},
            )
        await asyncio.sleep(OUTCOME_FORECAST_INTERVAL_SECONDS)


@app.on_event("startup")
async def outcome_collector_startup():
    global OUTCOME_COLLECTOR_TASK
    if os.getenv("PMSFX_OUTCOME_COLLECTOR") != "1":
        return
    if OUTCOME_COLLECTOR_TASK is None or OUTCOME_COLLECTOR_TASK.done():
        async def combined():
            resolver = asyncio.create_task(outcome_resolver_loop())
            forecaster = asyncio.create_task(outcome_forecast_loop())
            try:
                await asyncio.gather(resolver, forecaster)
            finally:
                resolver.cancel()
                forecaster.cancel()
        OUTCOME_COLLECTOR_TASK = asyncio.create_task(combined())
        print(
            "PMSF-X OUTCOME COLLECTOR: STARTED",
            {
                "resolver_interval_seconds": OUTCOME_RESOLVER_INTERVAL_SECONDS,
                "forecast_interval_seconds": OUTCOME_FORECAST_INTERVAL_SECONDS,
                "symbols": COLLECTOR_SYMBOLS,
            },
        )


@app.on_event("shutdown")
async def outcome_collector_shutdown():
    global OUTCOME_COLLECTOR_TASK
    if OUTCOME_COLLECTOR_TASK is not None:
        OUTCOME_COLLECTOR_TASK.cancel()
        try:
            await OUTCOME_COLLECTOR_TASK
        except asyncio.CancelledError:
            pass
        OUTCOME_COLLECTOR_TASK = None
        print("PMSF-X OUTCOME COLLECTOR: STOPPED")


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
    result = await quote(symbol)
    q = result["quote"]

    fields = _snapshot_fields(result["source"], q)
    last = fields["last"]
    bid = fields["bid"]
    ask = fields["ask"]
    bid_size = fields["bid_size"]
    ask_size = fields["ask_size"]
    feed_label = "Tiingo IEX TOPS" if result["source"] == "tiingo_iex_tops" else "Tiingo consolidated · liquidity reference"

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
    historical = (
        await get_historical_forecast(symbol, token)
        if token and healthy
        else {"status": "HISTORICAL_SKIPPED", "forecast": None, "evaluation": {}, "model_id": "historical-logit-v1"}
    )

    # Historical price model is primary until live flow has its own validation.
    hist_forecast = historical.get("forecast")
    flow_forecast = online.get("forecast")
    meta = combine_specialists(
        historical=hist_forecast,
        flow=flow_forecast,
        historical_evaluation=historical.get("evaluation", {}),
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
    payload = {
        "symbol": symbol,
        "model_id": result.get("model_id"),
        "lookback_days": result.get("lookback_days"),
        "bars": result.get("bars"),
        "evaluation": evaluation,
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
        "forecast_status": result.get("status"),
        "note": "Chronological holdout: first 80% train, final 20% OOS test. No future rows are used for fitting the OOS model.",
    }
