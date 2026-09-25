"""PMSF-X Nano — Outcome Engine.

Resolves persisted forecasts only after their target horizon has elapsed.
The resolver uses a fresh Tiingo quote as the first available observation
after the horizon and stores the realized move plus audit metadata.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx


TIINGO_IEX_URL = "https://api.tiingo.com/iex"
TIINGO_EQUITY_URL = "https://api.tiingo.com/tiingo/equity/intraday"
REALIZED_MOVE_THRESHOLD_BPS = 0.5
OUTCOME_RESAMPLE_FREQ = "1min"
MAX_TIMING_SLIPPAGE_SECONDS = 60.0


def _first_record(data: Any) -> dict[str, Any] | None:
    if isinstance(data, list):
        return data[0] if data else None
    if isinstance(data, dict):
        return data
    return None


def _price_fields(source: str, row: dict[str, Any]) -> dict[str, Any]:
    if source == "tiingo_iex_tops":
        bid = row.get("bidPrice")
        ask = row.get("askPrice")
        last = row.get("last")
        return {
            "last": last,
            "bid": bid,
            "ask": ask,
            "bid_size": row.get("bidSize") or 0,
            "ask_size": row.get("askSize") or 0,
            "timestamp": row.get("quoteTimestamp") or row.get("timestamp"),
        }
    return {
        "last": row.get("tngoLast") if row.get("tngoLast") is not None else row.get("last"),
        "bid": row.get("lqBidPrice"),
        "ask": row.get("lqAskPrice"),
        "bid_size": row.get("lqBidSize") or 0,
        "ask_size": row.get("lqAskSize") or 0,
        "timestamp": row.get("timestamp"),
    }




async def _fetch_future_bars_day(
    symbol: str,
    token: str,
    day,
) -> list[tuple[datetime, float]]:
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    end_date = day + timedelta(days=1)
    params = {
        "startDate": day.isoformat(),
        "endDate": end_date.isoformat(),
        "resampleFreq": OUTCOME_RESAMPLE_FREQ,
        "columns": "close",
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(
            f"{TIINGO_EQUITY_URL}/{symbol}/prices",
            headers=headers,
            params=params,
        )
    if response.status_code >= 400:
        raise RuntimeError(f"Tiingo historical outcome HTTP {response.status_code}")

    data = response.json()
    if not isinstance(data, list):
        data = data.get("data", []) if isinstance(data, dict) else []

    bars: list[tuple[datetime, float]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        ts = _parse_datetime(row.get("date") or row.get("timestamp"))
        close = row.get("close")
        if ts is None or close is None:
            continue
        try:
            bars.append((ts, float(close)))
        except (TypeError, ValueError):
            continue

    bars.sort(key=lambda item: item[0])
    return bars


async def _fetch_future_bar(
    symbol: str,
    token: str,
    due_at: datetime,
    cache: dict[tuple[str, str], list[tuple[datetime, float]]] | None = None,
) -> dict[str, Any] | None:
    cache_key = (symbol, due_at.date().isoformat())
    if cache is not None and cache_key in cache:
        bars = cache[cache_key]
    else:
        bars = await _fetch_future_bars_day(symbol, token, due_at.date())
        if cache is not None:
            cache[cache_key] = bars

    candidates = [(ts, price) for ts, price in bars if ts >= due_at]
    if not candidates:
        return None

    ts, price = min(candidates, key=lambda item: item[0])
    return {
        "price": price,
        "source": "tiingo_equity_intraday_1min",
        "quote_timestamp": ts.isoformat(),
    }


async def _fetch_fresh_price(symbol: str, token: str) -> dict[str, Any]:
    try:
        from quant.market_stream import get_snapshot
        snapshot = get_snapshot(symbol)
        if snapshot:
            bid = snapshot.get("bid")
            ask = snapshot.get("ask")
            last = snapshot.get("last")
            if last is None and bid is not None and ask is not None:
                last = (float(bid) + float(ask)) / 2.0
            if last is not None:
                price = (
                    (float(bid) + float(ask)) / 2.0
                    if bid is not None and ask is not None
                    else float(last)
                )
                return {
                    "price": price,
                    "source": snapshot.get("source") or "tiingo_iex_websocket",
                    "received_at": snapshot.get("received_at") or datetime.now(timezone.utc).isoformat(),
                    "quote_timestamp": snapshot.get("quote_timestamp"),
                    "bid": bid,
                    "ask": ask,
                    "bid_size": snapshot.get("bid_size") or 0,
                    "ask_size": snapshot.get("ask_size") or 0,
                    "last": last,
                }
    except Exception:
        pass

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(f"{TIINGO_IEX_URL}/{symbol}", headers=headers)
        if response.status_code >= 400:
            raise RuntimeError(f"Tiingo IEX HTTP {response.status_code}")

        iex = _first_record(response.json()) or {}
        fields = _price_fields("tiingo_iex_tops", iex)
        if fields["bid"] is not None and fields["ask"] is not None:
            bid = float(fields["bid"])
            ask = float(fields["ask"])
            return {
                "price": (bid + ask) / 2.0,
                "source": "tiingo_iex_tops",
                "received_at": datetime.now(timezone.utc).isoformat(),
                **fields,
            }

        response = await client.get(f"{TIINGO_EQUITY_URL}/{symbol}", headers=headers)
        if response.status_code >= 400:
            raise RuntimeError(f"Tiingo equity intraday HTTP {response.status_code}")

        equity = _first_record(response.json())
        if not equity:
            raise RuntimeError("Tiingo returned no equity quote")

        fields = _price_fields("tiingo_equity_intraday", equity)
        last = fields["last"]
        if last is None:
            raise RuntimeError("Tiingo equity quote missing last price")

        return {
            "price": float(last),
            "source": "tiingo_equity_intraday",
            "received_at": datetime.now(timezone.utc).isoformat(),
            **fields,
        }

def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


async def repair_timing_expired_forecast(
    forecast: dict[str, Any],
    token: str,
    historical_cache: dict[tuple[str, str], list[tuple[datetime, float]]] | None = None,
) -> dict[str, Any]:
    created = forecast["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    horizon = int(forecast["horizon_seconds"])
    due_at = created + timedelta(seconds=horizon)
    quote = await _fetch_future_bar(
        str(forecast["symbol"]),
        token,
        due_at,
        cache=historical_cache,
    )
    if quote is None:
        return {
            "status": "HISTORICAL_REPAIR_UNAVAILABLE",
            "forecast_id": int(forecast["id"]),
            "symbol": forecast["symbol"],
            "due_at": due_at.isoformat(),
        }

    quote_timestamp = _parse_datetime(
        quote.get("quote_timestamp") or quote.get("timestamp")
    )
    if quote_timestamp is None or quote_timestamp < due_at:
        return {
            "status": "HISTORICAL_REPAIR_INVALID_TIMESTAMP",
            "forecast_id": int(forecast["id"]),
            "symbol": forecast["symbol"],
            "due_at": due_at.isoformat(),
        }

    outcome = build_outcome(forecast, quote, resolved_at=quote_timestamp)
    outcome["metadata"]["resolution_mode"] = "tiingo_equity_intraday_1min_historical_repair"
    outcome["metadata"]["market_event_timestamp"] = quote_timestamp.isoformat()
    outcome["metadata"]["historical_repair"] = True
    outcome["status"] = "RESOLVED"
    return outcome


def _eligibility_reason(
    *,
    p_up: Any,
    forecast_direction: Any,
    realized_direction: Any,
    within_timing_tolerance: bool,
) -> str:
    if p_up is None:
        return "MISSING_P_UP"
    if realized_direction not in ("UP", "DOWN"):
        return "REALIZED_MOVE_BELOW_THRESHOLD"
    if not within_timing_tolerance:
        return "TIMING_EXPIRED"
    return "ELIGIBLE"


def build_outcome(forecast: dict[str, Any], quote: dict[str, Any], resolved_at: datetime | None = None) -> dict[str, Any]:
    resolved = resolved_at or datetime.now(timezone.utc)
    created = forecast["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    horizon = int(forecast["horizon_seconds"])
    elapsed = max(0.0, (resolved - created).total_seconds())
    entry = forecast.get("entry_price")
    exit_price = float(quote["price"])

    if entry is None or float(entry) <= 0:
        raise ValueError("forecast has no valid entry price")

    entry = float(entry)
    realized_return_bps = (exit_price / entry - 1.0) * 10000.0

    if realized_return_bps >= REALIZED_MOVE_THRESHOLD_BPS:
        realized_direction = "UP"
    elif realized_return_bps <= -REALIZED_MOVE_THRESHOLD_BPS:
        realized_direction = "DOWN"
    else:
        realized_direction = "FLAT"

    forecast_direction = forecast.get("direction")
    if forecast_direction in ("UP", "DOWN"):
        prediction_correct = forecast_direction == realized_direction
    elif forecast_direction == "NEUTRAL":
        prediction_correct = realized_direction == "FLAT"
    else:
        prediction_correct = None

    timing_slippage_seconds = max(0.0, elapsed - float(horizon))
    within_timing_tolerance = timing_slippage_seconds <= MAX_TIMING_SLIPPAGE_SECONDS
    eligibility_reason = _eligibility_reason(
        p_up=forecast.get("p_up"),
        forecast_direction=forecast_direction,
        realized_direction=realized_direction,
        within_timing_tolerance=within_timing_tolerance,
    )
    binary_eligible = eligibility_reason == "ELIGIBLE"
    realized_label = 1.0 if realized_direction == "UP" else 0.0
    brier_loss = (
        (float(forecast["p_up"]) - realized_label) ** 2
        if binary_eligible
        else None
    )

    quote_timestamp = _parse_datetime(
        quote.get("quote_timestamp") or quote.get("timestamp")
    )
    return {
        "resolved_at": resolved,
        "forecast_id": int(forecast["id"]),
        "symbol": forecast["symbol"],
        "forecast_created_at": created,
        "target_horizon_seconds": horizon,
        "actual_elapsed_seconds": round(elapsed, 3),
        "forecast_direction": forecast_direction,
        "forecast_p_up": float(forecast["p_up"]) if forecast.get("p_up") is not None else None,
        "forecast_confidence": float(forecast["confidence"]) if forecast.get("confidence") is not None else None,
        "entry_price": entry,
        "exit_price": exit_price,
        "realized_return_bps": round(realized_return_bps, 5),
        "realized_direction": realized_direction,
        "prediction_correct": prediction_correct,
        "binary_eligible": binary_eligible,
        "brier_loss": round(brier_loss, 6) if brier_loss is not None else None,
        "resolution_source": quote["source"],
        "metadata": {
            "quote_timestamp": quote_timestamp.isoformat() if quote_timestamp else None,
            "quote_received_at": quote.get("received_at"),
            "realized_move_threshold_bps": REALIZED_MOVE_THRESHOLD_BPS,
            "resolution_rule": "first Tiingo 1-minute observation at or after horizon; late live-quote fallback",
            "actual_elapsed_seconds": round(elapsed, 3),
            "timing_slippage_seconds": round(timing_slippage_seconds, 3),
            "timing_tolerance_seconds": MAX_TIMING_SLIPPAGE_SECONDS,
            "eligibility_reason": eligibility_reason,
            "evaluation_eligible": binary_eligible,
        },
    }


async def resolve_forecast(forecast: dict[str, Any], token: str) -> dict[str, Any]:
    created = forecast["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    horizon = int(forecast["horizon_seconds"])
    due_at = created + timedelta(seconds=horizon)
    now = datetime.now(timezone.utc)
    if now < due_at:
        return {
            "status": "NOT_DUE",
            "forecast_id": int(forecast["id"]),
            "due_at": due_at.isoformat(),
        }

    quote = await _fetch_fresh_price(str(forecast["symbol"]), token)
    quote_timestamp = _parse_datetime(
        quote.get("quote_timestamp") or quote.get("timestamp")
    )
    if quote_timestamp is None:
        return {
            "status": "ERROR",
            "forecast_id": int(forecast["id"]),
            "symbol": forecast["symbol"],
            "reason": "MISSING_MARKET_EVENT_TIMESTAMP",
        }

    if quote_timestamp < due_at:
        return {
            "status": "NOT_DUE",
            "forecast_id": int(forecast["id"]),
            "due_at": due_at.isoformat(),
            "market_event_at": quote_timestamp.isoformat(),
            "reason": "LATEST_MARKET_EVENT_PRE_HORIZON",
        }

    timing_slippage_seconds = max(0.0, (quote_timestamp - due_at).total_seconds())
    if timing_slippage_seconds > MAX_TIMING_SLIPPAGE_SECONDS:
        # A late live resolver must not destroy a valid historical label.
        # Recover the first 1-minute observation at/after the exact horizon.
        repaired = await repair_timing_expired_forecast(forecast, token)
        if repaired.get("status") == "RESOLVED":
            return repaired
        return {
            "status": "TIMING_EXPIRED",
            "forecast_id": int(forecast["id"]),
            "symbol": forecast["symbol"],
            "due_at": due_at.isoformat(),
            "market_event_at": quote_timestamp.isoformat(),
            "timing_slippage_seconds": round(timing_slippage_seconds, 3),
            "historical_repair_status": repaired.get("status"),
        }

    outcome = build_outcome(forecast, quote, resolved_at=quote_timestamp)
    outcome["metadata"]["resolution_mode"] = "tiingo_iex_websocket_event"
    outcome["metadata"]["market_event_timestamp"] = quote_timestamp.isoformat()
    outcome["metadata"]["timing_slippage_seconds"] = round(timing_slippage_seconds, 3)
    outcome["status"] = "RESOLVED"
    return outcome
