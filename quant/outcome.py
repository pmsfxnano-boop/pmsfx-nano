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




async def _fetch_future_bar(symbol: str, token: str, due_at: datetime) -> dict[str, Any] | None:
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    start_date = due_at.date()
    end_date = start_date + timedelta(days=1)
    params = {
        "startDate": start_date.isoformat(),
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

    candidates: list[tuple[datetime, float]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        ts = _parse_datetime(row.get("date") or row.get("timestamp"))
        close = row.get("close")
        if ts is None or close is None or ts < due_at:
            continue
        try:
            candidates.append((ts, float(close)))
        except (TypeError, ValueError):
            continue

    if not candidates:
        return None

    ts, price = min(candidates, key=lambda item: item[0])
    return {
        "price": price,
        "source": "tiingo_equity_intraday_1min",
        "quote_timestamp": ts.isoformat(),
    }


async def _fetch_fresh_price(symbol: str, token: str) -> dict[str, Any]:
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
            price = (bid + ask) / 2.0
            return {
                "price": price,
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

    binary_eligible = (
        forecast.get("p_up") is not None
        and forecast_direction in ("UP", "DOWN")
        and realized_direction in ("UP", "DOWN")
    )
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
            "resolution_rule": "first Tiingo 1-minute observation at or after forecast horizon",
            "actual_elapsed_seconds": round(elapsed, 3),
        },
    }


async def resolve_forecast(forecast: dict[str, Any], token: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    created = forecast["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    due_at = created.timestamp() + int(forecast["horizon_seconds"])
    if now.timestamp() < due_at:
        return {
            "status": "NOT_DUE",
            "forecast_id": int(forecast["id"]),
            "due_at": datetime.fromtimestamp(due_at, tz=timezone.utc).isoformat(),
        }

    due_at = datetime.fromtimestamp(due_at, tz=timezone.utc)
    quote = await _fetch_future_bar(str(forecast["symbol"]), token, due_at)

    if quote is None:
        return {
            "status": "NOT_AVAILABLE",
            "forecast_id": int(forecast["id"]),
            "due_at": due_at.isoformat(),
            "reason": "NO_FUTURE_1MIN_OBSERVATION_AVAILABLE",
        }

    outcome = build_outcome(forecast, quote, resolved_at=now)
    outcome["status"] = "RESOLVED"
    return outcome
