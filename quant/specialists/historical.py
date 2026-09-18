"""PMSF-X Nano — Historical 5-minute price specialist.

Uses Tiingo's documented consolidated historical intraday bars to train
and evaluate a chronological logistic model for the next 5-minute move.
The holdout is strictly later in time than the training segment.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from typing import Any

import httpx


HISTORY_URL = "https://api.tiingo.com/tiingo/equity/intraday"
LOOKBACK_DAYS = 45
RESAMPLE_FREQ = "5min"
MIN_TRAIN_ROWS = 300
MAX_ROWS = 2200
LABEL_THRESHOLD_BPS = 0.5
LEARNING_RATE = 0.06
TRAIN_STEPS = 160
L2 = 0.02
CACHE_SECONDS = 900.0

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def _fit(samples: list[tuple[list[float], int]]) -> list[float]:
    w = [0.0] * 6
    n = len(samples)
    if n == 0:
        return w

    for _ in range(TRAIN_STEPS):
        grad = [0.0] * 6
        for x, y in samples:
            z = w[0] + sum(w[j + 1] * x[j] for j in range(5))
            p = _sigmoid(z)
            err = p - y
            grad[0] += err
            for j in range(5):
                grad[j + 1] += err * x[j]
        for j in range(6):
            grad[j] /= n
            if j > 0:
                grad[j] += L2 * w[j]
            w[j] -= LEARNING_RATE * grad[j]
    return w


def _predict(w: list[float], x: list[float]) -> float:
    z = w[0] + sum(w[j + 1] * x[j] for j in range(5))
    return _sigmoid(z)


def _bars_to_samples(rows: list[dict[str, Any]]) -> tuple[list[tuple[list[float], int]], list[float] | None]:
    bars: list[dict[str, float]] = []
    for row in rows:
        o = _safe_float(row.get("open"))
        h = _safe_float(row.get("high"))
        lo = _safe_float(row.get("low"))
        c = _safe_float(row.get("close"))
        v = _safe_float(row.get("volume")) or 0.0
        if None in (o, h, lo, c) or c <= 0 or h < lo:
            continue
        bars.append({"open": o, "high": h, "low": lo, "close": c, "volume": v})

    samples: list[tuple[list[float], int]] = []
    for i in range(3, len(bars) - 1):
        b0, b1, b3 = bars[i], bars[i - 1], bars[i - 3]
        c0 = b0["close"]
        c1 = b1["close"]
        c3 = b3["close"]

        r1_bps = (c0 / c1 - 1.0) * 10000.0
        r3_bps = (c0 / c3 - 1.0) * 10000.0
        range_bps = (b0["high"] - b0["low"]) / c0 * 10000.0
        close_position = (
            (c0 - b0["low"]) / (b0["high"] - b0["low"]) - 0.5
            if b0["high"] > b0["low"]
            else 0.0
        )
        volume_change = (
            math.log1p(b0["volume"] + 1.0) - math.log1p(b1["volume"] + 1.0)
        )

        future_return_bps = (bars[i + 1]["close"] / c0 - 1.0) * 10000.0
        if abs(future_return_bps) < LABEL_THRESHOLD_BPS:
            continue

        x = [
            _clamp(r1_bps / 25.0, -3.0, 3.0),
            _clamp(r3_bps / 50.0, -3.0, 3.0),
            _clamp(range_bps / 100.0, 0.0, 3.0),
            _clamp(close_position, -0.5, 0.5),
            _clamp(volume_change, -3.0, 3.0),
        ]
        samples.append((x, 1 if future_return_bps > 0 else 0))

    latest_x: list[float] | None = None
    if len(bars) >= 4:
        i = len(bars) - 1
        b0, b1, b3 = bars[i], bars[i - 1], bars[i - 3]
        c0 = b0["close"]
        c1 = b1["close"]
        c3 = b3["close"]
        r1_bps = (c0 / c1 - 1.0) * 10000.0
        r3_bps = (c0 / c3 - 1.0) * 10000.0
        range_bps = (b0["high"] - b0["low"]) / c0 * 10000.0
        close_position = (
            (c0 - b0["low"]) / (b0["high"] - b0["low"]) - 0.5
            if b0["high"] > b0["low"]
            else 0.0
        )
        volume_change = (
            math.log1p(b0["volume"] + 1.0) - math.log1p(b1["volume"] + 1.0)
        )
        latest_x = [
            _clamp(r1_bps / 25.0, -3.0, 3.0),
            _clamp(r3_bps / 50.0, -3.0, 3.0),
            _clamp(range_bps / 100.0, 0.0, 3.0),
            _clamp(close_position, -0.5, 0.5),
            _clamp(volume_change, -3.0, 3.0),
        ]

    return samples, latest_x


def _evaluate(samples: list[tuple[list[float], int]]) -> dict[str, Any]:
    n = len(samples)
    if n < MIN_TRAIN_ROWS:
        return {
            "sample_count": n,
            "test_count": 0,
            "accuracy": None,
            "brier": None,
            "baseline_brier": None,
            "validated": False,
        }

    split = max(1, int(n * 0.8))
    train = samples[:split]
    test = samples[split:]
    if len(test) < 60 or len({y for _, y in test}) < 2:
        return {
            "sample_count": n,
            "test_count": len(test),
            "accuracy": None,
            "brier": None,
            "baseline_brier": None,
            "validated": False,
        }

    w = _fit(train)
    probs = [_predict(w, x) for x, _ in test]
    labels = [y for _, y in test]
    accuracy = sum((p >= 0.5) == bool(y) for p, y in zip(probs, labels)) / len(labels)
    brier = sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)
    base_rate = sum(labels) / len(labels)
    baseline_brier = min(base_rate, 1.0 - base_rate)

    return {
        "sample_count": n,
        "test_count": len(test),
        "accuracy": round(accuracy, 4),
        "brier": round(brier, 5),
        "baseline_brier": round(baseline_brier, 5),
        "validated": bool(
            n >= MIN_TRAIN_ROWS
            and accuracy >= 0.55
            and brier < baseline_brier
        ),
    }


async def get_historical_forecast(symbol: str, token: str) -> dict[str, Any]:
    cached = _CACHE.get(symbol)
    if cached and __import__("time").time() - cached[0] < CACHE_SECONDS:
        return cached[1]

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    params = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "resampleFreq": RESAMPLE_FREQ,
        "columns": "open,high,low,close,volume",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                f"{HISTORY_URL}/{symbol}/prices",
                headers=headers,
                params=params,
            )
        if response.status_code >= 400:
            result = {
                "status": "HISTORICAL_UNAVAILABLE",
                "forecast": None,
                "evaluation": {},
                "model_id": "historical-logit-v1",
                "lookback_days": LOOKBACK_DAYS,
                "error": f"Tiingo historical HTTP {response.status_code}",
            }
            _CACHE[symbol] = (__import__("time").time(), result)
            return result

        data = response.json()
        if not isinstance(data, list):
            data = data.get("data", []) if isinstance(data, dict) else []

        samples, latest_x = _bars_to_samples(data[-MAX_ROWS:])
        evaluation = _evaluate(samples)

        if len(samples) < MIN_TRAIN_ROWS or latest_x is None:
            result = {
                "status": "HISTORICAL_WARMUP",
                "forecast": None,
                "evaluation": evaluation,
                "model_id": "historical-logit-v1",
                "lookback_days": LOOKBACK_DAYS,
                "bars": len(data),
            }
            _CACHE[symbol] = (__import__("time").time(), result)
            return result

        weights = _fit(samples)
        x_last = latest_x
        p_up = _predict(weights, x_last)
        p_down = 1.0 - p_up
        confidence = abs(p_up - 0.5) * 2.0

        if p_up >= 0.55:
            direction = "UP"
        elif p_down >= 0.55:
            direction = "DOWN"
        else:
            direction = "NEUTRAL"

        result = {
            "status": "VALIDATED" if evaluation["validated"] else "EXPERIMENTAL",
            "forecast": {
                "direction": direction,
                "raw_probability_up": round(p_up, 4),
                "raw_probability_down": round(p_down, 4),
                "confidence_raw": round(confidence, 4),
                "model_id": "historical-logit-v1",
                "validated": bool(evaluation["validated"]),
                "calibrated": False,
                "horizon_seconds": 300,
                "label_threshold_bps": LABEL_THRESHOLD_BPS,
                "features": {
                    "return_1bar_scaled": round(x_last[0], 4),
                    "return_3bar_scaled": round(x_last[1], 4),
                    "range_scaled": round(x_last[2], 4),
                    "close_position": round(x_last[3], 4),
                    "volume_change_scaled": round(x_last[4], 4),
                },
            },
            "evaluation": evaluation,
            "model_id": "historical-logit-v1",
            "lookback_days": LOOKBACK_DAYS,
            "bars": len(data),
        }
        _CACHE[symbol] = (__import__("time").time(), result)
        return result
    except Exception as exc:
        result = {
            "status": "HISTORICAL_ERROR",
            "forecast": None,
            "evaluation": {},
            "model_id": "historical-logit-v1",
            "error": f"{type(exc).__name__}: {exc}",
        }
        _CACHE[symbol] = (__import__("time").time(), result)
        return result
