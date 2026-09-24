"""PMSF-X Nano — cross-asset lead/lag specialist.

Uses lagged peer returns only. A peer observation is eligible for a target event
at time t only when its timestamp is <= t - 5 minutes, preventing
same-bar leakage.

This specialist is research/experimental and is not allowed to replace the
primary historical forecast unless its own validation passes.
"""

from __future__ import annotations

import math
import time
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from quant.temporal import TemporalSample, walk_forward_splits

HISTORY_URL = "https://api.tiingo.com/tiingo/equity/intraday"
RESAMPLE_FREQ = "5min"
LOOKBACK_DAYS = 45
MAX_ROWS = 2200
LABEL_THRESHOLD_BPS = 0.5
MIN_TRAIN_ROWS = 300
LEARNING_RATE = 0.06
TRAIN_STEPS = 160
L2 = 0.02
CACHE_SECONDS = 3600.0
MODEL_ID = "cross-asset-leadlag-v1"

# Keep the peer graph intentionally small so the live collector stays near
# its established request budget after the existing historical fetches.
PEERS: dict[str, tuple[str, ...]] = {
    "AAPL": ("MSFT", "NVDA"),
    "MSFT": ("AAPL", "NVDA"),
    "NVDA": ("AAPL", "MSFT"),
    "TSLA": ("AAPL", "NVDA"),
}

_BAR_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        ts = _parse_time(row.get("date") or row.get("timestamp"))
        close = _safe_float(row.get("close"))
        if ts is None or close is None or close <= 0:
            continue
        out.append({"time": ts, "close": close})
    out.sort(key=lambda x: x["time"])
    return out[-MAX_ROWS:]


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def _fit(samples: list[tuple[list[float], int]]) -> list[float]:
    width = len(samples[0][0]) + 1 if samples else 1
    weights = [0.0] * width
    if not samples:
        return weights
    n = len(samples)
    for _ in range(TRAIN_STEPS):
        grad = [0.0] * width
        for x, y in samples:
            z = weights[0] + sum(weights[j + 1] * x[j] for j in range(len(x)))
            p = _sigmoid(z)
            err = p - y
            grad[0] += err
            for j in range(len(x)):
                grad[j + 1] += err * x[j]
        for j in range(width):
            grad[j] /= n
            if j > 0:
                grad[j] += L2 * weights[j]
            weights[j] -= LEARNING_RATE * grad[j]
    return weights


def _predict(weights: list[float], x: list[float]) -> float:
    return _sigmoid(weights[0] + sum(weights[j + 1] * x[j] for j in range(len(x))))


def _fetch_rows(symbol: str, token: str) -> list[dict[str, Any]]:
    cached = _BAR_CACHE.get(symbol)
    if cached and time.time() - cached[0] < CACHE_SECONDS:
        return cached[1]

    start_date = datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)
    end_date = datetime.now(timezone.utc).date()
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

    response = httpx.get(
        f"{HISTORY_URL}/{symbol}/prices",
        headers=headers,
        params=params,
        timeout=20.0,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        data = data.get("data", []) if isinstance(data, dict) else []
    rows = _normalize_rows(data)
    _BAR_CACHE[symbol] = (time.time(), rows)
    return rows


def _peer_observation(
    peer_rows: list[dict[str, Any]],
    target_time: datetime,
) -> tuple[float, float] | None:
    if len(peer_rows) < 4:
        return None

    peer_times = [row["time"] for row in peer_rows]
    cutoff = target_time - timedelta(minutes=5)
    j = bisect_right(peer_times, cutoff) - 1
    if j < 3:
        return None

    t0 = peer_rows[j]["time"]
    t1 = peer_rows[j - 1]["time"]
    t2 = peer_rows[j - 2]["time"]
    t3 = peer_rows[j - 3]["time"]
    expected = timedelta(minutes=5)
    if (
        t0 - t1 != expected
        or t1 - t2 != expected
        or t2 - t3 != expected
    ):
        return None

    c0 = peer_rows[j]["close"]
    c1 = peer_rows[j - 1]["close"]
    c3 = peer_rows[j - 3]["close"]
    r1 = (c0 / c1 - 1.0) * 10000.0
    r3 = (c0 / c3 - 1.0) * 10000.0
    return (
        _clamp(r1 / 25.0, -3.0, 3.0),
        _clamp(r3 / 50.0, -3.0, 3.0),
    )


def _build_samples(
    target_rows: list[dict[str, Any]],
    peer_rows: dict[str, list[dict[str, Any]]],
) -> tuple[list[tuple[list[float], int]], list[TemporalSample], list[float] | None, dict[str, Any] | None]:
    target = _normalize_rows(target_rows)
    samples: list[tuple[list[float], int]] = []
    temporal: list[TemporalSample] = []

    expected = timedelta(minutes=5)
    for i in range(0, len(target) - 1):
        if target[i + 1]["time"] - target[i]["time"] != expected:
            continue

        feature_values: list[float] = []
        contributions: dict[str, Any] = {}
        complete = True

        for peer, rows in peer_rows.items():
            obs = _peer_observation(rows, target[i]["time"])
            if obs is None:
                complete = False
                break
            r1, r3 = obs
            feature_values.extend([r1, r3])
            contributions[peer] = {
                "lag1_scaled": round(r1, 4),
                "lag3_scaled": round(r3, 4),
            }

        if not complete:
            continue

        future_return_bps = (
            target[i + 1]["close"] / target[i]["close"] - 1.0
        ) * 10000.0
        if abs(future_return_bps) < LABEL_THRESHOLD_BPS:
            continue

        label = 1 if future_return_bps > 0 else 0
        samples.append((feature_values, label))
        temporal.append(
            TemporalSample(
                event_time=target[i]["time"],
                label_end_time=target[i + 1]["time"],
                features=feature_values,
                label=label,
            )
        )

    latest_features: list[float] | None = None
    latest_context: dict[str, Any] | None = None
    if target:
        feature_values = []
        peer_context: dict[str, Any] = {}
        for peer, rows in peer_rows.items():
            obs = _peer_observation(rows, target[-1]["time"])
            if obs is None:
                feature_values = []
                break
            r1, r3 = obs
            feature_values.extend([r1, r3])
            peer_context[peer] = {
                "lag1_bps": round(r1 * 25.0, 4),
                "lag3_bps": round(r3 * 50.0, 4),
            }
        if feature_values:
            latest_features = feature_values
            signed = [
                value
                for peer in peer_context.values()
                for value in (peer["lag1_bps"],)
            ]
            latest_context = {
                "target_bar_time": target[-1]["time"].isoformat(),
                "peer_cutoff": (
                    target[-1]["time"] - timedelta(minutes=5)
                ).isoformat(),
                "peers": peer_context,
                "consensus_lag1_bps": round(
                    sum(signed) / len(signed), 4
                ) if signed else 0.0,
                "agreement": round(
                    abs(sum(1 if x > 0 else -1 if x < 0 else 0 for x in signed))
                    / len([x for x in signed if x != 0]),
                    4,
                ) if any(signed) else 0.0,
            }

    return samples, temporal, latest_features, latest_context


def _evaluate(
    samples: list[tuple[list[float], int]],
    temporal: list[TemporalSample],
) -> dict[str, Any]:
    if len(samples) < MIN_TRAIN_ROWS:
        return {
            "sample_count": len(samples),
            "test_count": 0,
            "fold_count": 0,
            "validated": False,
            "validation_reason": "INSUFFICIENT_SAMPLES",
            "validation_type": "walk_forward_purged_embargoed",
            "purge_minutes": 5,
            "embargo_minutes": 5,
        }

    folds = walk_forward_splits(
        temporal,
        train_size=300,
        test_size=60,
        purge=timedelta(minutes=5),
        embargo=timedelta(minutes=5),
    )
    accuracies: list[float] = []
    briers: list[float] = []
    baselines: list[float] = []
    total = 0

    for fold in folds:
        train = [(list(x.features), x.label) for x in fold.train]
        test = [(list(x.features), x.label) for x in fold.test]
        if len({y for _, y in train}) < 2 or len({y for _, y in test}) < 2:
            continue
        weights = _fit(train)
        probs = [_predict(weights, x) for x, _ in test]
        labels = [y for _, y in test]
        baseline_rate = sum(y for _, y in train) / len(train)
        accuracies.append(
            sum((p >= 0.5) == bool(y) for p, y in zip(probs, labels)) / len(labels)
        )
        briers.append(
            sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)
        )
        baselines.append(
            sum((baseline_rate - y) ** 2 for y in labels) / len(labels)
        )
        total += len(test)

    if not accuracies:
        return {
            "sample_count": len(samples),
            "test_count": 0,
            "fold_count": len(folds),
            "validated": False,
            "validation_reason": "NO_USABLE_FOLDS",
            "validation_type": "walk_forward_purged_embargoed",
            "purge_minutes": 5,
            "embargo_minutes": 5,
        }

    accuracy = sum(accuracies) / len(accuracies)
    brier = sum(briers) / len(briers)
    baseline = sum(baselines) / len(baselines)
    skill = 1.0 - brier / baseline if baseline > 0 else None
    validated = accuracy >= 0.55 and brier < baseline and len(accuracies) >= 2

    return {
        "sample_count": len(samples),
        "test_count": total,
        "fold_count": len(accuracies),
        "accuracy": round(accuracy, 4),
        "brier": round(brier, 5),
        "baseline_brier": round(baseline, 5),
        "brier_skill": round(skill, 5) if skill is not None else None,
        "validated": validated,
        "validation_reason": "PASS" if validated else "METRICS_BELOW_THRESHOLD",
        "validation_type": "walk_forward_purged_embargoed",
        "purge_minutes": 5,
        "embargo_minutes": 5,
    }


async def get_cross_asset_forecast(
    symbol: str,
    token: str,
    *,
    target_rows: list[dict[str, Any]] | None = None,
    evaluate: bool = False,
) -> dict[str, Any]:
    symbol = symbol.upper()
    peers = PEERS.get(symbol)
    if not peers:
        return {
            "status": "NO_PEER_GRAPH",
            "model_id": MODEL_ID,
            "forecast": None,
            "evaluation": {},
        }

    try:
        if target_rows is None:
            target_rows = _fetch_rows(symbol, token)

        peer_rows = {peer: _fetch_rows(peer, token) for peer in peers}
        samples, temporal, latest_features, context = _build_samples(
            target_rows,
            peer_rows,
        )

        evaluation = (
            _evaluate(samples, temporal)
            if evaluate
            else {
                "sample_count": len(samples),
                "test_count": 0,
                "fold_count": 0,
                "validated": False,
                "validation_reason": "LIVE_FORECAST_NO_RECALCULATION",
                "validation_type": "LIVE_FAST_PATH",
                "purge_minutes": 5,
                "embargo_minutes": 5,
            }
        )

        if len(samples) < MIN_TRAIN_ROWS or latest_features is None:
            return {
                "status": "WARMUP",
                "model_id": MODEL_ID,
                "forecast": None,
                "evaluation": evaluation,
                "context": context,
            }

        weights = _fit(samples)
        p_up = _predict(weights, latest_features)
        p_down = 1.0 - p_up
        confidence = abs(p_up - 0.5) * 2.0
        direction = "UP" if p_up >= 0.55 else ("DOWN" if p_down >= 0.55 else "NEUTRAL")

        return {
            "status": "VALIDATED" if evaluation.get("validated") else "EXPERIMENTAL",
            "model_id": MODEL_ID,
            "forecast": {
                "direction": direction,
                "raw_probability_up": round(p_up, 4),
                "raw_probability_down": round(p_down, 4),
                "confidence_raw": round(confidence, 4),
                "model_id": MODEL_ID,
                "validated": bool(evaluation.get("validated")),
                "calibrated": False,
                "horizon_seconds": 300,
            },
            "evaluation": evaluation,
            "context": context,
        }
    except Exception as exc:
        return {
            "status": "ERROR",
            "model_id": MODEL_ID,
            "forecast": None,
            "evaluation": {},
            "context": None,
            "error": f"{type(exc).__name__}: {exc}",
        }
