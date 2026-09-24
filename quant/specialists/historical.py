"""PMSF-X Nano — Historical 5-minute price specialist.

Uses Tiingo's documented consolidated historical intraday bars to train
and evaluate a chronological logistic model for the next 5-minute move.
The holdout is strictly later in time than the training segment.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from typing import Any

from quant.temporal import TemporalSample, walk_forward_splits
from quant.calibration import fit_platt, calibrate_platt, metrics
from quant.regime import classify_regime

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
CACHE_SECONDS = 3600.0

# Primary live horizon remains 300s for backward compatibility with the
# existing outcome engine. These additional horizons are analytical only.
MULTI_HORIZONS = (
    (300, 1),
    (900, 3),
    (1800, 6),
)
MULTI_HORIZON_MODEL_ID = "historical-logit-multihorizon-v1"

_CACHE: dict[tuple[str, bool], tuple[float, dict[str, Any]]] = {}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _parse_bar_time(value: Any) -> datetime | None:
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


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def _log_loss(p: float, y: int) -> float:
    p = _clamp(p, 1e-6, 1.0 - 1e-6)
    return -(y * math.log(p) + (1 - y) * math.log(1.0 - p))


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


def _bars_to_samples(
    rows: list[dict[str, Any]],
    *,
    horizon_bars: int = 1,
) -> tuple[list[tuple[list[float], int]], list[TemporalSample], list[float] | None]:
    bars: list[dict[str, float]] = []
    for row in rows:
        o = _safe_float(row.get("open"))
        h = _safe_float(row.get("high"))
        lo = _safe_float(row.get("low"))
        c = _safe_float(row.get("close"))
        v = _safe_float(row.get("volume")) or 0.0
        ts = _parse_bar_time(row.get("date") or row.get("timestamp"))
        if ts is None or None in (o, h, lo, c) or c <= 0 or h < lo:
            continue
        bars.append({"time": ts, "open": o, "high": h, "low": lo, "close": c, "volume": v})

    bars.sort(key=lambda row: row["time"])

    if horizon_bars < 1:
        raise ValueError("HORIZON_BARS_MUST_BE_POSITIVE")

    samples: list[tuple[list[float], int]] = []
    temporal_samples: list[TemporalSample] = []
    expected_step = timedelta(minutes=5)
    max_index = len(bars) - horizon_bars
    for i in range(3, max_index):
        required_indexes = range(i - 2, i + horizon_bars + 1)
        if any(
            bars[j]["time"] - bars[j - 1]["time"] != expected_step
            for j in required_indexes
            if j > 0
        ):
            continue
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

        future_return_bps = (bars[i + horizon_bars]["close"] / c0 - 1.0) * 10000.0
        if abs(future_return_bps) < LABEL_THRESHOLD_BPS:
            continue

        x = [
            _clamp(r1_bps / 25.0, -3.0, 3.0),
            _clamp(r3_bps / 50.0, -3.0, 3.0),
            _clamp(range_bps / 100.0, 0.0, 3.0),
            _clamp(close_position, -0.5, 0.5),
            _clamp(volume_change, -3.0, 3.0),
        ]
        label = 1 if future_return_bps > 0 else 0
        samples.append((x, label))
        temporal_samples.append(
            TemporalSample(
                event_time=b0["time"],
                label_end_time=bars[i + horizon_bars]["time"],
                features=x,
                label=label,
            )
        )

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

    return samples, temporal_samples, latest_x


def _evaluate(
    samples: list[tuple[list[float], int]],
    temporal_samples: list[TemporalSample],
    *,
    horizon_minutes: int = 5,
) -> dict[str, Any]:
    n = len(samples)
    if n < MIN_TRAIN_ROWS:
        return {'sample_count': n, 'test_count': 0, 'fold_count': 0, 'validated': False, 'validation_reason': 'INSUFFICIENT_SAMPLES'}
    horizon_delta = timedelta(minutes=horizon_minutes)
    folds = walk_forward_splits(
        temporal_samples,
        train_size=300,
        test_size=60,
        purge=horizon_delta,
        embargo=horizon_delta,
    )
    if not folds:
        return {'sample_count': n, 'test_count': 0, 'fold_count': 0, 'validated': False, 'validation_reason': 'NO_VALID_WALK_FORWARD_FOLDS'}
    accs=[]; bs=[]; bbs=[]; lls=[]; blls=[]; total=0
    calibration_probs=[]; calibration_labels=[]
    for fold in folds:
        train=[(list(x.features), x.label) for x in fold.train]; test=[(list(x.features), x.label) for x in fold.test]
        if len({y for _,y in train})<2 or len({y for _,y in test})<2: continue
        w=_fit(train); probs=[_predict(w,x) for x,_ in test]; labels=[y for _,y in test]
        calibration_probs.extend(probs); calibration_labels.extend(labels)
        rate=sum(y for _,y in train)/len(train); base=[rate]*len(labels)
        accs.append(sum((p>=0.5)==bool(y) for p,y in zip(probs,labels))/len(labels))
        bs.append(sum((p-y)**2 for p,y in zip(probs,labels))/len(labels)); bbs.append(sum((p-y)**2 for p,y in zip(base,labels))/len(labels))
        lls.append(sum(_log_loss(p,y) for p,y in zip(probs,labels))/len(labels)); blls.append(sum(_log_loss(p,y) for p,y in zip(base,labels))/len(labels)); total+=len(test)
    if not accs:
        return {'sample_count': n, 'test_count': 0, 'fold_count': len(folds), 'validated': False, 'validation_reason': 'NO_USABLE_FOLDS'}
    accuracy=sum(accs)/len(accs); brier=sum(bs)/len(bs); baseline=sum(bbs)/len(bbs); ll=sum(lls)/len(lls); bll=sum(blls)/len(blls)
    skill=1.0-brier/baseline if baseline>0 else None
    calibration_status="NOT_ENOUGH_DATA"; calibrated_brier=None; calibrated_logloss=None
    if len(calibration_labels)>=30 and len(set(calibration_labels))>=2:
        try:
            a,b=fit_platt(calibration_probs, calibration_labels)
            cp=[calibrate_platt(p,a,b) for p in calibration_probs]
            calibrated_brier, calibrated_logloss=metrics(cp,calibration_labels)
            calibration_status="PLATT_FIT"
        except ValueError:
            pass
    validated=accuracy>=0.55 and brier<baseline and len(accs)>=2
    regime = classify_regime([((x[0][0]) * 25.0) for x in samples[-40:]], [((x[0][2]) * 100.0) for x in samples[-40:]])
    return {
        'sample_count': n,
        'test_count': total,
        'fold_count': len(accs),
        'accuracy': round(accuracy, 4),
        'brier': round(brier, 5),
        'baseline_brier': round(baseline, 5),
        'log_loss': round(ll, 5),
        'baseline_log_loss': round(bll, 5),
        'brier_skill': round(skill, 5) if skill is not None else None,
        'calibration_method': 'PLATT',
        'calibration_status': calibration_status,
        'calibrated_brier': round(calibrated_brier, 5) if calibrated_brier is not None else None,
        'calibrated_log_loss': round(calibrated_logloss, 5) if calibrated_logloss is not None else None,
        'regime': regime,
        'validated': validated,
        'validation_reason': 'PASS' if validated else 'METRICS_BELOW_THRESHOLD',
        'validation_type': 'walk_forward_purged_embargoed',
        'purge_minutes': horizon_minutes,
        'embargo_minutes': horizon_minutes,
        'fold_test_size': 60,
    }

def _build_horizon_forecast(
    rows: list[dict[str, Any]],
    *,
    horizon_seconds: int,
    horizon_bars: int,
    evaluate: bool,
) -> dict[str, Any]:
    samples, temporal_samples, latest_x = _bars_to_samples(
        rows,
        horizon_bars=horizon_bars,
    )
    horizon_minutes = horizon_bars * 5
    if evaluate:
        evaluation = _evaluate(
            samples,
            temporal_samples,
            horizon_minutes=horizon_minutes,
        )
    else:
        evaluation = {
            "sample_count": len(samples),
            "test_count": 0,
            "fold_count": 0,
            "validated": False,
            "validation_reason": "LIVE_FORECAST_NO_RECALCULATION",
            "validation_type": "LIVE_FAST_PATH",
            "purge_minutes": horizon_minutes,
            "embargo_minutes": horizon_minutes,
        }

    if len(samples) < MIN_TRAIN_ROWS or latest_x is None:
        return {
            "status": "WARMUP",
            "horizon_seconds": horizon_seconds,
            "horizon_bars": horizon_bars,
            "forecast": None,
            "evaluation": evaluation,
        }

    weights = _fit(samples)
    p_up = _predict(weights, latest_x)
    p_down = 1.0 - p_up
    confidence = abs(p_up - 0.5) * 2.0
    direction = "UP" if p_up >= 0.55 else ("DOWN" if p_down >= 0.55 else "NEUTRAL")

    return {
        "status": "READY",
        "horizon_seconds": horizon_seconds,
        "horizon_bars": horizon_bars,
        "forecast": {
            "direction": direction,
            "raw_probability_up": round(p_up, 4),
            "raw_probability_down": round(p_down, 4),
            "confidence_raw": round(confidence, 4),
            "horizon_seconds": horizon_seconds,
            "validated": bool(evaluation.get("validated")),
            "calibrated": False,
        },
        "evaluation": evaluation,
    }


def _build_multi_horizon(rows: list[dict[str, Any]], *, evaluate: bool) -> dict[str, Any]:
    horizons: dict[str, Any] = {}
    for horizon_seconds, horizon_bars in MULTI_HORIZONS:
        result = _build_horizon_forecast(
            rows,
            horizon_seconds=horizon_seconds,
            horizon_bars=horizon_bars,
            evaluate=evaluate,
        )
        horizons[str(horizon_seconds)] = result

    ready = {
        key: value
        for key, value in horizons.items()
        if value.get("forecast") is not None
    }
    return {
        "status": "READY" if ready else "WARMUP",
        "model_id": MULTI_HORIZON_MODEL_ID,
        "horizons_seconds": [x[0] for x in MULTI_HORIZONS],
        "primary_horizon_seconds": 300,
        "forecast_vector": {
            key: value["forecast"]
            for key, value in ready.items()
        },
        "evaluation": {
            key: value["evaluation"]
            for key, value in horizons.items()
        },
    }


async def get_historical_forecast(
    symbol: str,
    token: str,
    *,
    evaluate: bool = True,
) -> dict[str, Any]:
    cache_key = (symbol, bool(evaluate))
    cached = _CACHE.get(cache_key)
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
            _CACHE[cache_key] = (__import__("time").time(), result)
            return result

        data = response.json()
        if not isinstance(data, list):
            data = data.get("data", []) if isinstance(data, dict) else []

        rows = data[-MAX_ROWS:]
        samples, temporal_samples, latest_x = _bars_to_samples(rows, horizon_bars=1)
        if evaluate:
            evaluation = _evaluate(samples, temporal_samples, horizon_minutes=5)
        else:
            evaluation = {
                "sample_count": len(samples),
                "test_count": 0,
                "fold_count": 0,
                "validated": False,
                "validation_reason": "LIVE_FORECAST_NO_RECALCULATION",
                "validation_type": "LIVE_FAST_PATH",
            }
        multi_horizon = _build_multi_horizon(
            rows,
            evaluate=evaluate,
        )
        evaluation["multi_horizon_status"] = multi_horizon["status"]
        evaluation["multi_horizon_model_id"] = MULTI_HORIZON_MODEL_ID
        evaluation["cpcv_status"] = "RESEARCH_MODULE_READY"
        evaluation["pbo_status"] = "RESEARCH_MODULE_READY"
        evaluation["dsr_status"] = "RESEARCH_MODULE_READY"
        evaluation["search_ledger"] = {"trials": 1, "selection_rule": "record_only"}

        if len(samples) < MIN_TRAIN_ROWS or latest_x is None:
            result = {
                "status": "HISTORICAL_WARMUP",
                "forecast": None,
                "evaluation": evaluation,
                "model_id": "historical-logit-v1",
                "lookback_days": LOOKBACK_DAYS,
                "bars": len(data),
                "multi_horizon": multi_horizon,
            }
            _CACHE[cache_key] = (__import__("time").time(), result)
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
            "multi_horizon": multi_horizon,
        }
        _CACHE[cache_key] = (__import__("time").time(), result)
        return result
    except Exception as exc:
        result = {
            "status": "HISTORICAL_ERROR",
            "forecast": None,
            "evaluation": {},
            "model_id": "historical-logit-v1",
            "error": f"{type(exc).__name__}: {exc}",
        }
        _CACHE[cache_key] = (__import__("time").time(), result)
        return result
