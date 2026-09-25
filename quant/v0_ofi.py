"""PMSF-X Nano V0 — OFI + volume research baseline.

Important data-boundary:
- The current historical source is 5-minute OHLCV.
- Therefore this module implements a BAR-LEVEL OFI PROXY, not true event-level
  order-flow imbalance from a full depth order book.
- The proxy is signed traded volume using close-location value (CLV), plus
  normalized volume and price features.
- 5s/30s horizons are explicitly unsupported until event-level trade/L2 data
  is available. Supported horizons here are 5m, 30m and 1d (78 x 5m bars).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import timedelta, datetime
from typing import Any, Iterable, Sequence

from quant.temporal import TemporalSample, walk_forward_splits

MODEL_ID = "v0-ofi-volume-proxy-v1"
FEATURES_VERSION = "v0-ofi-proxy-ohlcv-1"
BAR_MINUTES = 5
HORIZONS = {
    300: 1,      # 5m
    1800: 6,     # 30m
    23400: 78,   # 1 trading day at 5m bars
}
LABEL_THRESHOLD_BPS = 0.5
MIN_SAMPLES = 360
TRAIN_SIZE = 300
TEST_SIZE = 60
TRAIN_STEPS = 120
LEARNING_RATE = 0.05
L2 = 0.02
ROLLING_WINDOW = 20


@dataclass(frozen=True)
class Bar:
    event_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def _safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt
    except (TypeError, ValueError):
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-_clamp(x, -30.0, 30.0)))


def _log_loss(p: float, y: int) -> float:
    p = _clamp(p, 1e-6, 1.0 - 1e-6)
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


def _auc(probs: Sequence[float], labels: Sequence[int]) -> float | None:
    pos = [p for p, y in zip(probs, labels) if y == 1]
    neg = [p for p, y in zip(probs, labels) if y == 0]
    if not pos or not neg:
        return None
    wins = 0.0
    ties = 0.0
    for p in pos:
        for q in neg:
            if p > q:
                wins += 1.0
            elif p == q:
                ties += 1.0
    return (wins + 0.5 * ties) / (len(pos) * len(neg))


def _ece(probs: Sequence[float], labels: Sequence[int], bins: int = 10) -> float | None:
    if not labels:
        return None
    total = len(labels)
    score = 0.0
    for i in range(bins):
        lo = i / bins
        hi = (i + 1) / bins
        bucket = [
            (p, y)
            for p, y in zip(probs, labels)
            if (lo <= p < hi) or (i == bins - 1 and p <= hi)
        ]
        if not bucket:
            continue
        avg_p = sum(p for p, _ in bucket) / len(bucket)
        avg_y = sum(y for _, y in bucket) / len(bucket)
        score += (len(bucket) / total) * abs(avg_p - avg_y)
    return score


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((x - mu) ** 2 for x in values) / len(values))


def _normalize(value: float, history: Sequence[float]) -> float:
    if not history:
        return 0.0
    mu = _mean(history)
    sd = _std(history)
    if sd <= 1e-12:
        return 0.0
    return _clamp((value - mu) / sd, -5.0, 5.0)


def _prepare_bars(rows: Iterable[dict[str, Any]]) -> list[Bar]:
    bars: list[Bar] = []
    for row in rows:
        ts = _parse_time(row.get("date") or row.get("timestamp"))
        o = _safe_float(row.get("open"))
        h = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        c = _safe_float(row.get("close"))
        v = _safe_float(row.get("volume")) or 0.0
        if ts is None or None in (o, h, low, c):
            continue
        if c <= 0 or h < low or low <= 0:
            continue
        bars.append(Bar(ts, o, h, low, c, max(0.0, v)))
    bars.sort(key=lambda x: x.event_time)

    # Enforce 5-minute regularity. This prevents overnight/weekend gaps from
    # becoming artificial one-step returns or labels.
    clean: list[Bar] = []
    step = timedelta(minutes=BAR_MINUTES)
    for bar in bars:
        if not clean or bar.event_time - clean[-1].event_time == step:
            clean.append(bar)
    return clean


def _feature_row(bars: Sequence[Bar], i: int) -> tuple[list[float], dict[str, float]]:
    b = bars[i]
    prev = bars[i - 1]
    prev3 = bars[i - 3]

    eps = 1e-12
    rng = max(b.high - b.low, eps)
    clv = ((b.close - b.low) - (b.high - b.close)) / rng
    signed_volume = clv * b.volume

    prior_volume = [bars[j].volume for j in range(max(0, i - ROLLING_WINDOW), i)]
    prior_signed = []
    for j in range(max(0, i - ROLLING_WINDOW), i):
        bj = bars[j]
        rj = max(bj.high - bj.low, eps)
        cj = ((bj.close - bj.low) - (bj.high - bj.close)) / rj
        prior_signed.append(cj * bj.volume)

    ret1_bps = (b.close / prev.close - 1.0) * 10000.0
    ret3_bps = (b.close / prev3.close - 1.0) * 10000.0
    range_bps = (b.high - b.low) / b.close * 10000.0
    volume_z = _normalize(b.volume, prior_volume)
    signed_volume_z = _normalize(signed_volume, prior_signed)
    volume_imbalance = signed_volume / (sum(abs(x) for x in prior_signed) + abs(signed_volume) + eps)

    features = [
        _clamp(signed_volume_z / 3.0, -2.0, 2.0),          # OFI proxy
        _clamp(volume_z / 3.0, -2.0, 2.0),                 # normalized volume
        _clamp(volume_imbalance, -1.0, 1.0),               # signed-volume imbalance
        _clamp(clv, -1.0, 1.0),                            # bar pressure
        _clamp(ret1_bps / 25.0, -3.0, 3.0),               # short price state
        _clamp(ret3_bps / 50.0, -3.0, 3.0),               # medium price state
        _clamp(range_bps / 100.0, 0.0, 3.0),               # volatility/liquidity
    ]
    diagnostics = {
        "clv": clv,
        "signed_volume": signed_volume,
        "signed_volume_z": signed_volume_z,
        "volume_z": volume_z,
        "volume_imbalance": volume_imbalance,
        "ret1_bps": ret1_bps,
        "ret3_bps": ret3_bps,
        "range_bps": range_bps,
    }
    return features, diagnostics


def _dataset(
    bars: Sequence[Bar],
    horizon_bars: int,
    threshold_bps: float,
) -> tuple[list[TemporalSample], list[dict[str, float]]]:
    samples: list[TemporalSample] = []
    diagnostics: list[dict[str, float]] = []
    # Require enough prior data for the rolling normalizers.
    start = max(3, ROLLING_WINDOW)
    for i in range(start, len(bars) - horizon_bars):
        x, diag = _feature_row(bars, i)
        future_return_bps = (bars[i + horizon_bars].close / bars[i].close - 1.0) * 10000.0
        if abs(future_return_bps) < threshold_bps:
            continue
        label = 1 if future_return_bps > 0 else 0
        samples.append(
            TemporalSample(
                event_time=bars[i].event_time,
                label_end_time=bars[i + horizon_bars].event_time,
                features=x,
                label=label,
            )
        )
        diagnostics.append({**diag, "future_return_bps": future_return_bps})
    return samples, diagnostics


def _fit(samples: Sequence[TemporalSample]) -> list[float]:
    weights = [0.0] * 8
    if not samples:
        return weights
    n = len(samples)
    for _ in range(TRAIN_STEPS):
        grad = [0.0] * 8
        for sample in samples:
            z = weights[0] + sum(weights[j + 1] * float(x) for j, x in enumerate(sample.features))
            p = _sigmoid(z)
            e = p - sample.label
            grad[0] += e
            for j, x in enumerate(sample.features, start=1):
                grad[j] += e * float(x)
        for j in range(8):
            grad[j] /= n
            if j > 0:
                grad[j] += L2 * weights[j]
            weights[j] -= LEARNING_RATE * grad[j]
    return weights


def _predict(weights: Sequence[float], x: Sequence[float]) -> float:
    return _sigmoid(weights[0] + sum(weights[j + 1] * float(v) for j, v in enumerate(x)))


def _evaluate_folds(samples: Sequence[TemporalSample], horizon_minutes: int) -> dict[str, Any]:
    if len(samples) < MIN_SAMPLES:
        return {
            "sample_count": len(samples),
            "fold_count": 0,
            "oos_count": 0,
            "validated": False,
            "validation_reason": "INSUFFICIENT_SAMPLES",
            "validation_type": "walk_forward_purged_embargoed",
        }

    folds = walk_forward_splits(
        samples,
        train_size=TRAIN_SIZE,
        test_size=TEST_SIZE,
        purge=timedelta(minutes=horizon_minutes),
        embargo=timedelta(minutes=horizon_minutes),
    )
    if not folds:
        return {
            "sample_count": len(samples),
            "fold_count": 0,
            "oos_count": 0,
            "validated": False,
            "validation_reason": "NO_VALID_FOLDS",
            "validation_type": "walk_forward_purged_embargoed",
        }

    fold_metrics: list[dict[str, Any]] = []
    all_probs: list[float] = []
    all_labels: list[int] = []
    all_returns: list[float] = []

    for fold in folds:
        if len({s.label for s in fold.train}) < 2 or len({s.label for s in fold.test}) < 2:
            continue
        weights = _fit(fold.train)
        probs = [_predict(weights, s.features) for s in fold.test]
        labels = [s.label for s in fold.test]
        baseline = sum(s.label for s in fold.train) / len(fold.train)
        brier = _mean([(p - y) ** 2 for p, y in zip(probs, labels)])
        base_brier = _mean([(baseline - y) ** 2 for y in labels])
        ll = _mean([_log_loss(p, y) for p, y in zip(probs, labels)])
        base_ll = _mean([_log_loss(baseline, y) for y in labels])
        acc = _mean([1.0 if (p >= 0.5) == bool(y) else 0.0 for p, y in zip(probs, labels)])
        signed_edges = [
            (1.0 if p >= 0.5 else -1.0) * r
            for p, r in zip(probs, all_returns[-len(labels):])
        ] if len(all_returns) >= len(labels) else []
        fold_metrics.append({
            "fold": fold.fold,
            "train_count": len(fold.train),
            "test_count": len(fold.test),
            "test_start": str(fold.test_start),
            "test_end": str(fold.test_end),
            "accuracy": acc,
            "brier": brier,
            "baseline_brier": base_brier,
            "log_loss": ll,
            "baseline_log_loss": base_ll,
            "auc": _auc(probs, labels),
        })
        all_probs.extend(probs)
        all_labels.extend(labels)

    if not fold_metrics:
        return {
            "sample_count": len(samples),
            "fold_count": len(folds),
            "oos_count": 0,
            "validated": False,
            "validation_reason": "NO_USABLE_FOLDS",
            "validation_type": "walk_forward_purged_embargoed",
        }

    brier = _mean([m["brier"] for m in fold_metrics])
    base_brier = _mean([m["baseline_brier"] for m in fold_metrics])
    logloss = _mean([m["log_loss"] for m in fold_metrics])
    base_ll = _mean([m["baseline_log_loss"] for m in fold_metrics])
    accuracy = _mean([m["accuracy"] for m in fold_metrics])
    auc_values = [m["auc"] for m in fold_metrics if m["auc"] is not None]
    auc = _mean(auc_values) if auc_values else None

    # OOS Brier/log-loss are evaluated only on samples whose labels are in
    # the future test windows. The calibration number is descriptive, not a
    # model-promotion gate.
    brier_skill = 1.0 - brier / base_brier if base_brier > 0 else None
    ece = _ece(all_probs, all_labels)
    validated = (
        len(fold_metrics) >= 3
        and accuracy >= 0.55
        and brier_skill is not None
        and brier_skill > 0.0
        and auc is not None
        and auc >= 0.53
    )

    return {
        "sample_count": len(samples),
        "fold_count": len(fold_metrics),
        "oos_count": len(all_labels),
        "accuracy": round(accuracy, 4),
        "auc": round(auc, 4) if auc is not None else None,
        "brier": round(brier, 6),
        "baseline_brier": round(base_brier, 6),
        "brier_skill": round(brier_skill, 6) if brier_skill is not None else None,
        "log_loss": round(logloss, 6),
        "baseline_log_loss": round(base_ll, 6),
        "log_loss_improvement": round(1.0 - logloss / base_ll, 6) if base_ll > 0 else None,
        "ece": round(ece, 6) if ece is not None else None,
        "validation_type": "walk_forward_purged_embargoed",
        "purge_minutes": horizon_minutes,
        "embargo_minutes": horizon_minutes,
        "validated": validated,
        "validation_reason": "PASS" if validated else "METRICS_BELOW_GATE",
        "folds": fold_metrics,
    }


def _variant_subset(samples: Sequence[TemporalSample], indexes: Sequence[int]) -> list[TemporalSample]:
    out = []
    for s in samples:
        out.append(
            TemporalSample(
                event_time=s.event_time,
                label_end_time=s.label_end_time,
                features=[s.features[i] for i in indexes],
                label=s.label,
            )
        )
    return out


def _latest_snapshot(bars: Sequence[Bar], horizon_bars: int) -> dict[str, Any] | None:
    if len(bars) <= max(ROLLING_WINDOW, 3):
        return None
    x, diag = _feature_row(bars, len(bars) - 1)
    return {
        "event_time": bars[-1].event_time.isoformat(),
        "horizon_seconds": horizon_bars * BAR_MINUTES * 60,
        "features": {
            "ofi_proxy_z": round(x[0], 6),
            "volume_z": round(x[1], 6),
            "volume_imbalance": round(x[2], 6),
            "clv": round(x[3], 6),
            "ret1_bps": round(diag["ret1_bps"], 4),
            "ret3_bps": round(diag["ret3_bps"], 4),
            "range_bps": round(diag["range_bps"], 4),
        },
    }


def run_v0_research(
    rows: Sequence[dict[str, Any]],
    *,
    symbol: str,
    label_threshold_bps: float = LABEL_THRESHOLD_BPS,
) -> dict[str, Any]:
    bars = _prepare_bars(rows)
    result: dict[str, Any] = {
        "service": "pmsfx-nano",
        "model_id": MODEL_ID,
        "features_version": FEATURES_VERSION,
        "symbol": symbol.upper(),
        "data_granularity": "5min_ohlcv",
        "true_event_level_ofi_available": False,
        "ofi_definition": "bar_ofi_proxy = CLV * volume; signed-volume/volume normalization",
        "unsupported_horizons_seconds": [5, 30],
        "horizons": {},
        "bars_used": len(bars),
        "label_threshold_bps": label_threshold_bps,
    }
    if len(bars) < ROLLING_WINDOW + 10:
        result["status"] = "INSUFFICIENT_DATA"
        return result

    for horizon_seconds, horizon_bars in HORIZONS.items():
        horizon_minutes = horizon_bars * BAR_MINUTES
        samples, _ = _dataset(bars, horizon_bars, label_threshold_bps)
        metrics = _evaluate_folds(samples, horizon_minutes)
        latest = _latest_snapshot(bars, horizon_bars)
        latest_forecast = None
        if metrics.get("validated") and len(samples) >= MIN_SAMPLES and latest:
            weights = _fit(samples)
            p_up = _predict(weights, latest["features"].values())
            latest_forecast = {
                "direction": "UP" if p_up >= 0.55 else ("DOWN" if p_up <= 0.45 else "NEUTRAL"),
                "p_up": round(p_up, 4),
                "confidence": round(abs(p_up - 0.5) * 2.0, 4),
            }
        result["horizons"][str(horizon_seconds)] = {
            "horizon_bars": horizon_bars,
            "sample_count": len(samples),
            "latest": latest,
            "evaluation": metrics,
            "latest_forecast": latest_forecast,
        }

    result["status"] = "COMPLETED"
    result["validation_summary"] = {
        k: v["evaluation"].get("validated")
        for k, v in result["horizons"].items()
    }
    return result
