"""PMSF-X Nano — nested OOS validation for multi-horizon meta-signal.

Research-only. Builds 300s/900s/1800s specialist probabilities without
future leakage, creates inner OOS features for the meta learner, and evaluates
the resulting 300-second target forecast on an outer purged/embargoed
walk-forward. No production probability is modified by this module.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from quant.temporal import TemporalSample, walk_forward_splits

HORIZONS: tuple[tuple[int, int], ...] = (
    (300, 1),
    (900, 3),
    (1800, 6),
)
PRIMARY_HORIZON_SECONDS = 300
TRAIN_SIZE = 300
TEST_SIZE = 60
OUTER_PURGE_MINUTES = 5
OUTER_EMBARGO_MINUTES = 5
INNER_TRAIN_SIZE = 200
INNER_TEST_SIZE = 40
META_STEPS = 180
META_LEARNING_RATE = 0.05
META_L2 = 0.05
BOOTSTRAPS = 1000
BLOCK_LENGTH = 10
MIN_OUTER_FOLDS = 5
MIN_OOS_COUNT = 300
ECE_BINS = 10


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any) -> float | None:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _bars(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        ts = _parse_time(row.get("date") or row.get("timestamp"))
        o = _safe_float(row.get("open"))
        h = _safe_float(row.get("high"))
        lo = _safe_float(row.get("low"))
        c = _safe_float(row.get("close"))
        v = _safe_float(row.get("volume")) or 0.0
        if ts is None or None in (o, h, lo, c) or c <= 0 or h < lo:
            continue
        out.append({
            "time": ts,
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": v,
        })
    out.sort(key=lambda x: x["time"])
    return out


def _feature_map(rows: list[dict[str, Any]]) -> dict[datetime, list[float]]:
    bars = _bars(rows)
    expected = timedelta(minutes=5)
    out: dict[datetime, list[float]] = {}
    for i in range(3, len(bars)):
        if (
            bars[i]["time"] - bars[i - 1]["time"] != expected
            or bars[i - 1]["time"] - bars[i - 2]["time"] != expected
            or bars[i - 2]["time"] - bars[i - 3]["time"] != expected
        ):
            continue
        b0, b1, b3 = bars[i], bars[i - 1], bars[i - 3]
        c0, c1, c3 = b0["close"], b1["close"], b3["close"]
        range_bps = (b0["high"] - b0["low"]) / c0 * 10000.0
        close_position = (
            (c0 - b0["low"]) / (b0["high"] - b0["low"]) - 0.5
            if b0["high"] > b0["low"]
            else 0.0
        )
        volume_change = (
            math.log1p(b0["volume"] + 1.0)
            - math.log1p(b1["volume"] + 1.0)
        )
        out[b0["time"]] = [
            _clamp((c0 / c1 - 1.0) * 10000.0 / 25.0, -3.0, 3.0),
            _clamp((c0 / c3 - 1.0) * 10000.0 / 50.0, -3.0, 3.0),
            _clamp(range_bps / 100.0, 0.0, 3.0),
            _clamp(close_position, -0.5, 0.5),
            _clamp(volume_change, -3.0, 3.0),
        ]
    return out


def _target_samples(
    rows: list[dict[str, Any]],
    *,
    horizon_bars: int,
    feature_map: dict[datetime, list[float]] | None = None,
    threshold_bps: float = 0.5,
) -> list[TemporalSample]:
    bars = _bars(rows)
    feature_map = feature_map or _feature_map(rows)
    expected = timedelta(minutes=5)
    samples: list[TemporalSample] = []
    for i in range(3, len(bars) - horizon_bars):
        required = range(i - 2, i + horizon_bars + 1)
        if any(
            bars[j]["time"] - bars[j - 1]["time"] != expected
            for j in required
            if j > 0
        ):
            continue
        future_return_bps = (
            bars[i + horizon_bars]["close"] / bars[i]["close"] - 1.0
        ) * 10000.0
        if abs(future_return_bps) < threshold_bps:
            continue
        feature = feature_map.get(bars[i]["time"])
        if feature is None:
            continue
        samples.append(
            TemporalSample(
                event_time=bars[i]["time"],
                label_end_time=bars[i + horizon_bars]["time"],
                features=feature,
                label=1 if future_return_bps > 0 else 0,
            )
        )
    return samples


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def _fit_logistic(
    samples: list[TemporalSample],
    *,
    feature_count: int = 5,
    steps: int = 160,
    learning_rate: float = 0.06,
    l2: float = 0.02,
) -> list[float]:
    weights = [0.0] * (feature_count + 1)
    if not samples:
        return weights
    for _ in range(steps):
        grad = [0.0] * len(weights)
        for sample in samples:
            x = list(sample.features)
            z = weights[0] + sum(weights[j + 1] * x[j] for j in range(feature_count))
            error = _sigmoid(z) - sample.label
            grad[0] += error
            for j in range(feature_count):
                grad[j + 1] += error * x[j]
        n = len(samples)
        for j in range(len(weights)):
            grad[j] /= n
            if j > 0:
                grad[j] += l2 * weights[j]
            weights[j] -= learning_rate * grad[j]
    return weights


def _predict(weights: list[float], features: list[float]) -> float:
    return _sigmoid(
        weights[0]
        + sum(weights[j + 1] * features[j] for j in range(len(features)))
    )


def _fit_meta(features: list[list[float]], labels: list[int]) -> list[float]:
    weights = [0.0] * 4
    if not labels:
        return weights
    for _ in range(META_STEPS):
        grad = [0.0] * 4
        for x, y in zip(features, labels):
            p = _sigmoid(weights[0] + sum(weights[j + 1] * x[j] for j in range(3)))
            error = p - y
            grad[0] += error
            for j in range(3):
                grad[j + 1] += error * x[j]
        n = len(labels)
        for j in range(4):
            grad[j] /= n
            if j > 0:
                grad[j] += META_L2 * weights[j]
            weights[j] -= META_LEARNING_RATE * grad[j]
    return weights


def _meta_predict(weights: list[float], features: list[float]) -> float:
    return _sigmoid(
        weights[0]
        + sum(weights[j + 1] * features[j] for j in range(3))
    )


def _logloss(probability: float, label: int) -> float:
    p = _clamp(probability, 1e-6, 1.0 - 1e-6)
    return -(label * math.log(p) + (1 - label) * math.log(1 - p))


def _ece(probabilities: list[float], labels: list[int]) -> float:
    if not labels:
        return None
    total = len(labels)
    error = 0.0
    for bucket in range(ECE_BINS):
        lo = bucket / ECE_BINS
        hi = (bucket + 1) / ECE_BINS
        idx = [
            i for i, p in enumerate(probabilities)
            if (p >= lo and p < hi) or (bucket == ECE_BINS - 1 and p <= hi)
        ]
        if not idx:
            continue
        avg_p = sum(probabilities[i] for i in idx) / len(idx)
        avg_y = sum(labels[i] for i in idx) / len(idx)
        error += len(idx) / total * abs(avg_p - avg_y)
    return error


def _metrics(probabilities: list[float], labels: list[int], baseline: list[float]) -> dict[str, float]:
    n = max(1, len(labels))
    brier = sum((p - y) ** 2 for p, y in zip(probabilities, labels)) / n
    baseline_brier = sum((p - y) ** 2 for p, y in zip(baseline, labels)) / n
    logloss = sum(_logloss(p, y) for p, y in zip(probabilities, labels)) / n
    baseline_logloss = sum(_logloss(p, y) for p, y in zip(baseline, labels)) / n
    accuracy = sum((p >= 0.5) == bool(y) for p, y in zip(probabilities, labels)) / n
    return {
        "test_count": len(labels),
        "accuracy": accuracy,
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_skill": 1.0 - brier / baseline_brier if baseline_brier > 0 else None,
        "log_loss": logloss,
        "baseline_log_loss": baseline_logloss,
        "log_loss_delta": baseline_logloss - logloss,
        "ece": _ece(probabilities, labels),
    }


def _paired_block_bootstrap(
    probabilities: list[float],
    labels: list[int],
    baseline: list[float],
    *,
    seed: int,
) -> dict[str, float | int | None]:
    n = len(labels)
    if n < BLOCK_LENGTH:
        return {"iterations": 0, "delta_brier_ci_low": None, "delta_brier_ci_high": None}
    rng = random.Random(seed)
    deltas: list[float] = []
    block_count = math.ceil(n / BLOCK_LENGTH)
    starts = list(range(n))
    for _ in range(BOOTSTRAPS):
        indices: list[int] = []
        for _ in range(block_count):
            start = rng.choice(starts)
            indices.extend((start + k) % n for k in range(BLOCK_LENGTH))
        indices = indices[:n]
        model_brier = sum(
            (probabilities[i] - labels[i]) ** 2 for i in indices
        ) / n
        base_brier = sum(
            (baseline[i] - labels[i]) ** 2 for i in indices
        ) / n
        deltas.append(base_brier - model_brier)
    deltas.sort()
    return {
        "iterations": BOOTSTRAPS,
        "delta_brier_ci_low": deltas[int(0.025 * len(deltas))],
        "delta_brier_ci_high": deltas[int(0.975 * len(deltas)) - 1],
    }


def _folds_for(samples: list[TemporalSample], purge_minutes: int, embargo_minutes: int) -> list[Any]:
    return walk_forward_splits(
        samples,
        train_size=TRAIN_SIZE,
        test_size=TEST_SIZE,
        purge=timedelta(minutes=purge_minutes),
        embargo=timedelta(minutes=embargo_minutes),
    )


def _inner_oos_predictions(
    horizon_samples: list[TemporalSample],
    outer_train_times: set[datetime],
    outer_test_start: datetime,
    *,
    purge_minutes: int,
) -> dict[datetime, float]:
    outer_boundary = outer_test_start - timedelta(minutes=purge_minutes)
    restricted = [
        sample for sample in horizon_samples
        if sample.event_time in outer_train_times
        and sample.label_end_time <= outer_boundary
    ]
    if len(restricted) < INNER_TRAIN_SIZE + INNER_TEST_SIZE:
        return {}
    folds = walk_forward_splits(
        restricted,
        train_size=INNER_TRAIN_SIZE,
        test_size=INNER_TEST_SIZE,
        purge=timedelta(minutes=purge_minutes),
        embargo=timedelta(minutes=purge_minutes),
    )
    predictions: dict[datetime, float] = {}
    for fold in folds:
        weights = _fit_logistic(list(fold.train))
        for sample in fold.test:
            predictions[sample.event_time] = _predict(weights, list(sample.features))
    return predictions


def _outer_model_predictions(
    horizon_samples: list[TemporalSample],
    outer_train_times: set[datetime],
    outer_test: tuple[TemporalSample, ...],
    feature_map: dict[datetime, list[float]],
    *,
    horizon_minutes: int,
) -> dict[datetime, float]:
    outer_test_start = outer_test[0].event_time
    train = [
        sample for sample in horizon_samples
        if sample.event_time in outer_train_times
        and sample.label_end_time <= outer_test_start - timedelta(minutes=horizon_minutes)
    ]
    if len(train) < TRAIN_SIZE:
        return {}
    weights = _fit_logistic(train)
    return {
        sample.event_time: _predict(weights, feature_map[sample.event_time])
        for sample in outer_test
        if sample.event_time in feature_map
    }


def _nested_outer_fold(
    primary_outer: Any,
    horizon_samples_by_seconds: dict[int, list[TemporalSample]],
    feature_map: dict[datetime, list[float]],
) -> dict[str, Any] | None:
    outer_train_times = {sample.event_time for sample in primary_outer.train}
    outer_test = primary_outer.test
    inner_predictions: dict[int, dict[datetime, float]] = {}

    for horizon_seconds, _ in HORIZONS:
        inner_predictions[horizon_seconds] = _inner_oos_predictions(
            horizon_samples_by_seconds[horizon_seconds],
            outer_train_times,
            outer_test[0].event_time,
            purge_minutes=horizon_seconds // 60,
        )

    common_inner_times = set.intersection(
        *(set(predictions) for predictions in inner_predictions.values())
    ) if inner_predictions else set()
    primary_labels = {
        sample.event_time: sample.label
        for sample in primary_outer.train
    }
    meta_train_times = sorted(common_inner_times & set(primary_labels))
    if len(meta_train_times) < 80:
        return None

    meta_x = [
        [inner_predictions[300][t], inner_predictions[900][t], inner_predictions[1800][t]]
        for t in meta_train_times
    ]
    meta_y = [primary_labels[t] for t in meta_train_times]
    meta_weights = _fit_meta(meta_x, meta_y)

    outer_predictions: dict[int, dict[datetime, float]] = {}
    for horizon_seconds, _ in HORIZONS:
        outer_predictions[horizon_seconds] = _outer_model_predictions(
            horizon_samples_by_seconds[horizon_seconds],
            outer_train_times,
            outer_test,
            feature_map,
            horizon_minutes=horizon_seconds // 60,
        )

    common_test_times = set.intersection(
        *(set(predictions) for predictions in outer_predictions.values())
    ) if outer_predictions else set()
    test_rows = [
        sample for sample in outer_test
        if sample.event_time in common_test_times
    ]
    if len(test_rows) < 10:
        return None

    probabilities = [
        _meta_predict(
            meta_weights,
            [
                outer_predictions[300][sample.event_time],
                outer_predictions[900][sample.event_time],
                outer_predictions[1800][sample.event_time],
            ],
        )
        for sample in test_rows
    ]
    labels = [sample.label for sample in test_rows]

    baseline_rate = sum(meta_y) / len(meta_y)
    baseline = [baseline_rate] * len(labels)
    specialist_300 = [
        outer_predictions[300][sample.event_time]
        for sample in test_rows
    ]

    return {
        "fold": primary_outer.fold,
        "meta_train_count": len(meta_y),
        "test_count": len(labels),
        "meta_train_positive_rate": baseline_rate,
        "metrics": _metrics(probabilities, labels, baseline),
        "specialist_300_metrics": _metrics(specialist_300, labels, baseline),
        "probabilities": probabilities,
        "specialist_300_probabilities": specialist_300,
        "labels": labels,
    }


def validate_multi_horizon_meta(
    rows: list[dict[str, Any]],
    *,
    symbol: str | None = None,
    bootstrap_seed: int = 20260924,
) -> dict[str, Any]:
    feature_map = _feature_map(rows)
    horizon_samples_by_seconds = {
        horizon_seconds: _target_samples(
            rows,
            horizon_bars=horizon_bars,
            feature_map=feature_map,
        )
        for horizon_seconds, horizon_bars in HORIZONS
    }
    primary_samples = horizon_samples_by_seconds[PRIMARY_HORIZON_SECONDS]
    if len(primary_samples) < TRAIN_SIZE + TEST_SIZE:
        return {
            "status": "INSUFFICIENT_DATA",
            "validated": False,
            "validation_type": "nested_walk_forward_purged_embargoed",
            "symbol": symbol,
            "sample_counts": {
                str(seconds): len(samples)
                for seconds, samples in horizon_samples_by_seconds.items()
            },
        }

    outer_folds = _folds_for(
        primary_samples,
        purge_minutes=OUTER_PURGE_MINUTES,
        embargo_minutes=OUTER_EMBARGO_MINUTES,
    )

    fold_results = []
    for fold in outer_folds:
        result = _nested_outer_fold(
            fold,
            horizon_samples_by_seconds,
            feature_map,
        )
        if result is not None:
            fold_results.append(result)

    if not fold_results:
        return {
            "status": "NO_USABLE_FOLDS",
            "validated": False,
            "validation_type": "nested_walk_forward_purged_embargoed",
            "symbol": symbol,
            "outer_fold_count": len(outer_folds),
            "usable_fold_count": 0,
        }

    probabilities = [
        probability
        for fold in fold_results
        for probability in fold["probabilities"]
    ]
    specialist_300 = [
        probability
        for fold in fold_results
        for probability in fold["specialist_300_probabilities"]
    ]
    labels = [
        label
        for fold in fold_results
        for label in fold["labels"]
    ]
    baselines = []
    for fold in fold_results:
        baselines.extend(
            [fold["meta_train_positive_rate"]] * fold["test_count"]
        )

    metrics = _metrics(probabilities, labels, baselines)
    specialist_metrics = _metrics(specialist_300, labels, baselines)
    bootstrap = _paired_block_bootstrap(
        probabilities,
        labels,
        baselines,
        seed=bootstrap_seed,
    )

    improvement_over_300 = specialist_metrics["brier"] - metrics["brier"]
    gate_reasons = []

    if len(fold_results) < MIN_OUTER_FOLDS:
        gate_reasons.append("INSUFFICIENT_OUTER_FOLDS")
    if len(labels) < MIN_OOS_COUNT:
        gate_reasons.append("INSUFFICIENT_OOS_COUNT")
    if metrics["brier_skill"] is None or metrics["brier_skill"] <= 0:
        gate_reasons.append("NO_POSITIVE_BRIER_SKILL")
    if bootstrap["delta_brier_ci_low"] is None or bootstrap["delta_brier_ci_low"] <= 0:
        gate_reasons.append("BLOCK_BOOTSTRAP_CI_INCLUDES_ZERO")
    if metrics["log_loss_delta"] is None or metrics["log_loss_delta"] <= 0:
        gate_reasons.append("NO_LOGLOSS_IMPROVEMENT")
    if metrics["ece"] is None or metrics["ece"] > 0.05:
        gate_reasons.append("ECE_ABOVE_0_05")
    if improvement_over_300 <= 0:
        gate_reasons.append("NO_IMPROVEMENT_VS_PRIMARY_300S")

    validated = not gate_reasons

    return {
        "status": "VALIDATED" if validated else "RESEARCH_PASS_PENDING_GATE",
        "validated": validated,
        "validation_type": "nested_walk_forward_purged_embargoed",
        "symbol": symbol,
        "target_horizon_seconds": PRIMARY_HORIZON_SECONDS,
        "sample_counts": {
            str(seconds): len(samples)
            for seconds, samples in horizon_samples_by_seconds.items()
        },
        "outer_fold_count": len(outer_folds),
        "usable_fold_count": len(fold_results),
        "oos_count": len(labels),
        "metrics": metrics,
        "primary_300s_metrics": specialist_metrics,
        "delta_brier_vs_300s": improvement_over_300,
        "bootstrap": bootstrap,
        "folds": [
            {
                "fold": fold["fold"],
                "meta_train_count": fold["meta_train_count"],
                "test_count": fold["test_count"],
                "metrics": fold["metrics"],
                "primary_300s_metrics": fold["specialist_300_metrics"],
            }
            for fold in fold_results
        ],
        "gate": {
            "minimum_outer_folds": MIN_OUTER_FOLDS,
            "minimum_oos_count": MIN_OOS_COUNT,
            "minimum_ece": 0.05,
            "bootstrap": "paired moving-block bootstrap",
            "block_length": BLOCK_LENGTH,
            "bootstrap_iterations": BOOTSTRAPS,
            "criteria": [
                "positive Brier skill vs expanding-rate baseline",
                "95% block-bootstrap CI lower bound > 0",
                "positive log-loss improvement",
                "ECE <= 0.05",
                "strict improvement vs standalone 300s specialist",
            ],
            "reasons": gate_reasons,
        },
        "selection": {
            "pre_specified_trials": 1,
            "multiple_testing_adjustment": "not required for a single pre-specified candidate",
        },
        "production_eligible": False,
        "note": "Research validation only. Even a VALIDATED result does not modify the production probability or trigger.",
    }
