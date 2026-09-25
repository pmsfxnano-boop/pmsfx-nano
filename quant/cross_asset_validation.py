"""PMSF-X Nano — professional cross-asset lead/lag validation.

Research-only validator for the cross-asset specialist. It uses a purged and
embargoed walk-forward, aggregates truly OOS predictions, and gates the
specialist on pre-specified metrics plus a paired moving-block bootstrap.
It never modifies the production probability or trigger.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from typing import Any

from quant.cross_asset import (
    LABEL_THRESHOLD_BPS,
    MIN_TRAIN_ROWS,
    _build_samples,
    _fetch_rows,
    _fit,
    _normalize_rows,
    _looks_like_five_minute_series,
    _predict,
    PEERS,
)
from quant.temporal import walk_forward_splits

TRAIN_SIZE = 300
TEST_SIZE = 60
PURGE_MINUTES = 5
EMBARGO_MINUTES = 5
MIN_FOLDS = 5
MIN_OOS_COUNT = 300
BOOTSTRAPS = 1000
BLOCK_LENGTH = 10
MAX_ECE = 0.05
MIN_ACCURACY = 0.55
MIN_FOLD_IMPROVEMENT_FRACTION = 2 / 3


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _logloss(probability: float, label: int) -> float:
    p = _clamp(probability, 1e-6, 1.0 - 1e-6)
    return -(label * math.log(p) + (1 - label) * math.log(1.0 - p))


def _ece(probabilities: list[float], labels: list[int], bins: int = 10) -> float | None:
    if not labels:
        return None
    total = len(labels)
    value = 0.0
    for bucket in range(bins):
        lo = bucket / bins
        hi = (bucket + 1) / bins
        indices = [
            i for i, p in enumerate(probabilities)
            if (lo <= p < hi) or (bucket == bins - 1 and p <= hi)
        ]
        if not indices:
            continue
        mean_p = sum(probabilities[i] for i in indices) / len(indices)
        mean_y = sum(labels[i] for i in indices) / len(indices)
        value += len(indices) / total * abs(mean_p - mean_y)
    return value


def _metrics(probabilities: list[float], labels: list[int], baseline: list[float]) -> dict[str, Any]:
    n = len(labels)
    if not n:
        return {
            "test_count": 0,
            "accuracy": None,
            "brier": None,
            "baseline_brier": None,
            "brier_skill": None,
            "log_loss": None,
            "baseline_log_loss": None,
            "log_loss_delta": None,
            "ece": None,
        }

    brier = sum((p - y) ** 2 for p, y in zip(probabilities, labels)) / n
    baseline_brier = sum((p - y) ** 2 for p, y in zip(baseline, labels)) / n
    loss = sum(_logloss(p, y) for p, y in zip(probabilities, labels)) / n
    baseline_loss = sum(_logloss(p, y) for p, y in zip(baseline, labels)) / n
    accuracy = sum(
        (p >= 0.5) == bool(y)
        for p, y in zip(probabilities, labels)
    ) / n
    return {
        "test_count": n,
        "accuracy": accuracy,
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_skill": 1.0 - brier / baseline_brier if baseline_brier > 0 else None,
        "log_loss": loss,
        "baseline_log_loss": baseline_loss,
        "log_loss_delta": baseline_loss - loss,
        "ece": _ece(probabilities, labels),
    }


def _block_bootstrap_delta(
    model: list[float],
    baseline: list[float],
    labels: list[int],
    *,
    seed: int,
) -> dict[str, Any]:
    n = len(labels)
    if n < BLOCK_LENGTH:
        return {
            "iterations": 0,
            "delta_brier_ci_low": None,
            "delta_brier_ci_high": None,
        }

    rng = random.Random(seed)
    deltas: list[float] = []
    block_count = math.ceil(n / BLOCK_LENGTH)
    starts = list(range(n))

    for _ in range(BOOTSTRAPS):
        indices: list[int] = []
        for _ in range(block_count):
            start = rng.choice(starts)
            indices.extend(
                (start + offset) % n
                for offset in range(BLOCK_LENGTH)
            )
        indices = indices[:n]
        model_brier = sum(
            (model[i] - labels[i]) ** 2
            for i in indices
        ) / n
        baseline_brier = sum(
            (baseline[i] - labels[i]) ** 2
            for i in indices
        ) / n
        deltas.append(baseline_brier - model_brier)

    deltas.sort()
    return {
        "iterations": BOOTSTRAPS,
        "delta_brier_ci_low": deltas[int(0.025 * len(deltas))],
        "delta_brier_ci_high": deltas[int(0.975 * len(deltas)) - 1],
    }


def _folds(temporal_samples):
    return walk_forward_splits(
        temporal_samples,
        train_size=TRAIN_SIZE,
        test_size=TEST_SIZE,
        purge=timedelta(minutes=PURGE_MINUTES),
        embargo=timedelta(minutes=EMBARGO_MINUTES),
    )


def validate_cross_asset(
    target_rows: list[dict[str, Any]],
    peer_rows: dict[str, list[dict[str, Any]]],
    *,
    symbol: str,
    bootstrap_seed: int = 20260925,
) -> dict[str, Any]:
    target = _normalize_rows(target_rows)
    peers = {
        peer: _normalize_rows(rows)
        for peer, rows in peer_rows.items()
    }
    if symbol not in PEERS:
        return {
            "status": "NO_PEER_GRAPH",
            "validated": False,
            "production_eligible": False,
            "symbol": symbol,
            "validation_type": "cross_asset_walk_forward_purged_embargoed",
        }

    samples, temporal_samples, _, _ = _build_samples(target, peers)
    if len(samples) < TRAIN_SIZE + TEST_SIZE:
        return {
            "status": "INSUFFICIENT_DATA",
            "validated": False,
            "production_eligible": False,
            "symbol": symbol,
            "validation_type": "cross_asset_walk_forward_purged_embargoed",
            "sample_count": len(samples),
            "minimum_required": TRAIN_SIZE + TEST_SIZE,
        }

    folds = _folds(temporal_samples)
    fold_results: list[dict[str, Any]] = []
    all_probabilities: list[float] = []
    all_labels: list[int] = []
    all_baselines: list[float] = []
    fold_deltas: list[float] = []

    for fold in folds:
        train = list(fold.train)
        test = list(fold.test)
        if len(train) < MIN_TRAIN_ROWS:
            continue
        if len({sample.label for sample in train}) < 2:
            continue
        if len({sample.label for sample in test}) < 2:
            continue

        fit_rows = [(list(sample.features), sample.label) for sample in train]
        weights = _fit(fit_rows)
        probabilities = [
            _predict(weights, list(sample.features))
            for sample in test
        ]
        labels = [sample.label for sample in test]
        baseline_rate = sum(sample.label for sample in train) / len(train)
        baseline = [baseline_rate] * len(labels)
        metrics = _metrics(probabilities, labels, baseline)
        fold_delta = metrics["baseline_brier"] - metrics["brier"]

        fold_results.append(
            {
                "fold": fold.fold,
                "train_count": len(train),
                "test_count": len(test),
                "train_positive_rate": baseline_rate,
                "metrics": metrics,
                "delta_brier_vs_baseline": fold_delta,
            }
        )
        all_probabilities.extend(probabilities)
        all_labels.extend(labels)
        all_baselines.extend(baseline)
        fold_deltas.append(fold_delta)

    if not fold_results:
        return {
            "status": "NO_USABLE_FOLDS",
            "validated": False,
            "production_eligible": False,
            "symbol": symbol,
            "validation_type": "cross_asset_walk_forward_purged_embargoed",
            "fold_count": len(folds),
            "usable_fold_count": 0,
        }

    metrics = _metrics(all_probabilities, all_labels, all_baselines)
    bootstrap = _block_bootstrap_delta(
        all_probabilities,
        all_baselines,
        all_labels,
        seed=bootstrap_seed,
    )
    positive_fold_count = sum(delta > 0 for delta in fold_deltas)
    positive_fold_fraction = positive_fold_count / len(fold_deltas)

    gate_reasons: list[str] = []
    if len(fold_results) < MIN_FOLDS:
        gate_reasons.append("INSUFFICIENT_FOLDS")
    if len(all_labels) < MIN_OOS_COUNT:
        gate_reasons.append("INSUFFICIENT_OOS_COUNT")
    if metrics["accuracy"] is None or metrics["accuracy"] < MIN_ACCURACY:
        gate_reasons.append("ACCURACY_BELOW_0_55")
    if metrics["brier_skill"] is None or metrics["brier_skill"] <= 0:
        gate_reasons.append("NO_POSITIVE_BRIER_SKILL")
    if (
        bootstrap["delta_brier_ci_low"] is None
        or bootstrap["delta_brier_ci_low"] <= 0
    ):
        gate_reasons.append("BOOTSTRAP_CI_INCLUDES_ZERO")
    if metrics["log_loss_delta"] is None or metrics["log_loss_delta"] <= 0:
        gate_reasons.append("NO_LOGLOSS_IMPROVEMENT")
    if metrics["ece"] is None or metrics["ece"] > MAX_ECE:
        gate_reasons.append("ECE_ABOVE_0_05")
    if positive_fold_fraction < MIN_FOLD_IMPROVEMENT_FRACTION:
        gate_reasons.append("FOLD_IMPROVEMENT_INCONSISTENT")

    return {
        "status": "VALIDATED" if not gate_reasons else "RESEARCH_PASS_PENDING_GATE",
        "validated": not gate_reasons,
        "validation_type": "cross_asset_walk_forward_purged_embargoed",
        "symbol": symbol,
        "peer_graph": list(PEERS[symbol]),
        "sample_count": len(samples),
        "fold_count": len(folds),
        "usable_fold_count": len(fold_results),
        "oos_count": len(all_labels),
        "metrics": metrics,
        "bootstrap": bootstrap,
        "fold_consistency": {
            "positive_delta_fold_count": positive_fold_count,
            "positive_delta_fraction": positive_fold_fraction,
            "minimum_fraction": MIN_FOLD_IMPROVEMENT_FRACTION,
        },
        "gate": {
            "minimum_folds": MIN_FOLDS,
            "minimum_oos_count": MIN_OOS_COUNT,
            "minimum_accuracy": MIN_ACCURACY,
            "maximum_ece": MAX_ECE,
            "bootstrap": "paired moving-block bootstrap",
            "block_length": BLOCK_LENGTH,
            "bootstrap_iterations": BOOTSTRAPS,
            "criteria": [
                "accuracy >= 0.55",
                "positive Brier skill vs fold-local expanding-rate baseline",
                "95% block-bootstrap CI lower bound > 0",
                "positive log-loss improvement",
                "ECE <= 0.05",
                ">= 2/3 folds with positive Brier improvement",
            ],
            "reasons": gate_reasons,
        },
        "production_eligible": False,
        "note": "Standalone cross-asset research validation only.",
    }


def validate_cross_asset_symbol(
    symbol: str,
    token: str,
    target_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol = symbol.upper()
    peer_symbols = PEERS.get(symbol, ())
    if target_rows is None or not _looks_like_five_minute_series(target_rows):
        target_rows = _fetch_rows(symbol, token)

    peer_rows = {
        peer: _fetch_rows(peer, token)
        for peer in peer_symbols
    }

    return validate_cross_asset(
        target_rows,
        peer_rows,
        symbol=symbol,
    )
