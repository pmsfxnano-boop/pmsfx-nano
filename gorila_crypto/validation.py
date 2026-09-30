"""Reproducible OOS and economic validation for the Crypto cleanroom.

This module is research-only. It fits each fold using training observations only,
applies purging/embargo around the OOS boundary, evaluates probabilistic metrics
against fixed baselines, and evaluates a fixed non-optimized economic policy with
explicit costs/slippage. It never marks a model as production-admissible.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

import numpy as np

from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .research_gates import combinatorial_pbo, deflated_sharpe_p_value, holm_bonferroni
from .forecast import (
    DetectionFeatureSnapshot,
    ForecastTargetSpec,
    _log_return_bps,
    build_detection_features,
)
from .lead_lag import LeadLagConfig, PricePoint, build_price_points, detect_leader_impulses


def _dt(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sigmoid_np(z: np.ndarray) -> np.ndarray:
    clipped = np.clip(z, -60.0, 60.0)
    return 1.0 / (1.0 + np.exp(-clipped))


@dataclass(frozen=True)
class ForecastLabel:
    realized_target: int
    realized_signed_return_bps: float
    baseline_target_price: float
    future_target_price: float
    label_event_time: datetime
    label_received_time: datetime
    label_event_id: str
    horizon_ms: int
    actual_event_horizon_ms: int | None = None
    actual_receive_horizon_ms: int | None = None


@dataclass(frozen=True)
class ForecastDatasetRow:
    snapshot: DetectionFeatureSnapshot
    label: ForecastLabel


@dataclass(frozen=True)
class WalkForwardConfig:
    min_train_rows: int = 200
    test_rows: int = 50
    step_rows: int = 50
    purge_ms: int = 5000
    embargo_ms: int = 5000
    min_group_rows: int = 20
    min_fold_pass_fraction: float = 0.67
    temporal_max_logloss_rel_increase: float = 0.10
    temporal_max_brier_increase: float = 0.02
    temporal_max_net_drop_bps: float = 2.0
    ridge_alpha: float = 1.0
    max_iterations: int = 100
    convergence_tol: float = 1e-8
    seed: int = 20260929

    def validate(self) -> None:
        if self.min_train_rows < 1:
            raise ValueError("min_train_rows must be positive")
        if self.test_rows < 1:
            raise ValueError("test_rows must be positive")
        if self.step_rows < self.test_rows:
            raise ValueError("step_rows must be >= test_rows to avoid overlapping OOS windows")
        if self.purge_ms < 0 or self.embargo_ms < 0:
            raise ValueError("purge/embargo must be non-negative")
        if self.min_group_rows < 1:
            raise ValueError("min_group_rows must be positive")
        if not 0.5 <= self.min_fold_pass_fraction <= 1.0:
            raise ValueError("min_fold_pass_fraction must be in [0.5,1]")
        if self.temporal_max_logloss_rel_increase < 0:
            raise ValueError("temporal_max_logloss_rel_increase must be non-negative")
        if self.temporal_max_brier_increase < 0:
            raise ValueError("temporal_max_brier_increase must be non-negative")
        if self.temporal_max_net_drop_bps < 0:
            raise ValueError("temporal_max_net_drop_bps must be non-negative")
        if self.ridge_alpha < 0:
            raise ValueError("ridge_alpha must be non-negative")
        if self.max_iterations < 1:
            raise ValueError("max_iterations must be positive")
        if self.convergence_tol <= 0:
            raise ValueError("convergence_tol must be positive")


@dataclass(frozen=True)
class WalkForwardFold:
    fold_id: int
    train_indices: tuple[int, ...]
    test_indices: tuple[int, ...]
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


@dataclass(frozen=True)
class FittedValidationModel:
    model_id: str
    version: str
    feature_names: tuple[str, ...]
    coefficients: Mapping[str, float]
    intercept: float
    training_rows: int
    training_positive_rate: float
    spec_hash: str


@dataclass(frozen=True)
class ProbabilisticMetrics:
    n: int
    positive_rate: float
    brier: float
    log_loss: float
    auc: float | None
    ece_10: float


@dataclass(frozen=True)
class EconomicPolicySpec:
    long_threshold: float = 0.55
    short_threshold: float = 0.45
    round_trip_cost_bps: float = 1.0
    round_trip_slippage_bps: float = 1.0

    def validate(self) -> None:
        if not 0.0 <= self.short_threshold < self.long_threshold <= 1.0:
            raise ValueError("short_threshold must be below long_threshold in [0,1]")
        if self.round_trip_cost_bps < 0 or self.round_trip_slippage_bps < 0:
            raise ValueError("costs/slippage must be non-negative")


@dataclass(frozen=True)
class EconomicMetrics:
    n: int
    traded_fraction: float
    gross_mean_bps: float
    total_cost_bps: float
    total_slippage_bps: float
    net_mean_bps: float
    cumulative_net_bps: float


@dataclass(frozen=True)
class StressScenario:
    name: str
    return_haircut: float = 0.0
    cost_multiplier: float = 1.0
    slippage_multiplier: float = 1.0

    def validate(self) -> None:
        if not 0.0 <= self.return_haircut < 1.0:
            raise ValueError("return haircut must be in [0,1)")
        if self.cost_multiplier < 0 or self.slippage_multiplier < 0:
            raise ValueError("stress multipliers must be non-negative")


@dataclass(frozen=True)
class FoldEvaluation:
    fold: WalkForwardFold
    model: FittedValidationModel
    probabilistic: ProbabilisticMetrics
    baseline_fifty: ProbabilisticMetrics
    baseline_prevalence: ProbabilisticMetrics
    economic: EconomicMetrics


@dataclass(frozen=True)
class TemporalStabilityMetrics:
    early_fold_count: int
    late_fold_count: int
    early_log_loss: float
    late_log_loss: float
    early_brier: float
    late_brier: float
    early_net_mean_bps: float
    late_net_mean_bps: float
    early_baseline_pass_fraction: float
    late_baseline_pass_fraction: float
    early_economic_positive_fraction: float
    late_economic_positive_fraction: float
    log_loss_relative_change: float
    brier_change: float
    net_drop_bps: float
    passed: bool


@dataclass(frozen=True)
class ValidationReport:
    status: str
    dataset_rows: int
    folds: tuple[FoldEvaluation, ...]
    fold_baseline_pass_fraction: float
    fold_economic_positive_fraction: float
    temporal_stability: TemporalStabilityMetrics
    oos_probabilities: tuple[float, ...]
    oos_labels: tuple[int, ...]
    oos_returns_bps: tuple[float, ...]
    placebo_p_value: float | None
    placebo_iterations: int
    stability_by_symbol: Mapping[str, ProbabilisticMetrics]
    stability_by_horizon_ms: Mapping[int, ProbabilisticMetrics]
    stress_results: Mapping[str, EconomicMetrics]
    multiple_testing_p_value: float | None
    dsr_p_value: float | None
    pbo: float | None
    research_robustness_status: str
    research_robustness_reasons: tuple[str, ...]
    promotion_eligible: bool


def label_snapshot(
    target: Sequence[PricePoint],
    snapshot: DetectionFeatureSnapshot,
    target_spec: ForecastTargetSpec,
) -> ForecastLabel | None:
    target_spec.validate()
    baseline = None
    for point in target:
        if point.event_time <= snapshot.decision_event_time and point.received_time <= snapshot.decision_received_time:
            baseline = point
    if baseline is None:
        return None

    # Conservative causal horizon: the target must occur after BOTH
    # event-time and receive-time horizon cutoffs. We do not credit any move
    # that was available before the decision, and we persist the actual realized
    # horizon so downstream quality gates can measure drift explicitly.
    future_cutoff = max(
        snapshot.decision_event_time + timedelta(milliseconds=target_spec.horizon_ms),
        snapshot.decision_received_time + timedelta(milliseconds=target_spec.horizon_ms),
    )
    future = None
    for point in target:
        if point.event_time < future_cutoff:
            continue
        if point.received_time < snapshot.decision_received_time:
            continue
        future = point
        break
    if future is None:
        return None

    raw_return = _log_return_bps(future.price, baseline.price)
    direction = 1.0 if snapshot.feature_values["leader_direction"] >= 0 else -1.0
    signed_return = direction * raw_return
    return ForecastLabel(
        realized_target=1 if signed_return > 0 else 0,
        realized_signed_return_bps=float(signed_return),
        baseline_target_price=float(baseline.price),
        future_target_price=float(future.price),
        label_event_time=_dt(future.event_time),
        label_received_time=_dt(future.received_time),
        label_event_id=future.event_id,
        horizon_ms=target_spec.horizon_ms,
        actual_event_horizon_ms=int(round((future.event_time - snapshot.decision_event_time).total_seconds() * 1000.0)),
        actual_receive_horizon_ms=int(round((future.received_time - snapshot.decision_received_time).total_seconds() * 1000.0)),
    )


def build_forecast_dataset(
    rows: Iterable[Mapping[str, object]],
    leader_symbol: str,
    target_symbol: str,
    lead_lag_config: LeadLagConfig,
    target_spec: ForecastTargetSpec,
) -> tuple[ForecastDatasetRow, ...]:
    points = build_price_points(rows)
    leader = points.get(leader_symbol.upper()) or []
    target = points.get(target_symbol.upper()) or []
    if not leader or not target:
        return ()

    dataset: list[ForecastDatasetRow] = []
    seen_trigger_ids: set[str] = set()
    for trigger, leader_return in detect_leader_impulses(leader, lead_lag_config):
        if trigger.event_id in seen_trigger_ids:
            continue
        seen_trigger_ids.add(trigger.event_id)
        try:
            snapshot = build_detection_features(
                target,
                trigger,
                leader_return,
                lead_lag_config,
            )
        except ValueError:
            continue
        label = label_snapshot(target, snapshot, target_spec)
        if label is None:
            continue
        dataset.append(ForecastDatasetRow(snapshot=snapshot, label=label))

    dataset.sort(
        key=lambda row: (
            row.snapshot.decision_received_time,
            row.snapshot.decision_event_time,
            row.snapshot.leader_event_id,
        )
    )
    return tuple(dataset)


def make_walk_forward_folds(
    dataset: Sequence[ForecastDatasetRow],
    config: WalkForwardConfig,
) -> tuple[WalkForwardFold, ...]:
    config.validate()
    rows = list(dataset)
    if rows != sorted(
        rows,
        key=lambda row: (
            row.snapshot.decision_received_time,
            row.snapshot.decision_event_time,
            row.snapshot.leader_event_id,
        ),
    ):
        raise ValueError("dataset must be sorted by decision receive time")

    folds: list[WalkForwardFold] = []
    fold_id = 0
    test_start_index = config.min_train_rows
    while test_start_index + config.test_rows <= len(rows):
        test_rows = rows[test_start_index : test_start_index + config.test_rows]
        test_start = test_rows[0].snapshot.decision_received_time
        test_end = test_rows[-1].snapshot.decision_received_time
        train_cutoff = test_start - timedelta(milliseconds=config.embargo_ms)
        label_cutoff = test_start - timedelta(milliseconds=config.purge_ms)

        train_indices = tuple(
            idx
            for idx in range(test_start_index)
            if rows[idx].snapshot.decision_received_time < train_cutoff
            and rows[idx].label.label_received_time < label_cutoff
        )
        if len(train_indices) < config.min_train_rows:
            test_start_index += config.step_rows
            continue

        test_indices = tuple(
            range(test_start_index, test_start_index + config.test_rows)
        )
        folds.append(
            WalkForwardFold(
                fold_id=fold_id,
                train_indices=train_indices,
                test_indices=test_indices,
                train_start=rows[train_indices[0]].snapshot.decision_received_time,
                train_end=rows[train_indices[-1]].snapshot.decision_received_time,
                test_start=test_start,
                test_end=test_end,
            )
        )
        fold_id += 1
        test_start_index += config.step_rows
    return tuple(folds)


def _matrix(
    rows: Sequence[ForecastDatasetRow],
    indices: Sequence[int],
    feature_names: Sequence[str],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    X = np.asarray(
        [[float(rows[i].snapshot.feature_values[name]) for name in feature_names] for i in indices],
        dtype=float,
    )
    y = np.asarray([rows[i].label.realized_target for i in indices], dtype=float)
    means = np.mean(X, axis=0)
    scales = np.std(X, axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    Xs = (X - means) / scales
    return Xs, y, means, scales


def fit_ridge_logistic(
    rows: Sequence[ForecastDatasetRow],
    indices: Sequence[int],
    feature_names: Sequence[str],
    config: WalkForwardConfig,
    *,
    model_id: str,
    version: str,
) -> FittedValidationModel:
    if not indices:
        raise ValueError("training set cannot be empty")
    Xs, y, means, scales = _matrix(rows, indices, feature_names)
    if len(np.unique(y)) < 2:
        raise ValueError("training set must contain both classes")

    X = np.column_stack([np.ones(len(Xs)), Xs])
    w = np.zeros(X.shape[1], dtype=float)
    n = len(y)
    penalty = np.diag(np.concatenate([[0.0], np.full(len(feature_names), config.ridge_alpha)]))
    for _ in range(config.max_iterations):
        p = _sigmoid_np(X @ w)