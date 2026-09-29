"""Reproducible OOS and economic validation for the Crypto cleanroom.

This module is research-only. It fits each fold using training observations only,
applies purging/embargo around the OOS boundary, evaluates probabilistic metrics
against fixed baselines, and evaluates a fixed non-optimized economic policy with
explicit costs/slippage. It never marks a model as production-admissible.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence

import numpy as np

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


@dataclass(frozen=True)
class ForecastDatasetRow:
    snapshot: DetectionFeatureSnapshot
    label: ForecastLabel


@dataclass(frozen=True)
class WalkForwardConfig:
    min_train_rows: int = 200
    test_rows: int = 50
    step_rows: int = 50
    purge_ms: int = 1000
    embargo_ms: int = 1000
    min_group_rows: int = 20
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
    round_trip_cost_bps: float = 0.0
    round_trip_slippage_bps: float = 0.0

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
class ValidationReport:
    status: str
    dataset_rows: int
    folds: tuple[FoldEvaluation, ...]
    oos_probabilities: tuple[float, ...]
    oos_labels: tuple[int, ...]
    oos_returns_bps: tuple[float, ...]
    placebo_p_value: float | None
    placebo_iterations: int
    stability_by_symbol: Mapping[str, ProbabilisticMetrics]
    stability_by_horizon_ms: Mapping[int, ProbabilisticMetrics]
    stress_results: Mapping[str, EconomicMetrics]
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

    future_cutoff = snapshot.decision_event_time + timedelta(milliseconds=target_spec.horizon_ms)
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
        gradient = (X.T @ (p - y)) / n + penalty @ w / n
        weights = p * (1.0 - p)
        hessian = (X.T * weights) @ X / n + penalty / n
        try:
            step = np.linalg.solve(hessian + np.eye(hessian.shape[0]) * 1e-9, gradient)
        except np.linalg.LinAlgError:
            step = np.linalg.lstsq(
                hessian + np.eye(hessian.shape[0]) * 1e-9,
                gradient,
                rcond=None,
            )[0]
        next_w = w - step
        if float(np.max(np.abs(next_w - w))) <= config.convergence_tol:
            w = next_w
            break
        w = next_w

    std_coefficients = w[1:]
    raw_coefficients = {
        name: float(std_coefficients[i] / scales[i])
        for i, name in enumerate(feature_names)
    }
    raw_intercept = float(w[0] - np.sum(std_coefficients * means / scales))
    spec = {
        "model_id": model_id,
        "version": version,
        "intercept": raw_intercept,
        "coefficients": raw_coefficients,
    }
    spec_hash = hashlib.sha256(
        repr(sorted(spec.items(), key=lambda item: item[0])).encode("utf-8")
    ).hexdigest()
    return FittedValidationModel(
        model_id=model_id,
        version=version,
        feature_names=tuple(feature_names),
        coefficients=raw_coefficients,
        intercept=raw_intercept,
        training_rows=len(indices),
        training_positive_rate=float(np.mean(y)),
        spec_hash=spec_hash,
    )


def predict_probability(
    model: FittedValidationModel,
    snapshot: DetectionFeatureSnapshot,
) -> float:
    linear = model.intercept
    for name in model.feature_names:
        linear += model.coefficients[name] * float(snapshot.feature_values[name])
    clipped = min(60.0, max(-60.0, linear))
    return float(1.0 / (1.0 + math.exp(-clipped)))


def _rank_auc(labels: Sequence[int], probabilities: Sequence[float]) -> float | None:
    n = len(labels)
    positives = sum(1 for y in labels if y == 1)
    negatives = n - positives
    if positives == 0 or negatives == 0:
        return None
    order = sorted(range(n), key=lambda i: (probabilities[i], i))
    rank_sum = 0.0
    i = 0
    rank = 1
    while i < n:
        j = i + 1
        while j < n and probabilities[order[j]] == probabilities[order[i]]:
            j += 1
        avg_rank = (rank + rank + (j - i) - 1) / 2.0
        for k in range(i, j):
            if labels[order[k]] == 1:
                rank_sum += avg_rank
        rank += j - i
        i = j
    return float((rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives))


def probabilistic_metrics(labels: Sequence[int], probabilities: Sequence[float]) -> ProbabilisticMetrics:
    if len(labels) != len(probabilities) or not labels:
        raise ValueError("labels and probabilities must have equal non-zero length")
    eps = 1e-15
    probs = [min(1.0 - eps, max(eps, float(p))) for p in probabilities]
    brier = sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)
    log_loss = -sum(
        y * math.log(p) + (1 - y) * math.log(1 - p)
        for y, p in zip(labels, probs)
    ) / len(labels)

    bins = [[] for _ in range(10)]
    for y, p in zip(labels, probs):
        idx = min(9, int(p * 10.0))
        bins[idx].append((y, p))
    ece = 0.0
    for bucket in bins:
        if not bucket:
            continue
        ece += len(bucket) / len(labels) * abs(
            sum(y for y, _ in bucket) / len(bucket)
            - sum(p for _, p in bucket) / len(bucket)
        )
    return ProbabilisticMetrics(
        n=len(labels),
        positive_rate=float(sum(labels) / len(labels)),
        brier=float(brier),
        log_loss=float(log_loss),
        auc=_rank_auc(labels, probs),
        ece_10=float(ece),
    )


def economic_metrics(
    rows: Sequence[ForecastDatasetRow],
    probabilities: Sequence[float],
    policy: EconomicPolicySpec,
    *,
    return_haircut: float = 0.0,
    cost_multiplier: float = 1.0,
    slippage_multiplier: float = 1.0,
) -> EconomicMetrics:
    policy.validate()
    if len(rows) != len(probabilities) or not rows:
        raise ValueError("rows and probabilities must have equal non-zero length")
    if not 0.0 <= return_haircut < 1.0:
        raise ValueError("return haircut must be in [0,1)")
    if cost_multiplier < 0 or slippage_multiplier < 0:
        raise ValueError("stress multipliers must be non-negative")

    net_values: list[float] = []
    gross_values: list[float] = []
    traded = 0
    for row, probability in zip(rows, probabilities):
        if probability >= policy.long_threshold:
            action = 1
        elif probability <= policy.short_threshold:
            action = -1
        else:
            action = 0
        if action == 0:
            gross_values.append(0.0)
            net_values.append(0.0)
            continue
        signed = float(row.label.realized_signed_return_bps) * action
        signed *= 1.0 - return_haircut
        gross_values.append(signed)
        traded += 1
        cost = policy.round_trip_cost_bps * cost_multiplier
        slippage = policy.round_trip_slippage_bps * slippage_multiplier
        net_values.append(signed - cost - slippage)

    total_cost = traded * policy.round_trip_cost_bps * cost_multiplier
    total_slippage = traded * policy.round_trip_slippage_bps * slippage_multiplier
    return EconomicMetrics(
        n=len(rows),
        traded_fraction=float(traded / len(rows)),
        gross_mean_bps=float(sum(gross_values) / len(rows)),
        total_cost_bps=float(total_cost),
        total_slippage_bps=float(total_slippage),
        net_mean_bps=float(sum(net_values) / len(rows)),
        cumulative_net_bps=float(sum(net_values)),
    )


def baseline_constant(labels: Sequence[int], value: float) -> ProbabilisticMetrics:
    return probabilistic_metrics(labels, [value] * len(labels))


def block_permutation(labels: Sequence[int], block_size: int, seed: int) -> tuple[int, ...]:
    if block_size < 1:
        raise ValueError("block_size must be positive")
    rng = np.random.default_rng(seed)
    output = []
    labels = list(labels)
    for start in range(0, len(labels), block_size):
        block = labels[start : start + block_size]
        output.extend(rng.permutation(block).tolist())
    return tuple(int(x) for x in output)


def placebo_logloss_edge(
    labels: Sequence[int],
    probabilities: Sequence[float],
    *,
    block_size: int = 20,
    iterations: int = 500,
    seed: int = 20260929,
) -> tuple[float, int]:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    observed = math.log(2.0) - probabilistic_metrics(labels, probabilities).log_loss
    exceed = 0
    for iteration in range(iterations):
        placebo_labels = block_permutation(labels, block_size, seed + iteration)
        edge = math.log(2.0) - probabilistic_metrics(placebo_labels, probabilities).log_loss
        if edge >= observed:
            exceed += 1
    p_value = (1 + exceed) / (iterations + 1)
    return float(p_value), iterations


def stability_metrics(
    rows: Sequence[ForecastDatasetRow],
    probabilities: Sequence[float],
) -> tuple[dict[str, ProbabilisticMetrics], dict[int, ProbabilisticMetrics]]:
    by_symbol: dict[str, list[int]] = {}
    by_symbol_p: dict[str, list[float]] = {}
    by_horizon: dict[int, list[int]] = {}
    by_horizon_p: dict[int, list[float]] = {}
    for row, probability in zip(rows, probabilities):
        symbol = row.snapshot.target_symbol
        by_symbol.setdefault(symbol, []).append(row.label.realized_target)
        by_symbol_p.setdefault(symbol, []).append(probability)
        horizon = row.label.horizon_ms
        by_horizon.setdefault(horizon, []).append(row.label.realized_target)
        by_horizon_p.setdefault(horizon, []).append(probability)
    return (
        {
            key: probabilistic_metrics(by_symbol[key], by_symbol_p[key])
            for key in sorted(by_symbol)
        },
        {
            key: probabilistic_metrics(by_horizon[key], by_horizon_p[key])
            for key in sorted(by_horizon)
        },
    )


def run_walk_forward_validation(
    dataset: Sequence[ForecastDatasetRow],
    feature_names: Sequence[str],
    config: WalkForwardConfig,
    policy: EconomicPolicySpec,
    *,
    model_id: str = "crypto-ridge-logit-wf",
    model_version: str = "1",
    placebo_block_size: int = 20,
    placebo_iterations: int = 500,
    stress_scenarios: Sequence[StressScenario] = (),
) -> ValidationReport:
    config.validate()
    if not dataset:
        raise ValueError("validation dataset cannot be empty")
    folds = make_walk_forward_folds(dataset, config)
    if not folds:
        return ValidationReport(
            status="INSUFFICIENT_OOS_DATA",
            dataset_rows=len(dataset),
            folds=(),
            oos_probabilities=(),
            oos_labels=(),
            oos_returns_bps=(),
            placebo_p_value=None,
            placebo_iterations=0,
            stability_by_symbol={},
            stability_by_horizon_ms={},
            stress_results={},
            promotion_eligible=False,
        )

    fold_evaluations: list[FoldEvaluation] = []
    oos_probabilities: list[float] = []
    oos_labels: list[int] = []
    oos_returns: list[float] = []

    for fold in folds:
        model = fit_ridge_logistic(
            dataset,
            fold.train_indices,
            feature_names,
            config,
            model_id=model_id,
            version=f"{model_version}.fold{fold.fold_id}",
        )
        test_rows = [dataset[i] for i in fold.test_indices]
        probabilities = [predict_probability(model, row.snapshot) for row in test_rows]
        labels = [row.label.realized_target for row in test_rows]
        fold_metric = probabilistic_metrics(labels, probabilities)
        baseline_fifty = baseline_constant(labels, 0.5)
        baseline_prevalence = baseline_constant(labels, model.training_positive_rate)
        economic = economic_metrics(test_rows, probabilities, policy)

        fold_evaluations.append(
            FoldEvaluation(
                fold=fold,
                model=model,
                probabilistic=fold_metric,
                baseline_fifty=baseline_fifty,
                baseline_prevalence=baseline_prevalence,
                economic=economic,
            )
        )
        oos_probabilities.extend(probabilities)
        oos_labels.extend(labels)
        oos_returns.extend(row.label.realized_signed_return_bps for row in test_rows)

    stability_by_symbol, stability_by_horizon = stability_metrics(
        [dataset[i] for fold in folds for i in fold.test_indices],
        oos_probabilities,
    )
    placebo_p, placebo_n = placebo_logloss_edge(
        oos_labels,
        oos_probabilities,
        block_size=placebo_block_size,
        iterations=placebo_iterations,
        seed=config.seed,
    )

    stress_results = {}
    oos_rows = [dataset[i] for fold in folds for i in fold.test_indices]
    for scenario in stress_scenarios:
        scenario.validate()
        stress_results[scenario.name] = economic_metrics(
            oos_rows,
            oos_probabilities,
            policy,
            return_haircut=scenario.return_haircut,
            cost_multiplier=scenario.cost_multiplier,
            slippage_multiplier=scenario.slippage_multiplier,
        )

    aggregate = probabilistic_metrics(oos_labels, oos_probabilities)
    aggregate_baseline = baseline_constant(oos_labels, 0.5)
    aggregate_economic = economic_metrics(oos_rows, oos_probabilities, policy)
    minimum_fold_count = len(folds) >= 3
    baseline_beat = aggregate.log_loss < aggregate_baseline.log_loss and aggregate.brier < aggregate_baseline.brier
    economic_positive = aggregate_economic.net_mean_bps > 0.0
    placebo_pass = placebo_p <= 0.05
    stress_pass = all(result.net_mean_bps > 0.0 for result in stress_results.values()) if stress_results else False
    group_sample_pass = all(
        metric.n >= config.min_group_rows
        for metric in list(stability_by_symbol.values())
        + list(stability_by_horizon.values())
    )
    promotion_eligible = bool(
        minimum_fold_count
        and len(oos_labels) >= config.test_rows
        and baseline_beat
        and economic_positive
        and placebo_pass
        and stress_pass
        and group_sample_pass
    )
    return ValidationReport(
        status="OOS_EVALUATED",
        dataset_rows=len(dataset),
        folds=tuple(fold_evaluations),
        oos_probabilities=tuple(oos_probabilities),
        oos_labels=tuple(oos_labels),
        oos_returns_bps=tuple(oos_returns),
        placebo_p_value=placebo_p,
        placebo_iterations=placebo_n,
        stability_by_symbol=stability_by_symbol,
        stability_by_horizon_ms=stability_by_horizon,
        stress_results=stress_results,
        promotion_eligible=promotion_eligible,
    )


def validation_run_id(
    *,
    replay_fingerprint: str,
    model_id: str,
    model_version: str,
    target_spec: ForecastTargetSpec,
    config: WalkForwardConfig,
    policy: EconomicPolicySpec,
) -> str:
    identity = {
        "replay_fingerprint": replay_fingerprint,
        "model_id": model_id,
        "model_version": model_version,
        "target_horizon_ms": target_spec.horizon_ms,
        "target_kind": target_spec.kind,
        "walk_forward": asdict(config),
        "economic_policy": asdict(policy),
    }
    return hashlib.sha256(
        repr(sorted(identity.items(), key=lambda item: item[0])).encode("utf-8")
    ).hexdigest()[:32]


def persist_validation_report(
    store,
    report: ValidationReport,
    dataset: Sequence[ForecastDatasetRow],
    *,
    replay_fingerprint: str,
    target_spec: ForecastTargetSpec,
    config: WalkForwardConfig,
    policy: EconomicPolicySpec,
    model_id: str,
    model_version: str,
) -> dict[str, int | str | bool]:
    """Persist immutable research evidence generated by one OOS validation run."""
    from .storage import CryptoStore

    if not isinstance(store, CryptoStore):
        raise TypeError("store must be a CryptoStore")
    target_spec.validate()
    config.validate()
    policy.validate()

    run_id = validation_run_id(
        replay_fingerprint=replay_fingerprint,
        model_id=model_id,
        model_version=model_version,
        target_spec=target_spec,
        config=config,
        policy=policy,
    )
    if report.oos_labels:
        aggregate = probabilistic_metrics(report.oos_labels, report.oos_probabilities)
        aggregate_economic = economic_metrics(
            [dataset[i] for fold in report.folds for i in fold.fold.test_indices],
            report.oos_probabilities,
            policy,
        )
    else:
        aggregate = None
        aggregate_economic = None

    run_row = {
        "run_id": run_id,
        "replay_fingerprint": replay_fingerprint,
        "model_id": model_id,
        "model_version": model_version,
        "target_kind": target_spec.kind,
        "horizon_ms": target_spec.horizon_ms,
        "status": report.status,
        "placebo_p_value": report.placebo_p_value,
        "placebo_iterations": report.placebo_iterations,
        "promotion_eligible": report.promotion_eligible,
        "config": asdict(config),
        "aggregate_metrics": {
            "probabilistic": asdict(aggregate) if aggregate else {},
            "economic": asdict(aggregate_economic) if aggregate_economic else {},
        },
        "stability": {
            "by_symbol": {
                key: asdict(value)
                for key, value in report.stability_by_symbol.items()
            },
            "by_horizon_ms": {
                str(key): asdict(value)
                for key, value in report.stability_by_horizon_ms.items()
            },
        },
        "stress": {
            key: asdict(value)
            for key, value in report.stress_results.items()
        },
    }
    store.save_validation_run(run_row)

    fold_rows = []
    oos_rows = []
    probability_offset = 0
    for fold_eval in report.folds:
        fold = fold_eval.fold
        fold_rows.append(
            {
                "run_id": run_id,
                "fold_id": fold.fold_id,
                "train_start": fold.train_start.isoformat(),
                "train_end": fold.train_end.isoformat(),
                "test_start": fold.test_start.isoformat(),
                "test_end": fold.test_end.isoformat(),
                "train_rows": len(fold.train_indices),
                "test_rows": len(fold.test_indices),
                "model_spec_hash": fold_eval.model.spec_hash,
                "probabilistic": asdict(fold_eval.probabilistic),
                "baseline_fifty": asdict(fold_eval.baseline_fifty),
                "baseline_prevalence": asdict(fold_eval.baseline_prevalence),
                "economic": asdict(fold_eval.economic),
            }
        )
        for local_index, dataset_index in enumerate(fold.test_indices):
            row = dataset[dataset_index]
            probability = report.oos_probabilities[probability_offset + local_index]
            if probability >= policy.long_threshold:
                action = 1
            elif probability <= policy.short_threshold:
                action = -1
            else:
                action = 0
            signed = float(row.label.realized_signed_return_bps) * action
            cost = (
                policy.round_trip_cost_bps
                + policy.round_trip_slippage_bps
                if action
                else 0.0
            )
            oos_rows.append(
                {
                    "run_id": run_id,
                    "fold_id": fold.fold_id,
                    "row_index": local_index,
                    "leader_event_id": row.snapshot.leader_event_id,
                    "target_symbol": row.snapshot.target_symbol,
                    "horizon_ms": row.label.horizon_ms,
                    "decision_event_time": row.snapshot.decision_event_time.isoformat(),
                    "decision_received_time": row.snapshot.decision_received_time.isoformat(),
                    "label_event_time": row.label.label_event_time.isoformat(),
                    "label_received_time": row.label.label_received_time.isoformat(),
                    "probability": probability,
                    "realized_target": row.label.realized_target,
                    "realized_signed_return_bps": row.label.realized_signed_return_bps,
                    "net_return_bps": signed - cost,
                }
            )
        probability_offset += len(fold.test_indices)

    return {
        "run_id": run_id,
        "fold_rows": store.save_validation_folds(fold_rows),
        "oos_rows": store.save_validation_oos(oos_rows),
        "promotion_eligible": report.promotion_eligible,
    }
