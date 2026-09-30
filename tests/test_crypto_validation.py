"""A8 validation engine tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gorila_crypto.forecast import (
    DetectionFeatureSnapshot,
    ForecastTargetSpec,
)
from gorila_crypto.lead_lag import LeadLagConfig
from gorila_crypto.storage import CryptoStore
from gorila_crypto.validation import (
    EconomicPolicySpec,
    ForecastDatasetRow,
    ForecastLabel,
    StressScenario,
    WalkForwardConfig,
    build_forecast_dataset,
    economic_metrics,
    make_walk_forward_folds,
    probabilistic_metrics,
    run_walk_forward_validation,
    persist_validation_report,
)


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def snapshot(i: int, value: float, target_symbol: str = "ETHUSDT") -> DetectionFeatureSnapshot:
    t = BASE + timedelta(seconds=i)
    return DetectionFeatureSnapshot(
        feature_set_version="test",
        decision_event_time=t,
        decision_received_time=t,
        leader_symbol="BTCUSDT",
        target_symbol=target_symbol,
        leader_event_id=f"leader-{i}",
        feature_values={
            "leader_return_bps": value,
            "leader_abs_return_bps": abs(value),
            "leader_direction": 1.0 if value >= 0 else -1.0,
            "leader_transport_latency_ms": 2.0,
            "target_return_bps_lookback": value * 0.2,
            "target_abs_return_bps_lookback": abs(value * 0.2),
            "target_information_age_ms": 1.0,
            "target_market_age_ms": 1.0,
        },
        source_event_ids=(f"leader-{i}",),
        feature_set_hash=f"hash-{i}",
    )


def row(i: int) -> ForecastDatasetRow:
    value = 8.0 if i % 2 else -8.0
    y = 1 if i % 2 else 0
    return ForecastDatasetRow(
        snapshot=snapshot(i, value, "ETHUSDT" if i % 3 else "SOLUSDT"),
        label=ForecastLabel(
            realized_target=y,
            realized_signed_return_bps=12.0 if y else -8.0,
            baseline_target_price=100.0,
            future_target_price=100.12 if y else 99.92,
            label_event_time=BASE + timedelta(seconds=i, milliseconds=500),
            label_received_time=BASE + timedelta(seconds=i, milliseconds=500),
            label_event_id=f"target-{i}",
            horizon_ms=500,
        ),
    )



def test_label_horizon_is_conservative_when_leader_information_arrives_late() -> None:
    decision_event = BASE + timedelta(seconds=10)
    decision_received = decision_event + timedelta(seconds=1)
    snap = DetectionFeatureSnapshot(
        feature_set_version="test",
        decision_event_time=decision_event,
        decision_received_time=decision_received,
        leader_symbol="BTCUSDT",
        target_symbol="ETHUSDT",
        leader_event_id="late-leader",
        feature_values={"leader_direction": 1.0},
        source_event_ids=("late-leader",),
        feature_set_hash="late-horizon",
    )
    target = [
        type("P", (), {
            "event_time": decision_event - timedelta(milliseconds=1),
            "received_time": decision_received - timedelta(milliseconds=1),
            "price": 100.0,
            "event_id": "base",
        })(),
        type("P", (), {
            "event_time": decision_event + timedelta(milliseconds=600),
            "received_time": decision_received + timedelta(milliseconds=100),
            "price": 100.1,
            "event_id": "too-soon",
        })(),
        type("P", (), {
            "event_time": decision_received + timedelta(milliseconds=600),
            "received_time": decision_received + timedelta(milliseconds=600),
            "price": 100.2,
            "event_id": "valid",
        })(),
    ]
    from gorila_crypto.validation import label_snapshot
    label = label_snapshot(
        target,
        snap,
        ForecastTargetSpec(horizon_ms=500),
    )
    assert label is not None
    assert label.label_event_id == "valid"

def test_probabilistic_metrics_have_expected_extremes() -> None:
    perfect = probabilistic_metrics([0, 1, 0, 1], [0.01, 0.99, 0.01, 0.99])
    coin = probabilistic_metrics([0, 1, 0, 1], [0.5, 0.5, 0.5, 0.5])
    assert perfect.log_loss < coin.log_loss
    assert perfect.brier < coin.brier
    assert perfect.auc == 1.0


def test_walk_forward_purges_training_labels_that_extend_into_test() -> None:
    dataset = [row(i) for i in range(80)]
    late = row(59)
    dataset[59] = ForecastDatasetRow(
        snapshot=late.snapshot,
        label=ForecastLabel(
            realized_target=late.label.realized_target,
            realized_signed_return_bps=late.label.realized_signed_return_bps,
            baseline_target_price=late.label.baseline_target_price,
            future_target_price=late.label.future_target_price,
            label_event_time=BASE + timedelta(seconds=61),
            label_received_time=BASE + timedelta(seconds=61),
            label_event_id=late.label.label_event_id,
            horizon_ms=late.label.horizon_ms,
        ),
    )
    folds = make_walk_forward_folds(
        dataset,
        WalkForwardConfig(
            min_train_rows=40,
            test_rows=10,
            step_rows=10,
            purge_ms=500,
            embargo_ms=500,
        ),
    )
    assert folds
    first = folds[0]
    assert 59 not in first.train_indices
    assert max(first.train_indices) < min(first.test_indices)


def test_overlapping_test_windows_are_rejected() -> None:
    with pytest.raises(ValueError):
        WalkForwardConfig(test_rows=20, step_rows=10).validate()


def test_economic_metrics_apply_cost_and_slippage() -> None:
    rows = [row(1), row(2), row(3)]
    econ = economic_metrics(
        rows,
        [0.9, 0.1, 0.9],
        EconomicPolicySpec(
            long_threshold=0.55,
            short_threshold=0.45,
            round_trip_cost_bps=1.0,
            round_trip_slippage_bps=1.0,
        ),
    )
    assert econ.traded_fraction == 1.0
    assert econ.cumulative_net_bps > 0


def test_full_walk_forward_validation_stays_research_only() -> None:
    dataset = [row(i) for i in range(180)]
    report = run_walk_forward_validation(
        dataset,
        ["leader_return_bps", "leader_abs_return_bps", "target_return_bps_lookback"],
        WalkForwardConfig(
            min_train_rows=60,
            test_rows=20,
            step_rows=20,
            purge_ms=500,
            embargo_ms=500,
            ridge_alpha=0.1,
        ),
        EconomicPolicySpec(
            long_threshold=0.55,
            short_threshold=0.45,
            round_trip_cost_bps=0.25,
            round_trip_slippage_bps=0.25,
        ),
        placebo_block_size=5,
        placebo_iterations=100,
        stress_scenarios=(
            StressScenario(
                name="double_costs",
                return_haircut=0.10,
                cost_multiplier=2.0,
                slippage_multiplier=2.0,
            ),
        ),
    )
    assert report.status == "OOS_EVALUATED"
    assert len(report.folds) >= 3
    assert len(report.oos_labels) > 0
    assert report.placebo_p_value is not None
    assert report.fold_baseline_pass_fraction >= 0.67
    assert report.fold_economic_positive_fraction >= 0.67
    assert report.temporal_stability.passed is True
    assert "double_costs" in report.stress_results
    assert report.promotion_eligible is False
    assert report.research_robustness_status == "BLOCKED"
    assert report.multiple_testing_p_value is not None
    assert "DSR_GATE_FAILED" in report.research_robustness_reasons
    assert "PBO_GATE_FAILED" in report.research_robustness_reasons



def test_zero_friction_validation_cannot_be_promotion_eligible() -> None:
    dataset = [row(i) for i in range(180)]
    report = run_walk_forward_validation(
        dataset,
        ["leader_return_bps", "leader_abs_return_bps", "target_return_bps_lookback"],
        WalkForwardConfig(
            min_train_rows=60,
            test_rows=20,
            step_rows=20,
            purge_ms=500,
            embargo_ms=500,
            ridge_alpha=0.1,
        ),
        EconomicPolicySpec(
            long_threshold=0.55,
            short_threshold=0.45,
            round_trip_cost_bps=0.0,
            round_trip_slippage_bps=0.0,
        ),
        placebo_block_size=5,
        placebo_iterations=50,
        stress_scenarios=(
            StressScenario(
                name="zero_cost_stress",
                return_haircut=0.05,
                cost_multiplier=2.0,
                slippage_multiplier=2.0,
            ),
        ),
    )
    assert report.status == "OOS_EVALUATED"
    assert report.promotion_eligible is False

def test_build_forecast_dataset_returns_empty_without_both_series() -> None:
    data = [
        {
            "event_type": "trade",
            "event_id": "btc-1",
            "symbol": "BTCUSDT",
            "ledger_seq": 1,
            "event_time": BASE.isoformat(),
            "received_time": BASE.isoformat(),
            "payload": {"p": "100.0", "q": "1"},
        }
    ]
    dataset = build_forecast_dataset(
        data,
        "BTCUSDT",
        "ETHUSDT",
        LeadLagConfig(),
        ForecastTargetSpec(horizon_ms=1000),
    )
    assert dataset == ()


def test_null_benchmark_does_not_promote_without_predictive_information() -> None:
    dataset = []
    for i in range(180):
        item = row(i)
        # Remove the deterministic signal while preserving both classes and chronology.
        value = 0.0
        y = i % 2
        dataset.append(
            ForecastDatasetRow(
                snapshot=snapshot(i, value, "ETHUSDT"),
                label=ForecastLabel(
                    realized_target=y,
                    realized_signed_return_bps=1.0 if y else -1.0,
                    baseline_target_price=100.0,
                    future_target_price=100.01 if y else 99.99,
                    label_event_time=BASE + timedelta(seconds=i, milliseconds=500),
                    label_received_time=BASE + timedelta(seconds=i, milliseconds=500),
                    label_event_id=f"null-target-{i}",
                    horizon_ms=500,
                ),
            )
        )

    report = run_walk_forward_validation(
        dataset,
        ["leader_return_bps", "leader_abs_return_bps", "target_return_bps_lookback"],
        WalkForwardConfig(
            min_train_rows=60,
            test_rows=20,
            step_rows=20,
            purge_ms=500,
            embargo_ms=500,
            ridge_alpha=0.1,
        ),
        EconomicPolicySpec(
            long_threshold=0.55,
            short_threshold=0.45,
            round_trip_cost_bps=0.25,
            round_trip_slippage_bps=0.25,
        ),
        placebo_block_size=5,
        placebo_iterations=50,
        stress_scenarios=(
            StressScenario(
                name="null_stress",
                return_haircut=0.10,
                cost_multiplier=2.0,
                slippage_multiplier=2.0,
            ),
        ),
    )
    assert report.status == "OOS_EVALUATED"
    assert report.promotion_eligible is False


def test_validation_evidence_persists_by_run_fold_and_oos_group(tmp_path) -> None:
    dataset = [row(i) for i in range(120)]
    config = WalkForwardConfig(
        min_train_rows=40,
        test_rows=20,
        step_rows=20,
        purge_ms=500,
        embargo_ms=500,
        ridge_alpha=0.1,
    )
    policy = EconomicPolicySpec(
        long_threshold=0.55,
        short_threshold=0.45,
        round_trip_cost_bps=0.25,
        round_trip_slippage_bps=0.25,
    )
    report = run_walk_forward_validation(
        dataset,
        ["leader_return_bps", "leader_abs_return_bps", "target_return_bps_lookback"],
        config,
        policy,
        placebo_block_size=5,
        placebo_iterations=20,