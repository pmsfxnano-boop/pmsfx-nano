"""A7 tests for PIT-safe feature construction and forecast gating."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gorila_crypto.forecast import (
    FORECAST_SEMANTICS,
    ForecastModelSpec,
    ForecastTargetSpec,
    build_detection_features,
    deterministic_forecast_id,
    model_is_admissible,
    score_forecast,
)
from gorila_crypto.lead_lag import LeadLagConfig, PricePoint


def p(seq: int, symbol: str, ms: int, received_ms: int, price: float) -> PricePoint:
    base = datetime(2026, 9, 29, 15, 0, 0, tzinfo=timezone.utc)
    return PricePoint(
        ledger_seq=seq,
        symbol=symbol,
        event_time=base + timedelta(milliseconds=ms),
        received_time=base + timedelta(milliseconds=received_ms),
        price=price,
        size=1.0,
        event_id=f'{symbol}-{seq}',
    )


def data():
    leader = [
        p(1, 'BTCUSDT', 0, 0, 100.0),
        p(2, 'BTCUSDT', 1000, 1000, 100.06),
    ]
    target = [
        p(10, 'ETHUSDT', 0, 0, 200.0),
        p(11, 'ETHUSDT', 1000, 1000, 200.03),
    ]
    return leader, target


def test_feature_snapshot_uses_only_pit_information() -> None:
    leader, target = data()
    snap = build_detection_features(
        leader, target, leader[-1], 6.0, LeadLagConfig(lookback_seconds=1.0)
    )
    assert snap.leader_symbol == 'BTCUSDT'
    assert snap.target_symbol == 'ETHUSDT'
    assert snap.source_event_ids == ('BTCUSDT-2', 'ETHUSDT-11', 'ETHUSDT-10')
    assert 'probability' not in snap.feature_values
    assert 'target_return_bps' not in snap.feature_values
    assert snap.feature_set_hash


def test_late_future_target_is_not_allowed_into_features() -> None:
    leader, target = data()
    target.append(p(12, 'ETHUSDT', 1500, 500, 200.20))
    snap = build_detection_features(
        leader, target, leader[-1], 6.0, LeadLagConfig(lookback_seconds=1.0)
    )
    assert 'ETHUSDT-12' not in snap.source_event_ids


def test_feature_hash_changes_when_feature_information_changes() -> None:
    leader, target = data()
    one = build_detection_features(
        leader, target, leader[-1], 6.0, LeadLagConfig(lookback_seconds=1.0)
    )
    altered_leader = [leader[0], p(2, 'BTCUSDT', 1000, 1000, 100.07)]
    two = build_detection_features(
        altered_leader, target, altered_leader[-1], 7.0, LeadLagConfig(lookback_seconds=1.0)
    )
    assert one.feature_set_hash != two.feature_set_hash


def test_unvalidated_model_is_blocked_without_probability() -> None:
    leader, target = data()
    snap = build_detection_features(
        leader, target, leader[-1], 6.0, LeadLagConfig(lookback_seconds=1.0)
    )
    model = ForecastModelSpec(
        model_id='crypto-logit-v0',
        version='0',
        coefficients={'leader_return_bps': 0.1},
        intercept=0.0,
    )
    result = score_forecast(snap, ForecastTargetSpec(horizon_ms=1000), model)
    assert result.status == 'BLOCKED_NO_VALIDATED_MODEL'
    assert result.probability_response_positive is None
    assert result.semantics == FORECAST_SEMANTICS


def test_admissibility_requires_all_research_gates() -> None:
    base = dict(
        model_id='crypto-logit-v1',
        version='1',
        coefficients={'leader_return_bps': 0.01},
        intercept=0.0,
        validated=True,
        oos_status='PASS',
        economic_status='PASS',
        point_in_time=True,
        stress_pass=True,
    )
    assert model_is_admissible(ForecastModelSpec(**base)) is True
    assert model_is_admissible(ForecastModelSpec(**{**base, 'stress_pass': False})) is False
    assert model_is_admissible(ForecastModelSpec(**{**base, 'economic_status': 'BLOCKED'})) is False


def test_admissible_model_score_is_bounded_and_identity_is_deterministic() -> None:
    leader, target = data()
    snap = build_detection_features(
        leader, target, leader[-1], 6.0, LeadLagConfig(lookback_seconds=1.0)
    )
    target_spec = ForecastTargetSpec(horizon_ms=1000)
    model = ForecastModelSpec(
        model_id='crypto-logit-v1',
        version='1',
        coefficients={'leader_return_bps': 0.01},
        intercept=0.0,
        validated=True,
        oos_status='PASS',
        economic_status='PASS',
        point_in_time=True,
        stress_pass=True,
    )
    result = score_forecast(snap, target_spec, model)
    assert result.status == 'SHADOW_READY'
    assert 0.0 < result.probability_response_positive < 1.0
    first = deterministic_forecast_id(
        replay_fingerprint='fp', snapshot=snap, target=target_spec, model=model
    )
    second = deterministic_forecast_id(
        replay_fingerprint='fp', snapshot=snap, target=target_spec, model=model
    )
    assert first == second


def test_invalid_horizon_fails_closed() -> None:
    with pytest.raises(ValueError):
        ForecastTargetSpec(horizon_ms=0).validate()
