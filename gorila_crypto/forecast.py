"""PIT-safe forecast contract for the Crypto cleanroom.

A7 separates feature construction, model scoring, and promotion eligibility.
The default state is blocked because no Crypto model has passed genuine OOS and
economic validation yet.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

from .lead_lag import LeadLagConfig, PricePoint


FEATURE_SET_VERSION = "crypto_microstructure_alpha_v1"

# Frozen alpha family for the next validation cohort.  The current capture remains
# descriptive/shadow until the complete prospective protocol is mature.
MICROSTRUCTURE_FEATURES = (
    "leader_return_bps",
    "leader_abs_return_bps",
    "leader_direction",
    "leader_transport_latency_ms",
    "target_return_bps_lookback",
    "target_abs_return_bps_lookback",
    "target_information_age_ms",
    "target_market_age_ms",
    "leader_flow_imbalance_1s",
    "leader_flow_imbalance_5s",
    "leader_trade_intensity_1s",
    "leader_trade_intensity_5s",
    "target_flow_imbalance_1s",
    "target_flow_imbalance_5s",
    "target_trade_intensity_1s",
    "target_trade_intensity_5s",
    "leader_queue_imbalance",
    "leader_spread_bps",
    "leader_microprice_gap_bps",
    "target_queue_imbalance",
    "target_spread_bps",
    "target_microprice_gap_bps",
    "leader_flow_x_shock",
    "relative_flow_pressure",
    "leader_flow_persistence",
    "target_flow_persistence",
    "leader_flow_x_queue",
    "target_flow_x_queue",
    "target_adverse_selection_pressure",
    "cross_asset_dislocation_bps",
    "leader_book_age_ms",
    "target_book_age_ms",
    "book_age_gap_ms",
    "leader_book_confidence",
    "target_book_confidence",
    "leader_flow_x_book_confidence",
    "target_flow_x_book_confidence",
)
FORECAST_SEMANTICS = "P(SIGNED_TARGET_RETURN_BPS_POSITIVE)"


def _ms(a: datetime, b: datetime) -> float:
    return (a - b).total_seconds() * 1000.0


def _log_return_bps(current: float, reference: float) -> float:
    if current <= 0 or reference <= 0:
        raise ValueError("prices must be positive")
    return 10_000.0 * math.log(current / reference)


def _dt(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class ForecastTargetSpec:
    horizon_ms: int
    kind: str = "SIGNED_TARGET_RETURN_BPS_POSITIVE"
    alignment_tolerance_ms: int = 50

    def validate(self) -> None:
        if self.horizon_ms <= 0:
            raise ValueError("forecast horizon must be positive")
        if self.kind != "SIGNED_TARGET_RETURN_BPS_POSITIVE":
            raise ValueError("unsupported forecast target semantics")
        if self.alignment_tolerance_ms < 0:
            raise ValueError("alignment tolerance must be non-negative")


@dataclass(frozen=True)
class DetectionFeatureSnapshot:
    feature_set_version: str
    decision_event_time: datetime
    decision_received_time: datetime
    leader_symbol: str
    target_symbol: str
    leader_event_id: str
    feature_values: Mapping[str, float]
    source_event_ids: tuple[str, ...]
    feature_set_hash: str


@dataclass(frozen=True)
class ForecastModelSpec:
    model_id: str
    version: str
    coefficients: Mapping[str, float]
    intercept: float
    validated: bool = False
    oos_status: str = "UNVALIDATED"
    economic_status: str = "UNVALIDATED"
    point_in_time: bool = False
    stress_pass: bool = False


@dataclass(frozen=True)
class ForecastResult:
    status: str
    model_id: str
    model_version: str
    semantics: str
    target_kind: str
    horizon_ms: int
    probability_response_positive: float | None
    decision_event_time: datetime
    decision_received_time: datetime
    feature_set_hash: str


def _latest_available(
    series: list[PricePoint],
    *,
    event_cutoff: datetime,
    received_cutoff: datetime,
) -> PricePoint | None:
    candidate = None
    for point in series:
        if point.event_time > event_cutoff:
            continue
        if point.received_time > received_cutoff:
            continue
        candidate = point
    return candidate


def build_detection_features(
    target: list[PricePoint],
    trigger: PricePoint,
    leader_return_bps: float,
    config: LeadLagConfig,
) -> DetectionFeatureSnapshot:
    """Build only information available at the trigger receive time."""
    decision_event = _dt(trigger.event_time)
    decision_received = _dt(trigger.received_time)
    if decision_event > decision_received + timedelta(seconds=5):
        raise ValueError("invalid trigger clock ordering")

    historical_target = _latest_available(
        target, event_cutoff=decision_event, received_cutoff=decision_received
    )
    if historical_target is None:
        raise ValueError("no point-in-time target price is available")

    prior_target_candidates = [
        point for point in target
        if point.event_time <= decision_event - timedelta(seconds=config.lookback_seconds)
        and point.received_time <= decision_received
    ]
    prior_target = prior_target_candidates[-1] if prior_target_candidates else None
    if prior_target is None:
        raise ValueError("insufficient PIT target lookback")

    leader_latency = _ms(decision_received, decision_event)
    target_information_age = max(0.0, _ms(decision_received, historical_target.received_time))
    target_market_age = max(0.0, _ms(decision_event, historical_target.event_time))
    target_return = _log_return_bps(historical_target.price, prior_target.price)

    values = {
        "leader_return_bps": float(leader_return_bps),
        "leader_abs_return_bps": abs(float(leader_return_bps)),
        "leader_direction": 1.0 if leader_return_bps > 0 else -1.0,
        "leader_transport_latency_ms": float(max(0.0, leader_latency)),
        "target_return_bps_lookback": float(target_return),
        "target_abs_return_bps_lookback": abs(float(target_return)),
        "target_information_age_ms": float(target_information_age),
        "target_market_age_ms": float(target_market_age),
    }
    source_ids = tuple(dict.fromkeys([
        trigger.event_id,
        historical_target.event_id,
        prior_target.event_id,
    ]))
    canonical = {
        "version": FEATURE_SET_VERSION,
        "leader_symbol": trigger.symbol.upper(),
        "target_symbol": historical_target.symbol.upper(),
        "decision_event_time": decision_event.isoformat(),
        "decision_received_time": decision_received.isoformat(),
        "source_event_ids": source_ids,
        "features": dict(sorted(values.items())),
    }
    feature_hash = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return DetectionFeatureSnapshot(
        feature_set_version=FEATURE_SET_VERSION,
        decision_event_time=decision_event,
        decision_received_time=decision_received,
        leader_symbol=trigger.symbol.upper(),
        target_symbol=historical_target.symbol.upper(),
        leader_event_id=trigger.event_id,
        feature_values=values,
        source_event_ids=source_ids,
        feature_set_hash=feature_hash,
    )


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-min(value, 700.0))
        return 1.0 / (1.0 + z)
    z = math.exp(min(value, 700.0))
    return z / (1.0 + z)


def model_spec_hash(model: ForecastModelSpec) -> str:
    payload = {
        "model_id": model.model_id,
        "version": model.version,
        "intercept": float(model.intercept),
        "coefficients": {
            name: float(model.coefficients[name])
            for name in sorted(model.coefficients)
        },
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def model_is_admissible(model: ForecastModelSpec) -> bool:
    if not all((
        model.validated,
        model.oos_status == "PASS",
        model.economic_status == "PASS",
        model.point_in_time,
        model.stress_pass,
    )):
        return False
    if not math.isfinite(float(model.intercept)):
        return False
    return all(
        math.isfinite(float(value))
        for value in model.coefficients.values()
    )


def score_forecast(
    snapshot: DetectionFeatureSnapshot,
    target: ForecastTargetSpec,
    model: ForecastModelSpec,
) -> ForecastResult:
    target.validate()
    if not model_is_admissible(model):
        return ForecastResult(
            status="BLOCKED_NO_VALIDATED_MODEL",
            model_id=model.model_id,
            model_version=model.version,
            semantics=FORECAST_SEMANTICS,
            target_kind=target.kind,
            horizon_ms=target.horizon_ms,
            probability_response_positive=None,
            decision_event_time=snapshot.decision_event_time,
            decision_received_time=snapshot.decision_received_time,
            feature_set_hash=snapshot.feature_set_hash,
        )

    if not all(math.isfinite(float(value)) for value in snapshot.feature_values.values()):
        raise ValueError("non-finite feature value")
    linear = float(model.intercept)
    for name, coefficient in model.coefficients.items():
        if name not in snapshot.feature_values:
            raise ValueError(f"model requires unavailable feature: {name}")
        linear += float(coefficient) * float(snapshot.feature_values[name])
    probability = _sigmoid(linear)
    return ForecastResult(
        status="SHADOW_READY",
        model_id=model.model_id,
        model_version=model.version,
        semantics=FORECAST_SEMANTICS,
        target_kind=target.kind,
        horizon_ms=target.horizon_ms,
        probability_response_positive=probability,
        decision_event_time=snapshot.decision_event_time,
        decision_received_time=snapshot.decision_received_time,
        feature_set_hash=snapshot.feature_set_hash,
    )



def deterministic_forecast_id(
    *,
    replay_fingerprint: str,
    snapshot: DetectionFeatureSnapshot,
    target: ForecastTargetSpec,
    model: ForecastModelSpec,
) -> str:
    target.validate()
    identity = {
        "replay_fingerprint": replay_fingerprint,
        "feature_set_hash": snapshot.feature_set_hash,
        "leader_event_id": snapshot.leader_event_id,
        "model_id": model.model_id,
        "model_version": model.version,
        "model_spec_hash": model_spec_hash(model),
        "target_symbol": snapshot.target_symbol,
        "horizon_ms": target.horizon_ms,
        "target_kind": target.kind,
        "semantics": FORECAST_SEMANTICS,
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:32]