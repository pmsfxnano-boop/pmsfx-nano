"""PIT-bound shadow forecast orchestration for A7."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable

from .forecast import (
    ForecastModelSpec,
    ForecastTargetSpec,
    build_detection_features,
    deterministic_forecast_id,
    score_forecast,
)
from .lead_lag import LeadLagConfig, build_price_points, detect_leader_impulses
from .ledger import ReplaySpec, replay
from .storage import CryptoStore


def run_forecast_shadow(
    store: CryptoStore,
    replay_spec: ReplaySpec,
    leader_symbol: str,
    target_symbol: str,
    lead_lag_config: LeadLagConfig,
    target_spec: ForecastTargetSpec,
    model: ForecastModelSpec,
) -> dict[str, Any]:
    if replay_spec.order != "ingest":
        raise ValueError("A7 forecast requires ingest-order PIT replay")

    replay_result = replay(store, replay_spec)
    replay_manifest = {
        "replay_version": "1",
        "order": replay_spec.order,
        "symbol": replay_spec.symbol,
        "source": replay_spec.source,
        "start_received_time": replay_spec.start_received_time,
        "end_received_time": replay_spec.end_received_time,
        "start_event_time": replay_spec.start_event_time,
        "end_event_time": replay_spec.end_event_time,
        "limit": replay_spec.limit,
        "row_count": len(replay_result.rows),
        "first_ledger_seq": replay_result.first_ledger_seq,
        "last_ledger_seq": replay_result.last_ledger_seq,
        "fingerprint_sha256": replay_result.fingerprint,
        "analysis": "A7_FORECAST_SHADOW",
    }
    replay_manifest_id = store.save_replay_manifest(replay_manifest)
    points = build_price_points(replay_result.rows)
    leader = points.get(leader_symbol.upper()) or []
    target = points.get(target_symbol.upper()) or []
    if not leader or not target:
        return {
            "status": "NO_DATA",
            "replay_fingerprint": replay_result.fingerprint,
            "forecast_count": 0,
            "stored_count": 0,
            "automatic_promotion": False,
        }

    forecasts = []
    for trigger, leader_return_bps in detect_leader_impulses(
        leader, lead_lag_config
    ):
        try:
            snapshot = build_detection_features(
                target,
                trigger,
                leader_return_bps,
                lead_lag_config,
            )
        except ValueError:
            continue

        result = score_forecast(snapshot, target_spec, model)
        forecast_id = deterministic_forecast_id(
            replay_fingerprint=replay_result.fingerprint,
            snapshot=snapshot,
            target=target_spec,
            model=model,
        )
        row = {
            "forecast_id": forecast_id,
            "replay_fingerprint": replay_result.fingerprint,
            "model_id": result.model_id,
            "model_version": result.model_version,
            "symbol": snapshot.leader_symbol,
            "target_symbol": snapshot.target_symbol,
            "leader_event_id": snapshot.leader_event_id,
            "decision_event_time": snapshot.decision_event_time.isoformat(),
            "decision_received_time": snapshot.decision_received_time.isoformat(),
            "horizon_ms": result.horizon_ms,
            "target_kind": result.target_kind,
            "semantics": result.semantics,
            "probability_response_positive": result.probability_response_positive,
            "status": result.status,
            "feature_set_hash": snapshot.feature_set_hash,
            "features": dict(snapshot.feature_values),
            "source_event_ids": list(snapshot.source_event_ids),
            "metadata": {
                "forecast_domain": "CRYPTO",
                "research_mode": "SHADOW",
                "replay_manifest_id": replay_manifest_id,
            },
        }
        forecasts.append(row)

    stored = 0
    for row in forecasts:
        store.save_forecast_shadow(row)
        stored += 1

    return {
        "status": "SHADOW",
        "replay_fingerprint": replay_result.fingerprint,
        "forecast_count": len(forecasts),
        "stored_count": stored,
        "automatic_promotion": False,
        "execution": False,
        "model_id": model.model_id,
        "model_version": model.version,
        "target_kind": target_spec.kind,
        "horizon_ms": target_spec.horizon_ms,
        "results": [
            {
                "forecast_id": row["forecast_id"],
                "status": row["status"],
                "probability_response_positive": row[
                    "probability_response_positive"
                ],
                "feature_set_hash": row["feature_set_hash"],
            }
            for row in forecasts
        ],
    }
