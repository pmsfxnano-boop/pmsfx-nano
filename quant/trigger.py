"""PMSF-X Nano — advanced Gatillazo engine.

Deterministic convergence gate. Thresholds are engineering defaults and
must be calibrated with OOS outcomes before any promotion.
"""

from __future__ import annotations

from typing import Any


def evaluate_trigger(
    *,
    probability_up: float | None,
    agreement: float,
    confidence: float,
    model_health: dict[str, Any],
    regime: dict[str, Any] | None,
    spread_bps: float | None,
    expected_move_bps: float | None = None,
    estimated_cost_bps: float | None = None,
    persistence_count: int = 1,
    cooldown_active: bool = False,
) -> dict[str, Any]:
    reasons: list[str] = []

    if cooldown_active:
        reasons.append("COOLDOWN")
    if probability_up is None:
        reasons.append("NO_PROBABILITY")
    if model_health.get("safe_mode"):
        reasons.append("SAFE_MODE")
    if agreement < 0.50:
        reasons.append("LOW_EXPERT_AGREEMENT")
    if confidence < 0.20:
        reasons.append("LOW_CONFIDENCE")
    if spread_bps is None or spread_bps > 5.0:
        reasons.append("LIQUIDITY_GATE")
    if persistence_count < 2:
        reasons.append("PERSISTENCE_REQUIRED")

    cost_pass = False
    if expected_move_bps is not None and estimated_cost_bps is not None:
        cost_pass = expected_move_bps > estimated_cost_bps * 1.25
        if not cost_pass:
            reasons.append("EXPECTED_MOVE_BELOW_COST")

    p = float(probability_up) if probability_up is not None else 0.5
    agreement_factor = max(0.0, min(1.0, agreement))
    confidence_factor = max(0.0, min(1.0, confidence))
    health_factor = 0.0 if model_health.get("safe_mode") else 1.0
    regime_factor = 1.0 if (regime or {}).get("regime") not in (None, "UNKNOWN") else 0.5
    liquidity_factor = 1.0 if spread_bps is not None and spread_bps <= 5.0 else 0.0
    extreme_factor = 1.0

    directional_probability = max(p, 1.0 - p)
    score = (
        directional_probability
        * agreement_factor
        * confidence_factor
        * health_factor
        * regime_factor
        * liquidity_factor
        * extreme_factor
    )

    if reasons:
        status = "WATCH" if "PERSISTENCE_REQUIRED" in reasons and len(reasons) == 1 else "BLOCKED"
    elif score >= 0.92:
        status = "CRITICAL"
    elif score >= 0.84:
        status = "TRIGGER"
    elif score >= 0.75:
        status = "WATCH"
    else:
        status = "BLOCKED"

    direction = "UP" if p >= 0.5 else "DOWN"
    return {
        "status": status,
        "direction": direction,
        "score": round(score, 4),
        "reasons": reasons,
        "activation": {
            "min_score_watch": 0.75,
            "min_score_trigger": 0.84,
            "min_score_critical": 0.92,
            "min_persistence": 2,
        },
        "cost_gate": {
            "expected_move_bps": expected_move_bps,
            "estimated_cost_bps": estimated_cost_bps,
            "passes": cost_pass,
        },
        "anti_false_positive": {
            "persistence_count": persistence_count,
            "cooldown_active": cooldown_active,
        },
        "scientific_status": "ENGINEERING_DEFAULTS_NOT_OOS_CALIBRATED",
    }
