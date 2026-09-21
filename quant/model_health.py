"""PMSF-X Nano — model/data health gate and safe mode."""

from __future__ import annotations


def assess_model_health(
    *,
    data_health: dict,
    evaluation: dict,
    forecast: dict | None,
) -> dict:
    checks: dict[str, object] = {}
    reasons: list[str] = []

    data_ok = data_health.get("status") == "HEALTHY"
    checks["data_health"] = data_ok
    if not data_ok:
        reasons.append("DATA_HEALTH_FAILED")

    validated = bool(evaluation.get("validated"))
    checks["validation"] = validated
    if not validated:
        reasons.append("MODEL_NOT_VALIDATED")

    brier_skill = evaluation.get("brier_skill")
    brier_ok = brier_skill is not None and float(brier_skill) >= 0.0
    checks["oos_brier_skill"] = brier_ok
    if brier_skill is not None and not brier_ok:
        reasons.append("OOS_BRIER_SKILL_NEGATIVE")

    accuracy = evaluation.get("accuracy")
    accuracy_ok = accuracy is None or float(accuracy) >= 0.55
    checks["oos_accuracy"] = accuracy_ok
    if accuracy is not None and not accuracy_ok:
        reasons.append("OOS_ACCURACY_DRIFT")

    forecast_drift = None
    if forecast is not None and forecast.get("raw_probability_up") is not None:
        p = float(forecast["raw_probability_up"])
        forecast_drift = abs(p - 0.5) * 2.0
    checks["forecast_probability_drift"] = forecast_drift

    calibration_status = evaluation.get("calibration_status")
    checks["calibration"] = calibration_status in (None, "PLATT_FIT", "NOT_ENOUGH_DATA")
    if calibration_status not in (None, "PLATT_FIT", "NOT_ENOUGH_DATA"):
        reasons.append("CALIBRATION_UNAVAILABLE")

    if reasons:
        status = "FAILED" if ("DATA_HEALTH_FAILED" in reasons or "MODEL_NOT_VALIDATED" in reasons) else "DEGRADED"
    else:
        status = "HEALTHY"

    return {
        "status": status,
        "safe_mode": status != "HEALTHY",
        "reasons": reasons,
        "checks": checks,
        "thresholds": {
            "min_oos_accuracy": 0.55,
            "min_brier_skill": 0.0,
            "safe_mode_on": ["DATA_HEALTH_FAILED", "MODEL_NOT_VALIDATED"],
        },
    }
