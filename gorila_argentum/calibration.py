from __future__ import annotations

import math
from typing import Any


def _clip_probability(value: float) -> float:
    return min(1.0 - 1e-12, max(1e-12, float(value)))


def _logit(value: float) -> float:
    p = _clip_probability(value)
    return math.log(p / (1.0 - p))


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def fit_logit_intercept(
    probabilities: list[float],
    labels: list[float],
    *,
    max_iter: int = 80,
    tolerance: float = 1e-9,
) -> float:
    if len(probabilities) != len(labels) or not probabilities:
        raise ValueError("calibration_data_mismatch")
    if any(label not in (0.0, 1.0) for label in labels):
        raise ValueError("calibration_labels_must_be_binary")

    # The log-likelihood derivative is monotone decreasing in the intercept.
    # Bisection therefore gives a bounded, stable solution even for strongly
    # miscalibrated probabilities where an unconstrained Newton step can overshoot.
    base_logits = [_logit(p) for p in probabilities]
    low, high = -20.0, 20.0

    for _ in range(max_iter):
        intercept = (low + high) / 2.0
        fitted = [_sigmoid(z + intercept) for z in base_logits]
        gradient = sum(y - q for y, q in zip(labels, fitted))
        if abs(gradient) <= tolerance or (high - low) <= tolerance:
            return float(intercept)
        if gradient > 0.0:
            low = intercept
        else:
            high = intercept

    return float((low + high) / 2.0)


def apply_logit_intercept(probability: float, intercept: float) -> float:
    return _sigmoid(_logit(probability) + float(intercept))


def evaluate_probabilities(probabilities: list[float], labels: list[float]) -> dict[str, float]:
    if len(probabilities) != len(labels) or not probabilities:
        raise ValueError("evaluation_data_mismatch")
    brier = sum((float(p) - float(y)) ** 2 for p, y in zip(probabilities, labels)) / len(labels)
    logloss = -sum(
        math.log(_clip_probability(p)) if y == 1.0 else math.log(1.0 - _clip_probability(p))
        for p, y in zip(probabilities, labels)
    ) / len(labels)
    gap = sum(float(p) - float(y) for p, y in zip(probabilities, labels)) / len(labels)
    return {"brier": brier, "logloss": logloss, "prediction_gap": gap}


def build_recalibration_candidate(
    rows: list[dict[str, Any]],
    *,
    reference_size: int = 90,
    validation_size: int = 30,
    min_brier_improvement: float = 0.005,
    max_abs_validation_gap: float = 0.05,
) -> dict[str, Any]:
    usable = [
        row for row in rows
        if row.get("status") == "SETTLED"
        and row.get("realized_direction") in {"UP", "DOWN"}
        and row.get("probability_up") is not None
    ]
    usable.sort(key=lambda row: row.get("observed_at") or row.get("created_at") or "")
    needed = int(reference_size) + int(validation_size)
    if len(usable) < needed:
        return {
            "status": "INSUFFICIENT_DATA",
            "samples": len(usable),
            "required_samples": needed,
            "automatic_apply": False,
        }

    reference = usable[-needed:-int(validation_size)]
    validation = usable[-int(validation_size):]
    ref_p = [float(row["probability_up"]) for row in reference]
    ref_y = [1.0 if row["realized_direction"] == "UP" else 0.0 for row in reference]
    val_p = [float(row["probability_up"]) for row in validation]
    val_y = [1.0 if row["realized_direction"] == "UP" else 0.0 for row in validation]

    intercept = fit_logit_intercept(ref_p, ref_y)
    baseline = evaluate_probabilities(val_p, val_y)
    calibrated_p = [apply_logit_intercept(p, intercept) for p in val_p]
    calibrated = evaluate_probabilities(calibrated_p, val_y)

    brier_improvement = baseline["brier"] - calibrated["brier"]
    logloss_improvement = baseline["logloss"] - calibrated["logloss"]
    eligible = (
        brier_improvement >= float(min_brier_improvement)
        and logloss_improvement >= 0.0
        and abs(calibrated["prediction_gap"]) <= float(max_abs_validation_gap)
    )

    return {
        "status": "CANDIDATE_READY" if eligible else "CANDIDATE_REJECTED",
        "samples": len(usable),
        "reference_n": len(reference),
        "validation_n": len(validation),
        "intercept": intercept,
        "baseline": baseline,
        "calibrated": calibrated,
        "brier_improvement": brier_improvement,
        "logloss_improvement": logloss_improvement,
        "thresholds": {
            "min_brier_improvement": float(min_brier_improvement),
            "max_abs_validation_gap": float(max_abs_validation_gap),
        },
        "automatic_apply": False,
        "apply_gate": "PROMOTION_AND_DURABILITY_REQUIRED",
    }
