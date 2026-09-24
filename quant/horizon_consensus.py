"""PMSF-X Nano — multi-horizon consensus research layer.

This layer is descriptive/research-only. It summarizes agreement and dispersion
across the already computed 300s/900s/1800s forecasts. It must not alter the
production probability until independently validated.
"""

from __future__ import annotations

import math
from typing import Any

HORIZON_ORDER = (300, 900, 1800)
DIRECTION_THRESHOLD = 0.55


def _safe_probability(value: Any) -> float | None:
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(probability):
        return None
    return max(0.0, min(1.0, probability))


def _direction(probability: float) -> str:
    if probability >= DIRECTION_THRESHOLD:
        return "UP"
    if probability <= 1.0 - DIRECTION_THRESHOLD:
        return "DOWN"
    return "NEUTRAL"


def _sign(probability: float) -> int:
    direction = _direction(probability)
    if direction == "UP":
        return 1
    if direction == "DOWN":
        return -1
    return 0


def _population_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _pairwise_coherence(probabilities: list[float]) -> float:
    if len(probabilities) < 2:
        return 1.0
    pair_scores: list[float] = []
    for i in range(len(probabilities) - 1):
        for j in range(i + 1, len(probabilities)):
            pair_scores.append(1.0 - min(1.0, abs(probabilities[i] - probabilities[j]) / 0.20))
    return sum(pair_scores) / len(pair_scores) if pair_scores else 1.0


def build_horizon_consensus(
    multi_horizon: dict[str, Any] | None,
    *,
    cross_asset: dict[str, Any] | None = None,
) -> dict[str, Any]:
    multi_horizon = multi_horizon or {}
    vector = multi_horizon.get("forecast_vector") or {}
    evaluations = (multi_horizon.get("evaluation") or {}) if isinstance(multi_horizon, dict) else {}

    horizons: list[dict[str, Any]] = []
    for horizon_seconds in HORIZON_ORDER:
        item = vector.get(str(horizon_seconds)) or {}
        probability = _safe_probability(item.get("raw_probability_up"))
        if probability is None:
            continue
        evaluation = evaluations.get(str(horizon_seconds)) or {}
        horizons.append(
            {
                "horizon_seconds": horizon_seconds,
                "probability_up": round(probability, 4),
                "direction": _direction(probability),
                "validated_forecast": bool(item.get("validated")),
                "validated_evaluation": bool(evaluation.get("validated")),
                "sample_count": evaluation.get("sample_count"),
                "fold_count": evaluation.get("fold_count"),
            }
        )

    if not horizons:
        return {
            "status": "NO_DATA",
            "model_id": "horizon-consensus-research-v1",
            "research_only": True,
            "production_eligible": False,
            "horizon_count": 0,
            "confluence_index": None,
            "direction": "NEUTRAL",
            "horizons": [],
            "cross_asset_alignment": None,
            "methodology": {
                "threshold": "0.55/0.45 directional threshold",
                "dispersion_scale": "0.20 probability range",
                "note": "Descriptive only; no production weighting.",
            },
        }

    probabilities = [item["probability_up"] for item in horizons]
    mean_probability = sum(probabilities) / len(probabilities)
    dispersion = _population_std(probabilities)
    spread = max(probabilities) - min(probabilities)
    signs = [_sign(probability) for probability in probabilities]
    nonzero = [sign for sign in signs if sign]
    direction_agreement = (
        abs(sum(nonzero)) / len(nonzero)
        if nonzero
        else 0.0
    )
    strength = abs(mean_probability - 0.5) * 2.0
    coherence = _pairwise_coherence(probabilities)

    ordered_probabilities = [
        item["probability_up"]
        for item in sorted(horizons, key=lambda item: item["horizon_seconds"])
    ]
    slope = 0.0
    slope_direction = "FLAT"
    if len(ordered_probabilities) >= 2:
        slope = (
            ordered_probabilities[-1] - ordered_probabilities[0]
        )
        if slope > 0.025:
            slope_direction = "INCREASING"
        elif slope < -0.025:
            slope_direction = "DECREASING"

    if len(nonzero) >= 2 and direction_agreement == 1.0 and spread <= 0.08:
        status = "CONVERGENT"
    elif len(nonzero) >= 2 and direction_agreement < 1.0:
        status = "DIVERGENT"
    elif spread > 0.12:
        status = "DISPERSED"
    else:
        status = "MIXED"

    # This index is intentionally a descriptive engineering score. It is not
    # an alpha estimate and is never forwarded to the production probability.
    confluence_index = (
        0.45 * direction_agreement
        + 0.30 * coherence
        + 0.25 * strength
    )

    cross_alignment = None
    cross_forecast = (cross_asset or {}).get("forecast") if cross_asset else None
    if cross_forecast:
        cross_probability = _safe_probability(cross_forecast.get("raw_probability_up"))
        if cross_probability is not None:
            cross_sign = _sign(cross_probability)
            cross_alignment = {
                "available": True,
                "probability_up": round(cross_probability, 4),
                "direction": _direction(cross_probability),
                "alignment": (
                    1.0
                    if cross_sign == 0 or not nonzero
                    else round(abs(sum(nonzero) * cross_sign) / len(nonzero), 4)
                ),
                "validated": bool(
                    cross_forecast.get("validated")
                    and ((cross_asset or {}).get("evaluation") or {}).get("validated")
                ),
                "used_in_production": False,
            }

    validated_horizon_count = sum(
        1
        for item in horizons
        if item["validated_forecast"] and item["validated_evaluation"]
    )

    return {
        "status": status,
        "model_id": "horizon-consensus-research-v1",
        "research_only": True,
        "production_eligible": False,
        "horizon_count": len(horizons),
        "validated_horizon_count": validated_horizon_count,
        "mean_probability_up": round(mean_probability, 4),
        "probability_std": round(dispersion, 4),
        "probability_spread": round(spread, 4),
        "direction": _direction(mean_probability),
        "direction_agreement": round(direction_agreement, 4),
        "strength": round(strength, 4),
        "coherence": round(coherence, 4),
        "confluence_index": round(confluence_index, 4),
        "horizon_slope": round(slope, 4),
        "horizon_slope_direction": slope_direction,
        "cross_asset_alignment": cross_alignment,
        "horizons": horizons,
        "methodology": {
            "threshold": "0.55/0.45 directional threshold",
            "convergent_rule": "all non-neutral directions agree and spread <= 0.08",
            "dispersed_rule": "spread > 0.12",
            "index": "0.45 agreement + 0.30 coherence + 0.25 strength",
            "note": "Descriptive research metric; requires independent validation before any production gating.",
        },
    }
