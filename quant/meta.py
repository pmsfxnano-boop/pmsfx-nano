"""PMSF-X Nano — adaptive meta learner v1.

Research/ensemble layer. It does not create alpha by itself; it combines
specialist probabilities only when their inputs are usable.
"""

from __future__ import annotations

from typing import Any


def combine_specialists(
    *,
    historical: dict[str, Any] | None,
    flow: dict[str, Any] | None,
    historical_evaluation: dict[str, Any] | None,
    cross_asset: dict[str, Any] | None = None,
    cross_asset_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidates: list[tuple[str, float, float]] = []

    if historical and historical.get("raw_probability_up") is not None:
        weight = 1.0
        brier_skill = (historical_evaluation or {}).get("brier_skill")
        if brier_skill is not None:
            weight += max(0.0, min(1.0, float(brier_skill)))
        candidates.append(("historical", float(historical["raw_probability_up"]), weight))

    if flow and flow.get("raw_probability_up") is not None:
        # Flow is currently an unvalidated baseline, so it receives lower weight.
        weight = 0.35 if not flow.get("validated") else 1.0
        candidates.append(("flow", float(flow["raw_probability_up"]), weight))

    # Cross-asset is deliberately gated: it can be surfaced and audited before
    # it is allowed to alter the production probability.
    cross_asset_allowed = bool(
        cross_asset
        and cross_asset.get("raw_probability_up") is not None
        and cross_asset.get("validated")
        and (cross_asset_evaluation or {}).get("validated")
    )
    if cross_asset_allowed:
        weight = 1.0
        brier_skill = (cross_asset_evaluation or {}).get("brier_skill")
        if brier_skill is not None:
            weight += max(0.0, min(1.0, float(brier_skill)))
        candidates.append(("cross_asset", float(cross_asset["raw_probability_up"]), weight))

    if not candidates:
        return {
            "status": "NO_USABLE_SPECIALISTS",
            "probability_up": None,
            "weights": {},
            "agreement": 0.0,
        }

    total_weight = sum(w for _, _, w in candidates)
    p_up = sum(p * w for _, p, w in candidates) / total_weight
    directions = [1 if p >= 0.55 else -1 if p <= 0.45 else 0 for _, p, _ in candidates]
    nonzero = [d for d in directions if d]
    agreement = (abs(sum(nonzero)) / len(nonzero)) if nonzero else 0.0

    return {
        "status": "READY",
        "probability_up": round(max(0.0, min(1.0, p_up)), 4),
        "weights": {name: round(weight / total_weight, 4) for name, _, weight in candidates},
        "agreement": round(agreement, 4),
        "specialists_used": [name for name, _, _ in candidates],
        "cross_asset_gate": "ACTIVE" if cross_asset_allowed else "RESEARCH_ONLY",
    }
