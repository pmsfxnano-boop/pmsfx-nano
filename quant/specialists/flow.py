"""PMSF-X Nano — Flow Specialist v0.

This module is intentionally a transparent baseline, not a validated alpha model.
It converts the current quote/microstructure snapshot into a directional raw score.

Safety:
- no training data is assumed;
- probability is explicitly RAW / UNCALIBRATED;
- this specialist cannot arm the Gatillazo engine.
"""

from __future__ import annotations

import math
from typing import Any


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def run_flow_specialist(
    *,
    last: Any,
    bid: Any,
    ask: Any,
    bid_size: Any,
    ask_size: Any,
    spread_bps: Any,
    microprice: Any,
) -> dict[str, Any]:
    last_f = _safe_float(last)
    bid_f = _safe_float(bid)
    ask_f = _safe_float(ask)
    bid_size_f = _safe_float(bid_size) or 0.0
    ask_size_f = _safe_float(ask_size) or 0.0
    spread_f = _safe_float(spread_bps)
    micro_f = _safe_float(microprice)

    if None in (last_f, bid_f, ask_f, spread_f):
        return {
            "model_id": "flow-baseline-v0",
            "status": "INSUFFICIENT_DATA",
            "direction": None,
            "raw_probability_up": None,
            "raw_probability_down": None,
            "confidence_raw": 0.0,
            "validated": False,
            "calibrated": False,
        }

    mid = (bid_f + ask_f) / 2.0
    if mid <= 0:
        return {
            "model_id": "flow-baseline-v0",
            "status": "INVALID_MID",
            "direction": None,
            "raw_probability_up": None,
            "raw_probability_down": None,
            "confidence_raw": 0.0,
            "validated": False,
            "calibrated": False,
        }

    total_size = bid_size_f + ask_size_f
    imbalance = (
        (bid_size_f - ask_size_f) / total_size
        if total_size > 0
        else 0.0
    )

    micro_delta_bps = (
        (micro_f - mid) / mid * 10000.0
        if micro_f is not None
        else 0.0
    )
    last_delta_bps = (last_f - mid) / mid * 10000.0

    # Transparent weighted snapshot score.
    score = (
        0.55 * imbalance
        + 0.25 * _clamp(micro_delta_bps / 5.0, -1.0, 1.0)
        + 0.20 * _clamp(last_delta_bps / 5.0, -1.0, 1.0)
    )

    # Wide spreads reduce confidence but do not reverse direction.
    spread_penalty = _clamp((spread_f - 1.0) / 10.0, 0.0, 0.35)
    score *= 1.0 - spread_penalty

    p_up = _sigmoid(2.4 * score)
    p_down = 1.0 - p_up
    confidence = abs(p_up - 0.5) * 2.0

    if p_up >= 0.57:
        direction = "UP"
    elif p_down >= 0.57:
        direction = "DOWN"
    else:
        direction = "NEUTRAL"

    return {
        "model_id": "flow-baseline-v0",
        "status": "READY",
        "direction": direction,
        "raw_probability_up": round(p_up, 4),
        "raw_probability_down": round(p_down, 4),
        "confidence_raw": round(confidence, 4),
        "validated": False,
        "calibrated": False,
        "features": {
            "mid": round(mid, 6),
            "size_imbalance": round(imbalance, 4),
            "micro_delta_bps": round(micro_delta_bps, 4),
            "last_delta_bps": round(last_delta_bps, 4),
            "spread_bps": round(spread_f, 4),
            "spread_penalty": round(spread_penalty, 4),
        },
    }
