from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta


def validate_shadow_prediction(
    *,
    symbol: str,
    probability_up: float,
    horizon_seconds: int,
    entry_price: float,
) -> dict:
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("symbol_required")
    p = float(probability_up)
    if not math.isfinite(p) or not 0.0 <= p <= 1.0:
        raise ValueError("probability_up_out_of_range")
    horizon = int(horizon_seconds)
    if horizon <= 0:
        raise ValueError("horizon_seconds_must_be_positive")
    price = float(entry_price)
    if not math.isfinite(price) or price <= 0.0:
        raise ValueError("entry_price_must_be_positive")
    return {
        "symbol": symbol,
        "probability_up": p,
        "direction": "UP" if p >= 0.5 else "DOWN",
        "horizon_seconds": horizon,
        "entry_price": price,
    }


def compute_shadow_outcome(
    probability_up: float,
    entry_price: float,
    observed_price: float,
) -> dict:
    p = float(probability_up)
    entry = float(entry_price)
    observed = float(observed_price)
    if not math.isfinite(p) or not 0.0 <= p <= 1.0:
        raise ValueError("probability_up_out_of_range")
    if not math.isfinite(entry) or entry <= 0.0:
        raise ValueError("entry_price_must_be_positive")
    if not math.isfinite(observed) or observed <= 0.0:
        raise ValueError("observed_price_must_be_positive")

    delta = observed / entry - 1.0
    if delta > 0.0:
        realized = "UP"
        label = 1.0
    elif delta < 0.0:
        realized = "DOWN"
        label = 0.0
    else:
        realized = "FLAT"
        label = None

    p_clip = min(1.0 - 1e-12, max(1e-12, p))
    if label is None:
        correct = None
        brier = None
        logloss = None
    else:
        correct = (p >= 0.5) == (label == 1.0)
        brier = (p - label) ** 2
        logloss = -math.log(p_clip if label == 1.0 else 1.0 - p_clip)

    return {
        "observed_price": observed,
        "realized_direction": realized,
        "return_pct": delta * 100.0,
        "correct": correct,
        "brier": brier,
        "logloss": logloss,
    }


def validate_observed_at(created_at: str, horizon_seconds: int, observed_at: str) -> str:
    created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    created = created.astimezone(timezone.utc)
    observed = observed.astimezone(timezone.utc)
    if observed < created + timedelta(seconds=int(horizon_seconds)):
        raise ValueError("observed_before_horizon")
    return observed.isoformat()
