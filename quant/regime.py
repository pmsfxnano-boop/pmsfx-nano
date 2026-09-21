"""PMSF-X Nano — transparent market-regime classifier."""
from __future__ import annotations

from statistics import mean, pstdev


def classify_regime(returns_bps: list[float], ranges_bps: list[float]) -> dict:
    """Classify using observable volatility/trend features; research-only."""
    if len(returns_bps) < 10 or len(ranges_bps) < 10:
        return {"regime": "UNKNOWN", "confidence": 0.0, "features": {}}
    avg = mean(returns_bps[-20:])
    vol = pstdev(returns_bps[-20:])
    range_avg = mean(ranges_bps[-20:])
    if vol >= 25.0:
        regime = "HIGH_VOL"
    elif avg >= 2.0 and vol < 15.0:
        regime = "TREND_UP"
    elif avg <= -2.0 and vol < 15.0:
        regime = "TREND_DOWN"
    else:
        regime = "MEAN_REVERT"
    confidence = min(1.0, max(0.0, (vol / 25.0) if regime == "HIGH_VOL" else abs(avg) / 5.0))
    return {"regime": regime, "confidence": round(confidence, 4), "features": {"return_mean_bps": round(avg, 4), "return_vol_bps": round(vol, 4), "range_mean_bps": round(range_avg, 4)}}
