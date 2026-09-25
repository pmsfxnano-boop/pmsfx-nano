from __future__ import annotations
from datetime import datetime,timezone
from .timing import make_window

def compose_signal(symbol,p_up,horizon_seconds,entry_price=None,regime="UNKNOWN",confidence=None,validity_seconds=None):
    p=float(p_up)
    direction="UP" if p>=0.5 else "DOWN"
    window=make_window(horizon_seconds,validity_seconds)
    return {
        "symbol":symbol,
        "probability_up":round(p,6),
        "probability_down":round(1-p,6),
        "direction":direction,
        "confidence":round(float(confidence if confidence is not None else abs(p-0.5)*2),6),
        "regime":regime,
        "entry_price":entry_price,
        "timing":window.__dict__,
        "signal_age_seconds":0.0,
        "status":"LIVE"
    }
