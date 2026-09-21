"""PMSF-X Nano — canonical market-data health gate."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

MAX_QUOTE_AGE_SECONDS = 180.0
MAX_SPREAD_BPS = 500.0

def _float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None

def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None

def assess_quote(*, symbol: str, last: Any, bid: Any, ask: Any, bid_size: Any, ask_size: Any, quote_timestamp: Any, received_at: Any) -> dict[str, Any]:
    last_f, bid_f, ask_f = _float(last), _float(bid), _float(ask)
    bid_size_f, ask_size_f = _float(bid_size), _float(ask_size)
    reasons: list[str] = []

    if not symbol or not symbol.isalnum(): reasons.append("INVALID_SYMBOL")
    if last_f is None or bid_f is None or ask_f is None: reasons.append("MISSING_PRICE")
    if bid_f is not None and bid_f <= 0: reasons.append("INVALID_BID")
    if ask_f is not None and ask_f <= 0: reasons.append("INVALID_ASK")
    if bid_f is not None and ask_f is not None and bid_f > ask_f: reasons.append("CROSSED_QUOTE")
    if bid_size_f is None or ask_size_f is None: reasons.append("MISSING_SIZE")
    if bid_size_f is not None and bid_size_f < 0: reasons.append("NEGATIVE_BID_SIZE")
    if ask_size_f is not None and ask_size_f < 0: reasons.append("NEGATIVE_ASK_SIZE")

    spread_bps = None
    if bid_f is not None and ask_f is not None and bid_f > 0:
        mid = (bid_f + ask_f) / 2.0
        spread_bps = (ask_f - bid_f) / mid if mid > 0 else None
        spread_bps = spread_bps * 10000.0 if spread_bps is not None else None
        if spread_bps is None or spread_bps < 0: reasons.append("INVALID_SPREAD")
        elif spread_bps > MAX_SPREAD_BPS: reasons.append("EXTREME_SPREAD")

    quote_dt = _parse_time(quote_timestamp)
    received_dt = _parse_time(received_at) or datetime.now(timezone.utc)
    age_seconds = None
    if quote_dt is not None:
        age_seconds = max(0.0, (received_dt - quote_dt).total_seconds())
        if age_seconds > MAX_QUOTE_AGE_SECONDS: reasons.append("STALE_QUOTE")
    else:
        reasons.append("MISSING_QUOTE_TIMESTAMP")

    status = "HEALTHY" if not reasons else "DEGRADED"
    return {
        "status": status,
        "symbol": symbol,
        "reasons": reasons,
        "quote_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
        "spread_bps": round(spread_bps, 3) if spread_bps is not None else None,
        "checks": {
            "prices_present": last_f is not None and bid_f is not None and ask_f is not None,
            "bid_le_ask": bid_f is not None and ask_f is not None and bid_f <= ask_f,
            "sizes_nonnegative": bid_size_f is not None and ask_size_f is not None and bid_size_f >= 0 and ask_size_f >= 0,
            "spread_within_limit": spread_bps is not None and 0 <= spread_bps <= MAX_SPREAD_BPS,
            "timestamp_present": quote_dt is not None,
            "fresh": age_seconds is not None and age_seconds <= MAX_QUOTE_AGE_SECONDS,
        },
        "limits": {"max_quote_age_seconds": MAX_QUOTE_AGE_SECONDS, "max_spread_bps": MAX_SPREAD_BPS},
    }
