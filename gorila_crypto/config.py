"""Configuration for the isolated Crypto runtime.

No Argentina, US-equity, or legacy provider settings belong here.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _symbols(value: str) -> tuple[str, ...]:
    items = tuple(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    if not items:
        raise ValueError("GORILA_CRYPTO_SYMBOLS must contain at least one symbol")
    return items


@dataclass(frozen=True)
class CryptoSettings:
    symbols: tuple[str, ...]
    environment: str
    event_live_max_age_seconds: float
    event_delayed_max_age_seconds: float


settings = CryptoSettings(
    symbols=_symbols(os.getenv("GORILA_CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")),
    environment=os.getenv("GORILA_CRYPTO_ENVIRONMENT", "cleanroom"),
    event_live_max_age_seconds=max(30.0, float(os.getenv("GORILA_LIVE_EVENT_MAX_AGE_SECONDS", "90"))),
    event_delayed_max_age_seconds=max(
        31.0,
        float(os.getenv("GORILA_DELAYED_EVENT_MAX_AGE_SECONDS", "1800")),
    ),
)
