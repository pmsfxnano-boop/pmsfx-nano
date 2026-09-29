"""Configuration for the isolated Crypto runtime.

No Argentina, US-equity, or legacy provider settings belong here.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _csv_items(value: str, *, upper: bool = False) -> tuple[str, ...]:
    items = tuple(
        dict.fromkeys(
            (item.strip().upper() if upper else item.strip())
            for item in value.split(",")
            if item.strip()
        )
    )
    if not items:
        raise ValueError("configuration list cannot be empty")
    return items


def _symbols(value: str) -> tuple[str, ...]:
    return _csv_items(value, upper=True)


@dataclass(frozen=True)
class CryptoSettings:
    symbols: tuple[str, ...]
    streams: tuple[str, ...]
    depth_speed: str
    environment: str
    ingest_enabled: bool
    quality_monitor_enabled: bool
    quality_interval_seconds: float
    heartbeat_interval_seconds: float
    quality_row_limit: int
    quality_min_rows_per_symbol: int
    quality_min_duration_seconds: float
    quality_max_p99_transport_latency_ms: float
    event_live_max_age_seconds: float
    event_delayed_max_age_seconds: float


    def validate(self) -> None:
        supported = {"trade", "aggTrade", "bookTicker", "depth"}
        if not self.symbols:
            raise ValueError("Crypto settings require at least one symbol")
        if not self.streams:
            raise ValueError("Crypto settings require at least one stream")
        if any(stream not in supported for stream in self.streams):
            raise ValueError(f"unsupported crypto stream: {self.streams}")
        if self.depth_speed not in {"100ms", "1000ms"}:
            raise ValueError("depth_speed must be 100ms or 1000ms")
        if self.quality_interval_seconds < 60.0:
            raise ValueError("quality interval cannot be below 60 seconds")
        if self.heartbeat_interval_seconds < 30.0:
            raise ValueError("heartbeat interval cannot be below 30 seconds")
        if self.quality_row_limit < 1000:
            raise ValueError("quality row limit must be >= 1000")

settings = CryptoSettings(
    symbols=_symbols(os.getenv("GORILA_CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")),
    streams=_csv_items(os.getenv("GORILA_CRYPTO_STREAMS", "trade,bookTicker")),
    depth_speed=os.getenv("GORILA_CRYPTO_DEPTH_SPEED", "100ms"),
    environment=os.getenv("GORILA_CRYPTO_ENVIRONMENT", "cleanroom"),
    ingest_enabled=os.getenv("GORILA_CRYPTO_INGEST_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"},
    quality_monitor_enabled=os.getenv("GORILA_CRYPTO_QUALITY_MONITOR_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"},
    quality_interval_seconds=max(
        60.0, float(os.getenv("GORILA_CRYPTO_QUALITY_INTERVAL_SECONDS", "900"))
    ),
    heartbeat_interval_seconds=max(
        30.0, float(os.getenv("GORILA_CRYPTO_HEARTBEAT_INTERVAL_SECONDS", "60"))
    ),
    quality_row_limit=max(
        1000, int(os.getenv("GORILA_CRYPTO_QUALITY_ROW_LIMIT", "100000"))
    ),
    quality_min_rows_per_symbol=max(
        1, int(os.getenv("GORILA_CRYPTO_QUALITY_MIN_ROWS_PER_SYMBOL", "10000"))
    ),
    quality_min_duration_seconds=max(
        0.0, float(os.getenv("GORILA_CRYPTO_QUALITY_MIN_DURATION_SECONDS", "3600"))
    ),
    quality_max_p99_transport_latency_ms=max(
        0.0, float(os.getenv("GORILA_CRYPTO_QUALITY_MAX_P99_TRANSPORT_LATENCY_MS", "5000"))
    ),
    event_live_max_age_seconds=max(30.0, float(os.getenv("GORILA_LIVE_EVENT_MAX_AGE_SECONDS", "90"))),
    event_delayed_max_age_seconds=max(
        31.0,
        float(os.getenv("GORILA_DELAYED_EVENT_MAX_AGE_SECONDS", "1800")),
    ),
)

settings.validate()
