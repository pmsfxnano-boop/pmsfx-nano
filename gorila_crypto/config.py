"""Configuration for the isolated Cryptonita Crypto runtime."""

from __future__ import annotations

import os
from dataclasses import dataclass


BINANCE_PROSPECTIVE_RETENTION_HOURS = 7 * 24


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
    provider: str
    symbols: tuple[str, ...]
    streams: tuple[str, ...]
    depth_speed: str
    environment: str
    ingest_enabled: bool
    quality_monitor_enabled: bool
    quality_interval_seconds: float
    research_enabled: bool
    research_interval_seconds: float
    shadow_alpha_enabled: bool
    shadow_alpha_interval_seconds: float
    shadow_alpha_min_training_rows: int
    shadow_alpha_training_rows: int
    shadow_alpha_training_interval_seconds: float
    heartbeat_interval_seconds: float
    quality_row_limit: int
    quality_min_rows_per_symbol: int
    quality_min_duration_seconds: float
    quality_max_p99_transport_latency_ms: float
    event_live_max_age_seconds: float
    event_delayed_max_age_seconds: float
    persist_bookticker_interval_seconds: float
    persistence_queue_batches: int
    event_batch_size: int
    event_batch_flush_interval_seconds: float
    storage_maintenance_interval_seconds: float
    retention_trade_hours: float
    retention_bookticker_hours: float
    retention_depth_hours: float
    persistence_spool_path: str
    persistence_spool_max_bytes: int
    persistence_spool_max_batches: int
    persistence_spool_guard_ratio: float
    durability_live_max_age_seconds: float


    def validate(self) -> None:
        providers = {"binance", "kraken"}
        if self.provider not in providers:
            raise ValueError(f"unsupported crypto provider: {self.provider}")
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
        if self.research_interval_seconds < 60.0:
            raise ValueError("research interval cannot be below 60 seconds")
        if self.shadow_alpha_interval_seconds < 1.0:
            raise ValueError("shadow alpha interval cannot be below 1 second")
        if self.shadow_alpha_min_training_rows < 100:
            raise ValueError("shadow alpha minimum training rows must be >= 100")
        if self.shadow_alpha_training_rows < self.shadow_alpha_min_training_rows:
            raise ValueError("shadow alpha training rows must cover minimum training rows")
        if self.shadow_alpha_training_interval_seconds < 10.0:
            raise ValueError("shadow alpha training interval cannot be below 10 seconds")
        if self.heartbeat_interval_seconds < 30.0:
            raise ValueError("heartbeat interval cannot be below 30 seconds")
        if self.quality_row_limit < 1000:
            raise ValueError("quality row limit must be >= 1000")
        if self.persist_bookticker_interval_seconds < 0.25:
            raise ValueError("bookTicker persistence interval must be >= 0.25s")
        if self.persistence_queue_batches < 8:
            raise ValueError("persistence queue must have at least 8 batches")
        if self.event_batch_size < 128:
            raise ValueError("event batch size must be >= 128")
        if self.event_batch_flush_interval_seconds < 0.10:
            raise ValueError("event batch flush interval must be >= 0.10s")
        if self.storage_maintenance_interval_seconds < 300:
            raise ValueError("storage maintenance interval must be >= 300s")
        if self.retention_trade_hours <= 0:
            raise ValueError("trade retention must be positive")
        if self.provider == "binance" and self.retention_trade_hours < BINANCE_PROSPECTIVE_RETENTION_HOURS:
            raise ValueError(
                "Binance trade retention cannot be shorter than the 7-day prospective cohort"
            )
        if self.retention_bookticker_hours <= 0:
            raise ValueError("bookTicker retention must be positive")
        if self.provider == "binance" and self.retention_bookticker_hours < BINANCE_PROSPECTIVE_RETENTION_HOURS:
            raise ValueError(
                "Binance bookTicker retention cannot be shorter than the 7-day prospective cohort"
            )
        if self.retention_depth_hours <= 0:
            raise ValueError("depth retention must be positive")
        if not self.persistence_spool_path.strip():
            raise ValueError("persistence spool path cannot be empty")
        if self.persistence_spool_max_bytes < 1024 * 1024:
            raise ValueError("persistence spool max bytes must be >= 1 MiB")
        if self.persistence_spool_max_batches < 1:
            raise ValueError("persistence spool max batches must be positive")
        if not 0.50 <= self.persistence_spool_guard_ratio < 1.0:
            raise ValueError("persistence spool guard ratio must be in [0.50,1.0)")
        if self.durability_live_max_age_seconds < 30.0:
            raise ValueError("durability live max age must be >= 30s")

settings = CryptoSettings(
    provider=os.getenv("GORILA_CRYPTO_PROVIDER", "binance").strip().lower(),
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
    research_enabled=os.getenv("GORILA_CRYPTO_RESEARCH_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"},
    research_interval_seconds=max(
        60.0, float(os.getenv("GORILA_CRYPTO_RESEARCH_INTERVAL_SECONDS", "900"))
    ),
    shadow_alpha_enabled=os.getenv("GORILA_CRYPTO_SHADOW_ALPHA_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"},
    shadow_alpha_interval_seconds=max(
        1.0, float(os.getenv("GORILA_CRYPTO_SHADOW_ALPHA_INTERVAL_SECONDS", "5"))
    ),
    shadow_alpha_min_training_rows=max(
        100, int(os.getenv("GORILA_CRYPTO_SHADOW_ALPHA_MIN_TRAINING_ROWS", "500"))
    ),
    shadow_alpha_training_rows=max(
        500, int(os.getenv("GORILA_CRYPTO_SHADOW_ALPHA_TRAINING_ROWS", "5000"))
    ),
    shadow_alpha_training_interval_seconds=max(
        10.0, float(os.getenv("GORILA_CRYPTO_SHADOW_ALPHA_TRAINING_INTERVAL_SECONDS", "30"))
    ),
    heartbeat_interval_seconds=max(
        30.0, float(os.getenv("GORILA_CRYPTO_HEARTBEAT_INTERVAL_SECONDS", "30"))
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
    persist_bookticker_interval_seconds=max(
        0.25,
        float(os.getenv("GORILA_PERSIST_BOOKTICKER_INTERVAL_SECONDS", "1.0")),
    ),
    persistence_queue_batches=max(
        8,
        int(os.getenv("GORILA_PERSISTENCE_QUEUE_BATCHES", "256")),
    ),
    event_batch_size=max(
        128,
        int(os.getenv("GORILA_CRYPTO_EVENT_BATCH_SIZE", "1000")),
    ),
    event_batch_flush_interval_seconds=max(
        0.10,
        float(os.getenv("GORILA_CRYPTO_EVENT_BATCH_FLUSH_INTERVAL_SECONDS", "0.50")),
    ),
    storage_maintenance_interval_seconds=max(
        300.0,
        float(os.getenv("GORILA_STORAGE_MAINTENANCE_INTERVAL_SECONDS", "900")),
    ),
    retention_trade_hours=max(
        1.0,
        float(os.getenv("GORILA_RETENTION_TRADE_HOURS", str(BINANCE_PROSPECTIVE_RETENTION_HOURS))),
    ),
    retention_bookticker_hours=max(
        1.0,
        float(os.getenv("GORILA_RETENTION_BOOKTICKER_HOURS", str(BINANCE_PROSPECTIVE_RETENTION_HOURS))),
    ),
    retention_depth_hours=max(
        0.25,
        float(os.getenv("GORILA_RETENTION_DEPTH_HOURS", "1")),
    ),
    persistence_spool_path=os.getenv(
        "GORILA_PERSISTENCE_SPOOL_PATH",
        "/tmp/gorila_crypto_evidence_spool.sqlite3",
    ).strip(),
    persistence_spool_max_bytes=max(
        1024 * 1024,
        int(os.getenv("GORILA_PERSISTENCE_SPOOL_MAX_MB", "256")) * 1024 * 1024,
    ),
    persistence_spool_max_batches=max(
        1,
        int(os.getenv("GORILA_PERSISTENCE_SPOOL_MAX_BATCHES", "100000")),
    ),
    persistence_spool_guard_ratio=min(
        0.99,
        max(0.50, float(os.getenv("GORILA_PERSISTENCE_SPOOL_GUARD_RATIO", "0.90"))),
    ),
    durability_live_max_age_seconds=max(
        30.0,
        float(os.getenv("GORILA_DURABILITY_LIVE_MAX_AGE_SECONDS", "60")),
    ),
)

settings.validate()
