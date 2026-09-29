"""Isolated Crypto application boundary with optional prospective capture.

The default state remains inert. A dedicated cleanroom deployment can enable the
Binance ingestion worker via environment configuration; production/legacy domains
are not modified by this module.
"""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI

from gorila_core.market_freshness import (
    DELAYED_MAX_AGE_SECONDS,
    LIVE_MAX_AGE_SECONDS,
)
from gorila_crypto.binance import BinanceSpotMarketAdapter, BinanceStreamConfig
from gorila_crypto.config import settings
from gorila_crypto.quality import DataQualityConfig, evaluate_replay_quality, quality_fingerprint
from gorila_crypto.runtime import ProspectiveCryptoIngestor
from gorila_crypto.storage import CryptoStore
from gorila_crypto.ledger import replay_fingerprint as compute_replay_fingerprint


_runtime: ProspectiveCryptoIngestor | None = None
_runtime_thread: threading.Thread | None = None
_quality_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _new_store() -> CryptoStore:
    return CryptoStore()


def _quality_loop() -> None:
    store = _new_store()
    config = DataQualityConfig(
        min_rows_per_symbol=settings.quality_min_rows_per_symbol,
        min_duration_seconds=settings.quality_min_duration_seconds,
        max_p99_transport_latency_ms=settings.quality_max_p99_transport_latency_ms,
        required_event_types=("trade",),
    )
    while not _stop_event.is_set():
        try:
            rows = store.read_events(order="ingest", limit=settings.quality_row_limit)
            if rows:
                replay_fp = compute_replay_fingerprint(rows)
                report = evaluate_replay_quality(
                    rows,
                    replay_fingerprint=replay_fp,
                    config=config,
                    gap_rows=store.read_data_gaps(limit=10000),
                    reference_time=datetime.now(timezone.utc),
                )
                report_json = asdict(report)
                store.save_quality_report(report_json, quality_fingerprint(report))
        except Exception as exc:
            try:
                store.record_connection(
                    source="gorila.crypto.quality_monitor",
                    status="ERROR",
                    reason=f"{type(exc).__name__}: {exc}",
                    metadata={"interval_seconds": settings.quality_interval_seconds},
                )
            except Exception:
                pass
        _stop_event.wait(settings.quality_interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _runtime, _runtime_thread, _quality_thread
    _stop_event.clear()

    if settings.ingest_enabled:
        store = _new_store()
        adapter = BinanceSpotMarketAdapter(
            BinanceStreamConfig(
                symbols=settings.symbols,
                streams=settings.streams,
                depth_speed=settings.depth_speed,
            )
        )
        _runtime = ProspectiveCryptoIngestor(store, adapter)
        _runtime_thread = threading.Thread(
            target=_runtime.run,
            name="gorila-crypto-ingest",
            daemon=True,
        )
        _runtime_thread.start()

        if settings.quality_monitor_enabled:
            _quality_thread = threading.Thread(
                target=_quality_loop,
                name="gorila-crypto-quality",
                daemon=True,
            )
            _quality_thread.start()

    yield

    _stop_event.set()
    if _runtime is not None:
        _runtime.stop()
    if _runtime_thread is not None:
        _runtime_thread.join(timeout=5.0)
    if _quality_thread is not None:
        _quality_thread.join(timeout=5.0)


app = FastAPI(
    title="Gorila Crypto Cleanroom",
    version="0.2.0-cleanroom",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root() -> dict[str, Any]:
    stats = _new_store().prospective_stats() if settings.ingest_enabled else None
    return {
        "service": "gorila-crypto",
        "domain": "crypto",
        "status": "CAPTURE_ENABLED" if settings.ingest_enabled else "READY",
        "runtime_isolated": True,
        "prospective_capture": settings.ingest_enabled,
        "symbols": list(settings.symbols),
        "streams": list(settings.streams),
        "forecast_status": "BLOCKED_NO_VALIDATED_MODEL",
        "automatic_promotion": False,
        "execution": False,
        "ledger_stats": stats,
    }


@app.get("/api/crypto/health")
def health() -> dict[str, Any]:
    return {
        "service": "gorila-crypto",
        "domain": "crypto",
        "status": "CAPTURE_ENABLED" if settings.ingest_enabled else "READY",
        "runtime_isolated": True,
        "prospective_capture": settings.ingest_enabled,
        "worker_alive": bool(_runtime_thread and _runtime_thread.is_alive()),
        "quality_monitor_alive": bool(_quality_thread and _quality_thread.is_alive()),
        "symbols": list(settings.symbols),
        "streams": list(settings.streams),
        "forecast": {
            "status": "BLOCKED_NO_VALIDATED_MODEL",
            "semantics": "P(SIGNED_TARGET_RETURN_BPS_POSITIVE)",
            "automatic_promotion": False,
        },
        "freshness_contract": {
            "live_max_age_seconds": LIVE_MAX_AGE_SECONDS,
            "delayed_max_age_seconds": DELAYED_MAX_AGE_SECONDS,
        },
        "ledger": _new_store().prospective_stats(),
    }


@app.get("/api/crypto/prospective/status")
def prospective_status() -> dict[str, Any]:
    store = _new_store()
    health_rows = store.health()
    stats = store.prospective_stats()
    return {
        "status": "CAPTURE_ENABLED" if settings.ingest_enabled else "CAPTURE_DISABLED",
        "worker_alive": bool(_runtime_thread and _runtime_thread.is_alive()),
        "ledger": stats,
        "source_health": health_rows,
        "automatic_promotion": False,
        "execution": False,
    }


@app.get("/api/crypto/config")
def config_snapshot() -> dict[str, Any]:
    return {
        "environment": settings.environment,
        "symbols": list(settings.symbols),
        "streams": list(settings.streams),
        "depth_speed": settings.depth_speed,
        "ingest_enabled": settings.ingest_enabled,
        "quality_monitor_enabled": settings.quality_monitor_enabled,
        "quality_interval_seconds": settings.quality_interval_seconds,
        "quality_row_limit": settings.quality_row_limit,
        "quality_min_rows_per_symbol": settings.quality_min_rows_per_symbol,
        "quality_min_duration_seconds": settings.quality_min_duration_seconds,
        "quality_max_p99_transport_latency_ms": settings.quality_max_p99_transport_latency_ms,
    }
