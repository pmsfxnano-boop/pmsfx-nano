"""Isolated Crypto application boundary with optional prospective capture.

The default state remains inert. A dedicated cleanroom deployment can enable the
Binance ingestion worker via environment configuration; production/legacy domains
are not modified by this module.
"""

from __future__ import annotations

import json
import threading
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException

from gorila_core.market_freshness import (
    DELAYED_MAX_AGE_SECONDS,
    LIVE_MAX_AGE_SECONDS,
)
from gorila_crypto.config import settings
from gorila_crypto.quality import DataQualityConfig, evaluate_replay_quality, quality_fingerprint
from gorila_crypto.runtime import ProspectiveCryptoIngestor, build_market_adapter
from gorila_crypto.storage import CryptoStore
from gorila_crypto.ledger import replay_fingerprint as compute_replay_fingerprint


_runtime: ProspectiveCryptoIngestor | None = None
_runtime_thread: threading.Thread | None = None
_quality_thread: threading.Thread | None = None
_heartbeat_thread: threading.Thread | None = None
_stop_event = threading.Event()
_capture_block_reason: str | None = None


def _new_store() -> CryptoStore:
    return CryptoStore(require_durable=settings.ingest_enabled)


def _safe_store_stats() -> dict[str, Any] | None:
    if not settings.ingest_enabled:
        return None
    try:
        return _new_store().prospective_stats(
            source_prefix=f"{settings.provider}.websocket.",
        )
    except RuntimeError as exc:
        return None


def _storage_backend_status() -> str:
    if not settings.ingest_enabled:
        return "NOT_REQUIRED"
    try:
        return _new_store().backend
    except RuntimeError:
        return "BLOCKED_NO_DURABLE_STORAGE"


def _heartbeat_loop() -> None:
    store = _new_store()
    while not _stop_event.is_set():
        try:
            stats = store.prospective_stats(
                source_prefix=f"{settings.provider}.websocket.",
            )
            print(
                "GORILA_CAPTURE_HEARTBEAT "
                + json.dumps(
                    {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "stats": stats,
                    },
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
        except Exception as exc:
            print(
                "GORILA_CAPTURE_HEARTBEAT_ERROR "
                + f"{type(exc).__name__}: {exc}",
                flush=True,
            )
        _stop_event.wait(settings.heartbeat_interval_seconds)


def _required_quality_event_types() -> tuple[str, ...]:
    event_types = ["trade"]
    if settings.provider == "kraken":
        if "bookTicker" in settings.streams or "depth" in settings.streams:
            event_types.append("bookUpdate")
    else:
        if "bookTicker" in settings.streams:
            event_types.append("bookTicker")
        if "depth" in settings.streams:
            event_types.append("depthUpdate")
    return tuple(dict.fromkeys(event_types))


def _quality_loop() -> None:
    store = _new_store()
    required_event_types = _required_quality_event_types()
    config = DataQualityConfig(
        min_rows_per_symbol=settings.quality_min_rows_per_symbol,
        min_duration_seconds=settings.quality_min_duration_seconds,
        max_p99_transport_latency_ms=settings.quality_max_p99_transport_latency_ms,
        required_event_types=required_event_types,
        required_event_type_min_rows={
            event_type: settings.quality_min_rows_per_symbol
            for event_type in required_event_types
        },
        required_integrity_event_types=("bookUpdate",) if settings.provider == "kraken" else (),
    )
    while not _stop_event.is_set():
        try:
            rows = store.read_events(
                source_prefix=f"{settings.provider}.websocket.",
                order="ingest",
                limit=settings.quality_row_limit,
                include_payload=False,
            )
            if rows:
                replay_fp = compute_replay_fingerprint(rows)
                report = evaluate_replay_quality(
                    rows,
                    replay_fingerprint=replay_fp,
                    config=config,
                    gap_rows=store.read_data_gaps(
                        source_prefix=f"{settings.provider}.websocket.",
                        limit=10000,
                    ),
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
    global _runtime, _runtime_thread, _quality_thread, _heartbeat_thread, _capture_block_reason
    _stop_event.clear()
    _capture_block_reason = None

    if settings.ingest_enabled:
        try:
            store = _new_store()
        except RuntimeError as exc:
            _capture_block_reason = str(exc)
            print(
                "GORILA_CAPTURE_BLOCKED "
                + json.dumps(
                    {
                        "reason": _capture_block_reason,
                        "provider": settings.provider,
                        "symbols": list(settings.symbols),
                        "capture_block_reason": _capture_block_reason,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        else:
            adapter = build_market_adapter()
            _runtime = ProspectiveCryptoIngestor(store, adapter)
            _runtime_thread = threading.Thread(
                target=_runtime.run,
                name="gorila-crypto-ingest",
                daemon=True,
            )
            _runtime_thread.start()

            _heartbeat_thread = threading.Thread(
                target=_heartbeat_loop,
                name="gorila-crypto-heartbeat",
                daemon=True,
            )
            _heartbeat_thread.start()

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
    if _heartbeat_thread is not None:
        _heartbeat_thread.join(timeout=5.0)


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
    stats = _safe_store_stats()
    if stats is not None:
        print("GORILA_PROSPECTIVE_STATS " + json.dumps(stats, sort_keys=True, default=str), flush=True)
    return {
        "service": "gorila-crypto",
        "domain": "crypto",
        "status": (
            "CAPTURE_BLOCKED_NO_DURABLE_STORAGE"
            if _capture_block_reason
            else "CAPTURE_ENABLED" if settings.ingest_enabled else "READY"
        ),
        "runtime_isolated": True,
        "provider": settings.provider,
        "prospective_capture": settings.ingest_enabled and not _capture_block_reason,
        "capture_block_reason": _capture_block_reason,
        "symbols": list(settings.symbols),
        "streams": list(settings.streams),
        "forecast_status": "BLOCKED_NO_VALIDATED_MODEL",
        "automatic_promotion": False,
        "execution": False,
        "ledger_stats": stats,
    }


@app.head("/", include_in_schema=False)
def root_head() -> None:
    return None


@app.get("/api/crypto/health")
def health() -> dict[str, Any]:
    worker_alive = bool(_runtime_thread and _runtime_thread.is_alive())
    capture_enabled = settings.ingest_enabled and not _capture_block_reason
    payload = {
        "service": "gorila-crypto",
        "domain": "crypto",
        "status": (
            "CAPTURE_BLOCKED_NO_DURABLE_STORAGE"
            if _capture_block_reason
            else "CAPTURE_ENABLED" if settings.ingest_enabled else "READY"
        ),
        "runtime_isolated": True,
        "provider": settings.provider,
        "prospective_capture": capture_enabled,
        "capture_block_reason": _capture_block_reason,
        "worker_alive": worker_alive,
        "quality_monitor_alive": bool(_quality_thread and _quality_thread.is_alive()),
        "heartbeat_alive": bool(_heartbeat_thread and _heartbeat_thread.is_alive()),
        "symbols": list(settings.symbols),
        "streams": list(settings.streams),
        "freshness_contract": {
            "live_max_age_seconds": LIVE_MAX_AGE_SECONDS,
            "delayed_max_age_seconds": DELAYED_MAX_AGE_SECONDS,
        },
        "storage_backend": _storage_backend_status(),
        "health_contract": "lightweight_no_ledger_scan",
    }
    if capture_enabled and not worker_alive:
        payload["status"] = "CAPTURE_WORKER_DEAD"
        raise HTTPException(status_code=503, detail=payload)
    return payload


@app.get("/api/crypto/prospective/status")
def prospective_status() -> dict[str, Any]:
    if _capture_block_reason:
        return {
            "status": "CAPTURE_BLOCKED_NO_DURABLE_STORAGE",
            "worker_alive": False,
            "ledger": None,
            "source_health": [],
            "capture_block_reason": _capture_block_reason,
            "automatic_promotion": False,
            "execution": False,
        }
    store = _new_store()
    health_rows = store.health(
        source_prefix=f"{settings.provider}.websocket.",
    )
    stats = store.prospective_stats(
        source_prefix=f"{settings.provider}.websocket.",
    )
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
        "provider": settings.provider,
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
        "quality_required_event_types": list(_required_quality_event_types()),
        "quality_required_event_type_min_rows": {
            event_type: settings.quality_min_rows_per_symbol
            for event_type in _required_quality_event_types()
        },
        "quality_required_integrity_event_types": (
            ["bookUpdate"] if settings.provider == "kraken" else []
        ),
        "durable_storage_required_when_ingesting": settings.ingest_enabled,
        "storage_backend": _storage_backend_status(),
    }
