"""Isolated Crypto application boundary with optional prospective capture.

The default state remains inert. A dedicated cleanroom deployment can enable the
Binance ingestion worker via environment configuration; production/legacy domains
are not modified by this module.
"""

from __future__ import annotations

import json
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from gorila_core.market_freshness import (
    DELAYED_MAX_AGE_SECONDS,
    LIVE_MAX_AGE_SECONDS,
)
from gorila_crypto.config import settings
from gorila_crypto.quality import DataQualityConfig, evaluate_replay_quality, quality_fingerprint
from gorila_crypto.runtime import ProspectiveCryptoIngestor, build_market_adapter
from gorila_crypto.market_cache import MARKET_CACHE
from gorila_crypto.evidence import build_evidence_snapshot
from gorila_crypto.storage import CryptoStore
from gorila_crypto.quant_store import QuantCryptoStore
from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.ledger import replay_fingerprint as compute_replay_fingerprint
from gorila_crypto.research_runner import run_crypto_research_once


_runtime: ProspectiveCryptoIngestor | None = None
_runtime_thread: threading.Thread | None = None
_quality_thread: threading.Thread | None = None
_research_thread: threading.Thread | None = None
_heartbeat_thread: threading.Thread | None = None
_maintenance_thread: threading.Thread | None = None
_stop_event = threading.Event()
_capture_block_reason: str | None = None


def _retire_legacy_objects_if_enabled(store: CryptoStore) -> None:
    flag = __import__("os").getenv("GORILA_CRYPTO_RETIRE_LEGACY", "").strip().lower()
    if flag not in {"1", "true", "yes", "on"}:
        return
    if not store.durable:
        raise RuntimeError("legacy_retirement_requires_durable_postgres")
    conn = store.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_legacy_retirement (
                    retirement_id TEXT PRIMARY KEY,
                    executed_at TEXT NOT NULL,
                    code_version TEXT,
                    dropped_schema TEXT NOT NULL,
                    dropped_public_tables_json TEXT NOT NULL
                )
                """
            )
            cur.execute(
                "DROP SCHEMA IF EXISTS gorila_argentum CASCADE"
            )
            cur.execute(
                """
                DROP TABLE IF EXISTS
                    public.backtest_runs,
                    public.forecast_outcomes,
                    public.forecasts,
                    public.model_registry,
                    public.online_cohort_samples,
                    public.research_runs
                CASCADE
                """
            )
            cur.execute(
                """
                INSERT INTO crypto_legacy_retirement(
                    retirement_id, executed_at, code_version,
                    dropped_schema, dropped_public_tables_json
                )
                VALUES(%s,%s,%s,%s,%s)
                """,
                (
                    __import__("uuid").uuid4().hex,
                    datetime.now(timezone.utc).isoformat(),
                    __import__("os").getenv("RENDER_GIT_COMMIT")
                    or __import__("os").getenv("GORILA_CRYPTO_CODE_VERSION"),
                    "gorila_argentum",
                    json.dumps(
                        [
                            "public.backtest_runs",
                            "public.forecast_outcomes",
                            "public.forecasts",
                            "public.model_registry",
                            "public.online_cohort_samples",
                            "public.research_runs",
                        ],
                        sort_keys=True,
                    ),
                ),
            )
        conn.commit()
    finally:
        conn.close()



def _new_store() -> CryptoStore:
    return QuantCryptoStore(require_durable=settings.ingest_enabled)


def _safe_store_stats() -> dict[str, Any] | None:
    if not settings.ingest_enabled:
        return None
    try:
        store = _new_store()
        if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider:
            session_id = store.active_capture_session(PREREGISTERED_CRYPTO_PROTOCOL.study_id)
            return store.scoped_stats(
                study_id=PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                capture_session_id=session_id,
            )
        return store.prospective_stats(
            source_prefix=f"{settings.provider}.websocket.",
        )
    except RuntimeError:
        return None


def _runtime_operational_snapshot() -> dict[str, Any]:
    runtime = _runtime
    if runtime is None:
        return {
            "market_plane": "STARTING",
            "durability": "UNKNOWN",
            "persistence_queue_batches": 0,
        }
    method = getattr(runtime, "operational_snapshot", None)
    if callable(method):
        try:
            return dict(method())
        except Exception:
            pass
    return {
        "market_plane": "LIVE" if runtime is not None else "STARTING",
        "durability": "UNKNOWN",
        "persistence_queue_batches": 0,
    }


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
            now = datetime.now(timezone.utc)
            if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider:
                session_id = store.active_capture_session(PREREGISTERED_CRYPTO_PROTOCOL.study_id)
                conn = store.connect()
                try:
                    cutoff = (now - timedelta(seconds=60)).isoformat()
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT symbol, COUNT(*) AS rows, MAX(received_time) AS last_received
                            FROM crypto_events
                            WHERE received_time >= %s
                              AND source LIKE %s
                            GROUP BY symbol
                            ORDER BY symbol
                            """,
                            (cutoff, "binance.websocket.%"),
                        )
                        rows = cur.fetchall()
                    stats = {
                        "backend": store.backend,
                        "capture_session_id": session_id,
                        "window_seconds": 60,
                        "symbols": [
                            {
                                "symbol": str(row[0]),
                                "rows": int(row[1]),
                                "last_received": str(row[2]),
                            }
                            for row in rows
                        ],
                    }
                finally:
                    conn.close()
            else:
                stats = store.prospective_stats(
                    source_prefix=f"{settings.provider}.websocket.",
                )
            print(
                "GORILA_CAPTURE_HEARTBEAT "
                + json.dumps(
                    {
                        "at": now.isoformat(),
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


def _maintenance_loop() -> None:
    """Run bounded storage maintenance outside the market hot path."""
    store = _new_store()
    first_run = True
    while not _stop_event.is_set():
        try:
            result = store.maintain_storage(
                trade_retention_hours=settings.retention_trade_hours,
                bookticker_retention_hours=settings.retention_bookticker_hours,
                depth_retention_hours=settings.retention_depth_hours,
                vacuum=first_run,
            )
            print(
                "GORILA_STORAGE_MAINTENANCE "
                + json.dumps(
                    result,
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
            first_run = False
        except Exception as exc:
            print(
                "GORILA_STORAGE_MAINTENANCE_ERROR "
                + f"{type(exc).__name__}: {exc}",
                flush=True,
            )
        _stop_event.wait(settings.storage_maintenance_interval_seconds)

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
    if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider:
        quality_spec = PREREGISTERED_CRYPTO_PROTOCOL.quality_config()
        config = DataQualityConfig(
            min_rows_per_symbol=int(quality_spec["min_rows_per_symbol"]),
            min_duration_seconds=float(quality_spec["min_duration_seconds"]),
            max_p99_transport_latency_ms=float(quality_spec["max_p99_transport_latency_ms"]),
            required_event_types=tuple(quality_spec["required_event_types"]),
            required_event_type_min_rows=dict(quality_spec["required_event_type_min_rows"]),
        )
    else:
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
            if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider:
                session_id = store.active_capture_session(PREREGISTERED_CRYPTO_PROTOCOL.study_id)
                if session_id is None:
                    _stop_event.wait(settings.quality_interval_seconds)
                    continue
                rows = store.read_scoped_events(
                    study_id=PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                    capture_session_id=session_id,
                    source_prefix="binance.websocket.",
                    order="ingest",
                    limit=settings.quality_row_limit,
                    include_payload=False,
                )
                gap_rows = store.read_scoped_data_gaps(
                    study_id=PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                    capture_session_id=session_id,
                    source_prefix="binance.websocket.",
                    limit=10000,
                )
            else:
                rows = store.read_events(
                    source_prefix=f"{settings.provider}.websocket.",
                    order="ingest",
                    limit=settings.quality_row_limit,
                    include_payload=False,
                )
                gap_rows = store.read_data_gaps(
                    source_prefix=f"{settings.provider}.websocket.",
                    limit=10000,
                )
            if rows:
                replay_fp = compute_replay_fingerprint(rows)
                report = evaluate_replay_quality(
                    rows,
                    replay_fingerprint=replay_fp,
                    config=config,
                    gap_rows=gap_rows,
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


def _research_loop() -> None:
    store = _new_store()
    while not _stop_event.is_set():
        try:
            gate = (
                _runtime.recovery_gate()
                if _runtime is not None
                else {"status": "BLOCKED", "reasons": ["RUNTIME_NOT_READY"]}
            )
            if gate.get("status") != "PASS":
                print(
                    "GORILA_CRYPTO_RESEARCH_BLOCKED "
                    + json.dumps(gate, sort_keys=True, default=str),
                    flush=True,
                )
                _stop_event.wait(settings.research_interval_seconds)
                continue

            result = run_crypto_research_once(store)
            print(
                "GORILA_CRYPTO_RESEARCH "
                + json.dumps(result, sort_keys=True, default=str),
                flush=True,
            )
        except Exception as exc:
            try:
                store.record_connection(
                    source="gorila.crypto.research_runner",
                    status="ERROR",
                    reason=f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                pass
            print(
                "GORILA_CRYPTO_RESEARCH_ERROR "
                + f"{type(exc).__name__}: {exc}",
                flush=True,
            )
        _stop_event.wait(settings.research_interval_seconds)




@asynccontextmanager
async def lifespan(app: FastAPI):
    global _runtime, _runtime_thread, _quality_thread, _research_thread, _heartbeat_thread, _maintenance_thread, _capture_block_reason
    _stop_event.clear()
    _capture_block_reason = None

    retirement_flag = os.getenv("GORILA_CRYPTO_RETIRE_LEGACY", "").strip().lower()
    print(
        "GORILA_LEGACY_RETIREMENT_CHECK "
        + json.dumps(
            {
                "enabled": retirement_flag in {"1", "true", "yes", "on"},
                "ingest_enabled": settings.ingest_enabled,
                "database_configured": bool(CryptoStore().database_url),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if retirement_flag in {"1", "true", "yes", "on"}:
        try:
            _retire_legacy_objects_if_enabled(_new_store())
        except Exception as exc:
            _capture_block_reason = f"LEGACY_RETIREMENT_FAILED:{type(exc).__name__}:{exc}"
            print(
                "GORILA_LEGACY_RETIREMENT_FAILED "
                + _capture_block_reason,
                flush=True,
            )

    if settings.ingest_enabled and _capture_block_reason is None:
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

            _maintenance_thread = threading.Thread(
                target=_maintenance_loop,
                name="gorila-crypto-storage-maintenance",
                daemon=True,
            )
            _maintenance_thread.start()

            if settings.quality_monitor_enabled:
                _quality_thread = threading.Thread(
                    target=_quality_loop,
                    name="gorila-crypto-quality",
                    daemon=True,
                )
                _quality_thread.start()

            if settings.research_enabled:
                _research_thread = threading.Thread(
                    target=_research_loop,
                    name="gorila-crypto-research",
                    daemon=True,
                )
                _research_thread.start()

    yield

    _stop_event.set()
    if _runtime is not None:
        _runtime.stop()
    if _runtime_thread is not None:
        _runtime_thread.join(timeout=5.0)
    if _quality_thread is not None:
        _quality_thread.join(timeout=5.0)
    if _research_thread is not None:
        _research_thread.join(timeout=5.0)
    if _heartbeat_thread is not None:
        _heartbeat_thread.join(timeout=5.0)
    if _maintenance_thread is not None:
        _maintenance_thread.join(timeout=5.0)


app = FastAPI(
    title="Gorila Crypto Cleanroom",
    version="0.2.0-cleanroom",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)



def _cors_origins() -> list[str]:
    raw = os.getenv("GORILA_CRYPTO_CORS_ORIGINS", "*").strip()
    if not raw or raw == "*":
        return ["*"]
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=False,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
    max_age=600,
)


@app.get("/", include_in_schema=False)
def root() -> dict[str, Any]:
    stats = _runtime_operational_snapshot() if _runtime is not None else None
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



@app.get("/api/crypto/market/stream")
def market_stream(cursor: int = 0, limit: int = 360) -> dict[str, Any]:
    """Low-latency market contract backed by the independent hot market plane."""
    if not settings.ingest_enabled:
        raise HTTPException(status_code=503, detail="capture_not_enabled")

    safe_limit = max(32, min(int(limit), 600))
    symbols = tuple(settings.symbols)
    snapshot = MARKET_CACHE.snapshot(
        symbols=symbols,
        cursor=int(cursor),
        limit=safe_limit,
    )
    events = list(snapshot["events"])
    latest_book = dict(snapshot["latest_books"])

    health_rows = _runtime.symbol_health() if _runtime is not None else []
    health_by_symbol = {str(row.get("symbol")): row for row in health_rows}

    latest_price: dict[str, float] = {}
    first_price: dict[str, float] = {}
    for event in events:
        if event["event_type"] == "trade" and event["price"] is not None:
            first_price.setdefault(event["symbol"], float(event["price"]))
            latest_price[event["symbol"]] = float(event["price"])

    now = datetime.now(timezone.utc)
    summary: list[dict[str, Any]] = []
    for symbol in symbols:
        book = latest_book.get(symbol)
        price = latest_price.get(symbol)
        if price is None and book:
            price = float(book["price"])

        window_change = None
        if symbol in first_price and price is not None and first_price[symbol]:
            window_change = (price / first_price[symbol] - 1.0) * 100.0

        trade_times = [
            event["received_time"]
            for event in events
            if event["symbol"] == symbol and event["event_type"] == "trade"
        ]
        freshest = list(trade_times)
        if book:
            freshest.append(str(book["received_time"]))

        freshness_ms = None
        if freshest:
            try:
                newest = max(freshest)
                freshness_ms = max(
                    0.0,
                    (
                        now
                        - datetime.fromisoformat(newest.replace("Z", "+00:00"))
                    ).total_seconds()
                    * 1000.0,
                )
            except ValueError:
                freshness_ms = None

        status = health_by_symbol.get(symbol, {}).get("status", "UNKNOWN")
        spread_bps = None
        imbalance = None
        if book and book.get("bid") is not None and book.get("ask") is not None:
            bid_value = float(book["bid"])
            ask_value = float(book["ask"])
            if bid_value > 0:
                spread_bps = (ask_value / bid_value - 1.0) * 10000.0
            bid_qty_value = float(book.get("bid_qty") or 0.0)
            ask_qty_value = float(book.get("ask_qty") or 0.0)
            denom = bid_qty_value + ask_qty_value
            if denom > 0:
                imbalance = (bid_qty_value - ask_qty_value) / denom

        summary.append(
            {
                "symbol": symbol,
                "status": status,
                "price": price,
                "window_change_pct": window_change,
                "bid": book["bid"] if book else None,
                "ask": book["ask"] if book else None,
                "bid_qty": book["bid_qty"] if book else None,
                "ask_qty": book["ask_qty"] if book else None,
                "spread_bps": spread_bps,
                "imbalance": imbalance,
                "freshness_ms": freshness_ms,
                "last_trade_time": trade_times[-1] if trade_times else None,
                "last_book_time": book["received_time"] if book else None,
            }
        )

    all_symbols_live = bool(summary) and all(row["status"] == "LIVE" for row in summary)
    durability = snapshot.get("persistence") or {}
    durable_healthy = not bool(durability.get("degraded"))
    return {
        "status": "LIVE" if all_symbols_live and events else "DEGRADED",
        "market_status": "LIVE" if all_symbols_live and events else "DEGRADED",
        "durability_status": "LIVE" if durable_healthy else "DEGRADED",
        "server_time": now.isoformat(),
        "next_cursor": snapshot["next_cursor"],
        "cursor_kind": snapshot["cursor_kind"],
        "last_durable_stream_seq": snapshot["last_durable_stream_seq"],
        "symbols": summary,
        "events": events,
        "cache_events_available": snapshot["cache_events_available"],
        "persistence": durability,
        "forecast": {"automatic_promotion": False, "execution": False},
    }


@app.get("/api/crypto/market/history")
def market_history(
    symbol: str,
    resolution: str = "5m",
    limit: int = 240,
) -> dict[str, Any]:
    """Bounded historical OHLCV contract for charting outside the hot path."""
    if not settings.ingest_enabled:
        raise HTTPException(status_code=503, detail="capture_not_enabled")

    resolutions = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    bucket_seconds = resolutions.get(str(resolution).lower())
    if bucket_seconds is None:
        raise HTTPException(status_code=400, detail="unsupported_resolution")

    normalized = str(symbol).upper().strip()
    if normalized not in {str(item).upper() for item in settings.symbols}:
        raise HTTPException(status_code=400, detail="unsupported_symbol")

    safe_limit = max(30, min(int(limit), 600))
    lookback_seconds = int(bucket_seconds * safe_limit * 1.15)
    start_time = (
        datetime.now(timezone.utc) - timedelta(seconds=lookback_seconds)
    ).isoformat()

    store = _new_store()
    if not store.durable:
        raise HTTPException(status_code=503, detail="durable_storage_required")

    conn = store.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH buckets AS (
                    SELECT
                        to_timestamp(
                            floor(
                                extract(epoch from event_time::timestamptz) / %s
                            ) * %s
                        ) AS bucket,
                        event_time,
                        ledger_seq,
                        (payload_json::jsonb->>'p')::double precision AS price,
                        (payload_json::jsonb->>'q')::double precision AS quantity
                    FROM crypto_events
                    WHERE symbol=%s
                      AND event_type='trade'
                      AND event_time >= %s
                ),
                grouped AS (
                    SELECT
                        bucket,
                        count(*)::bigint AS trades,
                        sum(quantity)::double precision AS volume,
                        min(price)::double precision AS low,
                        max(price)::double precision AS high,
                        (array_agg(price ORDER BY event_time, ledger_seq))[1]::double precision AS open,
                        (array_agg(price ORDER BY event_time DESC, ledger_seq DESC))[1]::double precision AS close
                    FROM buckets
                    GROUP BY bucket
                    ORDER BY bucket DESC
                    LIMIT %s
                )
                SELECT
                    bucket AT TIME ZONE 'UTC' AS bucket,
                    open, high, low, close, volume, trades
                FROM grouped
                ORDER BY bucket ASC
                """,
                (
                    bucket_seconds,
                    bucket_seconds,
                    normalized,
                    start_time,
                    safe_limit,
                ),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    candles = [
        {
            "time": row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "trades": int(row[6]),
        }
        for row in rows
    ]

    return {
        "symbol": normalized,
        "resolution": resolution.lower(),
        "candles": candles,
    }


@app.get("/api/crypto/evidence")
def evidence_snapshot() -> dict[str, Any]:
    """Unified, bounded evidence read contract for the quant UI."""
    if not settings.ingest_enabled:
        raise HTTPException(status_code=503, detail="capture_not_enabled")
    try:
        payload = build_evidence_snapshot()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=f"evidence_unavailable:{exc}") from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"evidence_unavailable:{type(exc).__name__}",
        ) from exc
    return payload


@app.get("/api/crypto/operational/e2e")
def operational_e2e() -> dict[str, Any]:
    """
    Bounded end-to-end operational probe.

    This endpoint intentionally sits off the hot market path. It cross-checks
    the in-process market cursor/freshness plane against durable ledger
    reachability, bounded history and the quantitative evidence contract.
    It never promotes a model and returns DEGRADED rather than masking any
    unavailable stage.
    """
    started = time.perf_counter()
    failures: list[str] = []

    # Hot stream plane.
    stream_started = time.perf_counter()
    stream = market_stream(cursor=0, limit=180)
    stream_latency_ms = (time.perf_counter() - stream_started) * 1000.0
    events = list(stream.get("events") or [])
    stream_seqs = [int(event["stream_seq"]) for event in events if event.get("stream_seq") is not None]
    event_keys = [str(event["event_key"]) for event in events if event.get("event_key")]
    cursor_integrity = (
        stream_seqs == sorted(stream_seqs)
        and len(stream_seqs) == len(set(stream_seqs))
        and int(stream.get("next_cursor") or 0) >= (max(stream_seqs) if stream_seqs else 0)
    )
    if not cursor_integrity:
        failures.append("STREAM_CURSOR_INTEGRITY")

    freshness = {
        str(row["symbol"]): {
            "status": row.get("status"),
            "freshness_ms": row.get("freshness_ms"),
        }
        for row in (stream.get("symbols") or [])
    }
    if any(row.get("status") != "LIVE" for row in freshness.values()):
        failures.append("REQUIRED_SYMBOL_NOT_LIVE")

    now = datetime.now(timezone.utc)
    received_times = []
    transport_latencies_ms = []
    for event in events:
        received = event.get("received_time")
        event_time = event.get("event_time")
        if received:
            try:
                received_dt = datetime.fromisoformat(str(received).replace("Z", "+00:00"))
                received_times.append(received_dt)
            except ValueError:
                pass
        if event_time and received:
            try:
                event_dt = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
                received_dt = datetime.fromisoformat(str(received).replace("Z", "+00:00"))
                transport_latencies_ms.append(max(0.0, (received_dt - event_dt).total_seconds() * 1000.0))
            except ValueError:
                pass

    event_rate_eps = None
    if len(received_times) >= 2:
        span_s = max(0.001, (max(received_times) - min(received_times)).total_seconds())
        event_rate_eps = len(received_times) / span_s

    runtime = _runtime_operational_snapshot()
    if runtime.get("persistence_dropped_events", 0):
        failures.append("PERSISTENCE_DROPS")
    if runtime.get("evidence_spool", {}).get("batches", 0):
        failures.append("EVIDENCE_SPOOL_PENDING")
    if runtime.get("persistence_queue_batches", 0):
        failures.append("PERSISTENCE_QUEUE_PENDING")
    if runtime.get("durability") != "LIVE":
        failures.append("DURABILITY_NOT_LIVE")

    # Durable/read-model planes are deliberately bounded and independently timed.
    ledger: dict[str, Any] = {"status": "UNAVAILABLE"}
    ledger_latency_ms: float | None = None
    history: dict[str, Any] = {"status": "UNAVAILABLE"}
    history_latency_ms: float | None = None
    evidence: dict[str, Any] = {"status": "UNAVAILABLE"}
    evidence_latency_ms: float | None = None

    ledger_started = time.perf_counter()
    try:
        store = _new_store()
        conn = store.connect()
        try:
            cutoff = (now - timedelta(seconds=60)).isoformat()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COALESCE(MAX(ledger_seq), 0),
                        COUNT(*),
                        MAX(received_time)
                    FROM crypto_events
                    WHERE source LIKE %s
                      AND received_time >= %s
                    """,
                    ("binance.websocket.%", cutoff),
                )
                row = cur.fetchone()
            ledger = {
                "status": "LIVE",
                "last_ledger_seq": int(row[0] or 0),
                "rows_last_60s": int(row[1] or 0),
                "last_received_time": str(row[2]) if row[2] is not None else None,
            }
        finally:
            conn.close()
    except Exception as exc:
        ledger = {
            "status": "UNAVAILABLE",
            "error": f"{type(exc).__name__}: {exc}",
        }
        failures.append("LEDGER_UNAVAILABLE")
    ledger_latency_ms = (time.perf_counter() - ledger_started) * 1000.0

    history_started = time.perf_counter()
    try:
        symbols = list(settings.symbols)
        latest_history: dict[str, Any] = {}
        for symbol in symbols:
            result = market_history(symbol=symbol, resolution="1m", limit=30)
            latest_history[symbol] = {
                "candles": len(result.get("candles") or []),
                "last_time": (
                    result.get("candles")[-1].get("time")
                    if result.get("candles")
                    else None
                ),
            }
        history = {"status": "LIVE", "symbols": latest_history}
    except Exception as exc:
        history = {
            "status": "UNAVAILABLE",
            "error": f"{type(exc).__name__}: {exc}",
        }
        failures.append("HISTORY_UNAVAILABLE")
    history_latency_ms = (time.perf_counter() - history_started) * 1000.0

    evidence_started = time.perf_counter()
    try:
        payload = build_evidence_snapshot()
        evidence = {
            "status": "LIVE",
            "generated_at": payload.get("generated_at"),
            "cohort": payload.get("cohort", {}).get("status"),
            "quality_gate": payload.get("quality_gate", {}).get("state"),
            "pit_oos": payload.get("pit_oos", {}).get("state"),
            "opportunity_clock": payload.get("opportunity_clock", {}).get("state"),
        }
    except Exception as exc:
        evidence = {
            "status": "UNAVAILABLE",
            "error": f"{type(exc).__name__}: {exc}",
        }
        failures.append("EVIDENCE_UNAVAILABLE")
    evidence_latency_ms = (time.perf_counter() - evidence_started) * 1000.0

    if ledger.get("status") != "LIVE":
        failures.append("LEDGER_NOT_LIVE")
    if history.get("status") != "LIVE":
        failures.append("HISTORY_NOT_LIVE")
    if evidence.get("status") != "LIVE":
        failures.append("EVIDENCE_NOT_LIVE")

    # Dedupe reasons so a single outage does not inflate the health state.
    failures = list(dict.fromkeys(failures))
    status = "PASS" if not failures else "DEGRADED"

    return {
        "status": status,
        "checked_at": now.isoformat(),
        "latency_ms": {
            "total": round((time.perf_counter() - started) * 1000.0, 3),
            "stream": round(stream_latency_ms, 3),
            "ledger": round(ledger_latency_ms, 3) if ledger_latency_ms is not None else None,
            "history": round(history_latency_ms, 3) if history_latency_ms is not None else None,
            "evidence": round(evidence_latency_ms, 3) if evidence_latency_ms is not None else None,
        },
        "speed": {
            "stream_events_returned": len(events),
            "estimated_events_per_second": event_rate_eps,
            "transport_latency_ms_max": max(transport_latencies_ms) if transport_latencies_ms else None,
            "transport_latency_ms_median": (
                sorted(transport_latencies_ms)[len(transport_latencies_ms) // 2]
                if transport_latencies_ms
                else None
            ),
        },
        "stream": {
            "status": stream.get("status"),
            "next_cursor": stream.get("next_cursor"),
            "last_durable_stream_seq": stream.get("last_durable_stream_seq"),
            "events_returned": len(events),
            "duplicate_event_keys": len(event_keys) - len(set(event_keys)),
            "cursor_integrity": cursor_integrity,
        },
        "freshness": freshness,
        "runtime": runtime,
        "ledger": ledger,
        "history": history,
        "evidence": evidence,
        "failures": failures,
        "fail_closed": bool(failures),
    }


@app.head("/", include_in_schema=False)
def root_head() -> None:
    return None


@app.get("/api/crypto/health")
def health() -> dict[str, Any]:
    worker_alive = bool(_runtime_thread and _runtime_thread.is_alive())
    capture_enabled = settings.ingest_enabled and not _capture_block_reason
    symbol_health = _runtime.symbol_health() if _runtime is not None else []
    symbols_live = bool(symbol_health) and all(
        row.get("status") == "LIVE" for row in symbol_health
    )
    runtime_snapshot = _runtime_operational_snapshot()
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
        "symbol_health": symbol_health,
        "symbols_live": symbols_live,
        "freshness_contract": {
            "live_max_age_seconds": LIVE_MAX_AGE_SECONDS,
            "delayed_max_age_seconds": DELAYED_MAX_AGE_SECONDS,
        },
        "storage_backend": _storage_backend_status(),
        "market_plane": runtime_snapshot,
        "health_contract": "lightweight_no_ledger_scan",
        "forecast": {
            "automatic_promotion": False,
            "execution": False,
        },
    }
    if not worker_alive and capture_enabled:
        payload["status"] = "CAPTURE_WORKER_DEAD"
    return payload


@app.get("/api/crypto/readiness")
def readiness() -> dict[str, Any]:
    payload = health()
    capture_enabled = settings.ingest_enabled and not _capture_block_reason
    if capture_enabled and not payload.get("symbols_live"):
        payload["status"] = "CAPTURE_DATA_STALE"
        raise HTTPException(status_code=503, detail=payload)
    payload["status"] = "CAPTURE_READY"
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
    symbol_health = _runtime.symbol_health() if _runtime is not None else []
    runtime_snapshot = _runtime_operational_snapshot()
    store = _new_store()
    health_rows = [
        row for row in store.health(source_prefix="binance.websocket.")
        if row.get("last_event_time") is not None
    ]
    return {
        "status": "CAPTURE_ENABLED" if settings.ingest_enabled else "CAPTURE_DISABLED",
        "worker_alive": bool(_runtime_thread and _runtime_thread.is_alive()),
        "ledger": {
            "backend": store.backend,
            "capture_session_id": store.active_capture_session(
                PREREGISTERED_CRYPTO_PROTOCOL.study_id
            ) if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider else None,
            "hot_path": runtime_snapshot,
        },
        "source_health": health_rows,
        "symbol_health": symbol_health,
        "symbols_live": bool(symbol_health) and all(
            row.get("status") == "LIVE" for row in symbol_health
        ),
        "automatic_promotion": False,
        "execution": False,
    }


@app.get("/api/crypto/config")
def config_snapshot() -> dict[str, Any]:
    return {
        "environment": settings.environment,
        "provider": settings.provider,
        "study_id": PREREGISTERED_CRYPTO_PROTOCOL.study_id if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider else None,
        "protocol_version": PREREGISTERED_CRYPTO_PROTOCOL.version if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider else None,
        "protocol_hash": PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash if settings.provider == PREREGISTERED_CRYPTO_PROTOCOL.provider else None,
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
        "persist_bookticker_interval_seconds": settings.persist_bookticker_interval_seconds,
        "persistence_queue_batches": settings.persistence_queue_batches,
        "storage_maintenance_interval_seconds": settings.storage_maintenance_interval_seconds,
        "retention_trade_hours": settings.retention_trade_hours,
        "retention_bookticker_hours": settings.retention_bookticker_hours,
        "retention_depth_hours": settings.retention_depth_hours,
        "persistence_spool_path": settings.persistence_spool_path,
        "persistence_spool_max_bytes": settings.persistence_spool_max_bytes,
        "persistence_spool_max_batches": settings.persistence_spool_max_batches,
        "persistence_spool_guard_ratio": settings.persistence_spool_guard_ratio,
    }