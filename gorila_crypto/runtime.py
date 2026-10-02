"""Prospective multi-venue market-data ingestion for the Crypto cleanroom.

A9 turns the proven adapter into a persistence-only 24/7 research feed.
It records raw normalized events, connection lifecycle, sequence gaps, and
source freshness. It never forecasts, trades, or promotes a model.
"""

from __future__ import annotations

import json
import os
import hashlib
import queue
import threading
import time
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

from gorila_core.market_freshness import assess_observation

from .binance import BinanceSpotMarketAdapter, BinanceStreamConfig, NormalizedMarketEvent
from .kraken import KrakenSpotMarketAdapter, KrakenStreamConfig
from .market_cache import MARKET_CACHE
from .config import settings
from .evidence_spool import EvidenceSpool
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .forecast import FEATURE_SET_VERSION, build_microstructure_feature_vector, feature_set_hash
from .quant_store import QuantCryptoStore
from .storage import CryptoStore, CRYPTO_DATABASE_URL


class MarketAdapterProtocol(Protocol):
    config: Any
    source_family: str

    def iter_forever(
        self,
        *,
        stop_event=None,
        on_connection: Callable[[str, dict[str, Any]], None] | None = None,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 60.0,
    ):
        ...


@dataclass(frozen=True)
class IngestRuntimeConfig:
    kind: str = "PROSPECTIVE_MARKET_INGEST"
    gap_status: str = "GAP_DETECTED"
    health_status: str = "HEALTHY"
    error_status: str = "DEGRADED"
    health_flush_interval_seconds: float = 5.0
    event_batch_size: int = settings.event_batch_size
    event_batch_flush_interval_seconds: float = settings.event_batch_flush_interval_seconds

    def validate(self) -> None:
        if not self.kind.strip():
            raise ValueError("runtime kind cannot be empty")
        if not self.gap_status.strip() or not self.health_status.strip():
            raise ValueError("runtime statuses cannot be empty")
        if self.health_flush_interval_seconds <= 0:
            raise ValueError("health flush interval must be positive")
        if self.event_batch_size < 1:
            raise ValueError("event batch size must be positive")
        if self.event_batch_flush_interval_seconds <= 0:
            raise ValueError("event batch flush interval must be positive")


class SequenceContinuityMonitor:
    """Detect in-connection sequence gaps without inferring missing history."""

    def __init__(self) -> None:
        self.epoch = 0
        self._previous: dict[tuple[str, str], int] = {}

    def new_connection(self) -> None:
        self.epoch += 1
        self._previous.clear()

    def observe(self, event: NormalizedMarketEvent) -> tuple[int, int] | None:
        if event.sequence_start is None or event.sequence_end is None:
            return None
        if event.event_type != "depthUpdate":
            return None

        key = (event.symbol.upper(), event.event_type)
        previous = self._previous.get(key)
        current_start = int(event.sequence_start)
        current_end = int(event.sequence_end)

        if previous is not None and current_start > previous + 1:
            expected = previous + 1
            self._previous[key] = current_end
            return expected, current_start

        self._previous[key] = max(previous or current_end, current_end)
        return None


class ProspectiveCryptoIngestor:
    """Persistence boundary for an offline-replayable prospective market feed."""

    def __init__(
        self,
        store: CryptoStore,
        adapter: MarketAdapterProtocol,
        *,
        config: IngestRuntimeConfig | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.config = config or IngestRuntimeConfig()
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sequence = SequenceContinuityMonitor()
        self.stop_event = threading.Event()
        self.run_id: str | None = None
        self.events_inserted = 0
        self.events_duplicate = 0
        self.gaps_detected = 0
        self.last_error: str | None = None
        self.last_event: NormalizedMarketEvent | None = None
        self.protocol = PREREGISTERED_CRYPTO_PROTOCOL
        self.session_id: str | None = None
        self._last_runtime_heartbeat = 0.0
        self._health_last_persist_monotonic: dict[str, float] = {}
        self._health_last_status: dict[str, str] = {}
        self._health_pending: dict[str, dict[str, Any]] = {}
        self._health_lock = threading.Lock()
        self._symbol_lock = threading.Lock()
        self._symbol_first_received: dict[str, datetime] = {}
        self._symbol_last_received: dict[str, datetime] = {}
        self._capture_started_at = self.now()
        self._pending_event_rows: list[dict[str, Any]] = []
        self._pending_event_started_monotonic: float | None = None
        self._persistence_queue: queue.Queue[list[dict[str, Any]]] = queue.Queue(
            maxsize=max(8, settings.persistence_queue_batches)
        )
        self._persistence_thread: threading.Thread | None = None
        self._persistence_stop = threading.Event()
        self._persistence_error: str | None = None
        self._persistence_last_success_monotonic = 0.0
        self._persistence_dropped_events = 0
        self._bootstrap_thread: threading.Thread | None = None
        self._bootstrap_stop = threading.Event()
        self._persistence_spool_overflow = 0
        self._persistence_spool_error: str | None = None
        self._last_spool_log_monotonic = 0.0
        try:
            self._evidence_spool: EvidenceSpool | None = EvidenceSpool(
                path=settings.persistence_spool_path,
                max_bytes=settings.persistence_spool_max_bytes,
                max_batches=settings.persistence_spool_max_batches,
            )
        except Exception as exc:
            self._evidence_spool = None
            self._persistence_spool_error = (
                f"spool_init:{type(exc).__name__}: {exc}"
            )
        self._last_bookticker_persist_monotonic: dict[str, float] = {}
        self._bookticker_persist_interval = float(
            self.protocol.bookticker_persistence_interval_seconds
        )
        # Compact, non-blocking alpha evidence derived from the live hot plane.
        self._alpha_recent_5s: dict[str, deque[NormalizedMarketEvent]] = {}
        self._alpha_anchor_1s: dict[str, NormalizedMarketEvent | None] = {}
        self._alpha_anchor_candidates: dict[str, deque[NormalizedMarketEvent]] = {}
        self._alpha_latest_book: dict[str, NormalizedMarketEvent] = {}
        self._alpha_last_trigger_monotonic: dict[str, float] = {}
        self._alpha_trigger_times: dict[str, deque[float]] = {}
        self._alpha_min_refractory_seconds = max(1.0, float(self.protocol.refractory_seconds))
        self._alpha_max_triggers_per_minute = 8
        self._alpha_pending_snapshots: list[dict[str, Any]] = []
        self._alpha_pending_started_monotonic: float | None = None
        self._alpha_snapshot_queue: queue.Queue[list[dict[str, Any]]] = queue.Queue(maxsize=64)
        self._alpha_snapshots_created = 0
        self._alpha_snapshots_persisted = 0
        self._alpha_snapshots_dropped = 0

        self.config.validate()

    @property
    def source_family(self) -> str:
        return str(getattr(self.adapter, "source_family", "crypto.websocket.market"))

    def _write_durable_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        if isinstance(self.store, QuantCryptoStore):
            if self.session_id is None:
                raise RuntimeError("capture_session_not_started")
            return self.store.append_scoped_events(
                study_id=self.protocol.study_id,
                capture_session_id=self.session_id,
                events=rows,
            )
        return self.store.append_events(rows)

    def _record_persist_success(
        self,
        rows: list[dict[str, Any]],
        results: list[dict[str, Any]],
    ) -> None:
        MARKET_CACHE.mark_persisted(rows, results)
        self.events_inserted += sum(
            1 for result in results if result["inserted"]
        )
        self.events_duplicate += sum(
            1 for result in results if not result["inserted"]
        )
        self._persistence_error = None
        self._persistence_spool_error = None
        self._persistence_last_success_monotonic = time.monotonic()

    def _spool_failed_rows(self, rows: list[dict[str, Any]]) -> bool:
        if not rows:
            return True
        if self._evidence_spool is None:
            self._persistence_spool_error = (
                self._persistence_spool_error
                or "spool_unavailable"
            )
            dropped = len(rows)
            self._persistence_spool_overflow += dropped
            MARKET_CACHE.record_persistence_drop(dropped)
            return False

        spooled_at = datetime.now(timezone.utc).isoformat()
        spooled_rows: list[dict[str, Any]] = []
        for row in rows:
            copied = dict(row)
            metadata = dict(copied.get("metadata") or {})
            metadata.setdefault(
                "spool_original_capture_session_id",
                self.session_id,
            )
            metadata["durability_origin"] = "SESSION_LOCAL_SPOOL"
            metadata["spooled_at"] = spooled_at
            copied["metadata"] = metadata
            spooled_rows.append(copied)

        try:
            batch_id = self._evidence_spool.append(spooled_rows)
            self._persistence_error = self._persistence_error or (
                "durability_spooled_pending"
            )
            now = time.monotonic()
            if now - self._last_spool_log_monotonic >= 10.0:
                stats = self._evidence_spool.stats()
                utilization = (
                    float(stats["bytes"]) / float(stats["max_bytes"])
                    if stats["max_bytes"] > 0
                    else 1.0
                )
                print(
                    "GORILA_EVIDENCE_SPOOL "
                    + json.dumps(
                        {
                            "batch_id": batch_id,
                            "rows": len(spooled_rows),
                            "batches": stats["batches"],
                            "bytes": stats["bytes"],
                            "max_bytes": stats["max_bytes"],
                            "utilization": utilization,
                            "guard_ratio": settings.persistence_spool_guard_ratio,
                            "max_batches": stats["max_batches"],
                            "oldest_created_at": stats["oldest_created_at"],
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )
                self._last_spool_log_monotonic = now
                if utilization >= settings.persistence_spool_guard_ratio:
                    self._persistence_spool_error = "EVIDENCE_SPOOL_GUARD_ACTIVE"
                    self._persistence_error = "evidence_spool_capacity_guard"
                    self.stop_event.set()
                    print(
                        "GORILA_CAPTURE_STOPPED "
                        + json.dumps(
                            {
                                "reason": self._persistence_error,
                                "utilization": utilization,
                                "guard_ratio": settings.persistence_spool_guard_ratio,
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
            return True
        except OverflowError as exc:
            self._persistence_spool_error = str(exc)
            dropped = len(rows)
            self._persistence_spool_overflow += dropped
            MARKET_CACHE.record_persistence_drop(dropped)
            return False
        except Exception as exc:
            self._persistence_spool_error = (
                f"spool_write:{type(exc).__name__}: {exc}"
            )
            dropped = len(rows)
            self._persistence_spool_overflow += dropped
            MARKET_CACHE.record_persistence_drop(dropped)
            return False

    def _drain_one_spool_batch(self) -> bool:
        spool = self._evidence_spool
        if spool is None or self.session_id is None:
            return False
        batches = spool.peek_many(max_batches=16, max_rows=4000)
        if not batches:
            return False
        rows: list[dict[str, Any]] = []
        batch_ids: list[int] = []
        for batch in batches:
            rows.extend(batch.rows)
            batch_ids.append(batch.batch_id)
        try:
            results = self._write_durable_rows(rows)
            self._record_persist_success(rows, results)
            spool.delete_many(batch_ids)
            return True
        except Exception as exc:
            self._persistence_error = f"{type(exc).__name__}: {exc}"
            MARKET_CACHE.record_persistence_degradation(
                self._persistence_error
            )
            print(
                "GORILA_DURABLE_REPLAY_ERROR "
                + json.dumps(
                    {
                        "error": self._persistence_error,
                        "rows": len(rows),
                        "batches": len(batch_ids),
                        "capture_session_id": self.session_id,
                    },
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )
            return False

    def _persistence_worker(self) -> None:
        """Durable writer isolated from the market event loop."""
        backoff = 0.25
        while (
            not self._persistence_stop.is_set()
            or not self._persistence_queue.empty()
            or not self._alpha_snapshot_queue.empty()
        ):
            self._flush_source_health()
            if not self._persistence_stop.is_set() and self.session_id is not None:
                if self._drain_one_spool_batch():
                    backoff = 0.25
                    continue

            processed = False
            try:
                rows = self._persistence_queue.get(timeout=0.25)
            except queue.Empty:
                rows = None

            if rows is not None:
                processed = True
                if isinstance(self.store, QuantCryptoStore) and self.session_id is None:
                    self._spool_failed_rows(rows)
                else:
                    try:
                        results = self._write_durable_rows(rows)
                        self._record_persist_success(rows, results)
                        backoff = 0.25
                    except Exception as exc:
                        self._persistence_error = f"{type(exc).__name__}: {exc}"
                        MARKET_CACHE.record_persistence_degradation(self._persistence_error)
                        self._spool_failed_rows(rows)
                        backoff = min(10.0, backoff * 2.0)
                        if not self._persistence_stop.is_set():
                            time.sleep(backoff)

            try:
                alpha_rows = self._alpha_snapshot_queue.get_nowait()
            except queue.Empty:
                alpha_rows = None

            if alpha_rows is not None:
                processed = True
                if isinstance(self.store, QuantCryptoStore) and self.session_id is None:
                    self._alpha_snapshots_dropped += len(alpha_rows)
                elif isinstance(self.store, QuantCryptoStore):
                    try:
                        inserted = self.store.append_alpha_feature_snapshots(alpha_rows)
                        self._alpha_snapshots_persisted += int(inserted)
                    except Exception as exc:
                        self._persistence_error = f"alpha_snapshot:{type(exc).__name__}: {exc}"
                        self._alpha_snapshots_dropped += len(alpha_rows)
                else:
                    self._alpha_snapshots_dropped += len(alpha_rows)

            if not processed:
                continue

        while True:
            try:
                rows = self._persistence_queue.get_nowait()
            except queue.Empty:
                break
            self._spool_failed_rows(rows)

        while True:
            try:
                alpha_rows = self._alpha_snapshot_queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(self.store, QuantCryptoStore):
                try:
                    inserted = self.store.append_alpha_feature_snapshots(alpha_rows)
                    self._alpha_snapshots_persisted += int(inserted)
                except Exception:
                    self._alpha_snapshots_dropped += len(alpha_rows)
            else:
                self._alpha_snapshots_dropped += len(alpha_rows)

        self._flush_source_health(force=True)

    def _trade_sample_eligible(self, event: NormalizedMarketEvent) -> bool:
        """Deterministic, PIT-neutral sampling of durable trade observations."""
        if event.event_type != "trade":
            return True
        if not isinstance(self.store, QuantCryptoStore) or self.protocol.version not in {"3", "4", "5", "6"}:
            return True
        trade_id = event.sequence_start
        if trade_id is None:
            return False
        rate = float(self.protocol.trade_persistence_sample_rate)
        digest = hashlib.sha256(
            f"{self.protocol.protocol_hash}|{event.symbol.upper()}|{int(trade_id)}".encode("utf-8")
        ).digest()
        bucket = int.from_bytes(digest[:8], "big") / float(2**64)
        return bucket < rate

    @staticmethod
    def _compact_payload(event: NormalizedMarketEvent) -> dict[str, Any]:
        """Persist only canonical fields required for PIT/OOS replay."""
        payload = event.payload
        if event.event_type == "trade":
            return {
                "e": "trade",
                "s": event.symbol.upper(),
                "t": int(event.sequence_start) if event.sequence_start is not None else payload.get("t"),
                "p": payload.get("p"),
                "q": payload.get("q"),
                "m": payload.get("m"),
            }
        if event.event_type == "bookTicker":
            return {
                "e": "bookTicker",
                "s": event.symbol.upper(),
                "u": int(event.sequence_end) if event.sequence_end is not None else payload.get("u"),
                "b": payload.get("b"),
                "B": payload.get("B"),
                "a": payload.get("a"),
                "A": payload.get("A"),
            }
        return dict(payload)

    def _should_persist(self, event: NormalizedMarketEvent) -> bool:
        if event.event_type == "trade":
            return self._trade_sample_eligible(event)
        if event.event_type == "bookTicker":
            symbol = event.symbol.upper()
            now_monotonic = time.monotonic()
            last = self._last_bookticker_persist_monotonic.get(symbol, 0.0)
            if now_monotonic - last >= self._bookticker_persist_interval:
                self._last_bookticker_persist_monotonic[symbol] = now_monotonic
                return True
            return False
        # In the durable QuantCrypto production path raw depth is kept on
        # the hot order-book plane rather than written tick-for-tick. The
        # lightweight SQLite/unit-test harness still persists it to preserve
        # the original persistence semantics under offline tests.
        if event.event_type == "depthUpdate":
            return not isinstance(self.store, QuantCryptoStore)
        return False

    def _start_persistence_worker(self) -> None:
        self._persistence_stop.clear()
        self._persistence_thread = threading.Thread(
            target=self._persistence_worker,
            name="gorila-crypto-durable-writer",
            daemon=True,
        )
        self._persistence_thread.start()

    def _stop_persistence_worker(self) -> None:
        self._persistence_stop.set()
        if self._persistence_thread is not None:
            self._persistence_thread.join(timeout=5.0)
            self._persistence_thread = None

    def _queue_source_health(
        self,
        *,
        source: str,
        status: str,
        last_event_time: str | None,
        last_received_time: str | None,
        event_age_seconds: float | None,
        transport_age_seconds: float | None,
        rows_increment: int = 0,
        error: str | None = None,
        now_monotonic: float | None = None,
    ) -> None:
        now_mono = time.monotonic() if now_monotonic is None else now_monotonic
        with self._health_lock:
            state = self._health_pending.get(source)
            if state is None:
                state = {
                    "source": source,
                    "rows_last_batch": 0,
                    "ready": False,
                }
                self._health_pending[source] = state

            state.update(
                {
                    "status": status,
                    "last_event_time": last_event_time,
                    "last_received_time": last_received_time,
                    "event_age_seconds": event_age_seconds,
                    "transport_age_seconds": transport_age_seconds,
                    "error": error,
                }
            )
            state["rows_last_batch"] = int(state["rows_last_batch"]) + max(
                0, int(rows_increment)
            )

            last_persisted = self._health_last_persist_monotonic.get(source, 0.0)
            previous_status = self._health_last_status.get(source)
            if (
                previous_status != status
                or now_mono - last_persisted
                >= self.config.health_flush_interval_seconds
            ):
                state["ready"] = True

    def _flush_source_health(self, *, force: bool = False) -> None:
        with self._health_lock:
            snapshots: list[dict[str, Any]] = []
            for source, state in list(self._health_pending.items()):
                if not force and not state.get("ready"):
                    continue
                snapshot = dict(state)
                snapshot.pop("ready", None)
                snapshots.append(snapshot)
                self._health_pending.pop(source, None)

        if not snapshots:
            return

        try:
            self.store.upsert_source_health_batch(snapshots)
        except Exception as exc:
            self._persistence_error = (
                f"source_health:{type(exc).__name__}: {exc}"
            )
            MARKET_CACHE.record_persistence_degradation(self._persistence_error)
            with self._health_lock:
                for snapshot in snapshots:
                    source = str(snapshot["source"])
                    current = self._health_pending.get(source)
                    if current is None:
                        current = {
                            "source": source,
                            "rows_last_batch": 0,
                            "ready": True,
                        }
                        self._health_pending[source] = current
                    current.update(snapshot)
                    current["rows_last_batch"] = (
                        int(current.get("rows_last_batch", 0))
                        + int(snapshot.get("rows_last_batch", 0))
                    )
                    current["ready"] = True
            return

        persisted_at = time.monotonic()
        with self._health_lock:
            for snapshot in snapshots:
                source = str(snapshot["source"])
                self._health_last_persist_monotonic[source] = persisted_at
                self._health_last_status[source] = str(snapshot["status"])

    def _record_connection(self, status: str, metadata: dict[str, Any] | None = None) -> None:
        payload = {
            "status": status,
            "metadata": metadata or {},
            "run_id": self.run_id,
        }
        print(
            "GORILA_CAPTURE_CONNECTION " + json.dumps(payload, sort_keys=True, default=str),
            flush=True,
        )
        try:
            self.store.record_connection(
                source=self.source_family,
                status=status,
                metadata={
                    "run_id": self.run_id,
                    **(metadata or {}),
                },
            )
        except Exception as exc:
            self._persistence_error = f"connection_event:{type(exc).__name__}: {exc}"
            MARKET_CACHE.record_persistence_degradation(self._persistence_error)

    def _on_connection(self, status: str, metadata: dict[str, Any]) -> None:
        if self.run_id is not None and isinstance(self.store, QuantCryptoStore):
            try:
                self.store.heartbeat_runtime_run(self.run_id)
                self._last_runtime_heartbeat = time.monotonic()
            except Exception as exc:
                self._persistence_error = (
                    f"connection_heartbeat:{type(exc).__name__}: {exc}"
                )
                MARKET_CACHE.record_persistence_degradation(
                    self._persistence_error
                )
        if status == "CONNECTED":
            self.sequence.new_connection()
            self.last_error = None
            self._record_connection(status, metadata)
            # Connection state belongs in crypto_connection_events.
            # Source health is reserved for actual market-data observations.
            return

        self._record_connection(status, metadata)

        if status == "ERROR":
            self.last_error = str(metadata.get("error") or "unknown_error")
            # Multi-symbol Binance capture deliberately isolates one socket per
            # symbol.  A malformed control/aggregate socket must not contaminate
            # the market-data health plane used by the evidence gate.
            if metadata.get("connection_scope") == "SYMBOL_ISOLATED":
                return
            self._queue_source_health(
                source=self.source_family,
                status=self.config.error_status,
                last_event_time=(
                    self.last_event.event_time.isoformat()
                    if self.last_event is not None
                    else None
                ),
                last_received_time=(
                    self.last_event.received_time.isoformat()
                    if self.last_event is not None
                    else None
                ),
                event_age_seconds=None,
                transport_age_seconds=None,
                rows_increment=0,
                error=self.last_error,
            )

    @staticmethod
    def _signed_trade(event: NormalizedMarketEvent) -> tuple[float, float]:
        quantity = float(event.payload.get("q") or 0.0)
        return (-1.0 if bool(event.payload.get("m")) else 1.0) * quantity, quantity

    def _alpha_trim_and_anchor(
        self,
        event: NormalizedMarketEvent,
    ) -> tuple[NormalizedMarketEvent | None, deque[NormalizedMarketEvent]]:
        symbol = event.symbol.upper()
        recent = self._alpha_recent_5s.setdefault(symbol, deque())
        candidates = self._alpha_anchor_candidates.setdefault(symbol, deque())
        recent.append(event)
        candidates.append(event)
        now_ts = event.event_time.timestamp()
        while recent and now_ts - recent[0].event_time.timestamp() > 6.0:
            recent.popleft()
        anchor = self._alpha_anchor_1s.get(symbol)
        cutoff = now_ts - 1.0
        while candidates:
            head = candidates[0]
            if head.event_time.timestamp() <= cutoff and head.received_time <= event.received_time:
                anchor = candidates.popleft()
            else:
                break
        self._alpha_anchor_1s[symbol] = anchor
        return anchor, recent

    def _alpha_flow_metrics(
        self,
        symbol: str,
        *,
        now_event_time: datetime,
        now_received_time: datetime,
    ) -> dict[str, float]:
        recent = self._alpha_recent_5s.get(symbol)
        if not recent:
            return {
                "flow_imbalance_1s": 0.0,
                "flow_imbalance_5s": 0.0,
                "trade_intensity_1s": 0.0,
                "trade_intensity_5s": 0.0,
            }
        now_ts = now_event_time.timestamp()
        signed_1 = gross_1 = signed_5 = gross_5 = 0.0
        n_1 = n_5 = 0
        for item in reversed(recent):
            if item.received_time > now_received_time:
                continue
            age = now_ts - item.event_time.timestamp()
            if age < 0:
                continue
            if age > 5.0:
                break
            signed, gross = self._signed_trade(item)
            signed_5 += signed
            gross_5 += gross
            n_5 += 1
            if age <= 1.0:
                signed_1 += signed
                gross_1 += gross
                n_1 += 1
        return {
            "flow_imbalance_1s": signed_1 / gross_1 if gross_1 > 0 else 0.0,
            "flow_imbalance_5s": signed_5 / gross_5 if gross_5 > 0 else 0.0,
            "trade_intensity_1s": math.log1p(n_1),
            "trade_intensity_5s": math.log1p(n_5),
        }

    def _alpha_latest_trade(
        self,
        symbol: str,
        *,
        event_time: datetime,
        received_time: datetime,
    ) -> NormalizedMarketEvent | None:
        recent = self._alpha_recent_5s.get(symbol)
        if not recent:
            return None
        for item in reversed(recent):
            if item.event_time <= event_time and item.received_time <= received_time:
                return item
        return None

    def _alpha_reference_trade(
        self,
        symbol: str,
        *,
        event_time: datetime,
        received_time: datetime,
    ) -> NormalizedMarketEvent | None:
        recent = self._alpha_recent_5s.get(symbol)
        if not recent:
            return None
        cutoff = event_time - timedelta(seconds=1)
        for item in reversed(recent):
            if item.event_time <= cutoff and item.received_time <= received_time:
                return item
        return None

    def _alpha_rate_allowed(self, symbol: str, now_monotonic: float) -> bool:
        times = self._alpha_trigger_times.setdefault(symbol, deque())
        while times and now_monotonic - times[0] > 60.0:
            times.popleft()
        if len(times) >= self._alpha_max_triggers_per_minute:
            return False
        last = self._alpha_last_trigger_monotonic.get(symbol, 0.0)
        if last > 0.0 and now_monotonic - last < self._alpha_min_refractory_seconds:
            return False
        times.append(now_monotonic)
        self._alpha_last_trigger_monotonic[symbol] = now_monotonic
        return True

    def _build_alpha_snapshot(
        self,
        *,
        leader: NormalizedMarketEvent,
        leader_anchor: NormalizedMarketEvent,
        target_symbol: str,
    ) -> dict[str, Any] | None:
        leader_book = self._alpha_latest_book.get(leader.symbol.upper())
        target_book = self._alpha_latest_book.get(target_symbol.upper())
        target_latest = self._alpha_latest_trade(
            target_symbol,
            event_time=leader.event_time,
            received_time=leader.received_time,
        )
        target_reference = self._alpha_reference_trade(
            target_symbol,
            event_time=leader.event_time,
            received_time=leader.received_time,
        )
        if any(item is None for item in (leader_book, target_book, target_latest, target_reference)):
            return None
        assert leader_book is not None and target_book is not None
        assert target_latest is not None and target_reference is not None
        if (
            leader_book.received_time > leader.received_time
            or target_book.received_time > leader.received_time
            or target_latest.received_time > leader.received_time
            or target_reference.received_time > leader.received_time
        ):
            return None

        leader_price = float(leader.payload.get("p") or 0.0)
        leader_anchor_price = float(leader_anchor.payload.get("p") or 0.0)
        target_latest_price = float(target_latest.payload.get("p") or 0.0)
        target_reference_price = float(target_reference.payload.get("p") or 0.0)
        if min(leader_price, leader_anchor_price, target_latest_price, target_reference_price) <= 0.0:
            return None

        lb = (
            float(leader_book.payload.get("b") or 0.0),
            float(leader_book.payload.get("a") or 0.0),
            float(leader_book.payload.get("B") or 0.0),
            float(leader_book.payload.get("A") or 0.0),
        )
        tb = (
            float(target_book.payload.get("b") or 0.0),
            float(target_book.payload.get("a") or 0.0),
            float(target_book.payload.get("B") or 0.0),
            float(target_book.payload.get("A") or 0.0),
        )
        if min(*lb, *tb) <= 0.0:
            return None

        leader_latency_ms = max(
            0.0, (leader.received_time - leader.event_time).total_seconds() * 1000.0
        )
        target_information_age_ms = max(
            0.0, (leader.received_time - target_latest.received_time).total_seconds() * 1000.0
        )
        target_market_age_ms = max(
            0.0, (leader.event_time - target_latest.event_time).total_seconds() * 1000.0
        )
        leader_book_age_ms = max(
            0.0, (leader.received_time - leader_book.received_time).total_seconds() * 1000.0
        )
        target_book_age_ms = max(
            0.0, (leader.received_time - target_book.received_time).total_seconds() * 1000.0
        )
        leader_return_bps = 10_000.0 * math.log(leader_price / leader_anchor_price)
        target_return_bps = 10_000.0 * math.log(target_latest_price / target_reference_price)
        leader_flow = self._alpha_flow_metrics(
            leader.symbol.upper(),
            now_event_time=leader.event_time,
            now_received_time=leader.received_time,
        )
        target_flow = self._alpha_flow_metrics(
            target_symbol,
            now_event_time=leader.event_time,
            now_received_time=leader.received_time,
        )

        features = build_microstructure_feature_vector(
            leader_return_bps=leader_return_bps,
            leader_transport_latency_ms=leader_latency_ms,
            target_return_bps_lookback=target_return_bps,
            target_information_age_ms=target_information_age_ms,
            target_market_age_ms=target_market_age_ms,
            leader_flow_imbalance_1s=leader_flow["flow_imbalance_1s"],
            leader_flow_imbalance_5s=leader_flow["flow_imbalance_5s"],
            leader_trade_intensity_1s=leader_flow["trade_intensity_1s"],
            leader_trade_intensity_5s=leader_flow["trade_intensity_5s"],
            target_flow_imbalance_1s=target_flow["flow_imbalance_1s"],
            target_flow_imbalance_5s=target_flow["flow_imbalance_5s"],
            target_trade_intensity_1s=target_flow["trade_intensity_1s"],
            target_trade_intensity_5s=target_flow["trade_intensity_5s"],
            leader_bid=lb[0], leader_ask=lb[1], leader_bid_qty=lb[2], leader_ask_qty=lb[3],
            target_bid=tb[0], target_ask=tb[1], target_bid_qty=tb[2], target_ask_qty=tb[3],
            leader_book_age_ms=leader_book_age_ms,
            target_book_age_ms=target_book_age_ms,
            bookticker_interval_seconds=float(self.protocol.bookticker_persistence_interval_seconds),
        )
        source_ids = (
            f"trade:{leader.symbol.upper()}:{int(leader.sequence_start or 0)}",
            f"trade:{leader_anchor.symbol.upper()}:{int(leader_anchor.sequence_start or 0)}",
            f"trade:{target_symbol.upper()}:{int(target_latest.sequence_start or 0)}",
            f"trade:{target_symbol.upper()}:{int(target_reference.sequence_start or 0)}",
            f"bookTicker:{leader.symbol.upper()}:{int(leader_book.sequence_end or 0)}",
            f"bookTicker:{target_symbol.upper()}:{int(target_book.sequence_end or 0)}",
        )
        leader_id = source_ids[0]
        snapshot_id = hashlib.sha256(
            "|".join(
                (self.session_id or "", leader_id, target_symbol.upper(), FEATURE_SET_VERSION)
            ).encode("utf-8")
        ).hexdigest()[:32]
        return {
            "snapshot_id": snapshot_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "study_id": self.protocol.study_id,
            "protocol_hash": self.protocol.protocol_hash,
            "capture_session_id": self.session_id,
            "feature_set_version": FEATURE_SET_VERSION,
            "leader_symbol": leader.symbol.upper(),
            "target_symbol": target_symbol.upper(),
            "leader_event_id": leader_id,
            "leader_event_time": leader.event_time.isoformat(),
            "leader_received_time": leader.received_time.isoformat(),
            "source_event_ids": source_ids,
            "feature_set_hash": feature_set_hash(
                leader_symbol=leader.symbol,
                target_symbol=target_symbol,
                decision_event_time=leader.event_time,
                decision_received_time=leader.received_time,
                source_event_ids=source_ids,
                feature_values=features,
            ),
            "features": features,
            "trigger_threshold_bps": 5.0,
            "status": "SHADOW",
        }

    def _maybe_emit_alpha_snapshots(self, event: NormalizedMarketEvent) -> None:
        if event.event_type == "bookTicker":
            self._alpha_latest_book[event.symbol.upper()] = event
            return
        if event.event_type != "trade" or self.session_id is None:
            return
        anchor, _ = self._alpha_trim_and_anchor(event)
        if anchor is None:
            return
        try:
            price = float(event.payload.get("p") or 0.0)
            anchor_price = float(anchor.payload.get("p") or 0.0)
            if price <= 0.0 or anchor_price <= 0.0:
                return
            leader_return_bps = 10_000.0 * math.log(price / anchor_price)
        except (TypeError, ValueError, ZeroDivisionError):
            return
        if abs(leader_return_bps) < 5.0:
            return
        if not self._alpha_rate_allowed(event.symbol.upper(), time.monotonic()):
            return
        created = 0
        for target_symbol in self.protocol.symbols:
            target_symbol = target_symbol.upper()
            if target_symbol == event.symbol.upper():
                continue
            snapshot = self._build_alpha_snapshot(
                leader=event,
                leader_anchor=anchor,
                target_symbol=target_symbol,
            )
            if snapshot is not None:
                self._alpha_pending_snapshots.append(snapshot)
                created += 1
        if created:
            self._alpha_snapshots_created += created
            if self._alpha_pending_started_monotonic is None:
                self._alpha_pending_started_monotonic = time.monotonic()

    def _flush_pending_alpha_snapshots(self) -> None:
        if not self._alpha_pending_snapshots:
            self._alpha_pending_started_monotonic = None
            return
        rows = self._alpha_pending_snapshots
        self._alpha_pending_snapshots = []
        self._alpha_pending_started_monotonic = None
        try:
            self._alpha_snapshot_queue.put(rows, timeout=0.01)
        except queue.Full:
            self._alpha_snapshots_dropped += len(rows)

    def _flush_pending_events(self) -> None:
        if not self._pending_event_rows:
            self._pending_event_started_monotonic = None
            return
        rows = self._pending_event_rows
        self._pending_event_rows = []
        self._pending_event_started_monotonic = None

        try:
            self._persistence_queue.put(rows, timeout=0.05)
        except queue.Full:
            self._spool_failed_rows(rows)

    def _ingest(self, event: NormalizedMarketEvent) -> None:
        event_time = event.event_time.astimezone(timezone.utc)
        received_time = event.received_time.astimezone(timezone.utc)
        assessment = assess_observation(
            event_time,
            received_time,
            now=self.now(),
        )
        compact_v4 = (
            isinstance(self.store, QuantCryptoStore)
            and self.protocol.version == "4"
        )
        event_metadata = (
            {
                "sequence_kind": event.sequence_kind,
                "ingest_epoch": self.sequence.epoch,
            }
            if compact_v4
            else {
                "sequence_kind": event.sequence_kind,
                "receive_time_ns": event.receive_time_ns,
                "runtime_run_id": self.run_id,
                "ingest_epoch": self.sequence.epoch,
                "event_age_seconds": assessment["event_age_seconds"],
                "received_age_seconds": assessment["received_age_seconds"],
                "transport_latency_seconds": assessment["transport_latency_seconds"],
            }
        )
        event_kwargs = dict(
            symbol=event.symbol,
            event_type=event.event_type,
            event_time=event_time.isoformat(),
            received_time=received_time.isoformat(),
            provider_time=(
                event.provider_time.astimezone(timezone.utc).isoformat()
                if event.provider_time is not None
                else None
            ),
            source=event.source,
            payload=event.payload,
            sequence_start=event.sequence_start,
            sequence_end=event.sequence_end,
            quality=event.quality,
            metadata=event_metadata,
        )

        # Market truth is updated immediately, before any durable operation.
        if event.event_type in {"trade", "bookTicker"}:
            self.last_event = event
            MARKET_CACHE.append_observed([event_kwargs])
        else:
            self.last_event = event
        if self.session_id is not None and event.event_type in {"trade", "bookTicker"}:
            self._maybe_emit_alpha_snapshots(event)

        gap = self.sequence.observe(event)
        if gap is not None:
            expected, observed = gap
            self.gaps_detected += 1
            if self.session_id is not None or not isinstance(
                self.store, QuantCryptoStore
            ):
                try:
                    self.store.record_gap(
                        symbol=event.symbol,
                        source=event.source,
                        expected_sequence=expected,
                        observed_sequence=observed,
                        status=self.config.gap_status,
                        metadata={
                            "run_id": self.run_id,
                            "capture_session_id": self.session_id,
                            "crypto_study_id": self.protocol.study_id,
                            "event_type": event.event_type,
                            "ingest_epoch": self.sequence.epoch,
                            "message": (
                                "Continuity gap detected within one connection epoch; "
                                "missing events were not synthesized."
                            ),
                        },
                    )
                except Exception as exc:
                    self._persistence_error = (
                        f"gap_record:{type(exc).__name__}: {exc}"
                    )
                    MARKET_CACHE.record_persistence_degradation(
                        self._persistence_error
                    )

        now_monotonic = time.monotonic()
        source = event.source
        self._queue_source_health(
            source=source,
            status=assessment["status"],
            last_event_time=assessment["event_time"],
            last_received_time=assessment["received_time"],
            event_age_seconds=assessment["event_age_seconds"],
            transport_age_seconds=assessment["transport_age_seconds"],
            rows_increment=1,
            error=None,
            now_monotonic=now_monotonic,
        )

        with self._symbol_lock:
            symbol = event.symbol.upper()
            self._symbol_first_received.setdefault(symbol, received_time)
            self._symbol_last_received[symbol] = received_time

        if self._should_persist(event):
            sample_rate = (
                float(self.protocol.trade_persistence_sample_rate)
                if event.event_type == "trade"
                else 1.0
            )
            event_kwargs["payload"] = self._compact_payload(event)
            event_kwargs["metadata"] = {
                **dict(event_kwargs["metadata"]),
                "persistence_policy": (
                    "DETERMINISTIC_TRADE_SAMPLE"
                    if event.event_type == "trade"
                    else (
                        "BOOKTICKER_5S_SNAPSHOT"
                        if self.protocol.version in {"4", "5", "6"}
                        else "BOOKTICKER_1S_SNAPSHOT"
                    )
                ),
                "sampling_contract": self.protocol.persistence_contract_version,
                "sampling_rate": sample_rate,
                "sample_weight": (1.0 / sample_rate) if sample_rate > 0 else None,
            }
            if self._pending_event_started_monotonic is None:
                self._pending_event_started_monotonic = now_monotonic
            self._pending_event_rows.append(event_kwargs)

    def operational_snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        last_success_age = (
            None
            if self._persistence_last_success_monotonic <= 0
            else max(0.0, now - self._persistence_last_success_monotonic)
        )
        spool_stats = (
            self._evidence_spool.stats()
            if self._evidence_spool is not None
            else {
                "batches": 0,
                "bytes": 0,
                "max_bytes": settings.persistence_spool_max_bytes,
                "max_batches": settings.persistence_spool_max_batches,
                "oldest_created_at": None,
            }
        )

        durability = "LIVE"
        durability_reason = None
        if isinstance(self.store, QuantCryptoStore) and self.session_id is None:
            durability = "DEGRADED"
            durability_reason = "STORAGE_BOOTSTRAP_PENDING"
        elif self._persistence_error is not None:
            durability = "DEGRADED"
            durability_reason = self._persistence_error
        elif spool_stats["batches"] > 0:
            durability = (
                "RECOVERING"
                if isinstance(self.store, QuantCryptoStore) and self.session_id is not None
                else "DEGRADED"
            )
            durability_reason = "EVIDENCE_SPOOL_PENDING"
        elif self._persistence_spool_overflow > 0:
            durability = "DEGRADED"
            durability_reason = "EVIDENCE_SPOOL_OVERFLOW"
        elif self._persistence_spool_error is not None:
            durability = "DEGRADED"
            durability_reason = self._persistence_spool_error
        elif self._persistence_queue.qsize() > max(
            8, settings.persistence_queue_batches * 0.75
        ):
            durability = "DEGRADED"
            durability_reason = "PERSISTENCE_QUEUE_PRESSURE"
        elif (
            isinstance(self.store, QuantCryptoStore)
            and self.session_id is not None
            and self.last_event is not None
            and self._persistence_last_success_monotonic <= 0
        ):
            durability = "DEGRADED"
            durability_reason = "DURABLE_WRITE_UNCONFIRMED"
        elif (
            isinstance(self.store, QuantCryptoStore)
            and self.session_id is not None
            and last_success_age is not None
            and last_success_age > settings.durability_live_max_age_seconds
        ):
            durability = "DEGRADED"
            durability_reason = "DURABLE_WRITE_STALE"

        return {
            "market_plane": "LIVE" if self.last_event is not None else "STARTING",
            "durability": durability,
            "durability_reason": durability_reason,
            "persistence_error": self._persistence_error,
            "persistence_queue_batches": self._persistence_queue.qsize(),
            "persistence_last_success_age_seconds": last_success_age,
            "persistence_dropped_events": (
                self._persistence_dropped_events
                + self._persistence_spool_overflow
            ),
            "events_inserted": self.events_inserted,
            "events_duplicate": self.events_duplicate,
            "gaps_detected": self.gaps_detected,
            "last_event_time": (
                self.last_event.received_time.isoformat()
                if self.last_event is not None
                else None
            ),
            "evidence_spool": spool_stats,
            "evidence_spool_error": self._persistence_spool_error,
            "evidence_spool_guard_ratio": settings.persistence_spool_guard_ratio,
            "evidence_spool_utilization": (
                float(spool_stats["bytes"]) / float(spool_stats["max_bytes"])
                if spool_stats.get("max_bytes")
                else None
            ),
            "durability_live_max_age_seconds": settings.durability_live_max_age_seconds,
            "recovery_gate": self.recovery_gate(),
        }

    def recovery_gate(self) -> dict[str, Any]:
        """Block research until durable evidence is complete and fresh."""
        production_scoped = isinstance(self.store, QuantCryptoStore)
        if not production_scoped:
            return {
                "status": "NOT_APPLICABLE",
                "production_scoped": False,
                "reasons": [],
            }

        now = time.monotonic()
        last_success_age = (
            None
            if self._persistence_last_success_monotonic <= 0
            else max(0.0, now - self._persistence_last_success_monotonic)
        )
        spool_stats = (
            self._evidence_spool.stats()
            if self._evidence_spool is not None
            else {"batches": 0, "bytes": 0}
        )
        dropped = self._persistence_dropped_events + self._persistence_spool_overflow
        symbols = self.symbol_health()
        reasons: list[str] = []

        if self.session_id is None:
            reasons.append("STORAGE_SESSION_PENDING")
        if spool_stats["batches"] > 0:
            reasons.append("EVIDENCE_SPOOL_PENDING")
        if self._persistence_queue.qsize() > 0:
            reasons.append("PERSISTENCE_QUEUE_PENDING")
        if dropped > 0:
            reasons.append("PERSISTENCE_DROPS")
        if last_success_age is None:
            reasons.append("DURABLE_WRITE_UNCONFIRMED")
        elif last_success_age > settings.durability_live_max_age_seconds:
            reasons.append("DURABLE_WRITE_STALE")
        if not symbols or not all(row["status"] == "LIVE" for row in symbols):
            reasons.append("REQUIRED_SYMBOL_NOT_LIVE")

        return {
            "status": "PASS" if not reasons else "BLOCKED",
            "production_scoped": True,
            "reasons": reasons,
            "capture_session_id": self.session_id,
            "last_durable_success_age_seconds": last_success_age,
            "durability_live_max_age_seconds": settings.durability_live_max_age_seconds,
            "evidence_spool": {
                "batches": int(spool_stats.get("batches", 0)),
                "bytes": int(spool_stats.get("bytes", 0)),
            },
            "persistence_queue_batches": self._persistence_queue.qsize(),
            "persistence_dropped_events": dropped,
            "symbols": symbols,
        }

    def symbol_health(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        """Report readiness of every required symbol from observed receive times.

        A live worker is not enough for production readiness: every preregistered
        symbol must actually produce market-data events within the configured
        freshness window. Missing symbols remain STARTING only during the initial
        live-age grace period; after that they are NO_DATA.
        """
        reference = now or self.now()
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        reference = reference.astimezone(timezone.utc)

        with self._symbol_lock:
            first = dict(self._symbol_first_received)
            last = dict(self._symbol_last_received)
            capture_started = self._capture_started_at

        startup_age = max(0.0, (reference - capture_started).total_seconds())
        rows: list[dict[str, Any]] = []
        for symbol in tuple(self.adapter.config.symbols):
            normalized = str(symbol).upper()
            first_received = first.get(normalized)
            last_received = last.get(normalized)
            if last_received is None:
                age = startup_age
                status = (
                    "STARTING"
                    if startup_age <= settings.event_live_max_age_seconds
                    else "NO_DATA"
                )
            else:
                age = max(0.0, (reference - last_received).total_seconds())
                if age <= settings.event_live_max_age_seconds:
                    status = "LIVE"
                elif age <= settings.event_delayed_max_age_seconds:
                    status = "DELAYED"
                else:
                    status = "STALE"
            rows.append(
                {
                    "symbol": normalized,
                    "status": status,
                    "first_received_time": first_received.isoformat() if first_received else None,
                    "last_received_time": last_received.isoformat() if last_received else None,
                    "age_seconds": round(age, 3) if last_received is not None or startup_age else None,
                    "required": True,
                    "healthy": status == "LIVE",
                }
            )
        return rows

    def stop(self) -> None:
        self.stop_event.set()

    def _bootstrap_storage(self) -> None:
        """Acquire durable study/session ownership without blocking market ingest."""
        backoff = 1.0
        stale = 0
        effective_protocol_hash: str | None = None

        while not self._bootstrap_stop.is_set() and not self.stop_event.is_set():
            try:
                stale = self.store.reconcile_stale_runtime_runs(
                    stale_after_seconds=120.0
                )
                if (
                    isinstance(self.store, QuantCryptoStore)
                    and self.protocol.version in {"3", "4"}
                ):
                    self.store.purge_legacy_unvalidated_events(
                        keep_study_id=self.protocol.study_id
                    )
                self.store.register_study(self.protocol)
                effective_protocol_hash = self.store.get_study_protocol_hash(
                    self.protocol.study_id
                )

                if self.session_id is None:
                    self.session_id = self.store.start_capture_session(
                        study_id=self.protocol.study_id,
                        protocol_hash=effective_protocol_hash,
                        provider=settings.provider,
                        venue=self.protocol.venue,
                        symbols=tuple(self.adapter.config.symbols),
                        streams=tuple(settings.streams),
                        region=os.getenv("RENDER_REGION"),
                        instance_id=os.getenv("RENDER_INSTANCE_ID"),
                        code_version=(
                            os.getenv("RENDER_GIT_COMMIT")
                            or os.getenv("GORILA_CRYPTO_CODE_VERSION")
                        ),
                        metadata={"stale_runs_reconciled": stale},
                    )
                    if (
                        isinstance(self.store, QuantCryptoStore)
                        and self.protocol.version == "3"
                    ):
                        self.store.purge_legacy_unvalidated_events(
                            keep_study_id=self.protocol.study_id
                        )

                if self.run_id is None:
                    self.run_id = self.store.start_runtime_run_scoped(
                        kind=self.config.kind,
                        session_id=self.session_id,
                    )

                self._record_connection(
                    "RUN_STARTED",
                    {
                        "symbols": list(self.adapter.config.symbols),
                        "study_id": self.protocol.study_id,
                        "protocol_hash": effective_protocol_hash,
                        "capture_session_id": self.session_id,
                    },
                )
                self._persistence_error = None
                return

            except RuntimeError as exc:
                reason = str(exc)
                if reason.startswith("active_capture_session_exists:"):
                    self._record_connection(
                        "WAITING_FOR_ACTIVE_SESSION",
                        {"reason": reason},
                    )
                else:
                    self.last_error = reason
                    self._record_connection(
                        "STORAGE_BOOTSTRAP_DEGRADED",
                        {"error": reason},
                    )
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                self._record_connection(
                    "STORAGE_BOOTSTRAP_DEGRADED",
                    {"error": self.last_error},
                )

            self._bootstrap_stop.wait(backoff)
            backoff = min(30.0, backoff * 2.0)

    def run(self) -> dict[str, Any]:
        """Run one market stream while durable capture bootstraps independently."""
        production_scoped = isinstance(self.store, QuantCryptoStore)
        if production_scoped and not self.protocol.matches_runtime(
            provider=settings.provider,
            symbols=tuple(self.adapter.config.symbols),
            streams=tuple(settings.streams),
        ):
            raise RuntimeError("runtime_does_not_match_preregistered_protocol")

        self.events_inserted = 0
        self.events_duplicate = 0
        self.gaps_detected = 0
        self.last_error = None
        self._health_last_persist_monotonic.clear()
        self._health_last_status.clear()
        with self._health_lock:
            self._health_pending.clear()
        self._pending_event_rows.clear()
        self._pending_event_started_monotonic = None
        self._alpha_recent_5s.clear()
        self._alpha_anchor_1s.clear()
        self._alpha_anchor_candidates.clear()
        self._alpha_latest_book.clear()
        self._alpha_last_trigger_monotonic.clear()
        self._alpha_trigger_times.clear()
        self._alpha_pending_snapshots.clear()
        self._alpha_pending_started_monotonic = None
        self._alpha_snapshots_created = 0
        self._alpha_snapshots_persisted = 0
        self._alpha_snapshots_dropped = 0
        self._last_bookticker_persist_monotonic.clear()
        self._persistence_error = None
        self._persistence_dropped_events = 0
        self._persistence_spool_overflow = 0
        self._persistence_spool_error = None
        self._bootstrap_stop.clear()
        self.run_id = None
        self.session_id = None

        with self._symbol_lock:
            self._symbol_first_received.clear()
            self._symbol_last_received.clear()
            self._capture_started_at = self.now()

        if not production_scoped:
            self.run_id = self.store.start_runtime_run(kind=self.config.kind)
            self._record_connection(
                "RUN_STARTED",
                {"symbols": list(self.adapter.config.symbols)},
            )

        self._start_persistence_worker()

        if production_scoped:
            self._bootstrap_thread = threading.Thread(
                target=self._bootstrap_storage,
                name="gorila-crypto-storage-bootstrap",
                daemon=True,
            )
            self._bootstrap_thread.start()

        status = "STOPPED"
        result: dict[str, Any] = {}
        try:
            for event in self.adapter.iter_forever(
                stop_event=self.stop_event,
                on_connection=self._on_connection,
            ):
                self._ingest(event)
                now_monotonic = time.monotonic()
                alpha_pending_age = (
                    now_monotonic - self._alpha_pending_started_monotonic
                    if self._alpha_pending_started_monotonic is not None
                    else 0.0
                )
                if (
                    len(self._alpha_pending_snapshots) >= 16
                    or alpha_pending_age >= self.config.event_batch_flush_interval_seconds
                ):
                    self._flush_pending_alpha_snapshots()
                pending_age = (
                    now_monotonic - self._pending_event_started_monotonic
                    if self._pending_event_started_monotonic is not None
                    else 0.0
                )
                if (
                    len(self._pending_event_rows) >= self.config.event_batch_size
                    or pending_age >= self.config.event_batch_flush_interval_seconds
                ):
                    self._flush_pending_events()

                if self.stop_event.is_set():
                    status = "STOPPED"
                    break
            else:
                status = "STREAM_ENDED"
        except KeyboardInterrupt:
            status = "STOPPED"
        except Exception as exc:
            status = "FAILED"
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._record_connection("ERROR", {"error": self.last_error})
        finally:
            self._flush_pending_events()
            self._flush_pending_alpha_snapshots()
            self._bootstrap_stop.set()
            if self._bootstrap_thread is not None:
                self._bootstrap_thread.join(timeout=3.0)
                self._bootstrap_thread = None

            self._stop_persistence_worker()

            result = {
                "events_inserted": self.events_inserted,
                "events_duplicate": self.events_duplicate,
                "gaps_detected": self.gaps_detected,
                "last_error": self.last_error,
                "persistence_error": self._persistence_error,
                "persistence_dropped_events": self._persistence_dropped_events,
                "persistence_queue_batches": self._persistence_queue.qsize(),
                "alpha_snapshot_queue_batches": self._alpha_snapshot_queue.qsize(),
                "alpha_snapshots_created": self._alpha_snapshots_created,
                "alpha_snapshots_persisted": self._alpha_snapshots_persisted,
                "alpha_snapshots_dropped": self._alpha_snapshots_dropped,
                "last_event_time": (
                    self.last_event.event_time.isoformat()
                    if self.last_event is not None
                    else None
                ),
                "automatic_promotion": False,
                "forecast": False,
                "execution": False,
            }

            if production_scoped:
                if self.run_id is not None:
                    try:
                        self.store.finish_runtime_run_scoped(
                            run_id=self.run_id,
                            session_id=self.session_id,
                            status=status,
                            result=result,
                        )
                        if self.session_id is not None:
                            self.store.set_capture_session_status(
                                self.session_id,
                                status,
                            )
                    except Exception as exc:
                        self._persistence_error = (
                            f"runtime_finish:{type(exc).__name__}: {exc}"
                        )
                elif self.session_id is not None:
                    try:
                        self.store.set_capture_session_status(
                            self.session_id,
                            "ABORTED_STARTUP_DEGRADED",
                        )
                    except Exception:
                        pass
            else:
                if self.run_id is not None:
                    try:
                        self.store.finish_runtime_run(
                            run_id=self.run_id,
                            status=status,
                            result=result,
                        )
                    except Exception:
                        pass

            self._record_connection("RUN_FINISHED", result)
            try:
                self.store.close()
            except Exception:
                pass

        return {
            "status": status,
            "run_id": self.run_id,
            **result,
        }


def build_market_adapter():
    """Construct the configured market-data adapter without starting it."""
    if settings.provider == "kraken":
        return KrakenSpotMarketAdapter(
            KrakenStreamConfig(
                symbols=settings.symbols,
                streams=settings.streams,
                depth=10,
            )
        )
    return BinanceSpotMarketAdapter(
        BinanceStreamConfig(
            symbols=settings.symbols,
            streams=tuple(settings.streams),
            depth_speed=settings.depth_speed,
        )
    )


def build_prospective_runtime() -> ProspectiveCryptoIngestor:
    """Build the production-shaped prospective runtime without starting it."""
    if not CRYPTO_DATABASE_URL:
        raise RuntimeError(
            "GORILA_CRYPTO_DATABASE_URL or DATABASE_URL is required for durable prospective ingestion; "
            "refusing ephemeral SQLite accumulation"
        )
    adapter = build_market_adapter()
    return ProspectiveCryptoIngestor(
        QuantCryptoStore(database_url=CRYPTO_DATABASE_URL, require_durable=True),
        adapter,
    )


def main() -> None:
    build_prospective_runtime().run()


if __name__ == "__main__":
    main()