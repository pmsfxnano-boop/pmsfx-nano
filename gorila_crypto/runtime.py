"""Prospective multi-venue market-data ingestion for the Crypto cleanroom.

A9 turns the proven adapter into a persistence-only 24/7 research feed.
It records raw normalized events, connection lifecycle, sequence gaps, and
source freshness. It never forecasts, trades, or promotes a model.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from gorila_core.market_freshness import assess_observation

from .binance import BinanceSpotMarketAdapter, BinanceStreamConfig, NormalizedMarketEvent
from .kraken import KrakenSpotMarketAdapter, KrakenStreamConfig
from .market_cache import MARKET_CACHE
from .config import settings
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
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
    health_flush_interval_seconds: float = 1.0
    event_batch_size: int = 250
    event_batch_flush_interval_seconds: float = 0.050

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
        self._health_pending_rows: dict[str, int] = {}
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
        self._last_bookticker_persist_monotonic: dict[str, float] = {}
        self._bookticker_persist_interval = settings.persist_bookticker_interval_seconds

        self.config.validate()

    @property
    def source_family(self) -> str:
        return str(getattr(self.adapter, "source_family", "crypto.websocket.market"))

    def _persistence_worker(self) -> None:
        """Durable writer isolated from the market event loop."""
        backoff = 0.25
        while not self._persistence_stop.is_set() or not self._persistence_queue.empty():
            try:
                rows = self._persistence_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if isinstance(self.store, QuantCryptoStore):
                    if self.session_id is None:
                        raise RuntimeError("capture_session_not_started")
                    results = self.store.append_scoped_events(
                        study_id=self.protocol.study_id,
                        capture_session_id=self.session_id,
                        events=rows,
                    )
                else:
                    results = self.store.append_events(rows)
                MARKET_CACHE.mark_persisted(rows, results)
                self.events_inserted += sum(
                    1 for result in results if result["inserted"]
                )
                self.events_duplicate += sum(
                    1 for result in results if not result["inserted"]
                )
                self._persistence_error = None
                self._persistence_last_success_monotonic = time.monotonic()
                backoff = 0.25
            except Exception as exc:
                self._persistence_error = f"{type(exc).__name__}: {exc}"
                MARKET_CACHE.record_persistence_degradation(self._persistence_error)
                # Keep the exact batch intact and retry in order. The market
                # plane remains live while durability is degraded.
                if self.stop_event.is_set():
                    self._persistence_dropped_events += len(rows)
                    MARKET_CACHE.record_persistence_drop(len(rows))
                    continue
                time.sleep(backoff)
                backoff = min(10.0, backoff * 2.0)
                try:
                    self._persistence_queue.put(rows, timeout=0.25)
                except queue.Full:
                    self._persistence_dropped_events += len(rows)
                    MARKET_CACHE.record_persistence_drop(len(rows))

    def _should_persist(self, event: NormalizedMarketEvent) -> bool:
        if event.event_type == "trade":
            return True
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
            self.store.upsert_source_health(
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
                rows_last_batch=0,
                error=self.last_error,
            )

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
            # Preserve trades preferentially; sampled quotes may be dropped from
            # durability under sustained storage pressure, but hot market state
            # continues advancing and the drop is explicitly observable.
            trades = [row for row in rows if row.get("event_type") == "trade"]
            if trades:
                try:
                    self._persistence_queue.put(trades, timeout=0.05)
                except queue.Full:
                    dropped = len(trades)
                    self._persistence_dropped_events += dropped
                    MARKET_CACHE.record_persistence_drop(dropped)
            non_trades = len(rows) - len(trades)
            if non_trades:
                self._persistence_dropped_events += non_trades
                MARKET_CACHE.record_persistence_drop(non_trades)

    def _ingest(self, event: NormalizedMarketEvent) -> None:
        event_time = event.event_time.astimezone(timezone.utc)
        received_time = event.received_time.astimezone(timezone.utc)
        assessment = assess_observation(
            event_time,
            received_time,
            now=self.now(),
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
            metadata={
                "sequence_kind": event.sequence_kind,
                "receive_time_ns": event.receive_time_ns,
                "runtime_run_id": self.run_id,
                "ingest_epoch": self.sequence.epoch,
                "event_age_seconds": assessment["event_age_seconds"],
                "received_age_seconds": assessment["received_age_seconds"],
                "transport_latency_seconds": assessment["transport_latency_seconds"],
            },
        )

        # Market truth is updated immediately, before any durable operation.
        if event.event_type in {"trade", "bookTicker"}:
            self.last_event = event
            MARKET_CACHE.append_observed([event_kwargs])
        else:
            self.last_event = event

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

        source = event.source
        self._health_pending_rows[source] = (
            self._health_pending_rows.get(source, 0) + 1
        )
        now_monotonic = time.monotonic()
        last_persisted = self._health_last_persist_monotonic.get(source, 0.0)
        previous_status = self._health_last_status.get(source)
        should_persist_health = (
            previous_status != assessment["status"]
            or now_monotonic - last_persisted
            >= self.config.health_flush_interval_seconds
        )
        if should_persist_health and (
            self.session_id is not None or not isinstance(self.store, QuantCryptoStore)
        ):
            try:
                self.store.upsert_source_health(
                    source=source,
                    status=assessment["status"],
                    last_event_time=assessment["event_time"],
                    last_received_time=assessment["received_time"],
                    event_age_seconds=assessment["event_age_seconds"],
                    transport_age_seconds=assessment["transport_age_seconds"],
                    rows_last_batch=self._health_pending_rows[source],
                    error=None,
                )
            except Exception as exc:
                self._persistence_error = (
                    f"source_health:{type(exc).__name__}: {exc}"
                )
                MARKET_CACHE.record_persistence_degradation(
                    self._persistence_error
                )
            self._health_last_persist_monotonic[source] = now_monotonic
            self._health_last_status[source] = assessment["status"]
            self._health_pending_rows[source] = 0

        with self._symbol_lock:
            symbol = event.symbol.upper()
            self._symbol_first_received.setdefault(symbol, received_time)
            self._symbol_last_received[symbol] = received_time

        if self._should_persist(event):
            event_kwargs["metadata"] = {
                **dict(event_kwargs["metadata"]),
                "persistence_policy": (
                    "FULL_TRADE"
                    if event.event_type == "trade"
                    else "SAMPLED_BOOKTICKER"
                ),
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
        durability = "LIVE"
        durability_reason = None
        if (
            isinstance(self.store, QuantCryptoStore)
            and self.session_id is None
        ):
            durability = "DEGRADED"
            durability_reason = "STORAGE_BOOTSTRAP_PENDING"
        elif self._persistence_error is not None:
            durability = "DEGRADED"
            durability_reason = self._persistence_error
        elif self._persistence_queue.qsize() > max(8, settings.persistence_queue_batches * 0.75):
            durability = "DEGRADED"
        return {
            "market_plane": "LIVE" if self.last_event is not None else "STARTING",
            "durability": durability,
            "durability_reason": durability_reason,
            "persistence_error": self._persistence_error,
            "persistence_queue_batches": self._persistence_queue.qsize(),
            "persistence_last_success_age_seconds": last_success_age,
            "persistence_dropped_events": self._persistence_dropped_events,
            "events_inserted": self.events_inserted,
            "events_duplicate": self.events_duplicate,
            "gaps_detected": self.gaps_detected,
            "last_event_time": (
                self.last_event.received_time.isoformat()
                if self.last_event is not None
                else None
            ),
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
        self._health_pending_rows.clear()
        self._pending_event_rows.clear()
        self._pending_event_started_monotonic = None
        self._last_bookticker_persist_monotonic.clear()
        self._persistence_error = None
        self._persistence_dropped_events = 0
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

                if (
                    production_scoped
                    and self.run_id is not None
                    and now_monotonic - self._last_runtime_heartbeat >= 5.0
                ):
                    try:
                        self.store.heartbeat_runtime_run(self.run_id)
                        self._last_runtime_heartbeat = now_monotonic
                    except Exception as exc:
                        self._persistence_error = (
                            f"runtime_heartbeat:{type(exc).__name__}: {exc}"
                        )
                        MARKET_CACHE.record_persistence_degradation(
                            self._persistence_error
                        )
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