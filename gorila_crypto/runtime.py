"""Prospective multi-venue market-data ingestion for the Crypto cleanroom.

A9 turns the proven adapter into a persistence-only 24/7 research feed.
It records raw normalized events, connection lifecycle, sequence gaps, and
source freshness. It never forecasts, trades, or promotes a model.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from gorila_core.market_freshness import assess_observation

from .binance import BinanceSpotMarketAdapter, BinanceStreamConfig, NormalizedMarketEvent
from .kraken import KrakenSpotMarketAdapter, KrakenStreamConfig
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
        self._watchdog_thread: threading.Thread | None = None
        self._feed_stop_event: threading.Event | None = None
        self._feed_watchdog_grace_until: float = 0.0
        self._feed_watchdog_interval_seconds = 5.0
        self._feed_stale_timeout_seconds = max(
            30.0,
            float(settings.event_live_max_age_seconds),
        )

        self.config.validate()

    @property
    def source_family(self) -> str:
        return str(getattr(self.adapter, "source_family", "crypto.websocket.market"))

    def _feed_watchdog(self) -> None:
        while not self.stop_event.wait(self._feed_watchdog_interval_seconds):
            now = self.now()
            now_monotonic = time.monotonic()
            # A feed reconnect gets a bounded grace window so the old event
            # timestamp does not immediately trigger a second reset.
            if now_monotonic < self._feed_watchdog_grace_until:
                continue
            if self.last_event is None:
                age_seconds = max(
                    0.0,
                    (now - self._capture_started_at).total_seconds(),
                )
            else:
                age_seconds = max(
                    0.0,
                    (now - self.last_event.received_time).total_seconds(),
                )
            if age_seconds <= self._feed_stale_timeout_seconds:
                continue
            self.last_error = f"market_feed_stale_after_{age_seconds:.1f}s"
            self._record_connection(
                "STALE_FEED",
                {
                    "age_seconds": age_seconds,
                    "timeout_seconds": self._feed_stale_timeout_seconds,
                    "symbol_health": self.symbol_health(reference=now),
                    "action": "RESTART_FEED_KEEP_SESSION",
                },
            )
            feed_stop_event = self._feed_stop_event
            if feed_stop_event is not None:
                feed_stop_event.set()
                self._feed_watchdog_grace_until = (
                    now_monotonic + self._feed_stale_timeout_seconds
                )
            # Keep the watchdog alive for the entire durable capture session.
            # The feed worker is restarted without ending the cohort.
            continue

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
        self.store.record_connection(
            source=self.source_family,
            status=status,
            metadata={
                "run_id": self.run_id,
                **(metadata or {}),
            },
        )

    def _on_connection(self, status: str, metadata: dict[str, Any]) -> None:
        if self.run_id is not None and isinstance(self.store, QuantCryptoStore):
            if not self.store.heartbeat_runtime_run(self.run_id):
                self.last_error = "runtime_lease_lost"
                self.stop_event.set()
                return
            self._last_runtime_heartbeat = time.monotonic()
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
        self.events_inserted += sum(1 for result in results if result["inserted"])
        self.events_duplicate += sum(1 for result in results if not result["inserted"])

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
                "received_age_seconds": assessment["transport_age_seconds"],
                "transport_latency_seconds": assessment["transport_age_seconds"],
            },
        )

        gap = self.sequence.observe(event)
        if gap is not None:
            expected, observed = gap
            self.gaps_detected += 1
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
                    "message": "Continuity gap detected within one connection epoch; "
                    "missing events were not synthesized.",
                },
            )

        source = event.source
        self._health_pending_rows[source] = self._health_pending_rows.get(source, 0) + 1
        now_monotonic = time.monotonic()
        last_persisted = self._health_last_persist_monotonic.get(source, 0.0)
        previous_status = self._health_last_status.get(source)
        should_persist_health = (
            previous_status != assessment["status"]
            or now_monotonic - last_persisted >= self.config.health_flush_interval_seconds
        )
        if should_persist_health:
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
            self._health_last_persist_monotonic[source] = now_monotonic
            self._health_last_status[source] = assessment["status"]
            self._health_pending_rows[source] = 0
        with self._symbol_lock:
            symbol = event.symbol.upper()
            self._symbol_first_received.setdefault(symbol, received_time)
            self._symbol_last_received[symbol] = received_time
        self.last_event = event
        if self._pending_event_started_monotonic is None:
            self._pending_event_started_monotonic = now_monotonic
        self._pending_event_rows.append(event_kwargs)

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
        """Run one durable capture session with recoverable feed restarts."""
        production_scoped = isinstance(self.store, QuantCryptoStore)
        if production_scoped:
            if not self.protocol.matches_runtime(
                provider=settings.provider,
                symbols=tuple(self.adapter.config.symbols),
                streams=tuple(settings.streams),
            ):
                raise RuntimeError("runtime_does_not_match_preregistered_protocol")
            stale = self.store.reconcile_stale_runtime_runs(stale_after_seconds=120.0)
            self.store.register_study(self.protocol)
            effective_protocol_hash = self.store.get_study_protocol_hash(self.protocol.study_id)
            while not self.stop_event.is_set():
                try:
                    self.session_id = self.store.start_capture_session(
                        study_id=self.protocol.study_id,
                        protocol_hash=effective_protocol_hash,
                        provider=settings.provider,
                        venue=self.protocol.venue,
                        symbols=tuple(self.adapter.config.symbols),
                        streams=tuple(settings.streams),
                        region=os.getenv("RENDER_REGION"),
                        instance_id=os.getenv("RENDER_INSTANCE_ID"),
                        code_version=os.getenv("RENDER_GIT_COMMIT") or os.getenv("GORILA_CRYPTO_CODE_VERSION"),
                        metadata={"stale_runs_reconciled": stale},
                    )
                    break
                except RuntimeError as exc:
                    reason = str(exc)
                    if not reason.startswith("active_capture_session_exists:"):
                        raise
                    self._record_connection("WAITING_FOR_ACTIVE_SESSION", {"reason": reason})
                    self.stop_event.wait(2.0)
            if self.stop_event.is_set():
                return {
                    "status": "STOPPED",
                    "run_id": None,
                    "events_inserted": 0,
                    "events_duplicate": 0,
                    "gaps_detected": 0,
                    "last_error": None,
                    "last_event_time": None,
                    "automatic_promotion": False,
                    "forecast": False,
                    "execution": False,
                }
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
        else:
            self.run_id = self.store.start_runtime_run(kind=self.config.kind)
            self._record_connection(
                "RUN_STARTED",
                {"symbols": list(self.adapter.config.symbols)},
            )

        self.events_inserted = 0
        self.events_duplicate = 0
        self.gaps_detected = 0
        self.last_error = None
        self._health_last_persist_monotonic.clear()
        self._health_last_status.clear()
        self._health_pending_rows.clear()
        self._pending_event_rows.clear()
        self._pending_event_started_monotonic = None
        with self._symbol_lock:
            self._symbol_first_received.clear()
            self._symbol_last_received.clear()
            self._capture_started_at = self.now()

        status = "STOPPED"
        result: dict[str, Any] = {}
        self._feed_watchdog_grace_until = (
            time.monotonic() + self._feed_stale_timeout_seconds
        )
        self._watchdog_thread = threading.Thread(
            target=self._feed_watchdog,
            name="gorila-crypto-feed-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

        try:
            while not self.stop_event.is_set():
                self._feed_stop_event = threading.Event()
                self._feed_watchdog_grace_until = (
                    time.monotonic() + self._feed_stale_timeout_seconds
                )
                feed_restart_requested = False
                for event in self.adapter.iter_forever(
                    stop_event=self._feed_stop_event,
                    on_connection=self._on_connection,
                ):
                    self._ingest(event)
                    now_monotonic = time.monotonic()
                    self._feed_watchdog_grace_until = 0.0
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
                    if production_scoped and now_monotonic - self._last_runtime_heartbeat >= 5.0:
                        if not self.store.heartbeat_runtime_run(self.run_id):
                            self.last_error = "runtime_lease_lost"
                            status = "LEASE_LOST"
                            self.stop_event.set()
                            break
                        self._last_runtime_heartbeat = now_monotonic
                    if self.stop_event.is_set():
                        status = "STOPPED"
                        break

                if self.stop_event.is_set():
                    status = "STOPPED"
                    break

                feed_restart_requested = self._feed_stop_event.is_set()
                self._flush_pending_events()

                if production_scoped:
                    self._record_connection(
                        "FEED_RESTARTED",
                        {
                            "reason": (
                                "STALE_FEED"
                                if feed_restart_requested
                                else "STREAM_ENDED"
                            ),
                            "capture_session_id": self.session_id,
                        },
                    )
                    self.adapter = build_market_adapter()
                    self.last_error = None
                    continue

                status = "STREAM_ENDED"
                break

        except KeyboardInterrupt:
            status = "STOPPED"
        except Exception as exc:
            status = "FAILED"
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._record_connection("ERROR", {"error": self.last_error})
            raise
        finally:
            self._feed_stop_event = None
            self._feed_watchdog_grace_until = 0.0
            self._flush_pending_events()
            if self._watchdog_thread is not None and self._watchdog_thread is not threading.current_thread():
                self.stop_event.set()
                self._watchdog_thread.join(timeout=2.0)
            result = {
                "events_inserted": self.events_inserted,
                "events_duplicate": self.events_duplicate,
                "gaps_detected": self.gaps_detected,
                "last_error": self.last_error,
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
                self.store.finish_runtime_run_scoped(
                    run_id=self.run_id,
                    session_id=self.session_id,
                    status=status,
                    result=result,
                )
                if self.session_id is not None:
                    self.store.set_capture_session_status(self.session_id, status)
            else:
                self.store.finish_runtime_run(
                    run_id=self.run_id,
                    status=status,
                    result=result,
                )
            self._record_connection("RUN_FINISHED", result)
            self.store.close()

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