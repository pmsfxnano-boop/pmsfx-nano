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

    def validate(self) -> None:
        if not self.kind.strip():
            raise ValueError("runtime kind cannot be empty")
        if not self.gap_status.strip() or not self.health_status.strip():
            raise ValueError("runtime statuses cannot be empty")
        if self.health_flush_interval_seconds <= 0:
            raise ValueError("health flush interval must be positive")


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

        self.config.validate()

    @property
    def source_family(self) -> str:
        return str(getattr(self.adapter, "source_family", "crypto.websocket.market"))

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
            self.store.heartbeat_runtime_run(self.run_id)
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
        if isinstance(self.store, QuantCryptoStore):
            if self.session_id is None:
                raise RuntimeError("capture_session_not_started")
            event_kwargs["metadata"].update({
                "crypto_study_id": self.protocol.study_id,
                "capture_session_id": self.session_id,
            })
            result = self.store.append_scoped_event(
                study_id=self.protocol.study_id,
                capture_session_id=self.session_id,
                **event_kwargs,
            )
        else:
            result = self.store.append_event(**event_kwargs)

        if result["inserted"]:
            self.events_inserted += 1
        else:
            self.events_duplicate += 1

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
        self.last_event = event

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> dict[str, Any]:
        """Run the capture runtime, with the preregistered cohort enforced in production."""
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
            self.session_id = self.store.start_capture_session(
                study_id=self.protocol.study_id,
                protocol_hash=self.protocol.protocol_hash,
                provider=settings.provider,
                venue=self.protocol.venue,
                symbols=tuple(self.adapter.config.symbols),
                streams=tuple(settings.streams),
                region=os.getenv("RENDER_REGION"),
                instance_id=os.getenv("RENDER_INSTANCE_ID"),
                code_version=os.getenv("RENDER_GIT_COMMIT") or os.getenv("GORILA_CRYPTO_CODE_VERSION"),
                metadata={
                    "stale_runs_reconciled": stale,
                    "instrument_specs": [list(item) for item in self.protocol.instrument_specs],
                },
            )
            self.run_id = self.store.start_runtime_run_scoped(
                kind=self.config.kind,
                session_id=self.session_id,
            )
            self._record_connection("RUN_STARTED", {
                "symbols": list(self.adapter.config.symbols),
                "study_id": self.protocol.study_id,
                "protocol_hash": self.protocol.protocol_hash,
                "capture_session_id": self.session_id,
            })
        else:
            # Legacy/unit-test harness: persistence semantics are still exercised,
            # but production-only study binding is deliberately not activated.
            self.run_id = self.store.start_runtime_run(kind=self.config.kind)
            self._record_connection("RUN_STARTED", {"symbols": list(self.adapter.config.symbols)})
        self.events_inserted = 0
        self.events_duplicate = 0
        self.gaps_detected = 0
        self.last_error = None
        self._health_last_persist_monotonic.clear()
        self._health_last_status.clear()
        self._health_pending_rows.clear()

        status = "STOPPED"
        result: dict[str, Any] = {}
        try:
            for event in self.adapter.iter_forever(
                stop_event=self.stop_event,
                on_connection=self._on_connection,
            ):
                self._ingest(event)
                now_monotonic = time.monotonic()
                if production_scoped and now_monotonic - self._last_runtime_heartbeat >= 5.0:
                    self.store.heartbeat_runtime_run(self.run_id)
                    self._last_runtime_heartbeat = now_monotonic
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
            raise
        finally:
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