"""Prospective Binance market-data ingestion for the Crypto cleanroom.

A9 turns the proven adapter into a persistence-only 24/7 research feed.
It records raw normalized events, connection lifecycle, sequence gaps, and
source freshness. It never forecasts, trades, or promotes a model.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from gorila_core.market_freshness import assess_observation

from .binance import BinanceSpotMarketAdapter, BinanceStreamConfig, NormalizedMarketEvent
from .kraken import KrakenSpotMarketAdapter, KrakenStreamConfig
from .config import settings
from .storage import CryptoStore, CRYPTO_DATABASE_URL


@dataclass(frozen=True)
class IngestRuntimeConfig:
    kind: str = "PROSPECTIVE_MARKET_INGEST"
    gap_status: str = "GAP_DETECTED"
    health_status: str = "HEALTHY"
    error_status: str = "DEGRADED"

    def validate(self) -> None:
        if not self.kind.strip():
            raise ValueError("runtime kind cannot be empty")
        if not self.gap_status.strip() or not self.health_status.strip():
            raise ValueError("runtime statuses cannot be empty")


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
        adapter: BinanceSpotMarketAdapter,
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
        if status == "CONNECTED":
            self.sequence.new_connection()
            self.last_error = None
            self._record_connection(status, metadata)
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
        result = self.store.append_event(
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
            },
        )

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
                    "event_type": event.event_type,
                    "ingest_epoch": self.sequence.epoch,
                    "message": "Continuity gap detected within one connection epoch; "
                    "missing events were not synthesized.",
                },
            )

        assessment = assess_observation(
            event_time,
            received_time,
            now=self.now(),
        )
        self.store.upsert_source_health(
            source=event.source,
            status=assessment["status"],
            last_event_time=assessment["event_time"],
            last_received_time=assessment["received_time"],
            event_age_seconds=assessment["event_age_seconds"],
            transport_age_seconds=assessment["transport_age_seconds"],
            rows_last_batch=1,
            error=None,
        )
        # Keep the legacy storage column stable while preserving the true
        # transport latency in the event's provenance metadata.
        result_metadata = getattr(result, "metadata", None)
        if isinstance(result_metadata, dict):
            result_metadata.update({
                "event_age_seconds": assessment["event_age_seconds"],
                "received_age_seconds": assessment["received_age_seconds"],
                "transport_latency_seconds": assessment["transport_latency_seconds"],
            })
        self.last_event = event

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> dict[str, Any]:
        """Run until stop_event, adapter failure, or external interruption."""
        self.run_id = self.store.start_runtime_run(kind=self.config.kind)
        self._record_connection("RUN_STARTED", {"symbols": list(self.adapter.config.symbols)})
        self.events_inserted = 0
        self.events_duplicate = 0
        self.gaps_detected = 0
        self.last_error = None

        status = "STOPPED"
        result: dict[str, Any] = {}
        try:
            for event in self.adapter.iter_forever(
                stop_event=self.stop_event,
                on_connection=self._on_connection,
            ):
                self._ingest(event)
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
            self.store.finish_runtime_run(
                run_id=self.run_id,
                status=status,
                result=result,
            )
            self._record_connection("RUN_FINISHED", result)

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
            streams=("trade", "bookTicker", "depth"),
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
        CryptoStore(database_url=CRYPTO_DATABASE_URL, require_durable=True),
        adapter,
    )


def main() -> None:
    build_prospective_runtime().run()


if __name__ == "__main__":
    main()
