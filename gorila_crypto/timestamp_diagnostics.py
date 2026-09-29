"""Deterministic diagnosis of provider-event backlog versus local receive time.

The diagnostic never rewrites timestamps. It classifies a sequence of observations
from their original event_time and received_time fields so a stale provider clock,
a transport backlog, and a local clock inversion remain distinguishable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class TimestampDiagnostic:
    status: str
    observation_count: int
    median_transport_latency_ms: float | None
    first_transport_latency_ms: float | None
    last_transport_latency_ms: float | None
    latency_growth_ms: float | None
    event_time_span_seconds: float
    received_time_span_seconds: float
    reason: str


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def diagnose_timestamp_backlog(
    rows: Iterable[Mapping[str, Any]],
    *,
    minimum_observations: int = 10,
    growth_threshold_ms: float = 1000.0,
) -> TimestampDiagnostic:
    observations = []
    for row in rows:
        try:
            event = _dt(row["event_time"])
            received = _dt(row["received_time"])
        except (KeyError, TypeError, ValueError):
            continue
        observations.append((received, event))

    observations.sort(key=lambda item: item[0])
    n = len(observations)
    if n < minimum_observations:
        return TimestampDiagnostic(
            status="INSUFFICIENT_OBSERVATIONS",
            observation_count=n,
            median_transport_latency_ms=None,
            first_transport_latency_ms=None,
            last_transport_latency_ms=None,
            latency_growth_ms=None,
            event_time_span_seconds=0.0,
            received_time_span_seconds=0.0,
            reason=f"need_at_least_{minimum_observations}_valid_observations",
        )

    latencies = [
        (received - event).total_seconds() * 1000.0
        for received, event in observations
    ]
    first_latency = float(latencies[0])
    last_latency = float(latencies[-1])
    growth = last_latency - first_latency
    event_span = (observations[-1][1] - observations[0][1]).total_seconds()
    received_span = (observations[-1][0] - observations[0][0]).total_seconds()

    if any(latency < 0.0 for latency in latencies):
        return TimestampDiagnostic(
            status="CLOCK_OR_TIMESTAMP_INVERSION",
            observation_count=n,
            median_transport_latency_ms=float(median(latencies)),
            first_transport_latency_ms=first_latency,
            last_transport_latency_ms=last_latency,
            latency_growth_ms=float(growth),
            event_time_span_seconds=float(max(0.0, event_span)),
            received_time_span_seconds=float(max(0.0, received_span)),
            reason="at_least_one_event_time_is_after_receive_time",
        )

    if (
        received_span > 0.0
        and event_span >= 0.0
        and growth >= growth_threshold_ms
    ):
        return TimestampDiagnostic(
            status="PROVIDER_OR_TRANSPORT_BACKLOG",
            observation_count=n,
            median_transport_latency_ms=float(median(latencies)),
            first_transport_latency_ms=first_latency,
            last_transport_latency_ms=last_latency,
            latency_growth_ms=float(growth),
            event_time_span_seconds=float(event_span),
            received_time_span_seconds=float(received_span),
            reason="receive_clock_advanced_while_event_to_receive_lag_grew",
        )

    return TimestampDiagnostic(
        status="NO_PROGRESSIVE_BACKLOG_DETECTED",
        observation_count=n,
        median_transport_latency_ms=float(median(latencies)),
        first_transport_latency_ms=first_latency,
        last_transport_latency_ms=last_latency,
        latency_growth_ms=float(growth),
        event_time_span_seconds=float(max(0.0, event_span)),
        received_time_span_seconds=float(max(0.0, received_span)),
        reason="latency_did_not_grow_beyond_threshold",
    )
