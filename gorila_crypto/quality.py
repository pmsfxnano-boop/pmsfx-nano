"""Replay data-quality gate for empirical Crypto validation.

The gate is descriptive and conservative. It prevents the OOS layer from treating
an incomplete or temporally incoherent prospective feed as reliable evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from statistics import quantiles
from typing import Any, Iterable, Mapping


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class DataQualityConfig:
    min_rows_per_symbol: int = 100
    min_duration_seconds: float = 3600.0
    max_invalid_timestamps: int = 0
    max_receive_time_reversals: int = 0
    max_required_source_gaps: int = 0
    max_p99_transport_latency_ms: float = 5000.0
    required_event_types: tuple[str, ...] = ("trade",)
    future_tolerance_seconds: float = 5.0

    def validate(self) -> None:
        if self.min_rows_per_symbol < 1:
            raise ValueError("min_rows_per_symbol must be positive")
        if self.min_duration_seconds < 0:
            raise ValueError("min_duration_seconds must be non-negative")
        if self.max_invalid_timestamps < 0:
            raise ValueError("max_invalid_timestamps must be non-negative")
        if self.max_receive_time_reversals < 0:
            raise ValueError("max_receive_time_reversals must be non-negative")
        if self.max_required_source_gaps < 0:
            raise ValueError("max_required_source_gaps must be non-negative")
        if self.max_p99_transport_latency_ms < 0:
            raise ValueError("max_p99_transport_latency_ms must be non-negative")
        if self.future_tolerance_seconds < 0:
            raise ValueError("future_tolerance_seconds must be non-negative")
        if not self.required_event_types:
            raise ValueError("required_event_types cannot be empty")


@dataclass(frozen=True)
class QualitySymbolStats:
    symbol: str
    rows: int
    duration_seconds: float
    transport_latency_p50_ms: float | None
    transport_latency_p95_ms: float | None
    transport_latency_p99_ms: float | None


@dataclass(frozen=True)
class DataQualityReport:
    status: str
    replay_fingerprint: str
    rows: int
    symbols: tuple[str, ...]
    invalid_timestamp_count: int
    future_event_count: int
    receive_time_reversal_count: int
    required_source_gap_count: int
    symbol_stats: Mapping[str, QualitySymbolStats]
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


def _pct(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    cuts = quantiles(values, n=100, method="inclusive")
    index = min(99, max(0, int(math.ceil(probability * 100.0)) - 1))
    return cuts[index]


def evaluate_replay_quality(
    rows: Iterable[Mapping[str, Any]],
    *,
    replay_fingerprint: str,
    config: DataQualityConfig,
    gap_rows: Iterable[Mapping[str, Any]] = (),
    reference_time: datetime | None = None,
) -> DataQualityReport:
    config.validate()
    cutoff = reference_time or datetime.now(timezone.utc)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    cutoff = cutoff.astimezone(timezone.utc)

    ordered = list(rows)
    by_symbol: dict[str, list[Mapping[str, Any]]] = {}
    invalid_timestamp_count = 0
    future_event_count = 0
    receive_time_reversal_count = 0

    previous_receive: dict[tuple[str, str, int], datetime] = {}
    for row in ordered:
        symbol = str(row.get("symbol") or "").upper()
        source = str(row.get("source") or "")
        event_time = _dt(row.get("event_time"))
        received_time = _dt(row.get("received_time"))
        if not symbol or event_time is None or received_time is None:
            invalid_timestamp_count += 1
            continue
        if event_time > cutoff:
            future_event_count += 1
        epoch = int((row.get("metadata") or {}).get("ingest_epoch") or 0)
        key = (symbol, source, epoch)
        previous = previous_receive.get(key)
        if previous is not None and received_time < previous:
            receive_time_reversal_count += 1
        previous_receive[key] = max(previous or received_time, received_time)
        by_symbol.setdefault(symbol, []).append(row)

    gaps = [
        row
        for row in gap_rows
        if str(row.get("event_type") or (row.get("metadata") or {}).get("event_type") or "")
        in config.required_event_types
        or str(row.get("source") or "") in config.required_event_types
    ]
    required_source_gap_count = len(gaps)

    symbol_stats: dict[str, QualitySymbolStats] = {}
    reasons: list[str] = []

    for symbol in sorted(by_symbol):
        symbol_rows = by_symbol[symbol]
        event_times = [_dt(row.get("event_time")) for row in symbol_rows]
        event_times = [value for value in event_times if value is not None]
        latencies = []
        for row in symbol_rows:
            event_time = _dt(row.get("event_time"))
            received_time = _dt(row.get("received_time"))
            if event_time is None or received_time is None:
                continue
            latencies.append(max(0.0, (received_time - event_time).total_seconds() * 1000.0))
        duration = (
            max(event_times).timestamp() - min(event_times).timestamp()
            if len(event_times) >= 2
            else 0.0
        )
        symbol_stats[symbol] = QualitySymbolStats(
            symbol=symbol,
            rows=len(symbol_rows),
            duration_seconds=float(duration),
            transport_latency_p50_ms=_pct(latencies, 0.50),
            transport_latency_p95_ms=_pct(latencies, 0.95),
            transport_latency_p99_ms=_pct(latencies, 0.99),
        )
        if len(symbol_rows) < config.min_rows_per_symbol:
            reasons.append(f"{symbol}:INSUFFICIENT_ROWS")
        if duration < config.min_duration_seconds:
            reasons.append(f"{symbol}:INSUFFICIENT_DURATION")
        if (
            symbol_stats[symbol].transport_latency_p99_ms is not None
            and symbol_stats[symbol].transport_latency_p99_ms
            > config.max_p99_transport_latency_ms
        ):
            reasons.append(f"{symbol}:P99_TRANSPORT_LATENCY")

    if not ordered:
        reasons.append("NO_ROWS")
    if invalid_timestamp_count > config.max_invalid_timestamps:
        reasons.append("INVALID_TIMESTAMPS")
    if future_event_count:
        reasons.append("FUTURE_EVENTS")
    if receive_time_reversal_count > config.max_receive_time_reversals:
        reasons.append("RECEIVE_TIME_REVERSAL")
    if required_source_gap_count > config.max_required_source_gaps:
        reasons.append("REQUIRED_SOURCE_GAPS")

    status = "PASS" if not reasons else "FAIL"
    return DataQualityReport(
        status=status,
        replay_fingerprint=replay_fingerprint,
        rows=len(ordered),
        symbols=tuple(sorted(by_symbol)),
        invalid_timestamp_count=invalid_timestamp_count,
        future_event_count=future_event_count,
        receive_time_reversal_count=receive_time_reversal_count,
        required_source_gap_count=required_source_gap_count,
        symbol_stats=symbol_stats,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def quality_fingerprint(report: DataQualityReport) -> str:
    payload = asdict(report)
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
