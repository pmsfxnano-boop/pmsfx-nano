from __future__ import annotations

from gorila_crypto.timestamp_diagnostics import diagnose_timestamp_backlog


def _row(event_s: int, receive_s: int) -> dict:
    return {
        "event_time": f"2026-09-29T20:00:{event_s:02d}Z",
        "received_time": f"2026-09-29T20:00:{receive_s:02d}Z",
    }


def test_diagnostic_detects_progressive_provider_or_transport_backlog() -> None:
    rows = [_row(i, i + i // 2) for i in range(10, 20)]
    result = diagnose_timestamp_backlog(rows, growth_threshold_ms=500.0)
    assert result.status == "PROVIDER_OR_TRANSPORT_BACKLOG"
    assert result.latency_growth_ms is not None
    assert result.latency_growth_ms >= 4000.0


def test_diagnostic_detects_clock_inversion() -> None:
    rows = [_row(i, i - 1) for i in range(10, 20)]
    result = diagnose_timestamp_backlog(rows)
    assert result.status == "CLOCK_OR_TIMESTAMP_INVERSION"


def test_diagnostic_refuses_short_samples() -> None:
    result = diagnose_timestamp_backlog([_row(1, 1)])
    assert result.status == "INSUFFICIENT_OBSERVATIONS"
