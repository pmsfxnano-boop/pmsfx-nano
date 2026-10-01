from __future__ import annotations

from datetime import datetime, timezone, timedelta

from gorila_crypto.cross_venue import (
    InstrumentSpec,
    QuoteObservation,
    audit_quote_observations,
)


def _t(seconds: float = 0.0) -> datetime:
    return datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def test_cross_venue_audit_blocks_different_quote_currencies() -> None:
    a = QuoteObservation(
        InstrumentSpec("kraken", "BTC/USD", "BTC", "USD"),
        100000.0,
        _t(),
    )
    b = QuoteObservation(
        InstrumentSpec("binance", "BTCUSDT", "BTC", "USDT"),
        100010.0,
        _t(0.01),
    )
    result = audit_quote_observations(a, b)
    assert result.status == "BLOCKED_INCOMPATIBLE_QUOTE"
    assert result.spread_bps is None


def test_cross_venue_audit_requires_temporally_aligned_receive_times() -> None:
    a = QuoteObservation(
        InstrumentSpec("venue-a", "BTC/USD", "BTC", "USD"),
        100000.0,
        _t(),
    )
    b = QuoteObservation(
        InstrumentSpec("venue-b", "BTC/USD", "BTC", "USD"),
        100010.0,
        _t(1.0),
    )
    result = audit_quote_observations(a, b, max_time_delta_ms=100.0)
    assert result.status == "BLOCKED_TIME_MISALIGNMENT"


def test_cross_venue_audit_compares_only_explicitly_compatible_units() -> None:
    a = QuoteObservation(
        InstrumentSpec("venue-a", "BTC/USD", "BTC", "USD"),
        100000.0,
        _t(),
    )
    b = QuoteObservation(
        InstrumentSpec("venue-b", "BTC/USD", "BTC", "USD"),
        100050.0,
        _t(0.01),
    )
    result = audit_quote_observations(a, b)
    assert result.status == "COMPARABLE"
    assert result.spread_bps is not None
