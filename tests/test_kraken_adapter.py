from __future__ import annotations

from datetime import datetime, timezone

from gorila_crypto.kraken import (
    KrakenAdapterError,
    KrakenStreamConfig,
    normalize_trade_row,
    subscription_messages,
)


def test_kraken_subscriptions_are_explicit_and_deterministic() -> None:
    config = KrakenStreamConfig(
        symbols=("BTC/USD", "ETH/USD"),
        streams=("trade", "bookTicker"),
        depth=10,
    )
    messages = subscription_messages(config)
    assert messages[0]["params"]["channel"] == "trade"
    assert messages[0]["params"]["symbol"] == ["BTC/USD", "ETH/USD"]
    assert messages[1]["params"]["channel"] == "book"
    assert messages[1]["params"]["snapshot"] is True


def test_kraken_trade_normalization_preserves_provider_time_and_sequence() -> None:
    event = normalize_trade_row(
        {
            "symbol": "BTC/USD",
            "side": "buy",
            "price": 60000.1,
            "qty": 0.01,
            "ord_type": "limit",
            "trade_id": 123456,
            "timestamp": "2026-09-29T20:00:00.123456Z",
        },
        received_time=datetime(2026, 9, 29, 20, 0, 0, 130000, tzinfo=timezone.utc),
        receive_ns=123,
    )
    assert event.symbol == "BTC/USD"
    assert event.event_type == "trade"
    assert event.provider_time == event.event_time
    assert event.sequence_start == 123456
    assert event.quality == "OK"


def test_kraken_rejects_implicit_instrument_remapping() -> None:
    try:
        KrakenStreamConfig(symbols=("BTCUSDT",))
    except ValueError as exc:
        assert "explicit provider pairs" in str(exc)
    else:
        raise AssertionError("implicit BTCUSDT remapping was accepted")


def test_kraken_book_keeps_raw_l2_separate_from_derived_l1() -> None:
    from gorila_crypto.kraken import _book_event

    event = _book_event(
        {
            "symbol": "BTC/USD",
            "timestamp": "2026-09-29T20:00:00.123456Z",
            "bids": [{"price": "60000.0", "qty": "1.2"}],
            "asks": [{"price": "60001.0", "qty": "1.1"}],
        },
        received_time=datetime(2026, 9, 29, 20, 0, 0, 130000, tzinfo=timezone.utc),
        receive_ns=123,
        message_type="update",
    )
    assert event.event_type == "bookUpdate"
    assert event.quality == "INTEGRITY_UNVERIFIED"
    assert event.payload["bids"][0]["price"] == "60000.0"
    assert event.payload["_derived_l1"]["bid"]["price"] == "60000.0"
    assert "b" not in event.payload
    assert "a" not in event.payload
