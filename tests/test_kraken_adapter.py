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
    assert messages[0]["params"]["channel"] == "instrument"
    assert messages[0]["params"]["symbol"] == ["BTC/USD", "ETH/USD"]
    assert messages[1]["params"]["channel"] == "trade"
    assert messages[1]["params"]["snapshot"] is False
    assert messages[2]["params"]["channel"] == "book"
    assert messages[2]["params"]["snapshot"] is True


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


def test_kraken_adapter_verifies_book_after_loading_instrument_precision() -> None:
    from gorila_crypto.kraken import KrakenSpotMarketAdapter
    from gorila_crypto.kraken_integrity import KrakenBookState, KrakenPrecision, kraken_checksum

    adapter = KrakenSpotMarketAdapter(
        KrakenStreamConfig(
            symbols=("BTC/USD",),
            streams=("bookTicker",),
            depth=10,
        )
    )
    state = KrakenBookState(
        symbol="BTC/USD",
        depth=10,
        bids={60000.0: 1.2},
        asks={60001.0: 1.1},
    )
    checksum, _ = kraken_checksum(state, KrakenPrecision(price=1, qty=1))
    instrument = {
        "channel": "instrument",
        "type": "snapshot",
        "data": [{"symbol": "BTC/USD", "price_precision": 1, "qty_precision": 1}],
    }
    snapshot = {
        "channel": "book",
        "type": "snapshot",
        "data": [{
            "symbol": "BTC/USD",
            "timestamp": "2026-09-29T20:00:00.123456Z",
            "bids": [{"price": 60000.0, "qty": 1.2}],
            "asks": [{"price": 60001.0, "qty": 1.1}],
            "checksum": checksum,
        }],
    }
    received_time = datetime(2026, 9, 29, 20, 0, 0, 130000, tzinfo=timezone.utc)
    list(adapter._events_from_message(
        instrument,
        received_time=received_time,
        receive_ns=123,
    ))
    events = list(adapter._events_from_message(
        snapshot,
        received_time=received_time,
        receive_ns=123,
    ))
    assert len(events) == 1
    assert events[0].quality == "INTEGRITY_VERIFIED"
    assert events[0].payload["_checksum_expected"] == checksum
    assert events[0].payload["_checksum_computed"] == checksum


def test_kraken_wire_json_preserves_decimal_tokens_without_float_rounding() -> None:
    from gorila_crypto.kraken import KrakenSpotMarketAdapter

    class FakeWS:
        def __init__(self) -> None:
            self.sent: list[str] = []
            self.payloads = iter([
                '{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","timestamp":"2026-09-29T20:00:00.123456Z","bids":[{"price":60000.10,"qty":1.2300}],"asks":[{"price":60001.20,"qty":0.0100}]}]}',
                None,
            ])

        def send(self, message: str) -> None:
            self.sent.append(message)

        def recv(self):
            return next(self.payloads)

    adapter = KrakenSpotMarketAdapter(
        KrakenStreamConfig(
            symbols=("BTC/USD",),
            streams=("bookTicker",),
            depth=10,
            connection_max_seconds=1.0,
        )
    )
    events = list(adapter.iter_events_once(ws=FakeWS()))
    assert len(events) == 1
    assert str(events[0].payload["bids"][0]["price"]) == "60000.10"
    assert str(events[0].payload["bids"][0]["qty"]) == "1.2300"
    assert str(events[0].payload["asks"][0]["price"]) == "60001.20"
    assert str(events[0].payload["asks"][0]["qty"]) == "0.0100"
