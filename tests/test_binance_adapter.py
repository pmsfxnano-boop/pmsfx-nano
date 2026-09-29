"""Offline tests for the Binance A4 transport and sequencing boundary."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from gorila_crypto.binance import (
    BinanceAdapterError,
    BinanceStreamConfig,
    DepthGapDetected,
    InvalidMarketEvent,
    LocalOrderBookCoordinator,
    OrderBook,
    apply_book_delta,
    bootstrap_order_book,
    build_stream_names,
    build_ws_url,
    normalize_market_message,
    normalize_trade,
)


def _now() -> datetime:
    return datetime(2026, 9, 29, 15, 0, 0, tzinfo=timezone.utc)


def test_stream_names_are_deterministic_and_lowercase() -> None:
    config = BinanceStreamConfig(
        symbols=("BTCUSDT", "ETHUSDT"),
        streams=("trade", "bookTicker", "depth"),
    )
    assert build_stream_names(config) == (
        "btcusdt@trade",
        "btcusdt@bookTicker",
        "btcusdt@depth@100ms",
        "ethusdt@trade",
        "ethusdt@bookTicker",
        "ethusdt@depth@100ms",
    )
    url = build_ws_url(config)
    assert url.startswith("wss://stream.binance.com:9443/stream?")
    assert "btcusdt%40trade" in url


def test_trade_normalization_preserves_event_and_trade_time() -> None:
    raw = {
        "e": "trade",
        "E": 1770121200123,
        "s": "BTCUSDT",
        "t": 12345,
        "p": "60000.10",
        "q": "0.0100",
        "T": 1770121200119,
        "m": True,
        "M": True,
    }
    event = normalize_trade(
        raw,
        received_ns=1770121200123456789,
        received_time=_now(),
    )
    assert event.symbol == "BTCUSDT"
    assert event.event_type == "trade"
    assert event.sequence_start == 12345
    assert event.sequence_end == 12345
    assert event.provider_time is not None
    assert event.payload["p"] == "60000.10"
    assert event.receive_time_ns == 1770121200123456789


def test_combined_message_is_unwrapped_and_book_ticker_is_not_falsely_dated() -> None:
    raw = {
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "60000.0",
            "B": "1.2",
            "a": "60001.0",
            "A": "1.1",
        },
    }
    event = normalize_market_message(
        raw,
        received_ns=1770121200123456789,
        received_time=_now(),
    )
    assert event is not None
    assert event.event_type == "bookTicker"
    assert event.sequence_end == 400900217
    assert event.quality == "TRANSPORT_TIME_ONLY"
    assert event.payload["_event_time_semantics"] == "RECEIVE_TIME_ONLY"


def test_subscription_ack_is_not_market_event() -> None:
    assert normalize_market_message({"result": None, "id": "abc"}) is None


def test_depth_update_applies_levels_and_removes_zero_quantity() -> None:
    book = OrderBook(
        symbol="BTCUSDT",
        last_update_id=100,
        bids={Decimal("100.0"): Decimal("2.0")},
        asks={Decimal("101.0"): Decimal("3.0")},
    )
    event = {
        "U": 101,
        "u": 103,
        "b": [["100.0", "0"], ["99.5", "4.0"]],
        "a": [["101.0", "5.0"], ["101.5", "0.0"]],
    }
    apply_book_delta(book, event)
    assert book.last_update_id == 103
    assert Decimal("100.0") not in book.bids
    assert book.bids[Decimal("99.5")] == Decimal("4.0")
    assert book.asks[Decimal("101.0")] == Decimal("5.0")
    assert Decimal("101.5") not in book.asks


def test_stale_depth_update_is_ignored() -> None:
    book = OrderBook(symbol="BTCUSDT", last_update_id=100)
    apply_book_delta(
        book,
        {"U": 90, "u": 100, "b": [["100", "2"]], "a": []},
    )
    assert book.last_update_id == 100
    assert not book.bids


def test_depth_gap_is_hard_failure() -> None:
    book = OrderBook(symbol="BTCUSDT", last_update_id=100)
    with pytest.raises(DepthGapDetected):
        apply_book_delta(
            book,
            {"U": 103, "u": 105, "b": [], "a": []},
        )


def test_depth_bootstrap_accepts_only_the_first_bridging_event() -> None:
    snapshot = {
        "lastUpdateId": 100,
        "bids": [["100.0", "2.0"]],
        "asks": [["101.0", "3.0"]],
    }
    buffered = [
        {"U": 98, "u": 100, "b": [], "a": []},
        {"U": 101, "u": 102, "b": [["100.0", "1.0"]], "a": []},
        {"U": 103, "u": 104, "b": [], "a": [["101.0", "4.0"]]},
    ]
    book, applied = bootstrap_order_book(
        symbol="BTCUSDT",
        snapshot=snapshot,
        buffered_events=buffered,
    )
    assert book.last_update_id == 104
    assert applied[0]["U"] == 101
    assert book.bids[Decimal("100.0")] == Decimal("1.0")
    assert book.asks[Decimal("101.0")] == Decimal("4.0")


def test_depth_bootstrap_rejects_a_missing_bridge_instead_of_skipping() -> None:
    snapshot = {
        "lastUpdateId": 100,
        "bids": [],
        "asks": [],
    }
    buffered = [
        {"U": 102, "u": 103, "b": [], "a": []},
        {"U": 104, "u": 105, "b": [], "a": []},
    ]
    with pytest.raises(BinanceAdapterError, match="does not bridge"):
        bootstrap_order_book(
            symbol="BTCUSDT",
            snapshot=snapshot,
            buffered_events=buffered,
        )


def test_invalid_depth_shape_is_rejected() -> None:
    with pytest.raises(InvalidMarketEvent):
        normalize_market_message(
            {
                "e": "depthUpdate",
                "E": 1770121200123,
                "s": "BTCUSDT",
                "U": 10,
                "u": 11,
                "b": "not-a-list",
                "a": [],
            },
            received_time=_now(),
        )


def test_order_book_coordinator_resyncs_after_sequence_gap() -> None:
    coordinator = LocalOrderBookCoordinator("BTCUSDT")
    assert coordinator.buffer_or_apply(
        {"U": 101, "u": 102, "b": [], "a": []}
    ) == "BUFFERING"
    assert coordinator.install_snapshot(
        {
            "lastUpdateId": 100,
            "bids": [["100", "1"]],
            "asks": [["101", "1"]],
        }
    ) == "SYNCED"
    assert coordinator.state.book is not None
    assert coordinator.state.book.last_update_id == 102

    assert coordinator.buffer_or_apply(
        {"U": 105, "u": 106, "b": [], "a": []}
    ) == "RESYNC_REQUIRED"
    assert coordinator.state.book is None
    assert coordinator.state.resync_count == 1
    assert coordinator.state.last_gap is not None


def test_order_book_coordinator_requires_a_new_snapshot_after_gap() -> None:
    coordinator = LocalOrderBookCoordinator("BTCUSDT")
    assert coordinator.buffer_or_apply(
        {"U": 101, "u": 103, "b": [], "a": []}
    ) == "BUFFERING"
    assert coordinator.install_snapshot(
        {
            "lastUpdateId": 100,
            "bids": [],
            "asks": [],
        }
    ) == "SYNCED"
    assert coordinator.buffer_or_apply(
        {"U": 104, "u": 104, "b": [], "a": []}
    ) == "SYNCED"
