"""Offline tests for the Binance A4 transport and sequencing boundary."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from decimal import Decimal
from urllib.parse import parse_qs, urlparse
import time

import pytest
import websocket

from gorila_crypto.binance import (
    BinanceAdapterError,
    BinanceSpotMarketAdapter,
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
    assert url.startswith("wss://data-stream.binance.vision:443/stream?")
    query = parse_qs(urlparse(url).query)
    assert query["streams"][0] == (
        "btcusdt@trade/btcusdt@bookTicker/btcusdt@depth@100ms/"
        "ethusdt@trade/ethusdt@bookTicker/ethusdt@depth@100ms"
    )


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

def test_market_data_stall_is_a_hard_transport_failure() -> None:
    class SilentControlSocket:
        def recv(self):
            time.sleep(0.02)
            return {"result": None, "id": "ack"}

    adapter = BinanceSpotMarketAdapter(
        BinanceStreamConfig(
            symbols=("BTCUSDT",),
            streams=("trade", "bookTicker"),
            recv_timeout_s=1.0,
            market_data_stall_timeout_s=0.01,
            connection_max_seconds=1.0,
        )
    )

    with pytest.raises(BinanceAdapterError, match="market_data_stall_timeout"):
        next(adapter.iter_events_once(ws=SilentControlSocket()))


def test_market_data_stall_watchdog_does_not_fire_between_live_events() -> None:
    class MarketSocket:
        def __init__(self) -> None:
            self.n = 0

        def recv(self):
            self.n += 1
            time.sleep(0.01)
            return json.dumps({
                "stream": "btcusdt@bookTicker",
                "data": {
                    "u": 400900217 + self.n,
                    "s": "BTCUSDT",
                    "b": "60000.0",
                    "B": "1.2",
                    "a": "60001.0",
                    "A": "1.1",
                },
            })

    adapter = BinanceSpotMarketAdapter(
        BinanceStreamConfig(
            symbols=("BTCUSDT",),
            streams=("bookTicker",),
            recv_timeout_s=1.0,
            websocket_read_poll_timeout_s=0.01,
            market_data_stall_timeout_s=0.05,
            connection_max_seconds=1.0,
        )
    )

    iterator = adapter.iter_events_once(ws=MarketSocket())
    first = next(iterator)
    second = next(iterator)

    assert first is not None
    assert second is not None


def test_market_data_watchdog_progresses_through_read_timeouts() -> None:
    class SilentSocket:
        def recv(self):
            time.sleep(0.01)
            raise websocket.WebSocketTimeoutException("read timeout")

    adapter = BinanceSpotMarketAdapter(
        BinanceStreamConfig(
            symbols=("BTCUSDT",),
            streams=("trade",),
            websocket_read_poll_timeout_s=0.005,
            market_data_stall_timeout_s=0.025,
            connection_max_seconds=1.0,
        )
    )

    with pytest.raises(BinanceAdapterError, match="market_data_stall_timeout"):
        next(adapter.iter_events_once(ws=SilentSocket()))
