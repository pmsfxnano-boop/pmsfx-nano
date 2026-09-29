"""Kraken Spot WebSocket v2 market-data adapter for the Crypto cleanroom.

The adapter is intentionally read-only and normalizes Kraken public trade/L2
events into the same immutable ledger event contract used by the research
runtime. It keeps provider timestamps separate from local receive timestamps.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Callable

import websocket

from .binance import NormalizedMarketEvent
from .kraken_integrity import (
    KrakenBookState,
    KrakenChecksumMismatch,
    KrakenPrecision,
    apply_and_verify,
)


WS_BASE = "wss://ws.kraken.com/v2"


class KrakenAdapterError(RuntimeError):
    """Base error for Kraken public market-data transport."""


def _timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise KrakenAdapterError(f"{field_name} must be an RFC3339 timestamp")
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise KrakenAdapterError(f"invalid {field_name}: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _number(value: Any, field_name: str) -> str:
    text = str(value)
    try:
        number = float(text)
    except (TypeError, ValueError) as exc:
        raise KrakenAdapterError(f"{field_name} must be numeric") from exc
    if not number == number or number in (float("inf"), float("-inf")):
        raise KrakenAdapterError(f"{field_name} must be finite")
    return text


def _provider_symbol(symbol: str) -> str:
    text = symbol.strip().upper()
    if "/" in text and text.count("/") == 1 and all(text.split("/")):
        return text
    raise ValueError(
        f"Kraken symbols must be explicit provider pairs such as BASE/USD; received {symbol!r}"
    )


@dataclass(frozen=True)
class KrakenStreamConfig:
    symbols: tuple[str, ...]
    streams: tuple[str, ...] = ("trade", "bookTicker")
    depth: int = 10
    ws_url: str = WS_BASE
    connect_timeout_s: float = 10.0
    recv_timeout_s: float = 20.0
    connection_max_seconds: float = 23.5 * 3600.0
    ping_interval_s: float = 20.0

    def __post_init__(self) -> None:
        symbols = tuple(dict.fromkeys(s.strip().upper() for s in self.symbols if s.strip()))
        streams = tuple(dict.fromkeys(s.strip() for s in self.streams if s.strip()))
        if not symbols:
            raise ValueError("at least one Kraken symbol is required")
        if not streams:
            raise ValueError("at least one Kraken stream is required")
        if any(stream not in {"trade", "bookTicker", "depth"} for stream in streams):
            raise ValueError(f"unsupported Kraken stream: {streams}")
        if self.depth not in {10, 25, 100, 500, 1000}:
            raise ValueError("Kraken book depth must be one of 10,25,100,500,1000")
        if self.connect_timeout_s <= 0 or self.recv_timeout_s <= 0:
            raise ValueError("timeouts must be positive")
        for symbol in symbols:
            _provider_symbol(symbol)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "streams", streams)


def subscription_messages(config: KrakenStreamConfig) -> tuple[dict[str, Any], ...]:
    symbols = [_provider_symbol(symbol) for symbol in config.symbols]
    messages: list[dict[str, Any]] = []
    if "bookTicker" in config.streams or "depth" in config.streams:
        messages.append({
            "method": "subscribe",
            "params": {"channel": "instrument", "symbol": symbols, "snapshot": True},
            "req_id": 0,
        })
    if "trade" in config.streams:
        messages.append(
            {
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": symbols,
                    "snapshot": False,
                },
                "req_id": 1,
            }
        )
    if "bookTicker" in config.streams or "depth" in config.streams:
        messages.append(
            {
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": symbols,
                    "depth": config.depth,
                    "snapshot": True,
                },
                "req_id": 2,
            }
        )
    return tuple(messages)


def normalize_trade_row(
    row: Mapping[str, Any],
    *,
    received_time: datetime,
    receive_ns: int,
) -> NormalizedMarketEvent:
    symbol = str(row.get("symbol") or "").upper()
    trade_id = row.get("trade_id")
    if not symbol or trade_id is None:
        raise KrakenAdapterError("trade event missing symbol/trade_id")
    event_time = _timestamp(row.get("timestamp"), "timestamp")
    payload = dict(row)
    payload["_provider"] = "kraken"
    payload["_event_time_semantics"] = "PROVIDER_TIMESTAMP"
    return NormalizedMarketEvent(
        symbol=symbol,
        event_type="trade",
        event_time=event_time,
        received_time=received_time,
        source="kraken.websocket.trade",
        payload=payload,
        provider_time=event_time,
        sequence_start=int(trade_id),
        sequence_end=int(trade_id),
        sequence_kind="trade_id",
        receive_time_ns=receive_ns,
        quality="OK",
    )


def _book_event(
    row: Mapping[str, Any],
    *,
    received_time: datetime,
    receive_ns: int,
    message_type: str,
) -> NormalizedMarketEvent:
    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        raise KrakenAdapterError("book event missing symbol")
    event_time = _timestamp(row.get("timestamp"), "timestamp")
    bids = row.get("bids")
    asks = row.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise KrakenAdapterError("book event bids/asks must be arrays")

    payload = dict(row)
    payload["_provider"] = "kraken"
    payload["_event_time_semantics"] = "PROVIDER_TIMESTAMP"
    payload["_message_type"] = message_type
    payload["_raw_book_preserved"] = True

    derived_l1: dict[str, dict[str, str]] = {}
    if bids:
        best_bid = bids[0]
        if isinstance(best_bid, Mapping):
            derived_l1["bid"] = {
                "price": _number(best_bid.get("price"), "bid.price"),
                "qty": _number(best_bid.get("qty"), "bid.qty"),
            }
    if asks:
        best_ask = asks[0]
        if isinstance(best_ask, Mapping):
            derived_l1["ask"] = {
                "price": _number(best_ask.get("price"), "ask.price"),
                "qty": _number(best_ask.get("qty"), "ask.qty"),
            }
    payload["_derived_l1"] = derived_l1

    return NormalizedMarketEvent(
        symbol=symbol,
        event_type="bookUpdate",
        event_time=event_time,
        received_time=received_time,
        provider_time=event_time,
        source="kraken.websocket.book",
        payload=payload,
        sequence_start=None,
        sequence_end=None,
        sequence_kind="checksum" if row.get("checksum") is not None else None,
        receive_time_ns=receive_ns,
        quality="INTEGRITY_UNVERIFIED",
    )


class KrakenSpotMarketAdapter:
    """Reconnectable public Kraken v2 market-data adapter."""

    source_family = "kraken.websocket.market"

    def __init__(
        self,
        config: KrakenStreamConfig,
        *,
        event_sink: Callable[[NormalizedMarketEvent], None] | None = None,
    ) -> None:
        self.config = config
        self.event_sink = event_sink
        self._precisions: dict[str, KrakenPrecision] = {}
        self._books: dict[str, KrakenBookState] = {}

    def connection_url(self) -> str:
        return self.config.ws_url

    def connect(self):
        return websocket.create_connection(
            self.connection_url(),
            timeout=self.config.recv_timeout_s,
            ping_interval=self.config.ping_interval_s,
            enable_multithread=True,
        )

    def _events_from_message(
        self,
        message: Mapping[str, Any],
        *,
        received_time: datetime,
        receive_ns: int,
    ) -> Iterator[NormalizedMarketEvent]:
        channel = str(message.get("channel") or "")
        message_type = str(message.get("type") or "")
        data = message.get("data")
        if message.get("success") is False or message.get("error"):
            raise KrakenAdapterError(str(message.get("error") or "Kraken subscription error"))
        if channel == "instrument":
            if not isinstance(data, list):
                raise KrakenAdapterError("Kraken instrument data must be a list")
            for row in data:
                if not isinstance(row, Mapping):
                    raise KrakenAdapterError("Kraken instrument row must be an object")
                symbol = str(row.get("symbol") or "").upper()
                price_precision = row.get("price_precision")
                qty_precision = row.get("qty_precision")
                if symbol and price_precision is not None and qty_precision is not None:
                    precision = KrakenPrecision(price=int(price_precision), qty=int(qty_precision))
                    precision.validate()
                    self._precisions[symbol] = precision
            return
        if channel not in {"trade", "book"} or not isinstance(data, list):
            return
        for row in data:
            if not isinstance(row, Mapping):
                raise KrakenAdapterError("Kraken channel row must be an object")
            if channel == "trade":
                yield normalize_trade_row(
                    row,
                    received_time=received_time,
                    receive_ns=receive_ns,
                )
            else:
                event = _book_event(
                    row,
                    received_time=received_time,
                    receive_ns=receive_ns,
                    message_type=message_type,
                )
                symbol = event.symbol.upper()
                book = self._books.setdefault(
                    symbol,
                    KrakenBookState(symbol=symbol, depth=self.config.depth),
                )
                precision = self._precisions.get(symbol)
                integrity = apply_and_verify(
                    book,
                    row,
                    message_type=message_type,
                    precision=precision,
                )
                payload = dict(event.payload)
                payload["_derived_l1"] = dict(integrity.derived_l1)
                payload["_integrity_status"] = integrity.status
                payload["_integrity_reason"] = integrity.reason
                payload["_checksum_expected"] = integrity.expected_checksum
                payload["_checksum_computed"] = integrity.computed_checksum
                if integrity.checksum_payload is not None:
                    payload["_checksum_payload"] = integrity.checksum_payload
                if precision is not None:
                    payload["_checksum_precision"] = {
                        "price": precision.price,
                        "qty": precision.qty,
                    }
                event = NormalizedMarketEvent(
                    symbol=event.symbol,
                    event_type=event.event_type,
                    event_time=event.event_time,
                    received_time=event.received_time,
                    source=event.source,
                    payload=payload,
                    provider_time=event.provider_time,
                    sequence_start=event.sequence_start,
                    sequence_end=event.sequence_end,
                    sequence_kind="kraken_crc32",
                    receive_time_ns=event.receive_time_ns,
                    quality=integrity.status,
                )
                yield event
                if integrity.status == "INTEGRITY_CHECKSUM_FAIL":
                    raise KrakenChecksumMismatch(
                        f"{symbol}: expected checksum {integrity.expected_checksum}, "
                        f"computed {integrity.computed_checksum}; connection must resync"
                    )

    def iter_events_once(self, *, ws) -> Iterator[NormalizedMarketEvent]:
        for request in subscription_messages(self.config):
            ws.send(json.dumps(request, separators=(",", ":")))

        connected_ns = time.time_ns()
        while (time.time_ns() - connected_ns) / 1_000_000_000 < self.config.connection_max_seconds:
            raw = ws.recv()
            if raw is None:
                break
            receive_ns = time.time_ns()
            received_time = datetime.fromtimestamp(
                receive_ns / 1_000_000_000,
                tz=timezone.utc,
            )
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                message = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise KrakenAdapterError("Kraken websocket payload is not valid JSON") from exc
            if not isinstance(message, Mapping):
                raise KrakenAdapterError("Kraken websocket message must be an object")
            yield from self._events_from_message(
                message,
                received_time=received_time,
                receive_ns=receive_ns,
            )

    def close(self, ws) -> None:
        try:
            ws.close()
        except Exception:
            pass

    def iter_forever(
        self,
        *,
        stop_event=None,
        on_connection: Callable[[str, dict[str, Any]], None] | None = None,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 60.0,
    ) -> Iterator[NormalizedMarketEvent]:
        backoff = max(0.1, float(initial_backoff_s))
        while stop_event is None or not stop_event.is_set():
            ws = None
            try:
                if on_connection:
                    on_connection("CONNECTING", {"provider": "kraken"})
                ws = self.connect()
                backoff = max(0.1, float(initial_backoff_s))
                if on_connection:
                    on_connection("CONNECTED", {"provider": "kraken"})
                for event in self.iter_events_once(ws=ws):
                    if self.event_sink is not None:
                        self.event_sink(event)
                    yield event
                    if stop_event is not None and stop_event.is_set():
                        return
            except Exception as exc:
                if on_connection:
                    on_connection(
                        "ERROR",
                        {
                            "provider": "kraken",
                            "error": f"{type(exc).__name__}: {exc}",
                        },
                    )
                if stop_event is not None and stop_event.is_set():
                    return
                time.sleep(backoff)
                backoff = min(max_backoff_s, backoff * 2.0)
            finally:
                if ws is not None:
                    self.close(ws)
                if on_connection:
                    on_connection("DISCONNECTED", {"provider": "kraken"})
