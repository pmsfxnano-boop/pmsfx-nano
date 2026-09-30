"""Binance Spot market-data adapter for the Crypto cleanroom.

Design goals:
- preserve provider event time separately from local receive time;
- preserve sequence/update identifiers without inventing continuity;
- use the official combined public market streams;
- bootstrap local books from REST snapshots plus buffered depth updates;
- force a full resynchronization on a detected depth gap;
- expose deterministic pure parsing/state-machine functions for offline tests.

The adapter is intentionally not started by gorila_crypto.app yet. A4 proves the
transport and sequencing boundary before a 24/7 worker is introduced.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib.parse import urlencode

import httpx
import websocket


SPOT_WS_BASE = "wss://data-stream.binance.vision:443/stream"
SPOT_REST_BASE = "https://data-api.binance.vision"
DEFAULT_DEPTH_SPEED = "100ms"
DEFAULT_DEPTH_LIMIT = 5000

SUPPORTED_STREAMS = frozenset({"trade", "aggTrade", "bookTicker", "depth"})


class BinanceAdapterError(RuntimeError):
    """Base error for Binance cleanroom adapter failures."""


class InvalidMarketEvent(BinanceAdapterError):
    """Raised when a provider message cannot be normalized safely."""


class DepthGapDetected(BinanceAdapterError):
    """Raised when the local order-book sequence is no longer contiguous."""


@dataclass(frozen=True)
class BinanceStreamConfig:
    symbols: tuple[str, ...]
    streams: tuple[str, ...] = ("trade", "bookTicker", "depth")
    depth_speed: str = DEFAULT_DEPTH_SPEED
    ws_base_url: str = SPOT_WS_BASE
    rest_base_url: str = SPOT_REST_BASE
    depth_limit: int = DEFAULT_DEPTH_LIMIT
    connect_timeout_s: float = 10.0
    ping_interval_s: float | None = None
    recv_timeout_s: float = 20.0
    connection_max_seconds: float = 23.5 * 3600.0

    def __post_init__(self) -> None:
        symbols = tuple(dict.fromkeys(s.strip().upper() for s in self.symbols if s.strip()))
        if not symbols:
            raise ValueError("at least one Binance symbol is required")
        streams = tuple(dict.fromkeys(s.strip() for s in self.streams if s.strip()))
        if not streams:
            raise ValueError("at least one Binance stream is required")
        if any(stream not in SUPPORTED_STREAMS for stream in streams):
            raise ValueError(f"unsupported Binance stream: {streams}")
        if self.depth_speed not in {"100ms", "1000ms"}:
            raise ValueError("depth_speed must be 100ms or 1000ms")
        if not 1 <= self.depth_limit <= DEFAULT_DEPTH_LIMIT:
            raise ValueError("depth_limit must be between 1 and 5000")
        if self.connect_timeout_s <= 0 or self.recv_timeout_s <= 0:
            raise ValueError("timeouts must be positive")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "streams", streams)


@dataclass(frozen=True)
class NormalizedMarketEvent:
    symbol: str
    event_type: str
    event_time: datetime
    received_time: datetime
    source: str
    payload: dict[str, Any]
    provider_time: datetime | None = None
    sequence_start: int | None = None
    sequence_end: int | None = None
    sequence_kind: str | None = None
    receive_time_ns: int | None = None
    quality: str = "OK"


@dataclass
class OrderBook:
    symbol: str
    last_update_id: int
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)


def _epoch_ms(value: Any, field_name: str) -> datetime:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise InvalidMarketEvent(f"{field_name} must be epoch milliseconds")
    if number < 0:
        raise InvalidMarketEvent(f"{field_name} cannot be negative")
    return datetime.fromtimestamp(number / 1000.0, tz=timezone.utc)


def _decimal(value: Any, field_name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise InvalidMarketEvent(f"{field_name} must be numeric")
    if not number.is_finite():
        raise InvalidMarketEvent(f"{field_name} must be finite")
    return number


def build_stream_names(config: BinanceStreamConfig) -> tuple[str, ...]:
    result: list[str] = []
    for symbol in config.symbols:
        lower = symbol.lower()
        for stream in config.streams:
            if stream == "depth":
                result.append(f"{lower}@depth@{config.depth_speed}")
            else:
                result.append(f"{lower}@{stream}")
    return tuple(result)


def build_ws_url(config: BinanceStreamConfig) -> str:
    query = urlencode({"streams": "/".join(build_stream_names(config))})
    return f"{config.ws_base_url}?{query}"


def unwrap_message(message: Mapping[str, Any]) -> dict[str, Any] | None:
    if "result" in message and "id" in message:
        return None
    if "code" in message and "msg" in message:
        raise BinanceAdapterError(f"Binance websocket error {message.get('code')}: {message.get('msg')}")
    data = message.get("data") if "stream" in message else message
    if not isinstance(data, Mapping):
        raise InvalidMarketEvent("websocket message does not contain a market-data object")
    return dict(data)


def normalize_trade(
    data: Mapping[str, Any],
    *,
    received_ns: int | None = None,
    received_time: datetime | None = None,
) -> NormalizedMarketEvent:
    if data.get("e") != "trade":
        raise InvalidMarketEvent("expected trade event")
    symbol = str(data.get("s") or "").upper()
    trade_id = data.get("t")
    if not symbol or trade_id is None:
        raise InvalidMarketEvent("trade event missing symbol/trade id")
    received = received_time or datetime.now(timezone.utc)
    payload = dict(data)
    payload["_provider"] = "binance"
    payload["_event_time_semantics"] = "PROVIDER_EVENT_TIME"
    return NormalizedMarketEvent(
        symbol=symbol,
        event_type="trade",
        event_time=_epoch_ms(data.get("E"), "E"),
        received_time=received,
        provider_time=_epoch_ms(data.get("T"), "T") if data.get("T") is not None else None,
        source="binance.websocket.trade",
        payload=payload,
        sequence_start=int(trade_id),
        sequence_end=int(trade_id),
        sequence_kind="trade_id",
        receive_time_ns=received_ns,
    )


def normalize_book_ticker(
    data: Mapping[str, Any],
    *,
    received_ns: int | None = None,
    received_time: datetime | None = None,
) -> NormalizedMarketEvent:
    if data.get("s") is None or data.get("u") is None:
        raise InvalidMarketEvent("bookTicker missing symbol/update id")
    symbol = str(data["s"]).upper()
    received = received_time or datetime.now(timezone.utc)
    payload = dict(data)
    payload["_provider"] = "binance"
    payload["_event_time_semantics"] = "RECEIVE_TIME_ONLY"
    return NormalizedMarketEvent(
        symbol=symbol,
        event_type="bookTicker",
        event_time=received,
        received_time=received,
        source="binance.websocket.bookTicker",
        payload=payload,
        sequence_start=int(data["u"]),
        sequence_end=int(data["u"]),
        sequence_kind="book_update_id",
        receive_time_ns=received_ns,
        quality="TRANSPORT_TIME_ONLY",
    )


def normalize_depth(
    data: Mapping[str, Any],
    *,
    received_ns: int | None = None,
    received_time: datetime | None = None,
) -> NormalizedMarketEvent:
    if data.get("e") != "depthUpdate":
        raise InvalidMarketEvent("expected depthUpdate event")
    symbol = str(data.get("s") or "").upper()
    first_id = data.get("U")
    final_id = data.get("u")
    if not symbol or first_id is None or final_id is None:
        raise InvalidMarketEvent("depth event missing symbol/update ids")
    if int(final_id) < int(first_id):
        raise InvalidMarketEvent("depth event has u < U")
    if not isinstance(data.get("b"), list) or not isinstance(data.get("a"), list):
        raise InvalidMarketEvent("depth event bids/asks must be lists")
    received = received_time or datetime.now(timezone.utc)
    payload = dict(data)
    payload["_provider"] = "binance"
    payload["_event_time_semantics"] = "PROVIDER_EVENT_TIME"
    return NormalizedMarketEvent(
        symbol=symbol,
        event_type="depthUpdate",
        event_time=_epoch_ms(data.get("E"), "E"),
        received_time=received,
        source="binance.websocket.depth",
        payload=payload,
        provider_time=_epoch_ms(data.get("E"), "E"),
        sequence_start=int(first_id),
        sequence_end=int(final_id),
        sequence_kind="book_update_id",
        receive_time_ns=received_ns,
    )


def normalize_market_message(
    message: Mapping[str, Any],
    *,
    received_ns: int | None = None,
    received_time: datetime | None = None,
) -> NormalizedMarketEvent | None:
    stream_name = str(message.get("stream") or "").lower()
    data = unwrap_message(message)
    if data is None:
        return None
    event_type = str(data.get("e") or "")
    if event_type == "trade":
        return normalize_trade(data, received_ns=received_ns, received_time=received_time)
    if event_type == "depthUpdate":
        return normalize_depth(data, received_ns=received_ns, received_time=received_time)
    if event_type == "bookTicker" or stream_name.endswith("@bookticker"):
        return normalize_book_ticker(data, received_ns=received_ns, received_time=received_time)
    return None


def apply_book_delta(
    book: OrderBook,
    event: Mapping[str, Any],
) -> OrderBook:
    """Apply one contiguous depth update to a local book.

    The exact sequence rule is:
    - u <= last_update_id -> stale/duplicate, ignore;
    - U > last_update_id + 1 -> gap, invalidate and require resync;
    - otherwise apply all levels and advance to u.
    """
    first_id = int(event["U"])
    final_id = int(event["u"])

    if final_id <= book.last_update_id:
        return book
    if first_id > book.last_update_id + 1:
        raise DepthGapDetected(
            f"{book.symbol}: expected <= {book.last_update_id + 1}, "
            f"received range [{first_id}, {final_id}]"
        )

    for side_name, target in (("b", book.bids), ("a", book.asks)):
        updates = event.get(side_name, [])
        for row in updates:
            if not isinstance(row, (list, tuple)) or len(row) != 2:
                raise InvalidMarketEvent(f"invalid {side_name} depth row")
            price = _decimal(row[0], f"{side_name}.price")
            quantity = _decimal(row[1], f"{side_name}.quantity")
            if quantity < 0:
                raise InvalidMarketEvent(f"{side_name}.quantity cannot be negative")
            if quantity == 0:
                target.pop(price, None)
            else:
                target[price] = quantity

    book.last_update_id = final_id
    return book


def bootstrap_order_book(
    *,
    symbol: str,
    snapshot: Mapping[str, Any],
    buffered_events: Iterable[Mapping[str, Any]],
) -> tuple[OrderBook, list[Mapping[str, Any]]]:
    """Construct a local book from a REST snapshot plus pre-snapshot WS buffer."""
    snapshot_id = int(snapshot["lastUpdateId"])
    bids = snapshot.get("bids", [])
    asks = snapshot.get("asks", [])
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise InvalidMarketEvent("snapshot bids/asks must be lists")

    buffered = [dict(event) for event in buffered_events]
    if not buffered:
        raise BinanceAdapterError("cannot bootstrap an order book without buffered depth events")

    filtered = [
        event for event in buffered
        if int(event["u"]) > snapshot_id
    ]
    if not filtered:
        raise BinanceAdapterError("buffer contains no post-snapshot depth event")
    first_post_snapshot = filtered[0]
    if not (int(first_post_snapshot["U"]) <= snapshot_id + 1 <= int(first_post_snapshot["u"])):
        raise BinanceAdapterError("snapshot does not bridge the first buffered post-snapshot depth event")

    book = OrderBook(symbol=symbol.upper(), last_update_id=snapshot_id)
    for row in bids:
        if len(row) != 2:
            raise InvalidMarketEvent("invalid snapshot bid row")
        price = _decimal(row[0], "snapshot.bid.price")
        quantity = _decimal(row[1], "snapshot.bid.quantity")
        if quantity > 0:
            book.bids[price] = quantity
    for row in asks:
        if len(row) != 2:
            raise InvalidMarketEvent("invalid snapshot ask row")
        price = _decimal(row[0], "snapshot.ask.price")
        quantity = _decimal(row[1], "snapshot.ask.quantity")
        if quantity > 0:
            book.asks[price] = quantity

    ordered = filtered
    apply_book_delta(book, ordered[0])
    for event in ordered[1:]:
        apply_book_delta(book, event)
    return book, ordered


@dataclass
class OrderBookSyncState:
    symbol: str
    status: str = "AWAITING_SNAPSHOT"
    book: OrderBook | None = None
    buffered_events: list[Mapping[str, Any]] = field(default_factory=list)
    resync_count: int = 0
    last_gap: str | None = None


class LocalOrderBookCoordinator:
    """Explicit snapshot + diff-depth synchronization state machine."""

    def __init__(self, symbol: str) -> None:
        self.state = OrderBookSyncState(symbol=symbol.upper())

    def buffer_or_apply(self, event: Mapping[str, Any]) -> str:
        if self.state.book is None:
            self.state.buffered_events.append(dict(event))
            self.state.status = "BUFFERING"
            return self.state.status

        try:
            apply_book_delta(self.state.book, event)
        except DepthGapDetected as exc:
            self.state.last_gap = str(exc)
            self.state.resync_count += 1
            self.state.book = None
            self.state.buffered_events = [dict(event)]
            self.state.status = "RESYNC_REQUIRED"
            return self.state.status

        self.state.status = "SYNCED"
        return self.state.status

    def install_snapshot(self, snapshot: Mapping[str, Any]) -> str:
        try:
            book, applied = bootstrap_order_book(
                symbol=self.state.symbol,
                snapshot=snapshot,
                buffered_events=self.state.buffered_events,
            )
        except BinanceAdapterError as exc:
            self.state.last_gap = str(exc)
            self.state.resync_count += 1
            self.state.book = None
            self.state.status = "RESYNC_REQUIRED"
            return self.state.status

        self.state.book = book
        self.state.buffered_events = []
        self.state.status = "SYNCED"
        return self.state.status


class BinanceRestClient:
    def __init__(self, *, base_url: str = SPOT_REST_BASE, timeout_s: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s

    def depth_snapshot(self, symbol: str, limit: int = DEFAULT_DEPTH_LIMIT) -> dict[str, Any]:
        if not 1 <= limit <= DEFAULT_DEPTH_LIMIT:
            raise ValueError("snapshot limit must be between 1 and 5000")
        with httpx.Client(base_url=self.base_url, timeout=self.timeout_s) as client:
            response = client.get(
                "/api/v3/depth",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict) or "lastUpdateId" not in payload:
            raise BinanceAdapterError("invalid Binance depth snapshot response")
        return payload


class BinanceSpotMarketAdapter:
    """Single-connection market stream adapter.

    This object is a transport component. It does not start automatically and it
    does not make forecasts or trading decisions.
    """

    source_family = "binance.websocket.market"

    def __init__(
        self,
        config: BinanceStreamConfig,
        *,
        rest_client: BinanceRestClient | None = None,
        event_sink: Callable[[NormalizedMarketEvent], None] | None = None,
    ) -> None:
        self.config = config
        self.rest_client = rest_client or BinanceRestClient(
            base_url=config.rest_base_url,
            timeout_s=config.connect_timeout_s,
        )
        self.event_sink = event_sink

    def connection_url(self) -> str:
        return build_ws_url(self.config)

    def subscription_request(self) -> dict[str, Any]:
        return {
            "method": "SUBSCRIBE",
            "params": list(build_stream_names(self.config)),
            "id": uuid.uuid4().hex[:32],
        }

    def connect(self):
        return websocket.create_connection(
            self.connection_url(),
            timeout=self.config.recv_timeout_s,
            ping_interval=self.config.ping_interval_s,
            enable_multithread=True,
        )

    def iter_events_once(self, *, ws) -> Iterator[NormalizedMarketEvent]:
        connected_ns = time.time_ns()
        while (time.time_ns() - connected_ns) / 1_000_000_000 < self.config.connection_max_seconds:
            raw = ws.recv()
            if raw is None:
                break
            received_ns = time.time_ns()
            received_time = datetime.fromtimestamp(
                received_ns / 1_000_000_000,
                tz=timezone.utc,
            )
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                message = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise InvalidMarketEvent("Binance websocket payload is not valid JSON") from exc
            if not isinstance(message, Mapping):
                raise InvalidMarketEvent("Binance websocket message must be an object")
            event = normalize_market_message(
                message,
                received_ns=received_ns,
                received_time=received_time,
            )
            if event is None:
                continue
            if self.event_sink is not None:
                self.event_sink(event)
            yield event

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
        """Reconnect with bounded exponential backoff.

        The stream URL embeds the subscription, so no extra SUBSCRIBE control
        message is necessary after connect. A controlled reconnect is also the
        normal path before Binance's documented 24-hour connection boundary.
        """
        backoff = max(0.1, float(initial_backoff_s))
        while stop_event is None or not stop_event.is_set():
            ws = None
            connected_at = datetime.now(timezone.utc)
            try:
                if on_connection:
                    on_connection("CONNECTING", {"at": connected_at.isoformat()})
                ws = self.connect()
                backoff = max(0.1, float(initial_backoff_s))
                if on_connection:
                    on_connection("CONNECTED", {"at": datetime.now(timezone.utc).isoformat()})
                for event in self.iter_events_once(ws=ws):
                    yield event
                    if stop_event is not None and stop_event.is_set():
                        return
                if on_connection:
                    on_connection(
                        "ROTATE",
                        {
                            "at": datetime.now(timezone.utc).isoformat(),
                            "connection_max_seconds": self.config.connection_max_seconds,
                        },
                    )
            except Exception as exc:
                if on_connection:
                    on_connection(
                        "ERROR",
                        {
                            "at": datetime.now(timezone.utc).isoformat(),
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
                    on_connection(
                        "DISCONNECTED",
                        {"at": datetime.now(timezone.utc).isoformat()},
                    )
