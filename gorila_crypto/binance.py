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

import httpx
import websocket


# Binance's official market-data-only endpoints.
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
    # Binance documents the combined market-stream route as
    # ?streams=<stream1>/<stream2>; preserve the separators and '@' tokens
    # instead of applying form-style percent encoding to the whole value.
    streams = "/".join(build_stream_names(config))
    return f"{config.ws_base_url}?streams={streams}"


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