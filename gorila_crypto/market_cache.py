"""In-process market read model for the live web terminal.

Postgres remains the durable source of truth. This cache is only a bounded,
read-only projection fed *after* an event batch has committed successfully.
It exists to keep the UI hot path independent from database query latency.
"""

from __future__ import annotations

import json
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class MarketCacheEvent:
    ledger_seq: int
    symbol: str
    event_type: str
    event_time: str
    received_time: str
    price: float | None
    quantity: float | None
    side: str | None


class MarketReadCache:
    def __init__(self, *, max_events_per_symbol: int = 6000) -> None:
        if max_events_per_symbol < 128:
            raise ValueError("max_events_per_symbol must be >= 128")
        self.max_events_per_symbol = int(max_events_per_symbol)
        self._lock = threading.RLock()
        self._events: dict[str, deque[MarketCacheEvent]] = {}
        self._latest_book: dict[str, MarketCacheEvent] = {}

    @staticmethod
    def _view(
        row: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> MarketCacheEvent:
        payload = dict(row.get("payload") or {})
        event_type = str(row["event_type"])
        symbol = str(row["symbol"]).upper()
        price: float | None = None
        quantity: float | None = None
        side: str | None = None

        if event_type == "trade":
            price = float(payload["p"])
            quantity = float(payload["q"])
            side = "SELL" if bool(payload.get("m")) else "BUY"
        elif event_type == "bookTicker":
            bid = float(payload["b"])
            ask = float(payload["a"])
            price = (bid + ask) / 2.0

        return MarketCacheEvent(
            ledger_seq=int(result["ledger_seq"]),
            symbol=symbol,
            event_type=event_type,
            event_time=str(row["event_time"]),
            received_time=str(row["received_time"]),
            price=price,
            quantity=quantity,
            side=side,
        )

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._latest_book.clear()

    def append_persisted(
        self,
        rows: Iterable[Mapping[str, Any]],
        results: Iterable[Mapping[str, Any]],
    ) -> None:
        rows_list = [dict(row) for row in rows]
        results_list = [dict(result) for result in results]
        if len(rows_list) != len(results_list):
            raise ValueError("cache rows/results length mismatch")

        with self._lock:
            for row, result in zip(rows_list, results_list):
                event_type = str(row["event_type"])
                if event_type not in {"trade", "bookTicker"}:
                    continue
                event = self._view(row, result)
                bucket = self._events.setdefault(event.symbol, deque())
                bucket.append(event)
                while len(bucket) > self.max_events_per_symbol:
                    bucket.popleft()
                if event.event_type == "bookTicker":
                    self._latest_book[event.symbol] = event

    def snapshot(
        self,
        *,
        symbols: Iterable[str],
        cursor: int = 0,
        limit: int = 360,
    ) -> dict[str, Any]:
        safe_limit = max(32, min(int(limit), 600))
        requested = tuple(dict.fromkeys(str(s).upper() for s in symbols if str(s).strip()))

        with self._lock:
            events: list[MarketCacheEvent] = []
            if int(cursor) <= 0:
                per_symbol = max(24, min(160, safe_limit // max(len(requested), 1)))
                for symbol in requested:
                    trades = [
                        item
                        for item in self._events.get(symbol, ())
                        if item.event_type == "trade"
                    ]
                    events.extend(trades[-per_symbol:])
                events.sort(key=lambda item: item.ledger_seq)
            else:
                for symbol in requested:
                    events.extend(
                        item
                        for item in self._events.get(symbol, ())
                        if item.ledger_seq > int(cursor)
                    )
                events.sort(key=lambda item: item.ledger_seq)
                events = events[:safe_limit]

            latest_books = {
                symbol: self._latest_book[symbol]
                for symbol in requested
                if symbol in self._latest_book
            }

            return {
                "events": [event.__dict__ for event in events],
                "latest_books": {
                    symbol: book.__dict__ for symbol, book in latest_books.items()
                },
                "cache_events_available": sum(
                    len(self._events.get(symbol, ())) for symbol in requested
                ),
            }


MARKET_CACHE = MarketReadCache()
