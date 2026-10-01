"""Hot in-process market read model for the live Gorila terminal.

The market plane is intentionally independent from PostgreSQL durability.
Every provider event first reaches this cache and receives a monotonic market
cursor. Durable ledger sequence numbers are attached later when persistence
succeeds. This keeps UI price/tape/microstructure alive during database
degradation while making durability state explicit.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections import deque
from dataclasses import dataclass, replace
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class MarketCacheEvent:
    stream_seq: int
    ledger_seq: int | None
    durable: bool
    event_key: str
    symbol: str
    event_type: str
    event_time: str
    received_time: str
    price: float | None
    quantity: float | None
    side: str | None
    bid: float | None = None
    ask: float | None = None
    bid_qty: float | None = None
    ask_qty: float | None = None


class MarketReadCache:
    def __init__(self, *, max_events_per_symbol: int = 6000) -> None:
        if max_events_per_symbol < 128:
            raise ValueError("max_events_per_symbol must be >= 128")
        self.max_events_per_symbol = int(max_events_per_symbol)
        self._lock = threading.RLock()
        self._events: dict[str, deque[MarketCacheEvent]] = {}
        self._latest_book: dict[str, MarketCacheEvent] = {}
        self._next_stream_seq = 0
        self._by_key: dict[str, MarketCacheEvent] = {}
        self._last_durable_stream_seq = 0
        self._dropped_persistence_events = 0
        self._last_persistence_error: str | None = None

    @staticmethod
    def _event_key(row: Mapping[str, Any]) -> str:
        explicit = row.get("event_key")
        if explicit:
            return str(explicit)
        payload = dict(row.get("payload") or {})
        identity: dict[str, Any] = {
            "source": row.get("source"),
            "symbol": str(row["symbol"]).upper(),
            "event_type": row.get("event_type"),
            "sequence_start": row.get("sequence_start"),
            "sequence_end": row.get("sequence_end"),
        }
        if identity["sequence_start"] is None or identity["sequence_end"] is None:
            payload_hash = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
            ).hexdigest()
            identity.update(
                {
                    "event_time": row.get("event_time"),
                    "provider_time": row.get("provider_time"),
                    "payload_hash": payload_hash,
                }
            )
        return hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _view(
        row: Mapping[str, Any],
        *,
        stream_seq: int,
        ledger_seq: int | None,
        durable: bool,
        event_key: str,
    ) -> MarketCacheEvent:
        payload = dict(row.get("payload") or {})
        event_type = str(row["event_type"])
        symbol = str(row["symbol"]).upper()
        price: float | None = None
        quantity: float | None = None
        side: str | None = None
        bid: float | None = None
        ask: float | None = None
        bid_qty: float | None = None
        ask_qty: float | None = None

        if event_type == "trade":
            price = float(payload["p"])
            quantity = float(payload["q"])
            side = "SELL" if bool(payload.get("m")) else "BUY"
        elif event_type == "bookTicker":
            bid = float(payload["b"])
            ask = float(payload["a"])
            bid_qty = float(payload.get("B", 0.0))
            ask_qty = float(payload.get("A", 0.0))
            price = (bid + ask) / 2.0

        return MarketCacheEvent(
            stream_seq=int(stream_seq),
            ledger_seq=ledger_seq,
            durable=bool(durable),
            event_key=event_key,
            symbol=symbol,
            event_type=event_type,
            event_time=str(row["event_time"]),
            received_time=str(row["received_time"]),
            price=price,
            quantity=quantity,
            side=side,
            bid=bid,
            ask=ask,
            bid_qty=bid_qty,
            ask_qty=ask_qty,
        )

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._latest_book.clear()
            self._by_key.clear()
            self._next_stream_seq = 0
            self._last_durable_stream_seq = 0
            self._dropped_persistence_events = 0
            self._last_persistence_error = None

    def append_observed(self, rows: Iterable[Mapping[str, Any]]) -> list[MarketCacheEvent]:
        """Append provider observations before durability; returns hot events."""
        result: list[MarketCacheEvent] = []
        with self._lock:
            for row in rows:
                event_type = str(row["event_type"])
                if event_type not in {"trade", "bookTicker"}:
                    continue
                key = self._event_key(row)
                existing = self._by_key.get(key)
                if existing is not None:
                    result.append(existing)
                    continue

                self._next_stream_seq += 1
                event = self._view(
                    row,
                    stream_seq=self._next_stream_seq,
                    ledger_seq=None,
                    durable=False,
                    event_key=key,
                )
                self._store_event_locked(event)
                result.append(event)
        return result

    def mark_persisted(
        self,
        rows: Iterable[Mapping[str, Any]],
        results: Iterable[Mapping[str, Any]],
    ) -> None:
        """Attach durable ledger ids to already-observed hot events."""
        rows_list = [dict(row) for row in rows]
        results_list = [dict(result) for result in results]
        if len(rows_list) != len(results_list):
            raise ValueError("cache rows/results length mismatch")

        with self._lock:
            for row, result in zip(rows_list, results_list):
                event_type = str(row["event_type"])
                if event_type not in {"trade", "bookTicker"}:
                    continue
                key = str(result.get("event_key") or self._event_key(row))
                observed = self._by_key.get(key)
                if observed is None:
                    # Defensive recovery: persistence can succeed before a cache
                    # caller attaches the observation.
                    self._next_stream_seq += 1
                    observed = self._view(
                        row,
                        stream_seq=self._next_stream_seq,
                        ledger_seq=None,
                        durable=False,
                        event_key=key,
                    )
                    self._store_event_locked(observed)

                ledger_seq = int(result["ledger_seq"]) if result.get("ledger_seq") is not None else None
                updated = replace(
                    observed,
                    ledger_seq=ledger_seq,
                    durable=bool(result.get("inserted", True)) or observed.durable,
                )
                self._replace_event_locked(updated)
                if updated.durable:
                    self._last_durable_stream_seq = max(
                        self._last_durable_stream_seq,
                        updated.stream_seq,
                    )

            self._last_persistence_error = None

    def record_persistence_degradation(self, message: str) -> None:
        with self._lock:
            self._last_persistence_error = str(message)

    def record_persistence_drop(self, count: int = 1) -> None:
        with self._lock:
            self._dropped_persistence_events += max(0, int(count))

    def _store_event_locked(self, event: MarketCacheEvent) -> None:
        bucket = self._events.setdefault(event.symbol, deque())
        bucket.append(event)
        self._by_key[event.event_key] = event
        while len(bucket) > self.max_events_per_symbol:
            removed = bucket.popleft()
            if self._by_key.get(removed.event_key) == removed:
                self._by_key.pop(removed.event_key, None)
        if event.event_type == "bookTicker":
            self._latest_book[event.symbol] = event

    def _replace_event_locked(self, event: MarketCacheEvent) -> None:
        bucket = self._events.get(event.symbol)
        if bucket is None:
            self._store_event_locked(event)
            return
        for index, current in enumerate(bucket):
            if current.event_key == event.event_key:
                bucket[index] = event
                break
        self._by_key[event.event_key] = event
        if event.event_type == "bookTicker":
            current_book = self._latest_book.get(event.symbol)
            if current_book is None or current_book.stream_seq <= event.stream_seq:
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
                events.sort(key=lambda item: item.stream_seq)
            else:
                for symbol in requested:
                    events.extend(
                        item
                        for item in self._events.get(symbol, ())
                        if item.stream_seq > int(cursor)
                    )
                events.sort(key=lambda item: item.stream_seq)
                events = events[:safe_limit]

            latest_books = {
                symbol: self._latest_book[symbol]
                for symbol in requested
                if symbol in self._latest_book
            }

            all_events = sum(len(self._events.get(symbol, ())) for symbol in requested)
            return {
                "events": [event.__dict__ for event in events],
                "latest_books": {
                    symbol: book.__dict__ for symbol, book in latest_books.items()
                },
                "cache_events_available": all_events,
                "next_cursor": max(
                    [event.stream_seq for symbol in requested for event in self._events.get(symbol, ())] + [int(cursor)]
                ),
                "cursor_kind": "market_stream_v1",
                "last_durable_stream_seq": self._last_durable_stream_seq,
                "persistence": {
                    "degraded": self._last_persistence_error is not None,
                    "last_error": self._last_persistence_error,
                    "dropped_events": self._dropped_persistence_events,
                },
            }


MARKET_CACHE = MarketReadCache()
