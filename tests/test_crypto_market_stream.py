"""Tests for the low-latency market stream read contract."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

import gorila_crypto.app as app_module
from gorila_crypto.config import settings
from gorila_crypto.market_cache import MARKET_CACHE
from fastapi import HTTPException


def _seed_cache() -> None:
    MARKET_CACHE.clear()
    trade = {
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": "2026-10-01T03:40:00+00:00",
        "received_time": "2026-10-01T03:40:00.010000+00:00",
        "payload": {"p": "100.0", "q": "0.5", "m": False},
    }
    book = {
        "symbol": "BTCUSDT",
        "event_type": "bookTicker",
        "event_time": "2026-10-01T03:40:00.020000+00:00",
        "received_time": "2026-10-01T03:40:00.020000+00:00",
        "payload": {"b": "99.9", "a": "100.1", "B": "2.0", "A": "3.0"},
    }
    observed = MARKET_CACHE.append_observed([trade, book])
    results = [
        {"ledger_seq": 101, "event_id": "trade-1", "event_key": observed[0].event_key, "inserted": True},
        {"ledger_seq": 999, "event_id": "book-1", "event_key": observed[1].event_key, "inserted": True},
    ]
    MARKET_CACHE.mark_persisted([trade, book], results)


def test_market_stream_bootstrap_keeps_side_snapshot_out_of_cursor(monkeypatch) -> None:
    _seed_cache()
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=True, symbols=("BTCUSDT",)),
    )
    monkeypatch.setattr(
        app_module,
        "_runtime",
        SimpleNamespace(
            symbol_health=lambda: [
                {"symbol": "BTCUSDT", "status": "LIVE", "healthy": True}
            ]
        ),
    )

    payload = app_module.market_stream(cursor=0, limit=36)

    assert payload["next_cursor"] == 2
    assert [row["stream_seq"] for row in payload["events"]] == [1]
    assert payload["events"][0]["ledger_seq"] == 101
    assert payload["symbols"][0]["bid"] == 99.9
    assert payload["symbols"][0]["ask"] == 100.1
    assert payload["symbols"][0]["spread_bps"] > 0


def test_market_stream_incremental_cursor_includes_delivered_book_events(monkeypatch) -> None:
    _seed_cache()
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=True, symbols=("BTCUSDT",)),
    )
    monkeypatch.setattr(
        app_module,
        "_runtime",
        SimpleNamespace(
            symbol_health=lambda: [
                {"symbol": "BTCUSDT", "status": "LIVE", "healthy": True}
            ]
        ),
    )

    payload = app_module.market_stream(cursor=1, limit=36)

    assert payload["next_cursor"] == 2
    assert [row["stream_seq"] for row in payload["events"]] == [2]
    assert payload["events"][0]["ledger_seq"] == 999


def test_market_stream_fails_closed_when_capture_is_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=False),
    )
    with pytest.raises(HTTPException) as exc:
        app_module.market_stream()

    assert exc.value.status_code == 503
    assert exc.value.detail == "capture_not_enabled"


class _HistoryCursor:
    def execute(self, sql, params) -> None:
        self.params = params

    def fetchall(self):
        from datetime import datetime, timezone
        return [
            (
                datetime(2026, 10, 1, 4, 0, tzinfo=timezone.utc),
                100.0, 102.0, 99.0, 101.0, 12.5, 7,
            )
        ]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _HistoryConnection:
    def cursor(self):
        return _HistoryCursor()

    def close(self) -> None:
        return None


class _HistoryStore:
    durable = True

    def connect(self):
        return _HistoryConnection()


def test_market_history_returns_real_candle_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=True, symbols=("BTCUSDT",)),
    )
    monkeypatch.setattr(app_module, "_new_store", lambda: _HistoryStore())

    payload = app_module.market_history(symbol="BTCUSDT", resolution="5m", limit=60)

    assert payload["symbol"] == "BTCUSDT"
    assert payload["resolution"] == "5m"
    assert payload["candles"][0]["open"] == 100.0
    assert payload["candles"][0]["close"] == 101.0


def test_market_history_rejects_unsupported_resolution(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=True, symbols=("BTCUSDT",)),
    )
    with pytest.raises(HTTPException) as exc:
        app_module.market_history(symbol="BTCUSDT", resolution="13m", limit=60)

    assert exc.value.status_code == 400



def test_market_cache_advances_before_durable_commit() -> None:
    MARKET_CACHE.clear()
    row = {
        "symbol": "ETHUSDT",
        "event_type": "trade",
        "event_time": "2026-10-01T03:41:00+00:00",
        "received_time": "2026-10-01T03:41:00.010000+00:00",
        "source": "binance.websocket.trade",
        "sequence_start": 77,
        "sequence_end": 77,
        "payload": {"p": "2000.0", "q": "0.25", "m": True},
    }
    observed = MARKET_CACHE.append_observed([row])
    assert observed[0].stream_seq == 1
    assert observed[0].durable is False

    snapshot = MARKET_CACHE.snapshot(symbols=("ETHUSDT",), cursor=0, limit=32)
    assert snapshot["next_cursor"] == 1
    assert snapshot["events"][0]["durable"] is False

    MARKET_CACHE.mark_persisted(
        [row],
        [{"ledger_seq": 1234, "event_key": observed[0].event_key, "inserted": True}],
    )
    snapshot = MARKET_CACHE.snapshot(symbols=("ETHUSDT",), cursor=0, limit=32)
    assert snapshot["events"][0]["durable"] is True
    assert snapshot["events"][0]["ledger_seq"] == 1234


def test_market_cache_cursor_advances_only_through_delivered_events() -> None:
    from gorila_crypto.market_cache import MARKET_CACHE

    MARKET_CACHE.clear()
    rows = []
    for idx in range(40):
        rows.append(
            {
                "symbol": "BTCUSDT",
                "event_type": "trade",
                "event_time": f"2026-10-01T03:50:{idx:02d}+00:00",
                "received_time": f"2026-10-01T03:50:{idx:02d}.010000+00:00",
                "source": "binance.websocket.trade",
                "sequence_start": idx + 1,
                "sequence_end": idx + 1,
                "payload": {"p": str(100.0 + idx), "q": "0.1", "m": False},
            }
        )
    MARKET_CACHE.append_observed(rows)

    snapshot = MARKET_CACHE.snapshot(
        symbols=("BTCUSDT",),
        cursor=0,
        limit=32,
    )

    assert len(snapshot["events"]) == 32
    assert snapshot["events"][0]["stream_seq"] == 1
    assert snapshot["events"][-1]["stream_seq"] == 32
    assert snapshot["next_cursor"] == 32

    next_snapshot = MARKET_CACHE.snapshot(
        symbols=("BTCUSDT",),
        cursor=snapshot["next_cursor"],
        limit=32,
    )
    assert next_snapshot["events"][0]["stream_seq"] == 33
    assert next_snapshot["next_cursor"] == 40
