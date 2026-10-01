"""Tests for the low-latency market stream read contract."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import gorila_crypto.app as app_module
from gorila_crypto.app import app
from gorila_crypto.config import settings
from fastapi.testclient import TestClient


class _Cursor:
    def __init__(self, event_rows, book_rows) -> None:
        self.event_rows = event_rows
        self.book_rows = book_rows
        self._calls = 0

    def execute(self, sql, params) -> None:
        self._last_sql = sql
        self._last_params = params

    def fetchall(self):
        self._calls += 1
        return self.event_rows if self._calls == 1 else self.book_rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _Connection:
    def __init__(self, event_rows, book_rows) -> None:
        self.event_rows = event_rows
        self.book_rows = book_rows

    def cursor(self):
        return _Cursor(self.event_rows, self.book_rows)

    def close(self) -> None:
        return None


class _Store:
    backend = "postgres"
    durable = True

    def __init__(self, event_rows, book_rows) -> None:
        self.connection = _Connection(event_rows, book_rows)

    def connect(self):
        return self.connection

    def init(self):
        raise AssertionError("postgres market_stream must not run DDL per request")


def _rows():
    trade = (
        101,
        "BTCUSDT",
        "trade",
        "2026-10-01T03:40:00+00:00",
        "2026-10-01T03:40:00.010000+00:00",
        '{"p":"100.0","q":"0.5","m":false}',
    )
    book = (
        999,
        "BTCUSDT",
        "bookTicker",
        "2026-10-01T03:40:00.020000+00:00",
        "2026-10-01T03:40:00.020000+00:00",
        '{"b":"99.9","a":"100.1","B":"2.0","A":"3.0"}',
    )
    return [trade], [book]


def test_market_stream_is_incremental_and_cursor_does_not_skip_book_rows(monkeypatch) -> None:
    event_rows, book_rows = _rows()
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
    monkeypatch.setattr(
        app_module,
        "_new_store",
        lambda: _Store(event_rows, book_rows),
    )

    with TestClient(app) as client:
        response = client.get("/api/crypto/market/stream?cursor=100&limit=36")

    assert response.status_code == 200
    payload = response.json()
    assert payload["next_cursor"] == 101
    assert [row["ledger_seq"] for row in payload["events"]] == [101]
    assert payload["symbols"][0]["bid"] == 99.9
    assert payload["symbols"][0]["ask"] == 100.1


def test_market_stream_fails_closed_when_capture_is_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=False),
    )
    with TestClient(app) as client:
        response = client.get("/api/crypto/market/stream")

    assert response.status_code == 503
    assert response.json()["detail"] == "capture_not_enabled"
