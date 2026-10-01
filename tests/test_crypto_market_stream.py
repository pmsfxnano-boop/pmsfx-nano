"""Tests for the low-latency market stream read contract."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from fastapi.testclient import TestClient

import gorila_crypto.app as app_module
from gorila_crypto.app import app
from gorila_crypto.config import settings
from gorila_crypto.market_cache import MARKET_CACHE


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
    results = [
        {"ledger_seq": 101, "event_id": "trade-1", "event_key": "trade-key"},
        {"ledger_seq": 999, "event_id": "book-1", "event_key": "book-key"},
    ]
    MARKET_CACHE.append_persisted([trade, book], results)


def test_market_stream_is_hot_and_cursor_does_not_advance_from_side_snapshot(monkeypatch) -> None:
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

    with TestClient(app) as client:
        response = client.get("/api/crypto/market/stream?cursor=100&limit=36")

    assert response.status_code == 200
    payload = response.json()
    assert payload["next_cursor"] == 101
    assert [row["ledger_seq"] for row in payload["events"]] == [101]
    assert payload["symbols"][0]["bid"] == 99.9
    assert payload["symbols"][0]["ask"] == 100.1
    assert payload["symbols"][0]["spread_bps"] > 0


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
