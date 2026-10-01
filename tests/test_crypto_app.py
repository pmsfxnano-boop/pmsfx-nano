"""Tests for the opt-in prospective capture application boundary."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from fastapi import HTTPException
from fastapi.testclient import TestClient

import gorila_crypto.app as app_module
from gorila_crypto.app import app
from gorila_crypto.config import settings


def test_capture_is_disabled_in_cleanroom_default() -> None:
    assert settings.ingest_enabled is False
    with TestClient(app) as client:
        health = client.get("/api/crypto/health")
        assert health.status_code == 200
        payload = health.json()
        assert payload["prospective_capture"] is False
        assert payload["forecast"]["automatic_promotion"] is False



def test_capture_health_fails_closed_when_worker_dies(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "settings", replace(settings, ingest_enabled=True))
    monkeypatch.setattr(app_module, "_capture_block_reason", None)
    monkeypatch.setattr(app_module, "_runtime_thread", None)

    try:
        app_module.health()
    except HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail["status"] == "CAPTURE_WORKER_DEAD"
        assert exc.detail["worker_alive"] is False
    else:
        raise AssertionError("capture health accepted a dead ingest worker")

def test_capture_health_is_liveness_only_when_symbols_are_stale(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "settings", replace(settings, ingest_enabled=True))
    monkeypatch.setattr(app_module, "_capture_block_reason", None)
    monkeypatch.setattr(app_module, "_runtime_thread", SimpleNamespace(is_alive=lambda: True))
    monkeypatch.setattr(
        app_module,
        "_runtime",
        SimpleNamespace(symbol_health=lambda: [
            {"symbol": "BTCUSDT", "status": "LIVE", "healthy": True},
            {"symbol": "ETHUSDT", "status": "DELAYED", "healthy": False},
        ]),
    )

    payload = app_module.health()
    assert payload["symbols_live"] is False


def test_capture_readiness_fails_closed_on_stale_required_symbol(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "settings", replace(settings, ingest_enabled=True))
    monkeypatch.setattr(app_module, "_capture_block_reason", None)
    monkeypatch.setattr(app_module, "_runtime_thread", SimpleNamespace(is_alive=lambda: True))
    monkeypatch.setattr(
        app_module,
        "_runtime",
        SimpleNamespace(symbol_health=lambda: [
            {"symbol": "BTCUSDT", "status": "LIVE", "healthy": True},
            {"symbol": "ETHUSDT", "status": "DELAYED", "healthy": False},
        ]),
    )

    try:
        app_module.readiness()
    except HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail["status"] == "CAPTURE_DATA_STALE"
        assert exc.detail["symbols_live"] is False
    else:
        raise AssertionError("capture readiness accepted stale required-symbol data")


def test_prospective_status_route_exists_without_starting_network_worker() -> None:
    with TestClient(app) as client:
        response = client.get("/api/crypto/prospective/status")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "CAPTURE_DISABLED"
        assert payload["worker_alive"] is False


def test_market_history_uses_shared_short_lived_cache(monkeypatch) -> None:
    class FakeCursor:
        def __init__(self, owner) -> None:
            self.owner = owner

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args) -> None:
            self.owner.execute_calls += 1

        def fetchall(self):
            return [
                (
                    "2026-10-01T00:00:00+00:00",
                    100.0,
                    101.0,
                    99.0,
                    100.5,
                    12.0,
                    4,
                )
            ]

    class FakeConn:
        def __init__(self) -> None:
            self.execute_calls = 0
            self.closed = False

        def cursor(self):
            return FakeCursor(self)

        def close(self) -> None:
            self.closed = True

    class FakeStore:
        durable = True

        def __init__(self) -> None:
            self.conn = FakeConn()
            self.connect_calls = 0

        def connect(self):
            self.connect_calls += 1
            return self.conn

    store = FakeStore()
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=True, symbols=("BTCUSDT",)),
    )
    monkeypatch.setattr(app_module, "_new_store", lambda: store)
    app_module._HISTORY_CACHE.clear()

    first = app_module.market_history("BTCUSDT", "1m", 30)
    second = app_module.market_history("BTCUSDT", "1m", 30)

    assert first == second
    assert store.connect_calls == 1
    assert store.conn.execute_calls == 1

    app_module._HISTORY_CACHE.clear()
