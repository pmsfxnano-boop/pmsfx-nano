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

    payload = app_module.health()
    assert payload["status"] == "CAPTURE_WORKER_DEAD"
    assert payload["worker_alive"] is False

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
