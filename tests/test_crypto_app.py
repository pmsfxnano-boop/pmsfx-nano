"""Tests for the opt-in prospective capture application boundary."""

from __future__ import annotations

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
    monkeypatch.setattr(settings, "ingest_enabled", True)
    monkeypatch.setattr(app_module, "_capture_block_reason", None)
    monkeypatch.setattr(app_module, "_runtime_thread", None)

    try:
        app_module.health()
    except HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail["status"] == "CAPTURE_WORKER_DEAD"
    else:
        raise AssertionError("capture health accepted a dead ingest worker")

def test_prospective_status_route_exists_without_starting_network_worker() -> None:
    with TestClient(app) as client:
        response = client.get("/api/crypto/prospective/status")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "CAPTURE_DISABLED"
        assert payload["worker_alive"] is False
