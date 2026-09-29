"""Tests for the opt-in prospective capture application boundary."""

from __future__ import annotations

from fastapi.testclient import TestClient

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


def test_prospective_status_route_exists_without_starting_network_worker() -> None:
    with TestClient(app) as client:
        response = client.get("/api/crypto/prospective/status")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "CAPTURE_DISABLED"
        assert payload["worker_alive"] is False
