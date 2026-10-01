"""Tests for the unified quantitative evidence contract."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import gorila_crypto.app as app_module
from gorila_crypto.app import app
from gorila_crypto.config import settings
from gorila_crypto.evidence import _opportunity_clock


def test_opportunity_clock_stays_locked_without_all_validation_gates() -> None:
    payload = _opportunity_clock(
        cohort={"mature": True},
        quality={"state": "PASS"},
        validation={
            "state": "BLOCKED",
            "promotion_eligible": False,
            "latest": None,
        },
        forecast={"count": 0},
        opportunity={"count": 0},
    )
    assert payload["state"] == "LOCKED"
    assert payload["validated"] is False
    assert "PIT_OOS_NOT_PROMOTION_ELIGIBLE" in payload["blockers"]
    assert "NO_FORECAST_SHADOW" in payload["blockers"]
    assert "NO_OPPORTUNITY_SHADOW" in payload["blockers"]


def test_opportunity_clock_can_activate_only_after_all_gates() -> None:
    payload = _opportunity_clock(
        cohort={"mature": True},
        quality={"state": "PASS"},
        validation={
            "state": "PASS",
            "promotion_eligible": True,
            "latest": {"horizon_ms": 1000},
        },
        forecast={"count": 12},
        opportunity={"count": 4},
    )
    assert payload["state"] == "ACTIVE"
    assert payload["validated"] is True
    assert payload["mode"] == "VALIDATED"
    assert payload["horizon_ms"] == 1000
    assert payload["blockers"] == []


def test_evidence_endpoint_exposes_single_read_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=True),
    )
    payload = {
        "generated_at": "2026-10-01T04:00:00+00:00",
        "study": {"study_id": "crypto-binance-spot-prospective-v2"},
        "cohort": {"status": "ACCUMULATING", "mature": False},
        "quality_gate": {"state": "WAITING"},
        "research": {"status": "BLOCKED"},
        "pit_oos": {"state": "BLOCKED", "promotion_eligible": False, "oos_rows": 0},
        "forecast_shadow": {"count": 0},
        "lead_lag_shadow": {"observation_count": 0},
        "opportunity_shadow": {"count": 0},
        "regime": {"status": "OBSERVED_NOT_VALIDATED", "validated": False},
        "opportunity_clock": {"state": "LOCKED", "validated": False},
    }
    monkeypatch.setattr(app_module, "build_evidence_snapshot", lambda: payload)

    result = app_module.evidence_snapshot()

    assert result["opportunity_clock"]["validated"] is False
    assert result["regime"]["status"] == "OBSERVED_NOT_VALIDATED"


def test_evidence_endpoint_fails_closed_without_capture(monkeypatch) -> None:
    monkeypatch.setattr(
        app_module,
        "settings",
        replace(settings, ingest_enabled=False),
    )
    with pytest.raises(HTTPException) as exc:
        app_module.evidence_snapshot()

    assert exc.value.status_code == 503
    assert exc.value.detail == "capture_not_enabled"
