from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from gorila_core.market_freshness import assess_observation


BASE = datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc)


def test_assessment_distinguishes_event_age_receive_age_and_transport_latency() -> None:
    event = BASE
    received = BASE + timedelta(milliseconds=125)
    now = BASE + timedelta(seconds=2)

    result = assess_observation(event, received, now)

    assert result["status"] == "LIVE"
    assert result["event_age_seconds"] == pytest.approx(2.0)
    assert result["received_age_seconds"] == pytest.approx(1.875)
    assert result["transport_latency_seconds"] == pytest.approx(0.125)
    # Compatibility alias remains the age since receipt.
    assert result["transport_age_seconds"] == pytest.approx(1.875)


def test_assessment_preserves_negative_transport_latency_for_quality_gate() -> None:
    event = BASE + timedelta(milliseconds=200)
    received = BASE
    result = assess_observation(event, received, BASE)

    assert result["transport_latency_seconds"] == pytest.approx(-0.2)
