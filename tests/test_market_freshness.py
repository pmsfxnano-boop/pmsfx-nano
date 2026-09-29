from datetime import datetime, timedelta, timezone

import pytest

from gorila_argentum.market_freshness import assess_observation, aggregate_status
from gorila_argentum.signal_engine import build_signal


def test_freshness_uses_event_timestamp_not_receipt_timestamp():
    now = datetime.now(timezone.utc)
    event = (now - timedelta(minutes=20)).isoformat()
    received = (now - timedelta(seconds=1)).isoformat()

    result = assess_observation(event, received, now)

    assert result["status"] == "DELAYED"
    assert result["event_age_seconds"] == pytest.approx(1200, abs=0.01)
    assert result["transport_age_seconds"] == pytest.approx(1, abs=0.01)
    assert result["is_live"] is False


def test_freshness_marks_future_event_invalid():
    now = datetime.now(timezone.utc)
    event = (now + timedelta(minutes=2)).isoformat()

    result = assess_observation(event, now=now)

    assert result["status"] == "INVALID_TIMESTAMP"
    assert result["is_live"] is False


def test_aggregate_status_separates_transport_success_from_delayed_data():
    result = aggregate_status([
        {"status": "DELAYED", "event_age_seconds": 1200},
        {"status": "DELAYED", "event_age_seconds": 1190},
        {"status": "DELAYED", "event_age_seconds": 1210},
    ])

    assert result["status"] == "DELAYED"
    assert result["live_symbols"] == 0
    assert result["delayed_symbols"] == 3
    assert result["stale_symbols"] == 0


def test_signal_exposes_delayed_market_data_even_without_forecast():
    signal = build_signal(
        symbol="GGAL",
        state={
            "forecast": None,
            "evaluation": {},
            "engine_freshness": {"age_seconds": None},
            "market_freshness": {
                "age_seconds": 1200,
                "event_age_seconds": 1200,
                "transport_age_seconds": 1,
                "status": "DELAYED",
            },
            "market_session_open": True,
        },
        price_series=[],
        drift=None,
    )

    assert signal["status"] == "NO_DATA"
    assert "MARKET_DATA_DELAYED" in signal["risk_flags"]
    assert signal["market"]["market_event_age_seconds"] == 1200
    assert signal["market"]["market_transport_age_seconds"] == 1
    assert signal["market"]["market_status"] == "DELAYED"


def test_byma_live_panel_never_promotes_close_to_live(monkeypatch):
    from gorila_argentum import sources

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": [{
                    "symbol": "GGAL",
                    "trade": None,
                    "closingPrice": 5950,
                    "previousClosingPrice": 6000,
                    "tradeHour": "12:50:00",
                }]
            }

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def post(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(sources, "_byma_client", lambda: FakeClient())
    monkeypatch.setattr(sources.settings, "core_symbols", ("GGAL",))

    result = sources.byma_live_panel()

    assert result.rows == []
    assert result.error is not None
    assert "BYMA_LIVE_NO_CORE_TRADE_ROWS" in result.error
