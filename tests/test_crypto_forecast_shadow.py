from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gorila_crypto.forecast import ForecastModelSpec, ForecastTargetSpec
from gorila_crypto.forecast_shadow import run_forecast_shadow
from gorila_crypto.lead_lag import LeadLagConfig
from gorila_crypto.ledger import ReplaySpec
from gorila_crypto.storage import CryptoStore


def test_forecast_shadow_persists_blocked_model_without_probability(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "forecast.sqlite3"))
    base = datetime(2026, 9, 29, 15, 0, 0, tzinfo=timezone.utc)

    rows = [
        (1, "BTCUSDT", 0, 0, "100.0"),
        (2, "BTCUSDT", 1000, 1000, "100.06"),
        (10, "ETHUSDT", 0, 0, "200.0"),
        (11, "ETHUSDT", 1000, 1000, "200.03"),
    ]
    for seq, symbol, event_ms, received_ms, price in rows:
        event_time = (base + timedelta(milliseconds=event_ms)).isoformat()
        received_time = (base + timedelta(milliseconds=received_ms)).isoformat()
        store.append_event(
            symbol=symbol,
            event_type="trade",
            event_time=event_time,
            received_time=received_time,
            provider_time=event_time,
            source="binance.websocket.trade",
            sequence_start=seq,
            sequence_end=seq,
            payload={
                "e": "trade",
                "s": symbol,
                "t": seq,
                "p": price,
                "q": "1",
            },
        )

    model = ForecastModelSpec(
        model_id="crypto-logit-v0",
        version="0",
        coefficients={"leader_return_bps": 0.01},
        intercept=0.0,
    )
    report = run_forecast_shadow(
        store,
        ReplaySpec(order="ingest"),
        "BTCUSDT",
        "ETHUSDT",
        LeadLagConfig(shock_min_bps=5.0),
        ForecastTargetSpec(horizon_ms=1000),
        model,
    )

    assert report["status"] == "SHADOW"
    assert report["forecast_count"] == 1
    assert report["stored_count"] == 1
    assert report["automatic_promotion"] is False
    assert report["results"][0]["status"] == "BLOCKED_NO_VALIDATED_MODEL"
    assert report["results"][0]["probability_response_positive"] is None
    conn = store.connect()
    try:
        manifest_count = conn.execute(
            "SELECT COUNT(*) FROM crypto_replay_manifests"
        ).fetchone()[0]
        assert manifest_count == 1
    finally:
        conn.close()


def test_forecast_shadow_is_blocked_on_event_time_replay(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "forecast.sqlite3"))
    model = ForecastModelSpec(
        model_id="crypto-logit-v0",
        version="0",
        coefficients={},
        intercept=0.0,
    )
    try:
        run_forecast_shadow(
            store,
            ReplaySpec(order="event_time"),
            "BTCUSDT",
            "ETHUSDT",
            LeadLagConfig(),
            ForecastTargetSpec(horizon_ms=1000),
            model,
        )
    except ValueError as exc:
        assert "ingest-order PIT replay" in str(exc)
    else:
        raise AssertionError("event-time replay was accepted for forecast")
