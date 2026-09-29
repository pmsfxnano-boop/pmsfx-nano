from gorila_argentum.dashboard import HTML


def test_dashboard_contains_drift_monitor():
    html = HTML.body.decode('utf-8')
    assert "DRIFT MONITOR" in html
    assert "/api/drift?limit=50" in html
    assert "CONTROL ROOM — BATCH 13" in html
    assert "/api/control" in html
    assert "SHADOW LEDGER — BATCH 14" in html
    assert "/api/shadow/summary" in html


def test_terminal_frontend_uses_real_chart_contract():
    from gorila_argentum.dashboard_terminal import HTML as TERMINAL_HTML
    html = TERMINAL_HTML.body.decode("utf-8")
    assert 'id="priceCanvas"' in html
    assert "/api/gorila/chart/" in html
    assert "data-range=\"1D\"" in html
    assert "data-range=\"1Y\"" in html
    assert "refreshChart" in html
    assert "Number.isFinite(score)?score.toFixed(1):'—'" in html


def test_gorila_chart_endpoint_selects_requested_resolution(monkeypatch):
    from gorila_argentum import app as app_module

    class FakeStore:
        def init(self):
            return None

        def recent_series(self, symbol, field, limit=250):
            if field == "close_1m":
                return [("2026-09-29T14:00:00+00:00", 100.0), ("2026-09-29T14:05:00+00:00", 101.0)]
            return []

    monkeypatch.setattr(app_module, "Store", lambda: FakeStore())
    payload = app_module.gorila_chart("GGAL", timeframe="1D")

    assert payload["status"] == "READY"
    assert payload["symbol"] == "GGAL"
    assert payload["field"] == "close_1m"
    assert payload["resolution"] == "1m"
    assert payload["bars"] == 2
    assert payload["change_pct"] == 0.01


def test_gorila_chart_endpoint_fails_closed_for_invalid_timeframe():
    from fastapi import HTTPException
    from gorila_argentum import app as app_module

    try:
        app_module.gorila_chart("GGAL", timeframe="7D")
        assert False, "expected invalid timeframe"
    except HTTPException as exc:
        assert exc.status_code == 400


def test_terminal_frontend_uses_clear_forecast_unavailable_semantics():
    from gorila_argentum.dashboard_terminal import HTML as TERMINAL_HTML
    html = TERMINAL_HTML.body.decode("utf-8")
    assert "FORECAST UNAVAILABLE" in html
    assert "NOT SCORED · FORECAST UNAVAILABLE" in html
    assert "NO VALIDATED FORECAST EVIDENCE" in html
    assert "CHART CLOSE" in html


def test_signal_without_forecast_does_not_fake_stale_data():
    from gorila_argentum.signal_engine import build_signal

    signal = build_signal(
        symbol="GGAL",
        state={
            "forecast": None,
            "evaluation": {},
            "engine_freshness": {"age_seconds": None},
            "market_freshness": {"age_seconds": None},
        },
        price_series=[
            ("2026-09-29T14:00:00+00:00", 100.0),
            ("2026-09-29T14:01:00+00:00", 101.0),
        ],
        drift=None,
    )

    assert signal["status"] == "NO_DATA"
    assert "NO_FORECAST" in signal["risk_flags"]
    assert "MODEL_NOT_VALIDATED" in signal["risk_flags"]
    assert "DATA_NOT_FRESH" not in signal["risk_flags"]
    assert "DRIFT_UNAVAILABLE" in signal["risk_flags"]


def test_live_quote_reports_market_closed_without_live_cache(monkeypatch):
    from gorila_argentum import app as app_module

    monkeypatch.setattr(
        app_module,
        "argentina_session_state",
        lambda: {"open": False, "timezone": "America/Argentina/Buenos_Aires"},
    )
    monkeypatch.setattr(app_module, "_ARG_LIVE_CACHE", {})

    payload = app_module.gorila_live_quote("GGAL")

    assert payload["status"] == "MARKET_CLOSED"
    assert payload["quote"] is None


def test_source_health_ui_hides_retired_and_secondary_vendor_rows():
    from gorila_argentum.app import _visible_source_health

    rows = [
        {"source": "BYMADATA/MarketData", "status": "DEGRADED"},
        {"source": "YahooChart/GGAL.BA", "status": "DEGRADED"},
        {"source": "YahooChartLive/GGAL.BA", "status": "DEGRADED"},
        {"source": "BYMADATA/BMA/historical", "status": "HEALTHY"},
    ]
    visible = _visible_source_health(rows)

    assert [row["source"] for row in visible] == ["BYMADATA/BMA/historical"]


def test_terminal_telemetry_is_collapsed_into_diagnostics_layer():
    from gorila_argentum.dashboard_terminal import HTML as TERMINAL_HTML
    html = TERMINAL_HTML.body.decode("utf-8")
    assert '<details class="diagnostics">' in html
    assert '<summary>' in html
    assert 'SYSTEM DIAGNOSTICS' in html
    assert 'FULL DIAGNOSTICS AVAILABLE ABOVE' in html
    assert 'id="diagnosticSummary"' in html


def test_terminal_frontend_has_no_invalid_async_function_declaration():
    from gorila_argentum.dashboard_terminal import HTML as TERMINAL_HTML
    html = TERMINAL_HTML.body.decode("utf-8")
    assert "async async function" not in html
    assert "async function refreshControl" in html


def test_live_quote_uses_event_age_not_cache_receipt_age(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from gorila_argentum import app as app_module

    now = datetime.now(timezone.utc)
    event_time = (now - timedelta(minutes=20)).isoformat()
    received = (now - timedelta(seconds=1)).isoformat()

    monkeypatch.setattr(
        app_module,
        "argentina_session_state",
        lambda: {"open": True, "timezone": "America/Argentina/Buenos_Aires"},
    )
    monkeypatch.setattr(
        app_module,
        "_ARG_LIVE_CACHE",
        {"GGAL": {
            "symbol": "GGAL",
            "last": 5950.0,
            "quote_timestamp": event_time,
            "received_at": received,
            "source": "BYMADATA/leading-equity",
            "latency_ms": 100.0,
            "updated_epoch": now.timestamp(),
        }},
    )

    payload = app_module.gorila_live_quote("GGAL")

    assert payload["status"] == "DELAYED"
    assert payload["event_age_seconds"] >= 1199
    assert payload["transport_age_seconds"] <= 2
    assert payload["is_live"] is False


def test_gorila_chart_marks_intraday_freshness(monkeypatch):
    from datetime import datetime, timezone
    from gorila_argentum import app as app_module

    now = datetime.now(timezone.utc).isoformat()

    class FakeStore:
        def init(self):
            return None

        def recent_series(self, symbol, field, limit=250):
            if field == "close_1m":
                return [(now, 5950.0)]
            return []

    monkeypatch.setattr(app_module, "Store", lambda: FakeStore())
    monkeypatch.setattr(app_module, "_ARG_LIVE_CACHE", {})

    payload = app_module.gorila_chart("GGAL", timeframe="1D")

    assert payload["freshness"]["status"] == "LIVE"
    assert payload["freshness"]["event_age_seconds"] <= 1

def test_signal_without_forecast_cannot_expose_legacy_probability():
    from gorila_argentum.signal_engine import build_signal

    signal = build_signal(
        symbol="GGAL",
        state={
            "forecast": None,
            "p_up": 0.524,
            "evaluation": {},
            "engine_freshness": {"age_seconds": None},
            "market_freshness": {"age_seconds": None},
        },
    )

    assert signal["status"] == "NO_DATA"
    assert signal["probability"]["up"] is None
    assert signal["probability"]["down"] is None
    assert signal["probability"]["edge"] is None
    assert signal["probability"]["confidence"] is None


def test_terminal_does_not_present_v2_relative_probability_as_primary_probability():
    from gorila_argentum.dashboard_terminal import HTML as TERMINAL_HTML

    html = TERMINAL_HTML.body.decode('utf-8')
    assert "V2 RELATIVE EDGE" in html
    assert "V2 P(RES UP)" not in html
    assert "primaryForecastAvailable" in html
    assert "P(UP) — · P(DOWN) —" in html

