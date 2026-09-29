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
