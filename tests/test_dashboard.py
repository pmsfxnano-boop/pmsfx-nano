from gorila_argentum.dashboard import HTML


def test_dashboard_contains_drift_monitor():
    html = str(HTML.body)
    assert "DRIFT MONITOR" in html
    assert "/api/drift?limit=50" in html
    assert "CONTROL ROOM — BATCH 13" in html
    assert "/api/control" in html
    assert "SHADOW LEDGER — BATCH 14" in html
    assert "/api/shadow/summary" in html
