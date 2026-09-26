from gorila_argentum.dashboard import HTML


def test_dashboard_contains_drift_monitor():
    html = str(HTML.body)
    assert "DRIFT MONITOR" in html
    assert "/api/drift?limit=50" in html
