from gorila_argentum.storage import Store


def test_recent_series_deduplicates_same_event_time(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "dedupe.sqlite3"))
    store = Store()
    store.init()
    rows = [
        {
            "symbol": "GGAL",
            "field": "close",
            "value": 100.0,
            "event_time": "2026-09-25T12:00:00+00:00",
            "received_time": "2026-09-25T12:01:00+00:00",
            "source": "YahooChart",
        },
        {
            "symbol": "GGAL",
            "field": "close",
            "value": 101.0,
            "event_time": "2026-09-25T12:00:00+00:00",
            "received_time": "2026-09-25T12:02:00+00:00",
            "source": "TwelveData",
        },
        {
            "symbol": "GGAL",
            "field": "close",
            "value": 102.0,
            "event_time": "2026-09-25T13:00:00+00:00",
            "received_time": "2026-09-25T13:01:00+00:00",
            "source": "YahooChart",
        },
    ]
    store.insert_observations(rows)
    series = store.recent_series("GGAL", "close", limit=10)
    assert len(series) == 2
    assert series[0][0] == "2026-09-25T12:00:00+00:00"
    assert series[0][1] == 101.0
