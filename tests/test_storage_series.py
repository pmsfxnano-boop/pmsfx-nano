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


def test_calibration_persistence_roundtrip(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "calibration.sqlite3"))
    store = Store()
    store.init()
    saved = store.save_calibration_run(
        "shadow-probability-v0",
        {"status": "CANDIDATE_READY", "intercept": -0.42},
    )
    latest = store.latest_calibration(limit=1)
    assert latest[0]["id"] == saved["id"]
    assert latest[0]["status"] == "CANDIDATE_READY"
    assert latest[0]["result"]["intercept"] == -0.42



def test_persistence_roundtrip_uses_fresh_connection(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "persistence.sqlite3"))
    store = Store()
    store.init()
    proof = store.verify_persistence()
    assert proof["verified"] is True
    assert proof["backend"] == "sqlite-fallback"
    assert proof["heartbeat_id"]

def test_model_registry_serializes_datetime_metadata(monkeypatch):
    import json
    from datetime import datetime, timezone
    from contextlib import contextmanager

    import quant.db as db

    captured = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            captured["params"] = params

        def fetchone(self):
            return (42,)

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    @contextmanager
    def fake_connection():
        yield FakeConnection()

    monkeypatch.setattr(db, "database_url", lambda: "postgresql://unit-test")
    monkeypatch.setattr(db, "connection", fake_connection)

    registered_at = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    record_id = db.record_model_registry(
        {
            "registered_at": registered_at,
            "model_id": "test-model",
            "version": "v1",
            "status": "CANDIDATE_REJECTED",
            "dataset_version": "dataset-test",
        }
    )

    assert record_id == 42
    metadata = json.loads(captured["params"]["metadata"])
    assert metadata["registered_at"] == registered_at.isoformat()

def test_freshness_aware_secondary_selection():
    from datetime import datetime, timezone

    from gorila_argentum.market_freshness import (
        assess_observation,
        choose_fresher_observation,
    )

    now = datetime(2026, 9, 30, 15, 0, tzinfo=timezone.utc)
    primary = {
        "symbol": "GGAL",
        "value": 6000.0,
        "event_time": "2026-09-30T14:40:00+00:00",
        "received_time": "2026-09-30T15:00:01+00:00",
        "source": "BYMADATA/leading-equity",
    }
    secondary = {
        "symbol": "GGAL",
        "value": 6012.0,
        "event_time": "2026-09-30T14:59:30+00:00",
        "received_time": "2026-09-30T15:00:01+00:00",
        "source": "TwelveDataLive/GGAL",
    }

    selected, freshness, role = choose_fresher_observation(primary, secondary, now=now)

    assert selected is secondary
    assert role == "secondary"
    assert freshness["status"] == "LIVE"
    assert assess_observation(primary["event_time"], now=now)["status"] == "DELAYED"


def test_live_primary_beats_delayed_secondary():
    from datetime import datetime, timezone

    from gorila_argentum.market_freshness import choose_fresher_observation

    now = datetime(2026, 9, 30, 15, 0, tzinfo=timezone.utc)
    primary = {
        "symbol": "GGAL",
        "value": 6000.0,
        "event_time": "2026-09-30T14:59:40+00:00",
        "received_time": "2026-09-30T15:00:01+00:00",
        "source": "BYMADATA/leading-equity",
    }
    secondary = {
        "symbol": "GGAL",
        "value": 6012.0,
        "event_time": "2026-09-30T14:58:00+00:00",
        "received_time": "2026-09-30T15:00:01+00:00",
        "source": "TwelveDataLive/GGAL",
    }

    selected, freshness, role = choose_fresher_observation(primary, secondary, now=now)

    assert selected is primary
    assert role == "primary"
    assert freshness["status"] == "LIVE"

