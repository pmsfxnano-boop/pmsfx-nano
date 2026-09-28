from datetime import datetime, timedelta, timezone

from gorila_argentum.storage import Store


def test_shadow_settlement_batches_observation_reads(monkeypatch, tmp_path):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    db_path = tmp_path / "gorila.sqlite3"
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(db_path))

    store = Store()
    store.init()

    created_at = datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)
    store.insert_observations([
        {
            "symbol": "GGAL",
            "field": "close",
            "value": 101.0,
            "event_time": (created_at + timedelta(seconds=10)).isoformat(),
            "received_time": created_at.isoformat(),
            "source": "TEST",
            "quality": "OK",
        }
    ])
    prediction = store.save_shadow_prediction(
        symbol="GGAL",
        model_version="test",
        probability_up=0.6,
        horizon_seconds=10,
        regime="TEST",
        entry_price=100.0,
        feature_hash="test-feature-1",
        created_at=created_at.isoformat(),
    )

    result = store.settle_due_shadow_from_observations(limit=10)

    assert result["attempted"] == 1
    assert result["settled"] == 1
    row = store.get_shadow_prediction(prediction["id"])
    assert row["status"] == "SETTLED"
    assert row["observed_at"] == (created_at + timedelta(seconds=10)).isoformat()
