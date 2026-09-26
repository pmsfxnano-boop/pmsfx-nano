import math
from datetime import datetime, timedelta, timezone

from gorila_argentum.learning import build_training_dataset, run_learning_cycle
from gorila_argentum.storage import Store


def _seed_prices(store):
    rows = []
    for i in range(260):
        value = 100.0 + 8.0 * math.sin(i / 5.0) + 0.04 * i
        rows.append({
            "symbol": "GGAL",
            "field": "close",
            "value": value,
            "event_time": (datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(days=i)).isoformat(),
            "received_time": "2026-09-26T00:00:00+00:00",
            "source": "TEST",
        })
    store.insert_observations(rows)


def test_learning_cycle_candidate_is_persisted(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "learning.sqlite3"))
    store = Store()
    store.init()
    _seed_prices(store)

    dataset = build_training_dataset(store, "GGAL", horizon_days=5)
    assert dataset["status"] == "READY"
    assert dataset["samples"] > 100
    assert len(dataset["dataset_hash"]) == 64

    result = run_learning_cycle("GGAL", horizon_days=5, store=store)
    assert result["status"] in {"CANDIDATE_ELIGIBLE", "CANDIDATE_REJECTED"}
    assert result["promotion"] == "BLOCKED"
    assert 0.0 <= result["latest_probability_up"] <= 1.0
    rows = store.latest_learning("GGAL")
    assert len(rows) == 1
