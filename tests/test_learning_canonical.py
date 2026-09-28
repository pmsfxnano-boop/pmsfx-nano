from datetime import date, timedelta

import gorila_argentum.learning as learning


def _synthetic_series(n=180):
    start = date(2026, 1, 1)
    rows = []
    price = 100.0
    for i in range(n):
        price *= 1.0 + (0.0005 if i % 3 else -0.0002)
        rows.append(((start + timedelta(days=i)).isoformat(), price))
    return rows


def test_learning_dataset_uses_canonical_fabric(monkeypatch):
    series = _synthetic_series()
    called = {}

    def fake_canonical(store, symbol, field, limit):
        called.update({"symbol": symbol, "field": field, "limit": limit})
        return series

    monkeypatch.setattr(learning, "canonical_daily_series", fake_canonical)

    result = learning.build_training_dataset(object(), "GGAL", horizon_days=5)

    assert called == {"symbol": "GGAL", "field": "close", "limit": 5000}
    assert result["data_fabric"] == "CANONICAL_DAILY_V1"
    assert result["feature_names"] == list(learning.FEATURE_NAMES)
    assert result["status"] == "READY"
    assert result["samples"] > 100
    assert result["dataset_hash"]
    assert result["canonical_content_hash"] == "canonical-hash-test"


def test_candidate_result_contains_reproducibility_identity(monkeypatch):
    series = _synthetic_series()
    monkeypatch.setattr(learning, "canonical_daily_series", lambda *args, **kwargs: series)
    monkeypatch.setattr(learning, "canonical_content_hash", lambda *args, **kwargs: "canonical-hash-test")

    class FakeStore:
        def save_learning_run(self, symbol, horizon_days, result):
            self.saved = result
            return {"id": "test"}

    store = FakeStore()
    result = learning.run_learning_cycle(
        "GGAL",
        horizon_days=5,
        train_size=80,
        test_size=20,
        store=store,
        initialize_store=False,
    )

    assert "learner_id" in result
    assert result["data_fabric"] == "CANONICAL_DAILY_V1"
    assert result["model_hash"]
    assert result["dataset_hash"]
    assert result["candidate_policy"]["automatic_promotion"] is False
    assert result["candidate_policy"]["serving_model_mutation"] is False


def test_rejected_learning_candidate_is_not_shadowed():
    from scripts.gorila_runtime_tick import _create_learning_shadow_predictions

    class FakeStore:
        def __init__(self):
            self.saved = []

        def latest_shadow(self, symbol=None, limit=1):
            return []

        def save_shadow_prediction(self, **kwargs):
            self.saved.append(kwargs)
            return {"id": "shadow-1", "status": "OPEN"}

    store = FakeStore()
    result = _create_learning_shadow_predictions(
        store,
        [{
            "symbol": "GGAL",
            "status": "CANDIDATE_REJECTED",
            "latest_probability_up": 0.9,
            "data_fabric": "CANONICAL_DAILY_V1",
        }],
    )

    assert result["created_count"] == 0
    assert store.saved == []
    assert result["skipped"][0]["reason"] == "CANDIDATE_NOT_ELIGIBLE"
