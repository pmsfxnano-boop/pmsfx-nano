from datetime import date, timedelta

import gorila_argentum.learning as learning
import scripts.gorila_runtime_tick as runtime_tick


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


def test_candidate_result_contains_reproducibility_identity(monkeypatch):
    series = _synthetic_series()
    monkeypatch.setattr(learning, "canonical_daily_series", lambda *args, **kwargs: series)
    monkeypatch.setattr(learning, "canonical_content_hash", lambda *args, **kwargs: "canonical-hash-test")

    class FakeStore:
        def latest_learning(self, symbol=None, limit=20):
            return []

        def shadow_feedback_fingerprint(self, *, model_version, symbol, horizon_seconds):
            return {
                "hash": "feedback-hash-test",
                "sample_count": 3,
                "last_observed_at": "2026-09-28T12:00:00+00:00",
            }

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
    assert result["feedback_hash"] == "feedback-hash-test"
    assert result["feedback_sample_count"] == 3
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


def test_learning_shadow_creation_deduplicates_and_batches():
    from scripts.gorila_runtime_tick import _create_learning_shadow_predictions

    class FakeStore:
        def __init__(self):
            self.saved = []

        def shadow_existing_feature_hashes(self, hashes):
            return {"existing-hash"} & set(hashes)

        def save_shadow_predictions_bulk(self, predictions):
            self.saved.extend(predictions)
            return {
                "created": len(predictions),
                "items": [{"id": f"shadow-{i}", "feature_hash": row["feature_hash"]} for i, row in enumerate(predictions)],
            }

    store = FakeStore()
    base = {
        "status": "CANDIDATE_ELIGIBLE",
        "data_fabric": "CANONICAL_DAILY_V1",
        "learner_id": "learner-test",
        "dataset_hash": "dataset-test",
        "canonical_content_hash": "canonical-test",
        "model_hash": "model-test",
        "latest_probability_up": 0.61,
        "feature_names": list(learning.FEATURE_NAMES),
        "validation": {"accuracy": 0.56},
        "candidate_policy": {"automatic_promotion": False},
    }
    class Canonical:
        pass

    original = runtime_tick.canonical_daily_series
    try:
        runtime_tick.canonical_daily_series = lambda store, symbol, field, limit: [("2026-09-25", 100.0)]
        result = _create_learning_shadow_predictions(
            store,
            [{**base, "symbol": "GGAL"}],
        )
    finally:
        runtime_tick.canonical_daily_series = original

    assert result["created_count"] == 1
    assert len(store.saved) == 1
    assert store.saved[0]["feature_hash"] != "existing-hash"


def test_shadow_feedback_fingerprint_changes_after_settlement(tmp_path, monkeypatch):
    from gorila_argentum.storage import Store

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "feedback.sqlite3"))

    store = Store()
    store.init()

    first = store.shadow_feedback_fingerprint(
        model_version="learner-test",
        symbol="GGAL",
        horizon_seconds=5 * 24 * 3600,
    )
    prediction = store.save_shadow_prediction(
        symbol="GGAL",
        model_version="learner-test",
        probability_up=0.6,
        horizon_seconds=5 * 24 * 3600,
        regime="TEST",
        entry_price=100.0,
        feature_hash="feedback-test-1",
        created_at="2026-09-20T12:00:00+00:00",
    )
    second = store.shadow_feedback_fingerprint(
        model_version="learner-test",
        symbol="GGAL",
        horizon_seconds=5 * 24 * 3600,
    )
    assert first["hash"] == second["hash"]
    assert second["sample_count"] == 0

    store.settle_shadow_prediction(
        prediction["id"],
        {
            "observed_price": 101.0,
            "realized_direction": "UP",
            "return_pct": 1.0,
            "correct": True,
            "brier": 0.16,
            "logloss": 0.51,
        },
        "2026-09-25T12:00:00+00:00",
    )
    third = store.shadow_feedback_fingerprint(
        model_version="learner-test",
        symbol="GGAL",
        horizon_seconds=5 * 24 * 3600,
    )
    assert third["hash"] != second["hash"]
    assert third["sample_count"] == 1
    assert third["last_observed_at"]
