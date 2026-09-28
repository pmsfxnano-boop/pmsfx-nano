[object Object]

def test_recalibration_candidate_enters_prospective_shadow(monkeypatch):
    import scripts.gorila_runtime_tick as runtime_tick

    class FakeStore:
        def __init__(self):
            self.saved = []

        def shadow_existing_feature_hashes(self, hashes):
            return set()

        def save_shadow_predictions_bulk(self, predictions):
            self.saved.extend(predictions)
            return {
                "created": len(predictions),
                "items": [{"id": f"cal-{i}"} for i, _ in enumerate(predictions)],
            }

    monkeypatch.setattr(
        runtime_tick,
        "canonical_daily_series",
        lambda store, symbol, field, limit: [("2026-09-25", 100.0)],
    )

    store = FakeStore()
    result = runtime_tick._create_recalibration_shadow_predictions(
        store,
        [{
            "symbol": "GGAL",
            "status": "CANDIDATE_ELIGIBLE",
            "latest_probability_up": 0.70,
            "learner_id": "learner-test",
            "model_hash": "model-test",
            "dataset_hash": "dataset-test",
            "canonical_content_hash": "canonical-test",
        }],
        {
            "status": "CANDIDATE_READY",
            "intercept": -0.25,
            "reference_n": 80,
            "validation_n": 30,
            "brier_improvement": 0.02,
            "logloss_improvement": 0.03,
        },
    )

    assert result["status"] == "COMPLETED"
    assert result["created_count"] == 1
    assert store.saved[0]["model_version"] == "gorila-calibration-intercept-v1"
    assert store.saved[0]["probability_up"] < 0.70
    assert store.saved[0]["metadata"]["automatic_apply"] is False
