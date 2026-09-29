from scripts import gorila_runtime_tick as runtime_tick


def test_runtime_tick_refuses_non_durable_storage(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "tick.sqlite3"))
    try:
        runtime_tick.main()
        assert False, "expected durable storage refusal"
    except SystemExit as exc:
        assert str(exc) == "durable_storage_required"



def test_runtime_tick_http_route_is_fail_closed_and_executes(monkeypatch):
    from fastapi.testclient import TestClient
    import gorila_argentum.app as app_module

    monkeypatch.setenv("GORILA_RUNTIME_TICK_KEY", "test-runtime-key")
    monkeypatch.setattr(
        app_module,
        "run_runtime_tick",
        lambda: {"status": "COMPLETED", "persisted": True},
    )

    client = TestClient(app_module.app)
    assert client.post("/api/runtime/tick").status_code == 401

    response = client.post(
        "/api/runtime/tick",
        headers={"X-Gorila-Runtime-Key": "test-runtime-key"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "COMPLETED"


def test_runtime_tick_http_route_returns_409_without_postgres(monkeypatch):
    from fastapi.testclient import TestClient
    import gorila_argentum.app as app_module

    monkeypatch.setenv("GORILA_RUNTIME_TICK_KEY", "test-runtime-key")
    monkeypatch.setattr(
        app_module,
        "run_runtime_tick",
        lambda: (_ for _ in ()).throw(RuntimeError("durable_storage_required")),
    )

    client = TestClient(app_module.app)
    response = client.post(
        "/api/runtime/tick",
        headers={"X-Gorila-Runtime-Key": "test-runtime-key"},
    )
    assert response.status_code == 409
    assert response.json()["detail"] == "durable_storage_required"


def test_learning_shadow_capture_is_idempotent(monkeypatch):
    from scripts.gorila_runtime_tick import _create_learning_shadow_predictions

    class FakeStore:
        def __init__(self):
            self.created = []
            self.feature_hashes = set()

    store = FakeStore()

    monkeypatch.setattr(
        runtime_tick,
        "canonical_daily_series",
        lambda store, symbol, field, limit: [("2026-09-25", 100.0)],
    )

    def shadow_existing_feature_hashes(hashes):
        return set(hashes) & store.feature_hashes

    def save_shadow_predictions_bulk(predictions):
        new = []
        for i, row in enumerate(predictions):
            store.feature_hashes.add(row["feature_hash"])
            item = {"id": f"shadow-{len(store.created)+i+1}"}
            new.append(item)
            store.created.append({"id": item["id"], **row})
        return {"created": len(new), "items": new}

    store.shadow_existing_feature_hashes = shadow_existing_feature_hashes
    store.save_shadow_predictions_bulk = save_shadow_predictions_bulk

    result = _create_learning_shadow_predictions(
        store,
        [{
            "symbol": "GGAL",
            "status": "CANDIDATE_ELIGIBLE",
            "latest_probability_up": 0.57,
            "dataset_hash": "dataset-1",
            "learner_id": "gorila-learning-5d-v2",
            "data_fabric": "CANONICAL_DAILY_V1",
            "canonical_content_hash": "canonical-1",
            "model_hash": "model-1",
        }],
    )
    assert result["created_count"] == 1
    assert store.created[0]["model_version"] == "gorila-learning-5d-v2"

    result2 = _create_learning_shadow_predictions(
        store,
        [{
            "symbol": "GGAL",
            "status": "CANDIDATE_ELIGIBLE",
            "latest_probability_up": 0.57,
            "dataset_hash": "dataset-1",
            "learner_id": "gorila-learning-5d-v2",
            "data_fabric": "CANONICAL_DAILY_V1",
            "canonical_content_hash": "canonical-1",
            "model_hash": "model-1",
        }],
    )
    assert result2["created_count"] == 0
    assert result2["skipped"][0]["reason"] == "ALREADY_CAPTURED"
