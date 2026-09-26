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


def test_learning_shadow_capture_is_idempotent():
    from scripts.gorila_runtime_tick import _create_learning_shadow_predictions

    class FakeStore:
        def __init__(self):
            self.created = []
            self.latest = []
        def recent_series(self, symbol, field, limit=1):
            assert field == "close"
            return [("2026-09-25T15:30:00+00:00", 100.0)]
        def latest_shadow(self, symbol=None, status=None, limit=1):
            return list(self.latest)
        def save_shadow_prediction(self, **kwargs):
            row = {"id": "shadow-1", **kwargs}
            self.created.append(row)
            self.latest = [{"feature_hash": kwargs["feature_hash"]}]
            return {"id": row["id"], "created_at": "2026-09-26T00:00:00+00:00", "status": "OPEN"}

    store = FakeStore()
    result = _create_learning_shadow_predictions(
        store,
        [{
            "symbol": "GGAL",
            "status": "CANDIDATE_REJECTED",
            "latest_probability_up": 0.57,
            "dataset_hash": "dataset-1",
        }],
    )
    assert result["created_count"] == 1
    assert store.created[0]["model_version"] == "gorila-learning-5d-v1"

    result2 = _create_learning_shadow_predictions(
        store,
        [{
            "symbol": "GGAL",
            "status": "CANDIDATE_REJECTED",
            "latest_probability_up": 0.57,
            "dataset_hash": "dataset-1",
        }],
    )
    assert result2["created_count"] == 0
    assert result2["skipped"][0]["reason"] == "ALREADY_CAPTURED"
