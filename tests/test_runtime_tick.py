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
