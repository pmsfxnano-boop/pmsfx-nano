from gorila_argentum import runtime_tick


def test_runtime_tick_refuses_non_durable_storage(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "tick.sqlite3"))
    try:
        runtime_tick.main()
        assert False, "expected durable storage refusal"
    except SystemExit as exc:
        assert str(exc) == "durable_storage_required"
