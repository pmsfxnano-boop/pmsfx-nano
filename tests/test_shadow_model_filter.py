from gorila_argentum.storage import Store


def test_latest_shadow_can_filter_model_version(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "shadow-model-filter.sqlite3"))

    store = Store()
    store.init()

    for model, suffix in (
        ("gorila-univariate-logit-candidate-v2", "a"),
        ("pmsfx-x-upstream-shadow-v1", "b"),
    ):
        store.save_shadow_prediction(
            symbol="GGAL",
            model_version=model,
            probability_up=0.6,
            horizon_seconds=5 * 24 * 3600,
            regime="TEST",
            entry_price=100.0,
            feature_hash=f"filter-{suffix}",
        )

    rows = store.latest_shadow(
        status="OPEN",
        model_version="gorila-univariate-logit-candidate-v2",
        limit=20,
    )

    assert len(rows) == 1
    assert rows[0]["model_version"] == "gorila-univariate-logit-candidate-v2"
