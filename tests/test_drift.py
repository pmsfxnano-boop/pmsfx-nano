from gorila_argentum.drift import evaluate_drift, rolling_drift


def test_no_drift_is_ok():
    base = [float(i) / 10.0 for i in range(10)]
    reference = base * 10
    current = base * 3
    result = evaluate_drift(reference, current)
    assert result["status"] in {"OK", "WARN"}
    assert result["psi"] < 0.25


def test_strong_location_shift_is_alert():
    reference = [float(i) for i in range(1, 101)]
    current = [1000.0 + float(i) for i in range(30)]
    result = evaluate_drift(reference, current)
    assert result["status"] == "ALERT"
    assert result["mean_shift_z"] > 3.0


def test_rolling_drift_requires_enough_data():
    result = rolling_drift([1.0] * 50, current_size=20, reference_size=40)
    assert result["status"] == "INSUFFICIENT_DATA"


def test_constant_reference_handles_scale_change():
    reference = [10.0] * 30
    current = [20.0] * 10
    result = evaluate_drift(reference, current, bins=5)
    assert result["status"] == "ALERT"
    assert result["std_ratio"] == 1.0


def test_drift_snapshot_roundtrip(tmp_path, monkeypatch):
    from gorila_argentum.storage import Store

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "drift.sqlite3"))

    store = Store()
    store.init()
    store.save_drift(
        "GGAL",
        "close",
        {
            "status": "WARN",
            "reference_n": 90,
            "current_n": 30,
            "psi": 0.12,
            "ks": 0.11,
            "mean_shift_z": 2.1,
            "std_ratio": 1.2,
            "reference_mean": 100.0,
            "current_mean": 105.0,
            "reference_window": 90,
            "current_window": 30,
        },
        metadata={"trigger": "test"},
    )

    rows = store.latest_drift(symbol="GGAL", field="close")
    assert len(rows) == 1
    assert rows[0]["status"] == "WARN"
    assert rows[0]["metadata"]["trigger"] == "test"
