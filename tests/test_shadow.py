import math

from gorila_argentum.shadow import compute_shadow_outcome, validate_shadow_prediction
from gorila_argentum.storage import Store


def test_shadow_prediction_validation():
    result = validate_shadow_prediction(
        symbol="ggal",
        probability_up=0.72,
        horizon_seconds=900,
        entry_price=100.0,
    )
    assert result["symbol"] == "GGAL"
    assert result["direction"] == "UP"

def test_shadow_outcome_metrics():
    outcome = compute_shadow_outcome(0.72, 100.0, 105.0)
    assert outcome["realized_direction"] == "UP"
    assert outcome["correct"] is True
    assert math.isclose(outcome["return_pct"], 5.0)
    assert math.isclose(outcome["brier"], (0.72 - 1.0) ** 2)
    assert math.isclose(outcome["logloss"], -math.log(0.72))

def test_shadow_storage_roundtrip(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "shadow.sqlite3"))

    store = Store()
    store.init()
    created = store.save_shadow_prediction(
        "GGAL", "V0", 0.72, 900, "TREND_UP", 100.0, feature_hash="abc"
    )
    prediction = store.get_shadow_prediction(created["id"])
    assert prediction["status"] == "OPEN"
    assert prediction["feature_hash"] == "abc"

    outcome = compute_shadow_outcome(0.72, 100.0, 105.0)
    settled = store.settle_shadow_prediction(created["id"], outcome)
    assert settled["prediction_id"] == created["id"]

    summary = store.shadow_summary()
    assert summary["predictions"] == 1
    assert summary["open"] == 0
    assert summary["settled"] == 1
    assert math.isclose(summary["accuracy"], 1.0)
    assert math.isclose(summary["mean_brier"], (0.72 - 1.0) ** 2)
