import os

from gorila_argentum.control import build_control_state
from gorila_argentum.storage import Store


def test_control_room_reports_blocked_and_drift_alerts(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "control.sqlite3"))
    monkeypatch.delenv("GORILA_PREDICTOR_PROMOTION", raising=False)

    store = Store()
    store.init()
    store.save_drift(
        "GGAL",
        "close",
        {
            "status": "ALERT",
            "reference_n": 90,
            "current_n": 30,
            "psi": 0.31,
            "ks": 0.22,
            "mean_shift_z": 3.4,
            "std_ratio": 1.2,
            "reference_mean": 100.0,
            "current_mean": 130.0,
            "reference_window": 90,
            "current_window": 30,
        },
        metadata={"trigger": "test"},
    )

    state = build_control_state(store)

    assert state["batch"] == 13
    assert state["runtime"]["mode"] == "RESEARCH"
    assert state["promotion_gate"]["status"] == "BLOCKED"
    assert state["promotion_gate"]["automatic_promotion"] is False
    assert state["monitoring"]["data_distribution_drift"] == "IMPLEMENTED"
    assert state["monitoring"]["automatic_kill_switch"] == "NOT_IMPLEMENTED"
    assert state["drift"]["snapshots_seen"] == 1
    assert len(state["drift"]["warnings_or_alerts"]) == 1
    assert state["drift"]["warnings_or_alerts"][0]["status"] == "ALERT"
