from gorila_argentum import audit


def test_audit_reuses_control_promotion_and_learning(monkeypatch):
    calls = {"promotion": 0, "learning": 0, "shadow_summary": 0}

    control_payload = {
        "runtime": {
            "mode": "RESEARCH",
            "storage": "postgres",
        },
        "promotion_gate": {
            "live_evidence": {
                "status": "BLOCKED",
                "eligible": False,
                "reasons": ["TEST"],
            }
        },
        "shadow": {"predictions": 1, "open": 0, "settled": 1},
        "learning": {
            "runs": [{"symbol": "GGAL"}],
            "latest_run": {"symbol": "GGAL"},
        },
        "recalibration": {},
        "drift": {},
        "source_health": [],
    }

    class Store:
        pg = True

        def init(self):
            return None

        def shadow_summary(self):
            calls["shadow_summary"] += 1
            return control_payload["shadow"]

    monkeypatch.setattr(audit, "build_control_state", lambda store: control_payload)
    monkeypatch.setattr(audit, "evaluate_live_promotion", lambda store: calls.__setitem__("promotion", calls["promotion"] + 1))
    monkeypatch.setattr(Store, "latest_learning", lambda self, limit=5: calls.__setitem__("learning", calls["learning"] + 1) or control_payload["learning"]["runs"])

    result = audit.build_audit_state(Store())

    assert result["promotion"]["status"] == "BLOCKED"
    assert result["learning"]["runs"] == 1
    assert calls["promotion"] == 0
    assert calls["learning"] == 0
    assert calls["shadow_summary"] == 1
