from gorila_argentum.audit import build_audit_state


def test_audit_state_preserves_research_invariants():
    state = build_audit_state()
    assert state["mode"] == "RESEARCH"
    assert state["promotion"]["automatic_promotion"] is False
    assert state["invariants"]["trading_execution"] is False
    assert state["invariants"]["point_in_time_shadow_settlement"] is True
    assert state["readiness"]["storage_durable"] is False
    assert state["readiness"]["promotion_operational_gate"] == "BLOCKED"
