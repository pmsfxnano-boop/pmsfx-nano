from gorila_argentum.promotion import CURRENT_BATCH10_EVIDENCE, evaluate_promotion


def test_current_evidence_is_blocked():
    decision = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    assert decision["status"] == "BLOCKED"
    assert decision["eligible"] is False
    assert decision["automatic_promotion"] is False
    assert "MIN_OOS_ACCURACY" in decision["reasons"]
    assert "MIN_RANK_IC" in decision["reasons"]
    assert "MIN_CPCV_RETURN" in decision["reasons"]
    assert "MIN_DSR" in decision["reasons"]
    assert "STRESS_GATE_FAILED" in decision["reasons"]


def test_complete_passing_evidence_is_eligible():
    evidence = {
        "oos_accuracy": 0.61,
        "brier_skill": 0.08,
        "rank_ic": 0.03,
        "cpcv_pbo": 0.02,
        "cpcv_dsr_mean": 0.15,
        "cpcv_mean_return_pct": 2.4,
        "execution_delta_mean": 0.25,
        "stress_pass": True,
        "data_health": True,
        "point_in_time": True,
    }
    decision = evaluate_promotion(evidence)
    assert decision["status"] == "ELIGIBLE"
    assert decision["eligible"] is True
    assert decision["automatic_promotion"] is False
