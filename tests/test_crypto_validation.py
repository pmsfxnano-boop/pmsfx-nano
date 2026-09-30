                cost_multiplier=2.0,
                slippage_multiplier=2.0,
            ),
        ),
    )
    assert report.status == "OOS_EVALUATED"
    assert len(report.folds) >= 3
    assert len(report.oos_labels) > 0
    assert report.placebo_p_value is not None
    assert report.fold_baseline_pass_fraction >= 0.67
    assert report.fold_economic_positive_fraction >= 0.67
    assert report.temporal_stability.passed is True
    assert "double_costs" in report.stress_results
    assert report.promotion_eligible is False
    assert report.research_robustness_status == "BLOCKED"
    assert set(report.research_robustness_reasons) == {
        "MULTIPLE_TESTING_GATE_FAILED",
        "DSR_GATE_FAILED",
        "PBO_GATE_FAILED",
    }



def test_zero_friction_validation_cannot_be_promotion_eligible() -> None:
    dataset = [row(i) for i in range(180)]
    report = run_walk_forward_validation(
        dataset,