import math
def test_live_evidence_contract_module_imports():
    from gorila_argentum.evidence import manifest_digest
    digest = manifest_digest({"schema": "x", "evidence": []})
    assert len(digest) == 64


def test_quantitative_validator_gate_is_conservative():
    from research.gorila_quantitative_v1_oos import validation_gate
    gate = validation_gate({
        "status": "COMPLETE",
        "oos_samples": 800,
        "brier_skill": 0.04,
        "brier_skill_ci95": [0.01, 0.07],
        "rank_ic": 0.03,
        "strategy_costs": {"50": {"net_return": 0.20}},
        "accuracy": 0.54,
        "placebo_accuracy_p95": 0.52,
        "stress": {
            "0": {"50": {"net_return": 0.10}},
            "1": {"50": {"net_return": 0.02}},
            "2": {"50": {"net_return": 0.01}},
        },
        "pbo": {"status": "COMPLETE", "pbo": 0.0},
        "dsr": 0.20,
        "execution_delta_vs_momentum_50bps": 0.10,
    })
    assert gate["validation_status"] == "VALIDATED"
    assert gate["prediction_status"] == "VALIDATED"
    assert gate["strategy_status"] == "VALIDATED"
    assert gate["prediction_reasons"] == []
    assert gate["strategy_reasons"] == []


def test_execution_uses_simple_return_not_log_return():
    from research.gorila_quantitative_v1_oos import strategy_from_probs
    result = strategy_from_probs([1.0], [math.log(1.10)], 0, 1)
    assert abs(result["net_return"] - 0.10) < 1e-9
