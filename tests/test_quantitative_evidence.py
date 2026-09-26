def test_live_evidence_contract_module_imports():
    from gorila_argentum.evidence import manifest_digest
    digest = manifest_digest({"schema": "x", "evidence": []})
    assert len(digest) == 64


def test_quantitative_validator_gate_is_conservative():
    from research.gorila_quantitative_v1_oos import validation_gate
    status, reasons = validation_gate({
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
    })
    assert status == "VALIDATED"
    assert reasons == []
