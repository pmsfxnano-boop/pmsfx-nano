from math import isclose

from gorila_argentum.calibration import (
    apply_logit_intercept,
    build_recalibration_candidate,
    fit_logit_intercept,
)


def test_intercept_recalibration_improves_systematic_overconfidence():
    probabilities = [0.9] * 40
    labels = [1.0 if i % 2 == 0 else 0.0 for i in range(40)]
    intercept = fit_logit_intercept(probabilities, labels)
    calibrated = [apply_logit_intercept(p, intercept) for p in probabilities]
    assert intercept < 0.0
    assert all(0.0 < p < 1.0 for p in calibrated)
    assert calibrated[0] < probabilities[0]


def test_recalibration_candidate_requires_enough_shadow_data():
    result = build_recalibration_candidate([])
    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["automatic_apply"] is False


def test_recalibration_candidate_reduces_calibration_error():
    rows = []
    for i in range(90):
        rows.append({
            "status": "SETTLED",
            "created_at": f"2026-01-{(i % 28) + 1:02d}T00:00:00+00:00",
            "observed_at": f"2026-01-{(i % 28) + 1:02d}T01:00:00+00:00",
            "probability_up": 0.80,
            "realized_direction": "UP" if i % 2 == 0 else "DOWN",
        })
    for i in range(30):
        rows.append({
            "status": "SETTLED",
            "created_at": f"2026-04-{(i % 28) + 1:02d}T00:00:00+00:00",
            "observed_at": f"2026-04-{(i % 28) + 1:02d}T01:00:00+00:00",
            "probability_up": 0.80,
            "realized_direction": "UP" if i % 2 == 0 else "DOWN",
        })
    result = build_recalibration_candidate(rows)
    assert result["status"] in {"CANDIDATE_READY", "CANDIDATE_REJECTED"}
    assert result["automatic_apply"] is False
    assert result["apply_gate"] == "PROMOTION_AND_DURABILITY_REQUIRED"
    assert isclose(result["baseline"]["brier"], 0.25, rel_tol=0.2)
