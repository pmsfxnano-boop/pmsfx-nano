from gorila_argentum.drift import evaluate_drift, rolling_drift


def test_no_drift_is_ok():
    reference = [float(i % 20) for i in range(100)]
    current = [float(i % 20) for i in range(30)]
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
