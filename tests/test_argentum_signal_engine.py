from math import isclose

from gorila_argentum.signal_engine import build_signal, build_matrix


def base_state(**forecast_overrides):
    forecast = {
        "raw_probability_up": 0.68,
        "confidence_raw": 0.36,
        "direction": "UP",
        "validated": True,
        "multi_horizon": {
            "horizons": {
                "300": {"latest_forecast": {"p_up": 0.66}},
                "900": {"latest_forecast": {"p_up": 0.69}},
                "1800": {"latest_forecast": {"p_up": 0.71}},
            }
        },
        "horizon_consensus": {"confluence_index": 0.84},
    }
    forecast.update(forecast_overrides)
    return {
        "forecast": forecast,
        "evaluation": {
            "validated": True,
            "accuracy": 0.61,
            "brier": 0.23,
            "brier_skill": 0.08,
            "log_loss": 0.61,
            "oos_count": 420,
        },
        "last": 100.0,
        "bid": 99.9,
        "ask": 100.1,
        "spread_bps": 20.0,
        "quote_timestamp": "2026-09-26T14:00:00+00:00",
        "data_source": "research-test",
        "engine_freshness": {"age_seconds": 5.0},
    }


def test_signal_is_research_gated():
    result = build_signal(
        symbol="GGAL",
        state=base_state(),
        price_series=[],
        drift={"status": "OK", "psi": 0.5},
        shadow_summary={"accuracy": 0.64, "settled": 105},
    )
    assert result["signal"] == "UP"
    assert result["research_only"] is True
    assert result["no_execution_authority"] is True
    assert result["signal_score"] > 50


def test_drift_alert_blocks_actionability():
    result = build_signal(
        symbol="YPFD",
        state=base_state(),
        price_series=[],
        drift={"status": "ALERT", "psi": 11.8},
        shadow_summary={"accuracy": 0.64, "settled": 105},
    )
    assert result["actionable"] is False
    assert result["status"] in {"WATCH", "BLOCKED"}
    assert "DRIFT_ALERT" in result["risk_flags"]


def test_unvalidated_model_cannot_arm():
    result = build_signal(
        symbol="BMA",
        state=base_state(validated=False),
        price_series=[],
        drift={"status": "OK", "psi": 0.5},
        shadow_summary={"accuracy": 0.64, "settled": 105},
    )
    result["validation"]["validated"] = False
    assert result["actionable"] is False
    assert "MODEL_NOT_VALIDATED" in result["risk_flags"]


def test_matrix_reports_counts():
    items = [
        {"symbol": "GGAL", "status": "WATCH", "signal_score": 55},
        {"symbol": "BMA", "status": "BLOCKED", "signal_score": 20},
        {"symbol": "YPFD", "status": "NO_DATA", "signal_score": 0},
    ]
    matrix = build_matrix(items)
    assert matrix["counts"] == {
        "total": 3,
        "armed_research": 0,
        "watch": 1,
        "blocked": 1,
        "no_data": 1,
    }


def test_signal_score_uses_single_0_to_100_conversion():
    result = build_signal(
        symbol="GGAL",
        state=base_state(),
        price_series=[],
        drift={"status": "OK", "psi": 0.5},
        shadow_summary={"accuracy": 0.64, "settled": 105},
    )
    # 0.30*.36 + 0.18*.36 + 0.18*.84 + 0.16*1 + 0.10*1 + 0.08*.933333...
    # = 0.658666..., hence 65.9 on the public 0-100 scale.
    assert result["signal_score"] == 65.9
    assert result["signal_score"] < 100
    assert result["score_audit"]["normalized_pre_penalty"] == 0.658667
    assert isclose(sum(result["score_audit"]["weights"].values()), 1.0, rel_tol=0.0, abs_tol=1e-12)


def test_signal_score_is_bounded_at_100_with_perfect_factors():
    state = base_state()
    state["forecast"]["raw_probability_up"] = 1.0
    state["forecast"]["confidence_raw"] = 1.0
    state["forecast"]["horizon_consensus"] = {"confluence_index": 1.0}
    state["evaluation"]["accuracy"] = 1.0
    state["engine_freshness"] = {"age_seconds": 0.0}
    result = build_signal(
        symbol="GGAL",
        state=state,
        price_series=[],
        drift={"status": "OK", "psi": 0.0},
        shadow_summary={"accuracy": 0.65, "settled": 100},
    )
    assert result["signal_score"] == 100.0
    assert 0.0 <= result["score_audit"]["normalized_pre_penalty"] <= 1.0


def test_low_evidence_state_cannot_become_100_from_scale_error():
    state = {
        "forecast": {
            "raw_probability_up": 0.513,
            "confidence_raw": 0.025,
            "direction": "NEUTRAL",
            "validated": False,
        },
        "evaluation": {
            "validated": False,
            "accuracy": 0.487,
            "brier_skill": -0.00088,
            "oos_count": 1856,
        },
        "engine_freshness": {"age_seconds": 900.0},
        "last": 41.775,
    }
    result = build_signal(
        symbol="GGAL",
        state=state,
        price_series=[],
        drift={"status": "INSUFFICIENT_DATA"},
        shadow_summary={"accuracy": 0.644, "settled": 105},
    )
    assert 0.0 <= result["signal_score"] < 15.0
    assert result["signal_score"] != 100.0
    assert result["actionable"] is False


def test_no_forecast_is_not_rendered_as_numeric_zero_score():
    state = {
        "forecast": None,
        "evaluation": {},
        "engine_freshness": {"age_seconds": None},
    }
    result = build_signal(
        symbol="CEPU",
        state=state,
        price_series=[],
        drift=None,
        shadow_summary={"accuracy": None, "settled": 0},
    )
    assert result["status"] == "NO_DATA"
    assert result["signal_score"] is None
    assert result["probability"]["up"] is None


def test_matrix_preserves_no_data_semantics():
    rows = build_matrix([
        {"symbol": "GGAL", "status": "NO_DATA", "signal": "NEUTRAL", "signal_score": None},
        {"symbol": "BMA", "status": "WATCH", "signal": "UP", "signal_score": 51.0},
    ])
    assert rows["counts"]["no_data"] == 1
    assert rows["items"][0]["symbol"] == "BMA"
    assert rows["items"][1]["symbol"] == "GGAL"
