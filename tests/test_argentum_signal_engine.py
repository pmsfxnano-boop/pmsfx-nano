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
