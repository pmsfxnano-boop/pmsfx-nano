from datetime import date, timedelta

import numpy as np

from gorila_argentum.g2_frozen_scorer import _rolling_std
from gorila_argentum.g2_frozen_scorer import G2FrozenModel, score_g2_h10


def _synthetic_model():
    symbols = np.array(["BMA", "CEPU", "GGAL", "PAMP", "TGSU2", "YPFD"])
    feature_names = np.array([
        "momentum_3", "momentum_5", "momentum_20", "momentum_60", "momentum_120",
        "reversal_2", "vol_20", "vol_60", "momentum20_over_vol20",
        "momentum60_over_vol60", "momentum20_minus_momentum60_div3", "drawdown_120",
    ])
    path = __import__("pathlib").Path("/tmp/g2-synthetic.npz")
    np.savez(
        path,
        symbols=symbols,
        feature_names=feature_names,
        reg_names=np.array(["market20", "market60", "dispersion20", "marketvol20"]),
        reg_mu=np.zeros(4),
        reg_sd=np.ones(4),
        scaler_mean=np.zeros(60),
        scaler_scale=np.ones(60),
        coef=np.ones((1, 60)),
        intercept=np.array([0.0]),
        Sigma=np.eye(6),
        beta=np.array([0.0344124970654915]),
        train_idx=np.arange(1216),
        pair_i=np.array([0,0,0,0,0,1,1,1,1,2,2,2,3,3,4]),
        pair_j=np.array([1,2,3,4,5,2,3,4,5,3,4,5,4,5,5]),
        H=np.array([10]),
        C=np.array([0.25]),
    )
    return G2FrozenModel.load(path, verify_hash=False)


def test_g2_scorer_requires_common_history(monkeypatch):
    model = _synthetic_model()

    def sparse_series(store, symbol, field, limit):
        return [("2026-09-25", 100.0)] * 30

    monkeypatch.setattr("gorila_argentum.g2_frozen_scorer.canonical_daily_series", sparse_series)
    result = score_g2_h10(object(), model)
    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["research_only"] is True
    assert result["no_execution_authority"] is True


def test_g2_scorer_returns_deterministic_cross_section(monkeypatch):
    model = _synthetic_model()

    def synthetic_series(store, symbol, field, limit):
        index = ["2026-01-01"] * 130
        base = 100.0
        offset = {
            "BMA": 0.0000,
            "CEPU": 0.0005,
            "GGAL": 0.0010,
            "PAMP": 0.0015,
            "TGSU2": 0.0020,
            "YPFD": 0.0025,
        }[symbol]
        rows = []
        start = date(2026, 1, 1)
        for i in range(130):
            day = (start + timedelta(days=i)).isoformat()
            rows.append((day, base * (1.0 + offset * i + 0.0002 * np.sin(i / 3.0))))
        return rows

    monkeypatch.setattr("gorila_argentum.g2_frozen_scorer.canonical_daily_series", synthetic_series)
    first = score_g2_h10(object(), model)
    second = score_g2_h10(object(), model)

    assert first["status"] == "READY"
    assert first["common_dates"] >= 121
    assert first["model_id"] == "G2_PIT_FIXED_C0.25_H10"
    assert len(first["items"]) == 6
    assert [x["symbol"] for x in first["items"]] == [x["symbol"] for x in second["items"]]
    assert np.allclose(
        [x["score"] for x in first["items"]],
        [x["score"] for x in second["items"]],
    )
    assert first["runtime_serving"] == "DISABLED"


def test_g2_rolling_std_matches_pandas_window_alignment():
    values = np.asarray([1.0, 2.0, 3.0, 4.0])
    result = _rolling_std(values, 3)
    assert np.isnan(result[0])
    assert np.isnan(result[1])
    assert np.isclose(result[2], 1.0)
    assert np.isclose(result[3], 1.0)
