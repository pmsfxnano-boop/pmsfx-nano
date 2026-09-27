def test_frozen_relative_lockbox_module_imports():
    from research.gorila_frozen_relative_lockbox_v2 import SCORE_VARIANTS, LOCKBOX_DAYS
    assert SCORE_VARIANTS == ("probability_delta", "vol_scaled_delta", "blend_momentum")
    assert LOCKBOX_DAYS == 756


def test_execution_cost_grid_contains_required_50bps():
    from research.gorila_frozen_relative_lockbox_v2 import COSTS_BPS_PER_LEG, REQUIRED_EXECUTION_COST_BPS
    assert 50 in COSTS_BPS_PER_LEG
    assert REQUIRED_EXECUTION_COST_BPS == 50


def test_pair_trade_respects_execution_spread_threshold():
    from research.gorila_frozen_relative_lockbox_v2 import pair_trade
    scores = {"A": -0.01, "B": 0.01, "C": 0.03}
    returns = {"A": 0.02, "B": 0.01, "C": 0.04}
    assert pair_trade(scores, returns, ["A","B","C"], 0, min_spread=0.05) is not None
    assert pair_trade(scores, returns, ["A","B","C"], 0, min_spread=0.05) == pair_trade(scores, returns, ["A","B","C"], 0)
    assert pair_trade(scores, returns, ["A","B","C"], 0, min_spread=0.05) is not None
    assert pair_trade(scores, returns, ["A","B","C"], 0, min_spread=0.051) is None
