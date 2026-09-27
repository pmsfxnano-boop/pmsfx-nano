def test_frozen_relative_lockbox_module_imports():
    from research.gorila_frozen_relative_lockbox_v2 import SCORE_VARIANTS, LOCKBOX_DAYS
    assert SCORE_VARIANTS == ("probability_delta", "vol_scaled_delta", "blend_momentum")
    assert LOCKBOX_DAYS == 756


def test_execution_cost_grid_contains_required_50bps():
    from research.gorila_frozen_relative_lockbox_v2 import COSTS_BPS_PER_LEG, REQUIRED_EXECUTION_COST_BPS
    assert 50 in COSTS_BPS_PER_LEG
    assert REQUIRED_EXECUTION_COST_BPS == 50
