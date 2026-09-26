def test_frozen_relative_lockbox_module_imports():
    from research.gorila_frozen_relative_lockbox_v2 import SCORE_VARIANTS, LOCKBOX_DAYS
    assert SCORE_VARIANTS == ("probability_delta", "vol_scaled_delta", "blend_momentum")
    assert LOCKBOX_DAYS == 756
