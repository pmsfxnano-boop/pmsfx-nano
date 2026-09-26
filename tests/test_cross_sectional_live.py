def test_live_cross_sectional_exports_frozen_contract():
    from gorila_argentum.cross_sectional_live import FEATURE_NAMES, HORIZON_DAYS, L2
    assert FEATURE_NAMES == ("r1", "r3", "r5")
    assert HORIZON_DAYS == 5
    assert L2 == 0.001
