import math


def test_v2_fixed_spec_has_no_candidate_search():
    from research.gorila_cross_sectional_predictor_v2 import FEATURE_NAMES, L2, _portfolio_row
    assert FEATURE_NAMES == ("r1", "r3", "r5")
    assert L2 == 0.001
    result = _portfolio_row(
        scores=[0.6, 0.4],
        returns=[math.log(1.10), math.log(0.90)],
        cost_bps=10,
    )
    assert result > 0.0


def test_v2_round_trip_cost_is_two_sides():
    from research.gorila_cross_sectional_predictor_v2 import _portfolio_row
    # Zero spread with only costs must be negative.
    assert _portfolio_row([0.6, 0.4], [0.0, 0.0], 10) == -0.002
