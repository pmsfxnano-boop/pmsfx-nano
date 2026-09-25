from quant.research_validation import (
    audit_returns,
    combinatorial_splits,
    pbo_from_train_test,
)


def test_cpcv_split_count_and_partition():
    splits = combinatorial_splits(6, 2)
    assert len(splits) == 15
    for train, test in splits:
        assert len(train) == 4
        assert len(test) == 2
        assert set(train).isdisjoint(test)
        assert set(train) | set(test) == set(range(6))


def test_pbo_train_winner_below_median():
    result = pbo_from_train_test(
        train_matrix=[[3.0, 3.0], [2.0, 2.0]],
        test_matrix=[[0.0, 0.0], [1.0, 1.0]],
    )
    assert result["status"] == "COMPLETE"
    assert result["pbo"] == 1.0


def test_audit_returns_is_finite_and_selection_aware():
    result = audit_returns(
        [0.01, -0.005, 0.02, -0.01, 0.015],
        trials=12,
        periods_per_year=252,
    )
    assert result["status"] == "COMPLETE"
    assert result["observations"] == 5
    assert result["trials"] == 12
    assert result["sharpe"] is not None
    assert 0.0 <= result["deflated_sharpe_probability"] <= 1.0
