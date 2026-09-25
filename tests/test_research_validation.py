import unittest

from quant.research_validation import (
    SearchRecord,
    audit_returns,
    combinatorial_splits,
    deflated_sharpe_ratio,
    pbo_from_train_test,
    probability_of_backtest_overfitting,
    search_ledger_summary,
)


class ResearchValidationTests(unittest.TestCase):
    def test_combinatorial_splits(self):
        splits = combinatorial_splits(6, 2)
        self.assertEqual(len(splits), 15)
        for train, test in splits:
            self.assertEqual(len(set(train) & set(test)), 0)
            self.assertEqual(len(train), 4)
            self.assertEqual(len(test), 2)

    def test_formal_pbo(self):
        train = [
            [0.30, 0.10, 0.40, 0.20],
            [0.10, 0.20, 0.30, 0.10],
            [0.05, 0.15, 0.25, 0.05],
        ]
        test = [
            [0.10, 0.40, 0.00, 0.30],
            [0.20, 0.10, 0.30, 0.20],
            [0.15, 0.20, 0.20, 0.10],
        ]
        result = pbo_from_train_test(train, test)
        self.assertEqual(result["status"], "FORMAL_CSCV_PBO")
        self.assertEqual(result["split_count"], 4)
        self.assertAlmostEqual(result["pbo"], 0.75)

    def test_legacy_wrapper(self):
        matrix = [
            [0.30, 0.10, 0.40, 0.20],
            [0.10, 0.20, 0.30, 0.10],
            [0.05, 0.15, 0.25, 0.05],
        ]
        self.assertAlmostEqual(
            probability_of_backtest_overfitting(matrix), 0.75
        )

    def test_dsr_probability(self):
        self.assertIsNotNone(deflated_sharpe_ratio(2.5, 12, 252))
        self.assertLess(deflated_sharpe_ratio(0.0, 12, 252), 0.5)

    def test_return_audit(self):
        result = audit_returns(
            [0.01, -0.005, 0.007, 0.004, -0.002],
            trials=12,
            periods_per_year=252,
        )
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["observations"], 5)
        self.assertIn("dsr_probability", result)

    def test_ledger(self):
        rows = [
            SearchRecord("r1", "a", 0.1, 100, True),
            SearchRecord("r2", "b", 0.2, 100, False),
            SearchRecord("r3", "a", 0.15, 100, False),
        ]
        summary = search_ledger_summary(rows)
        self.assertEqual(summary["trials"], 3)
        self.assertEqual(summary["selected_count"], 1)
        self.assertEqual(summary["unique_candidates"], 2)


if __name__ == "__main__":
    unittest.main()
