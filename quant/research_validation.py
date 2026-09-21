"""PMSF-X Nano — CPCV/PBO/DSR research accounting (research-only).

These statistics do not promote a model. They quantify selection risk and
multiple-testing effects and must be interpreted only on OOS/CV results.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import erf, exp, log, sqrt
from typing import Sequence


@dataclass(frozen=True)
class SearchRecord:
    run_id: str
    candidate_id: str
    metric: float
    observations: int
    selected: bool = False


def combinatorial_splits(n_groups: int, test_groups: int) -> list[tuple[tuple[int, ...], tuple[int, ...]]]:
    """Enumerate CPCV train/test group combinations."""
    if n_groups <= 0 or test_groups <= 0 or test_groups >= n_groups:
        return []
    from itertools import combinations
    groups = tuple(range(n_groups))
    out = []
    for test in combinations(groups, test_groups):
        ts = set(test)
        train = tuple(g for g in groups if g not in ts)
        out.append((train, test))
    return out


def probability_of_backtest_overfitting(performance_matrix: Sequence[Sequence[float]]) -> float | None:
    """Simple CSCV/PBO estimate from relative train/test rankings.

    Matrix rows are candidate strategies and columns are paired train/test
    observations. Each column is interpreted as one combinatorial split.
    """
    if not performance_matrix:
        return None
    rows = [list(r) for r in performance_matrix if r]
    if len(rows) < 2:
        return None
    m = min(len(r) for r in rows)
    if m < 2:
        return None
    overfit = 0
    total = 0
    for j in range(0, m, 2):
        if j + 1 >= m:
            break
        train = [r[j] for r in rows]
        test = [r[j + 1] for r in rows]
        winner = max(range(len(rows)), key=lambda i: train[i])
        if test[winner] < sorted(test)[len(test) // 2]:
            overfit += 1
        total += 1
    return overfit / total if total else None


def deflated_sharpe_ratio(sharpe: float, trials: int, observations: int, skew: float = 0.0, kurtosis: float = 3.0) -> float | None:
    """Approximate DSR as a normal-tail probability after a multiple-trial penalty."""
    if observations < 2 or trials < 1:
        return None
    penalty = sqrt(max(0.0, 2.0 * log(max(2, trials))))
    se = sqrt(max(1e-12, (1.0 - skew * sharpe + ((kurtosis - 1.0) / 4.0) * sharpe * sharpe) / observations))
    z = (sharpe - penalty) / se
    return 0.5 * (1.0 + erf(z / sqrt(2.0)))


def search_ledger_summary(records: Sequence[SearchRecord]) -> dict:
    if not records:
        return {"trials": 0, "selected_count": 0, "best_metric": None}
    return {
        "trials": len(records),
        "selected_count": sum(r.selected for r in records),
        "best_metric": max(r.metric for r in records),
        "unique_candidates": len({r.candidate_id for r in records}),
    }
