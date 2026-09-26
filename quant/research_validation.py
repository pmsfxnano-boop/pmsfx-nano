"""PMSF-X Nano — CPCV/PBO/DSR research accounting (research-only).

These statistics do not promote a model. They quantify selection risk and
multiple-testing effects and must be interpreted only on OOS/CV results.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import erf, log, sqrt
from statistics import mean
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


def pbo_from_train_test(
    train_matrix: Sequence[Sequence[float]],
    test_matrix: Sequence[Sequence[float]],
) -> dict:
    """Estimate PBO from candidate-by-split train/test performance matrices.

    Each row is a candidate and each column is one split. For every split, the
    candidate maximizing train performance is selected and its percentile rank
    on the test set is computed. PBO is the fraction of splits for which the
    selected candidate falls below the test median.
    """
    if not train_matrix or not test_matrix:
        return {"status": "INSUFFICIENT_DATA", "candidate_count": 0, "split_count": 0}

    candidates = min(len(train_matrix), len(test_matrix))
    splits = min(
        min((len(r) for r in train_matrix[:candidates]), default=0),
        min((len(r) for r in test_matrix[:candidates]), default=0),
    )
    if candidates < 2 or splits < 1:
        return {
            "status": "INSUFFICIENT_DATA",
            "candidate_count": candidates,
            "split_count": splits,
        }

    overfit = 0
    percentiles = []
    valid_splits = 0
    for j in range(splits):
        train_vals = [float(train_matrix[i][j]) for i in range(candidates)]
        test_vals = [float(test_matrix[i][j]) for i in range(candidates)]
        if not all(map(_finite, train_vals + test_vals)):
            continue
        winner = max(range(candidates), key=lambda i: train_vals[i])
        ordered = sorted(test_vals)
        rank = sum(v < test_vals[winner] for v in ordered)
        ties = sum(v == test_vals[winner] for v in ordered)
        percentile = (rank + 0.5 * ties) / candidates
        percentiles.append(percentile)
        valid_splits += 1
        if percentile < 0.5:
            overfit += 1

    if valid_splits == 0:
        return {
            "status": "INSUFFICIENT_DATA",
            "candidate_count": candidates,
            "split_count": 0,
        }
    return {
        "status": "COMPLETE",
        "candidate_count": candidates,
        "split_count": valid_splits,
        "overfit_splits": overfit,
        "pbo": overfit / valid_splits,
        "selected_test_percentiles": percentiles,
    }


def _finite(x: float) -> bool:
    return x == x and abs(x) != float("inf")


def deflated_sharpe_ratio(
    sharpe: float,
    trials: int,
    observations: int,
    skew: float = 0.0,
    kurtosis: float = 3.0,
) -> float | None:
    """Approximate DSR as a normal-tail probability after a multiple-trial penalty."""
    if observations < 2 or trials < 1:
        return None
    penalty = sqrt(max(0.0, 2.0 * log(trials))) if trials > 1 else 0.0
    se = sqrt(
        max(
            1e-12,
            (
                1.0
                - skew * sharpe
                + ((kurtosis - 1.0) / 4.0) * sharpe * sharpe
            )
            / observations,
        )
    )
    z = (sharpe - penalty) / se
    return 0.5 * (1.0 + erf(z / sqrt(2.0)))


def audit_returns(
    returns: Sequence[float],
    trials: int,
    periods_per_year: float,
) -> dict:
    """Audit a stitched OOS return series for selection-aware Sharpe/DSR.

    The function does not annualize overlapping observations as independent
    samples: the caller is responsible for constructing a non-overlapping
    trade/return series when horizon trades overlap.
    """
    clean = [float(r) for r in returns if _finite(float(r))]
    if len(clean) < 2:
        return {
            "status": "INSUFFICIENT_DATA",
            "observations": len(clean),
            "trials": int(trials),
        }

    mu = mean(clean)
    variance = sum((r - mu) ** 2 for r in clean) / (len(clean) - 1)
    std = sqrt(max(0.0, variance))
    sharpe = None if std == 0 else mu / std * sqrt(max(0.0, periods_per_year))

    if std == 0:
        skew = 0.0
        kurtosis = 3.0
    else:
        m2 = sum((r - mu) ** 2 for r in clean) / len(clean)
        m3 = sum((r - mu) ** 3 for r in clean) / len(clean)
        m4 = sum((r - mu) ** 4 for r in clean) / len(clean)
        skew = m3 / max(1e-18, m2 ** 1.5)
        kurtosis = m4 / max(1e-18, m2 ** 2)

    dsr = (
        deflated_sharpe_ratio(
            sharpe,
            trials=trials,
            observations=len(clean),
            skew=skew,
            kurtosis=kurtosis,
        )
        if sharpe is not None
        else None
    )

    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for r in clean:
        equity *= 1.0 + r
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)

    return {
        "status": "COMPLETE",
        "observations": len(clean),
        "trials": int(trials),
        "periods_per_year": float(periods_per_year),
        "mean_return": mu,
        "std_return": std,
        "sharpe": sharpe,
        "skew": skew,
        "kurtosis": kurtosis,
        "deflated_sharpe_probability": dsr,
        "max_drawdown": max_drawdown,
        "compound_return": equity - 1.0,
    }


def search_ledger_summary(records: Sequence[SearchRecord]) -> dict:
    if not records:
        return {"trials": 0, "selected_count": 0, "best_metric": None}
    return {
        "trials": len(records),
        "selected_count": sum(r.selected for r in records),
        "best_metric": max(r.metric for r in records),
        "unique_candidates": len({r.candidate_id for r in records}),
    }
