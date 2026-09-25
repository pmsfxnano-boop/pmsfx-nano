"""PMSF-X Nano — formal CPCV/PBO/DSR accounting (research-only).

These functions operate on OOS/CV outputs only. They do not promote a model
and never turn a research result into a live trading authorization.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import exp, isfinite, log, sqrt
from statistics import NormalDist
from typing import Sequence


_NORMAL = NormalDist()
_EULER_GAMMA = 0.5772156649015329


@dataclass(frozen=True)
class SearchRecord:
    run_id: str
    candidate_id: str
    metric: float
    observations: int
    selected: bool = False


def _expected_max_normal_score(trials: int) -> float:
    """Gumbel/Euler approximation to E[max(Z_1..Z_N)] for Gaussian trials."""
    n = max(2, int(trials))
    z1 = _NORMAL.inv_cdf(1.0 - 1.0 / n)
    z2 = _NORMAL.inv_cdf(1.0 - 1.0 / (n * exp(1.0)))
    return (1.0 - _EULER_GAMMA) * z1 + _EULER_GAMMA * z2


def combinatorial_splits(
    n_groups: int, test_groups: int
) -> list[tuple[tuple[int, ...], tuple[int, ...]]]:
    """Enumerate non-overlapping CPCV train/test group combinations."""
    if n_groups <= 1 or test_groups <= 0 or test_groups >= n_groups:
        return []
    groups = tuple(range(int(n_groups)))
    out: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
    for test in combinations(groups, int(test_groups)):
        ts = set(test)
        train = tuple(g for g in groups if g not in ts)
        out.append((train, tuple(test)))
    return out


def pbo_from_train_test(
    train_matrix: Sequence[Sequence[float]],
    test_matrix: Sequence[Sequence[float]],
) -> dict:
    """Formal CSCV/PBO from candidate-by-split train/test performance.

    Each column is a combinatorial split. The train winner is located in the
    test ranking; its percentile rank is transformed into a logit. PBO is the
    fraction of splits whose logit is negative (below the test median).
    """
    train = [list(map(float, row)) for row in train_matrix]
    test = [list(map(float, row)) for row in test_matrix]
    if len(train) != len(test) or len(train) < 2:
        raise ValueError("train/test matrices must have same candidate count >= 2")

    cols = min((len(row) for row in train), default=0)
    cols = min(cols, min((len(row) for row in test), default=0))
    if cols < 1:
        raise ValueError("train/test matrices need at least one split")

    records: list[dict] = []
    negative = 0
    candidates = len(train)
    for split in range(cols):
        train_scores = [train[i][split] for i in range(candidates)]
        test_scores = [test[i][split] for i in range(candidates)]
        winner = max(range(candidates), key=lambda i: train_scores[i])
        ordered = sorted(range(candidates), key=lambda i: test_scores[i])
        rank = ordered.index(winner) + 1
        percentile = (rank - 0.5) / candidates
        percentile = min(1.0 - 1e-9, max(1e-9, percentile))
        logit = log(percentile / (1.0 - percentile))
        below_median = logit < 0.0
        negative += int(below_median)
        records.append({
            "split": split,
            "selected_candidate_index": winner,
            "selected_train_score": train_scores[winner],
            "selected_test_score": test_scores[winner],
            "test_percentile": percentile,
            "logit": logit,
            "below_test_median": below_median,
        })

    return {
        "status": "FORMAL_CSCV_PBO",
        "candidate_count": candidates,
        "split_count": cols,
        "pbo": negative / cols,
        "negative_logit_splits": negative,
        "splits": records,
    }


def probability_of_backtest_overfitting(
    performance_matrix: Sequence[Sequence[float]],
) -> float | None:
    """Backward-compatible wrapper for paired train/test aggregate columns.

    For formal work use pbo_from_train_test() directly with explicit matrices.
    """
    rows = [list(r) for r in performance_matrix if r]
    if len(rows) < 2:
        return None
    m = min(len(r) for r in rows)
    pairs = m // 2
    if pairs < 1:
        return None
    train = [[r[2 * j] for j in range(pairs)] for r in rows]
    test = [[r[2 * j + 1] for j in range(pairs)] for r in rows]
    return pbo_from_train_test(train, test)["pbo"]


def deflated_sharpe_ratio(
    sharpe: float,
    trials: int,
    observations: int,
    skew: float = 0.0,
    kurtosis: float = 3.0,
) -> float | None:
    """Probability that Sharpe exceeds the multiple-testing SR* threshold.

    observations must be independent return observations (normally daily or
    trade-level), never CV folds.
    """
    if observations < 2 or trials < 1 or not isfinite(sharpe):
        return None
    skew = skew if isfinite(skew) else 0.0
    kurtosis = kurtosis if isfinite(kurtosis) else 3.0
    sr_star = _expected_max_normal_score(trials)
    variance_factor = (
        1.0
        - skew * sharpe
        + ((kurtosis - 1.0) / 4.0) * sharpe * sharpe
    )
    se = sqrt(max(1e-12, variance_factor / max(1.0, observations - 1.0)))
    z = (sharpe - sr_star) / se
    return _NORMAL.cdf(z)


def audit_returns(
    returns: Sequence[float],
    *,
    trials: int,
    periods_per_year: float | None = None,
) -> dict:
    """Audit a raw return series with Sharpe, moments and DSR probability."""
    values = [float(x) for x in returns if isfinite(float(x))]
    n = len(values)
    if n < 2:
        return {"status": "INSUFFICIENT", "observations": n, "trials": int(trials)}

    mean_return = sum(values) / n
    variance = sum((x - mean_return) ** 2 for x in values) / (n - 1)
    std_return = sqrt(max(0.0, variance))
    if std_return == 0.0:
        sharpe = 0.0
        skew = 0.0
        kurtosis = 3.0
    else:
        scale = sqrt(float(periods_per_year)) if periods_per_year else 1.0
        sharpe = (mean_return / std_return) * scale
        m3 = sum((x - mean_return) ** 3 for x in values) / n
        m4 = sum((x - mean_return) ** 4 for x in values) / n
        skew = m3 / (sqrt(variance) ** 3)
        kurtosis = m4 / (variance * variance)

    return {
        "status": "COMPLETE",
        "observations": n,
        "trials": int(trials),
        "mean_return": mean_return,
        "std_return": std_return,
        "sharpe": sharpe,
        "skew": skew,
        "kurtosis_pearson": kurtosis,
        "dsr_probability": deflated_sharpe_ratio(
            sharpe, trials, n, skew, kurtosis
        ),
        "periods_per_year": periods_per_year,
    }


def search_ledger_summary(records: Sequence[SearchRecord]) -> dict:
    """Summarize the searchable hypothesis ledger used by the validation gate."""
    if not records:
        return {
            "trials": 0,
            "selected_count": 0,
            "best_metric": None,
            "unique_candidates": 0,
            "candidate_ids": [],
        }
    return {
        "trials": len(records),
        "selected_count": sum(r.selected for r in records),
        "best_metric": max(r.metric for r in records),
        "unique_candidates": len({r.candidate_id for r in records}),
        "candidate_ids": sorted({r.candidate_id for r in records}),
    }
