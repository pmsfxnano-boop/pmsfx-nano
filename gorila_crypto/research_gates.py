"""Multiple-testing, DSR and CSCV/PBO research gates.

These functions never manufacture a PASS when the required statistical object
cannot be estimated. An un-estimable gate is an explicit block.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class PBOResult:
    status: str
    pbo: float | None
    comparisons: int
    selected_strategy_indices: tuple[int, ...] = ()


@dataclass(frozen=True)
class DSRResult:
    status: str
    adjusted_p_value: float | None
    observed_sharpe: float | None
    expected_max_null_sharpe: float | None
    n_observations: int
    n_trials: int


def holm_bonferroni(p_values: Sequence[float], alpha: float = 0.05) -> tuple[float, ...]:
    """Return Holm-adjusted p-values in original order."""
    if not p_values:
        return ()
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0,1)")
    indexed = sorted(enumerate(float(p) for p in p_values), key=lambda item: item[1])
    adjusted = [0.0] * len(indexed)
    running = 0.0
    m = len(indexed)
    for rank, (original_index, p_value) in enumerate(indexed):
        value = min(1.0, (m - rank) * max(0.0, min(1.0, p_value)))
        running = max(running, value)
        adjusted[original_index] = running
    return tuple(adjusted)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_ppf(probability: float) -> float:
    if not 0.0 < probability < 1.0:
        raise ValueError("probability must be strictly between 0 and 1")
    # Peter John Acklam's rational approximation.
    a = (-39.6968302866538, 220.946098424521, -275.928510446969,
         138.357751867269, -30.6647980661472, 2.50662827745924)
    b = (-54.4760987982241, 161.585836858041, -155.698979859887,
         66.8013118877197, -13.2806815528857)
    c = (-0.00778489400243029, -0.322396458041136, -2.40075827716184,
         -2.54973253934373, 4.37466414146497, 2.93816398269878)
    d = (0.00778469570904146, 0.32246712907004, 2.445134137143,
         3.75440866190742)
    plow = 0.02425
    phigh = 1 - plow
    if probability < plow:
        q = math.sqrt(-2 * math.log(probability))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
                ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1))
    if probability > phigh:
        q = math.sqrt(-2 * math.log(1 - probability))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
                 (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = probability - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q /
            (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + b[5]) * r + 1))


def deflated_sharpe_p_value(
    returns: Sequence[float],
    *,
    n_trials: int,
) -> DSRResult:
    n = len(returns)
    if n < 30:
        return DSRResult("INSUFFICIENT_DATA", None, None, None, n, n_trials)
    if n_trials < 2:
        return DSRResult("INSUFFICIENT_TRIAL_FAMILY", None, None, None, n, n_trials)
    mean = sum(float(x) for x in returns) / n
    variance = sum((float(x) - mean) ** 2 for x in returns) / max(1, n - 1)
    if variance <= 1e-18:
        return DSRResult("DEGENERATE_RETURNS", None, None, None, n, n_trials)
    std = math.sqrt(variance)
    sharpe = mean / std
    skew = sum(((float(x) - mean) / std) ** 3 for x in returns) / n
    kurtosis_excess = sum(((float(x) - mean) / std) ** 4 for x in returns) / n - 3.0
    expected_max = _normal_ppf(1.0 - 1.0 / float(n_trials))
    variance_sr = (
        1.0 - skew * sharpe + ((kurtosis_excess + 2.0) / 4.0) * sharpe * sharpe
    ) / n
    se = math.sqrt(max(1e-18, variance_sr))
    z = (sharpe - expected_max) / se
    p_value = 1.0 - _normal_cdf(z)
    return DSRResult(
        "ESTIMATED",
        float(max(0.0, min(1.0, p_value))),
        float(sharpe),
        float(expected_max),
        n,
        n_trials,
    )


def combinatorial_pbo(
    strategy_returns: Sequence[Sequence[float]],
    *,
    groups: int = 6,
    test_groups: int = 3,
) -> PBOResult:
    """Compute PBO from symmetric combinatorial purged groups.

    Each candidate supplies one return per chronological observation. The best
    in-sample candidate is selected on each half-split, then its out-of-sample
    rank is measured. PBO is the fraction of splits where the selected candidate
    lands below the median out-of-sample rank.
    """
    candidates = [list(map(float, row)) for row in strategy_returns]
    n_candidates = len(candidates)
    if n_candidates < 2:
        return PBOResult("INSUFFICIENT_CANDIDATES", None, 0)
    n_obs = len(candidates[0])
    if n_obs < groups or any(len(row) != n_obs for row in candidates):
        return PBOResult("INSUFFICIENT_DATA", None, 0)
    if groups < 4 or groups % 2 or test_groups != groups // 2:
        raise ValueError("groups must be even and test_groups must be half of groups")
    block = n_obs // groups
    if block < 1:
        return PBOResult("INSUFFICIENT_DATA", None, 0)

    splits = list(itertools.combinations(range(groups), test_groups))
    selected: list[int] = []
    failures = 0
    for test_group_set in splits:
        test_set = set(test_group_set)
        train_indices = [
            i for g in range(groups) if g not in test_set
            for i in range(g * block, (g + 1) * block)
        ]
        test_indices = [
            i for g in test_group_set
            for i in range(g * block, (g + 1) * block)
        ]
        train_scores = [
            sum(candidates[c][i] for i in train_indices) / len(train_indices)
            for c in range(n_candidates)
        ]
        best = max(range(n_candidates), key=lambda c: (train_scores[c], -c))
        selected.append(best)
        oos_scores = [
            sum(candidates[c][i] for i in test_indices) / len(test_indices)
            for c in range(n_candidates)
        ]
        rank = sorted(range(n_candidates), key=lambda c: (oos_scores[c], c)).index(best)
        percentile = rank / max(1, n_candidates - 1)
        if percentile < 0.5:
            failures += 1
    pbo = failures / len(splits)
    return PBOResult("ESTIMATED", float(pbo), len(splits), tuple(selected))