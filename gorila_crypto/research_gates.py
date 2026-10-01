"""Multiple-testing, DSR and CSCV/PBO research gates.

These functions never manufacture a PASS when the required statistical object
cannot be estimated. An un-estimable gate is an explicit block.
"""

from __future__ import annotations

import itertools
import math
from statistics import NormalDist
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
    return float(NormalDist().inv_cdf(probability))

def _effective_observation_count(returns: Sequence[float]) -> float:
    values = [float(x) for x in returns]
    n = len(values)
    if n < 3:
        return float(n)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    if variance <= 1e-18:
        return float(n)
    # Bartlett-weighted autocorrelation adjustment. This makes the DSR gate
    # conservative when returns are serially dependent rather than pretending
    # every event is an independent draw.
    max_lag = min(50, n // 4)
    rho_sum = 0.0
    for lag in range(1, max_lag + 1):
        numerator = sum(
            (values[t] - mean) * (values[t - lag] - mean)
            for t in range(lag, n)
        )
        rho = numerator / max(1, (n - lag) * variance)
        weight = 1.0 - lag / (max_lag + 1.0)
        rho_sum += weight * rho
    variance_inflation = max(1.0, 1.0 + 2.0 * rho_sum)
    return float(max(1.0, min(float(n), n / variance_inflation)))


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

    n_eff = _effective_observation_count(returns)
    se_sharpe = math.sqrt(
        max(
            1e-18,
            (
                1.0
                - skew * sharpe
                + ((kurtosis_excess + 2.0) / 4.0) * sharpe * sharpe
            )
            / n_eff,
        )
    )

    # Under the null, the maximum of N approximately-standardized Sharpe
    # estimates has an expected level proportional to 1/sqrt(T_eff), not 1.
    z_max = _normal_ppf(1.0 - 1.0 / float(n_trials))
    expected_max = z_max / math.sqrt(n_eff)

    z = (sharpe - expected_max) / se_sharpe
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
    if n_obs % groups != 0:
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