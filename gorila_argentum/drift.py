from __future__ import annotations

import math
from typing import Iterable, Sequence


def _clean(values: Iterable[float]) -> list[float]:
    out = []
    for value in values:
        x = float(value)
        if math.isfinite(x):
            out.append(x)
    return out


def _quantile(sorted_values: Sequence[float], q: float) -> float:
    if not sorted_values:
        raise ValueError("empty_reference")
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def _reference_edges(reference: Sequence[float], bins: int) -> list[float]:
    values = sorted(reference)
    if len(values) < 2:
        raise ValueError("insufficient_reference_data")
    edges = [_quantile(values, i / bins) for i in range(1, bins)]
    unique = sorted(set(edges))
    if len(unique) < 1:
        center = values[len(values) // 2]
        span = max(abs(center) * 1e-6, 1e-6)
        return [center - span, center + span]
    return unique


def _bucket(value: float, edges: Sequence[float]) -> int:
    for i, edge in enumerate(edges):
        if value <= edge:
            return i
    return len(edges)


def _proportions(values: Sequence[float], edges: Sequence[float], epsilon: float = 1e-6) -> list[float]:
    counts = [0] * (len(edges) + 1)
    for value in values:
        counts[_bucket(value, edges)] += 1
    n = len(values)
    return [(count + epsilon) / (n + epsilon * len(counts)) for count in counts]


def population_stability_index(reference: Iterable[float], current: Iterable[float], bins: int = 10) -> float:
    ref = _clean(reference)
    cur = _clean(current)
    if len(ref) < max(20, bins * 2) or len(cur) < max(10, bins):
        raise ValueError("insufficient_data")
    edges = _reference_edges(ref, bins)
    p = _proportions(ref, edges)
    q = _proportions(cur, edges)
    return sum((b - a) * math.log(b / a) for a, b in zip(p, q))


def ks_statistic(reference: Iterable[float], current: Iterable[float]) -> float:
    ref = sorted(_clean(reference))
    cur = sorted(_clean(current))
    if len(ref) < 2 or len(cur) < 2:
        raise ValueError("insufficient_data")
    i = j = 0
    d = 0.0
    while i < len(ref) and j < len(cur):
        x = ref[i]
        y = cur[j]
        if x <= y:
            while i < len(ref) and ref[i] == x:
                i += 1
        if y <= x:
            while j < len(cur) and cur[j] == y:
                j += 1
        d = max(d, abs(i / len(ref) - j / len(cur)))
    return d


def standardized_mean_shift(reference: Iterable[float], current: Iterable[float]) -> float:
    ref = _clean(reference)
    cur = _clean(current)
    if len(ref) < 2 or len(cur) < 2:
        raise ValueError("insufficient_data")
    mean_ref = sum(ref) / len(ref)
    mean_cur = sum(cur) / len(cur)
    var_ref = sum((x - mean_ref) ** 2 for x in ref) / max(1, len(ref) - 1)
    var_cur = sum((x - mean_cur) ** 2 for x in cur) / max(1, len(cur) - 1)
    se = math.sqrt(var_ref / len(ref) + var_cur / len(cur))
    if se <= 1e-12:
        return 0.0 if abs(mean_cur - mean_ref) <= 1e-12 else math.inf
    return (mean_cur - mean_ref) / se


def std_ratio(reference: Iterable[float], current: Iterable[float]) -> float:
    ref = _clean(reference)
    cur = _clean(current)
    if len(ref) < 2 or len(cur) < 2:
        raise ValueError("insufficient_data")
    mean_ref = sum(ref) / len(ref)
    mean_cur = sum(cur) / len(cur)
    sd_ref = math.sqrt(sum((x - mean_ref) ** 2 for x in ref) / max(1, len(ref) - 1))
    sd_cur = math.sqrt(sum((x - mean_cur) ** 2 for x in cur) / max(1, len(cur) - 1))
    if sd_ref <= 1e-12:
        return math.inf if sd_cur > 1e-12 else 1.0
    return sd_cur / sd_ref


def classify_drift(psi: float, ks: float, mean_shift_z: float) -> str:
    if psi >= 0.25 or ks >= 0.20 or abs(mean_shift_z) >= 3.0:
        return "ALERT"
    if psi >= 0.10 or ks >= 0.10 or abs(mean_shift_z) >= 2.0:
        return "WARN"
    return "OK"


def evaluate_drift(
    reference: Sequence[float],
    current: Sequence[float],
    *,
    bins: int = 10,
) -> dict:
    ref = _clean(reference)
    cur = _clean(current)
    if len(ref) < max(20, bins * 2) or len(cur) < max(10, bins):
        return {
            "status": "INSUFFICIENT_DATA",
            "reference_n": len(ref),
            "current_n": len(cur),
        }

    psi = population_stability_index(ref, cur, bins=bins)
    ks = ks_statistic(ref, cur)
    z = standardized_mean_shift(ref, cur)
    ratio = std_ratio(ref, cur)

    return {
        "status": classify_drift(psi, ks, z),
        "reference_n": len(ref),
        "current_n": len(cur),
        "psi": psi,
        "ks": ks,
        "mean_shift_z": z,
        "std_ratio": ratio,
        "reference_mean": sum(ref) / len(ref),
        "current_mean": sum(cur) / len(cur),
    }


def rolling_drift(values: Sequence[float], current_size: int = 30, reference_size: int = 90) -> dict:
    clean = _clean(values)
    if len(clean) < reference_size + current_size:
        return {
            "status": "INSUFFICIENT_DATA",
            "total_n": len(clean),
            "reference_required": reference_size,
            "current_required": current_size,
        }
    split = len(clean) - current_size
    reference = clean[max(0, split - reference_size):split]
    current = clean[split:]
    result = evaluate_drift(reference, current)
    result.update(
        {
            "reference_window": reference_size,
            "current_window": current_size,
            "total_n": len(clean),
        }
    )
    return result
