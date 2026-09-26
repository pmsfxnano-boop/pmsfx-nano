"""Advanced research signal layer for Gorila Argentum.

This module deliberately separates *prediction* from *evidence quality*.
It never upgrades a research model into a trading signal just because its
raw probability is extreme. The output combines point-in-time forecast data,
temporal price structure, validation/calibration evidence, data freshness,
and drift controls into an auditable research score.

The score is not a guarantee of future performance. It is an evidence and
signal-strength index whose hard gates remain tied to the existing research
controls.
"""

from __future__ import annotations

from math import log, sqrt
from typing import Any


CORE_SYMBOLS = ("GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU")
DEFAULT_SIGNAL_THRESHOLD = 62.0


def _finite(value: Any) -> float | None:
    try:
        if value is None:
            return None
        x = float(value)
        return x if x == x and abs(x) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _pct_change(series: list[float], lag: int) -> float | None:
    if len(series) <= lag or series[-1] <= 0 or series[-1 - lag] <= 0:
        return None
    return series[-1] / series[-1 - lag] - 1.0


def _volatility(series: list[float], window: int) -> float | None:
    if len(series) <= window:
        return None
    sample = series[-window:]
    returns = [
        log(sample[i] / sample[i - 1])
        for i in range(1, len(sample))
        if sample[i - 1] > 0 and sample[i] > 0
    ]
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return sqrt(max(0.0, var))


def _trend_z(series: list[float], window: int = 20) -> float | None:
    if len(series) < window + 1:
        return None
    sample = series[-(window + 1):]
    returns = [
        (sample[i] / sample[i - 1]) - 1.0
        for i in range(1, len(sample))
        if sample[i - 1] > 0
    ]
    if len(returns) < 5:
        return None
    mean = sum(returns) / len(returns)
    tail = returns[-5:]
    tail_mean = sum(tail) / len(tail)
    variance = sum((x - mean) ** 2 for x in returns) / max(1, len(returns) - 1)
    sd = sqrt(max(variance, 1e-12))
    return (tail_mean - mean) / sd


def _consensus_strength(forecast: dict[str, Any]) -> float:
    consensus = forecast.get("horizon_consensus") or {}
    value = _finite(consensus.get("confluence_index"))
    if value is not None:
        return _clamp(value)
    mh = forecast.get("multi_horizon") or {}
    horizons = mh.get("horizons") or {}
    probabilities: list[float] = []
    for row in horizons.values():
        p = _finite((row or {}).get("latest_forecast", {}).get("p_up"))
        if p is not None:
            probabilities.append(p)
    if len(probabilities) < 2:
        return 0.0
    center = sum(probabilities) / len(probabilities)
    dispersion = sum(abs(p - center) for p in probabilities) / len(probabilities)
    return _clamp(1.0 - 4.0 * dispersion)


def _freshness_factor(age_seconds: Any) -> float:
    age = _finite(age_seconds)
    if age is None:
        return 0.0
    if age <= 10:
        return 1.0
    if age <= 30:
        return 0.9
    if age <= 120:
        return 0.7
    if age <= 600:
        return 0.35
    return 0.0


def _validation_factor(evaluation: dict[str, Any]) -> float:
    if evaluation.get("validated") is True:
        return 1.0
    accuracy = _finite(evaluation.get("accuracy"))
    if accuracy is None:
        return 0.0
    return _clamp((accuracy - 0.50) / 0.10)


def _data_grade(age_seconds: Any) -> str:
    age = _finite(age_seconds)
    if age is None:
        return "UNKNOWN"
    if age <= 15:
        return "A"
    if age <= 60:
        return "B"
    if age <= 300:
        return "C"
    return "D"


def build_signal(
    *,
    symbol: str,
    state: dict[str, Any],
    price_series: list[tuple[Any, float]] | None = None,
    drift: dict[str, Any] | None = None,
    shadow_summary: dict[str, Any] | None = None,
    threshold: float = DEFAULT_SIGNAL_THRESHOLD,
) -> dict[str, Any]:
    forecast = state.get("forecast") or {}
    evaluation = state.get("evaluation") or {}
    p_up = _finite(forecast.get("raw_probability_up"))
    if p_up is None:
        p_up = _finite(state.get("p_up"))

    p_down = 1.0 - p_up if p_up is not None else None
    confidence = _finite(forecast.get("confidence_raw"))
    age_seconds = _finite((state.get("engine_freshness") or {}).get("age_seconds"))
    validated = bool(forecast.get("validated") or evaluation.get("validated"))
    brier_skill = _finite(evaluation.get("brier_skill"))

    closes = [_finite(v) for _, v in (price_series or [])]
    closes = [v for v in closes if v is not None and v > 0]

    momentum = {
        "return_1": _pct_change(closes, 1),
        "return_3": _pct_change(closes, 3),
        "return_5": _pct_change(closes, 5),
        "return_10": _pct_change(closes, 10),
        "volatility": _volatility(closes, min(20, max(5, len(closes) - 1))),
        "trend_z": _trend_z(closes, min(20, max(8, len(closes) - 1))) if len(closes) >= 9 else None,
    }

    edge = abs(p_up - 0.5) * 2.0 if p_up is not None else 0.0
    consensus = _consensus_strength(forecast)
    freshness = _freshness_factor(age_seconds)
    validation = _validation_factor(evaluation)

    shadow_acc = _finite((shadow_summary or {}).get("accuracy"))
    shadow_support = 0.5 if shadow_acc is None else _clamp((shadow_acc - 0.50) / 0.15)

    drift_status = str((drift or {}).get("status") or "UNKNOWN").upper()
    drift_psi = _finite((drift or {}).get("psi"))
    drift_penalty = 0.0
    if drift_status == "ALERT":
        drift_penalty = 0.35
    elif drift_status == "WATCH":
        drift_penalty = 0.15
    elif drift_psi is not None and drift_psi > 10:
        drift_penalty = 0.20

    signal_score = 100.0 * (
        0.30 * edge
        + 0.18 * (confidence if confidence is not None else edge)
        + 0.18 * consensus
        + 0.16 * validation
        + 0.10 * freshness
        + 0.08 * shadow_support
    )
    signal_score *= (1.0 - drift_penalty)
    signal_score = round(_clamp(signal_score, 0.0, 1.0) * 100.0, 1)

    reasons: list[str] = []
    risk_flags: list[str] = []

    if not state.get("forecast"):
        risk_flags.append("NO_FORECAST")
    if not validated:
        risk_flags.append("MODEL_NOT_VALIDATED")
        reasons.append("research model has not cleared its OOS gate")
    if freshness < 0.7:
        risk_flags.append("DATA_NOT_FRESH")
    if brier_skill is not None and brier_skill <= 0:
        risk_flags.append("NEGATIVE_BRIER_SKILL")
    if drift_status == "ALERT":
        risk_flags.append("DRIFT_ALERT")
        reasons.append("distribution drift is above the operational threshold")
    if momentum["volatility"] is None:
        risk_flags.append("LIMITED_PRICE_HISTORY")

    raw_direction = str(forecast.get("direction") or "NEUTRAL").upper()
    if raw_direction not in {"UP", "DOWN", "NEUTRAL"}:
        raw_direction = "NEUTRAL"

    actionable = (
        validated
        and freshness >= 0.7
        and drift_status not in {"ALERT", "HALTED"}
        and signal_score >= threshold
        and raw_direction != "NEUTRAL"
        and confidence is not None
        and confidence >= 0.20
    )

    if actionable:
        status = "ARMED_RESEARCH"
        signal = raw_direction
    elif signal_score >= 50:
        status = "WATCH"
        signal = raw_direction if raw_direction != "NEUTRAL" else "NEUTRAL"
    else:
        status = "BLOCKED"
        signal = "NEUTRAL"

    if p_up is None:
        signal = "NEUTRAL"
        status = "NO_DATA"

    return {
        "symbol": str(symbol).upper(),
        "signal": signal,
        "status": status,
        "actionable": actionable,
        "signal_score": signal_score,
        "threshold": threshold,
        "probability": {
            "up": round(p_up, 4) if p_up is not None else None,
            "down": round(p_down, 4) if p_down is not None else None,
            "edge": round(edge, 4) if p_up is not None else None,
            "confidence": round(confidence, 4) if confidence is not None else None,
        },
        "validation": {
            "validated": validated,
            "accuracy": evaluation.get("accuracy"),
            "brier": evaluation.get("brier"),
            "brier_skill": brier_skill,
            "log_loss": evaluation.get("log_loss"),
            "oos_count": evaluation.get("oos_count") or evaluation.get("sample_count"),
        },
        "market": {
            "last": state.get("last"),
            "bid": state.get("bid"),
            "ask": state.get("ask"),
            "spread_bps": state.get("spread_bps"),
            "quote_timestamp": state.get("quote_timestamp"),
            "engine_age_seconds": age_seconds,
            "data_grade": _data_grade(age_seconds),
            "source": state.get("data_source") or state.get("engine_source"),
        },
        "structure": momentum,
        "multi_horizon": {
            "consensus_strength": round(consensus, 4),
            "horizons": (forecast.get("multi_horizon") or {}).get("horizons") or {},
        },
        "drift": {
            "status": drift_status,
            "psi": drift_psi,
            "ks": (drift or {}).get("ks"),
            "mean_shift_z": (drift or {}).get("mean_shift_z"),
            "std_ratio": (drift or {}).get("std_ratio"),
        },
        "shadow": {
            "accuracy": shadow_acc,
            "settled": (shadow_summary or {}).get("settled"),
            "brier": (shadow_summary or {}).get("mean_brier"),
        },
        "risk_flags": risk_flags,
        "reasons": reasons,
        "research_only": True,
        "no_execution_authority": True,
    }


def build_matrix(
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked = sorted(
        items,
        key=lambda row: float(row.get("signal_score") or 0.0),
        reverse=True,
    )
    counts = {
        "total": len(ranked),
        "armed_research": sum(r.get("status") == "ARMED_RESEARCH" for r in ranked),
        "watch": sum(r.get("status") == "WATCH" for r in ranked),
        "blocked": sum(r.get("status") == "BLOCKED" for r in ranked),
        "no_data": sum(r.get("status") == "NO_DATA" for r in ranked),
    }
    return {
        "version": "argentum-signal-v1",
        "universe": list(CORE_SYMBOLS),
        "items": ranked,
        "counts": counts,
        "research_only": True,
        "method": {
            "components": [
                "point-in-time forecast edge",
                "forecast confidence",
                "multi-horizon consensus",
                "OOS validation evidence",
                "data freshness",
                "shadow outcome support",
                "drift penalty",
            ],
            "hard_gates": [
                "validated OOS model",
                "fresh market state",
                "no drift alert",
                "signal score above threshold",
                "minimum confidence",
            ],
        },
    }
