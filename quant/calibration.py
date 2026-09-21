"""PMSF-X Nano — probability calibration for OOS forecasts."""
from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class CalibrationResult:
    method: str
    calibrated_probability: float
    brier_before: float
    brier_after: float
    log_loss_before: float
    log_loss_after: float
    sample_count: int


def _clip(p: float) -> float:
    return max(1e-6, min(1.0 - 1e-6, float(p)))


def _logloss(p: float, y: int) -> float:
    p = _clip(p)
    return -(y * math.log(p) + (1-y) * math.log(1-p))


def fit_platt(probabilities: list[float], labels: list[int]) -> tuple[float, float]:
    """Fit a small logistic calibrator on logit(probability)."""
    if len(probabilities) != len(labels) or len(labels) < 30 or len(set(labels)) < 2:
        raise ValueError("INSUFFICIENT_CALIBRATION_DATA")
    a, b = 1.0, 0.0
    for _ in range(120):
        ga = gb = 0.0
        for p, y in zip(probabilities, labels):
            p = _clip(p)
            z = math.log(p / (1-p))
            q = 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, a*z+b))))
            e = q-y
            ga += e*z
            gb += e
        n=len(labels)
        a -= 0.03*ga/n
        b -= 0.03*gb/n
    return a, b


def calibrate_platt(probability: float, a: float, b: float) -> float:
    p=_clip(probability)
    z=math.log(p/(1-p))
    return 1.0/(1.0+math.exp(-max(-30.0,min(30.0,a*z+b))))


def metrics(probabilities: list[float], labels: list[int]) -> tuple[float,float]:
    n=max(1,len(labels))
    brier=sum((_clip(p)-y)**2 for p,y in zip(probabilities,labels))/n
    ll=sum(_logloss(p,y) for p,y in zip(probabilities,labels))/n
    return brier,ll
