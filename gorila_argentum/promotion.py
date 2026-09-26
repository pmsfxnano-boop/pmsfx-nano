from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PromotionCriteria:
    min_oos_accuracy: float = 0.55
    min_brier_skill: float = 0.0
    min_rank_ic: float = 0.0
    min_cpcv_mean_return_pct: float = 0.0
    max_pbo: float = 0.05
    min_dsr_mean: float = 0.0
    min_execution_delta: float = 0.0


REQUIRED_FIELDS = (
    "oos_accuracy",
    "brier_skill",
    "rank_ic",
    "cpcv_pbo",
    "cpcv_dsr_mean",
    "cpcv_mean_return_pct",
    "execution_delta_mean",
    "stress_pass",
    "data_health",
    "point_in_time",
)


def evaluate_promotion(
    evidence: dict[str, Any],
    *,
    criteria: PromotionCriteria | None = None,
) -> dict[str, Any]:
    criteria = criteria or PromotionCriteria()
    reasons: list[str] = []
    checks: dict[str, Any] = {}

    missing = [field for field in REQUIRED_FIELDS if field not in evidence]
    checks["evidence_complete"] = not missing
    checks["missing_fields"] = missing
    if missing:
        reasons.append("MISSING_REQUIRED_EVIDENCE")

    if evidence.get("data_health") is not True:
        reasons.append("DATA_HEALTH_FAILED")
    if evidence.get("point_in_time") is not True:
        reasons.append("POINT_IN_TIME_VALIDATION_FAILED")
    if evidence.get("stress_pass") is not True:
        reasons.append("STRESS_GATE_FAILED")

    numeric = (
        ("oos_accuracy", evidence.get("oos_accuracy"), criteria.min_oos_accuracy, "MIN_OOS_ACCURACY"),
        ("brier_skill", evidence.get("brier_skill"), criteria.min_brier_skill, "MIN_BRIER_SKILL"),
        ("rank_ic", evidence.get("rank_ic"), criteria.min_rank_ic, "MIN_RANK_IC"),
        ("cpcv_mean_return_pct", evidence.get("cpcv_mean_return_pct"), criteria.min_cpcv_mean_return_pct, "MIN_CPCV_RETURN"),
        ("cpcv_dsr_mean", evidence.get("cpcv_dsr_mean"), criteria.min_dsr_mean, "MIN_DSR"),
        ("execution_delta_mean", evidence.get("execution_delta_mean"), criteria.min_execution_delta, "MIN_EXECUTION_DELTA"),
    )
    for key, value, threshold, reason in numeric:
        if value is None:
            checks[key] = False
            reasons.append(f"{reason}_MISSING")
        else:
            # DSR is an evidence-strength statistic: exactly zero means no
            # positive deflated-Sharpe evidence and therefore must not pass.
            ok = float(value) > float(threshold) if key == "cpcv_dsr_mean" else float(value) >= float(threshold)
            checks[key] = {"value": float(value), "threshold": float(threshold), "pass": ok}
            if not ok:
                reasons.append(reason)

    pbo = evidence.get("cpcv_pbo")
    if pbo is None:
        checks["cpcv_pbo"] = False
        reasons.append("PBO_MISSING")
    else:
        ok = float(pbo) <= criteria.max_pbo
        checks["cpcv_pbo"] = {"value": float(pbo), "threshold": criteria.max_pbo, "pass": ok}
        if not ok:
            reasons.append("MAX_PBO_EXCEEDED")

    eligible = not reasons
    return {
        "status": "ELIGIBLE" if eligible else "BLOCKED",
        "eligible": eligible,
        "automatic_promotion": False,
        "reasons": reasons,
        "checks": checks,
        "criteria": {
            "min_oos_accuracy": criteria.min_oos_accuracy,
            "min_brier_skill": criteria.min_brier_skill,
            "min_rank_ic": criteria.min_rank_ic,
            "min_cpcv_mean_return_pct": criteria.min_cpcv_mean_return_pct,
            "max_pbo": criteria.max_pbo,
            "min_dsr_mean": criteria.min_dsr_mean,
            "min_execution_delta": criteria.min_execution_delta,
        },
        "evidence": evidence,
    }


CURRENT_BATCH10_EVIDENCE = {
    "oos_accuracy": 0.4998941798941799,
    "brier_skill": None,
    "rank_ic": -0.014512471655328799,
    "cpcv_pbo": 0.0,
    "cpcv_dsr_mean": 0.0,
    "cpcv_mean_return_pct": -16.62,
    "execution_delta_mean": -0.054897,    "cpcv_snapshot_sha256": "4f5274af78f6de30d8854ae80d6a9e62b2e6cfcd9eeea31c76ab2e8d31949946",
    "stress_pass": False,
    "data_health": True,
    "point_in_time": True,
    "source": "Batch 10 V2 archived research evidence",
    "source_digest": "sha256:afebda5082e1ab512480183b9db7927e733f12b6c8f2ade1bd89e3fbe8794545",
}


def evaluate_live_promotion(
    store,
    *,
    symbols=("GGAL","BMA","YPFD","PAMP","TGSU2","CEPU"),
    horizons=(5,10),
) -> dict[str, Any]:
    """Evaluate promotion only from the freshest persisted evidence matrix.

    Historical Batch-10 evidence is deliberately not used as a live gate.
    A production candidate must have fresh PIT/OOS evidence for every required
    symbol/horizon cell, and every cell must satisfy the same conservative gate.
    """
    from .evidence import latest_evidence

    rows = latest_evidence(store, limit=500)
    required = {(s, int(h)) for s in symbols for h in horizons}
    latest = {}
    for row in rows:
        key = (str(row.get("symbol","")).upper(), int(row.get("horizon_days",0)))
        if key not in latest:
            latest[key] = row

    missing = sorted(required - set(latest))
    if missing:
        return {
            "status": "BLOCKED",
            "eligible": False,
            "automatic_promotion": False,
            "reasons": ["FRESH_EVIDENCE_INCOMPLETE"],
            "missing_cells": [{"symbol": s, "horizon_days": h} for s, h in missing],
            "cells": list(latest.values()),
            "source": "persisted_research_evidence",
        }

    failed = []
    for key in sorted(required):
        row = latest[key]
        if row.get("validation_status") != "VALIDATED":
            failed.append({
                "symbol": key[0],
                "horizon_days": key[1],
                "status": row.get("validation_status"),
                "reasons": row.get("validation_reasons") or [],
            })

    dataset_hashes = {str(latest[k].get("dataset_sha256")) for k in required}
    same_dataset = len(dataset_hashes) == 1 and "None" not in dataset_hashes
    if not same_dataset:
        failed.append({"reason": "DATASET_HASH_MISMATCH"})

    eligible = not failed
    return {
        "status": "ELIGIBLE" if eligible else "BLOCKED",
        "eligible": eligible,
        "automatic_promotion": False,
        "reasons": [] if eligible else ["SYMBOL_HORIZON_EVIDENCE_FAILED"],
        "failed_cells": failed,
        "dataset_sha256": next(iter(dataset_hashes)) if same_dataset else None,
        "cells": [latest[k] for k in sorted(required)],
        "source": "persisted_research_evidence",
    }
