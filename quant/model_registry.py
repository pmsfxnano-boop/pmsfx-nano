"""PMSF-X Nano — lightweight persistent model registry."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_registry_record(
    *,
    model_id: str,
    version: str,
    status: str,
    evaluation: dict[str, Any],
    features_version: str = "features-v1",
    dataset_version: str = "tiingo-5min-v1",
) -> dict[str, Any]:
    return {
        "model_id": model_id,
        "version": version,
        "status": status,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "dataset_version": dataset_version,
        "features_version": features_version,
        "validation_type": evaluation.get("validation_type"),
        "accuracy": evaluation.get("accuracy"),
        "brier": evaluation.get("brier"),
        "brier_skill": evaluation.get("brier_skill"),
        "calibration_status": evaluation.get("calibration_status"),
        "regime": evaluation.get("regime"),
        "cpcv_status": evaluation.get("cpcv_status"),
        "pbo_status": evaluation.get("pbo_status"),
        "dsr_status": evaluation.get("dsr_status"),
        "selection_rule": evaluation.get("search_ledger", {}).get("selection_rule"),
    }
