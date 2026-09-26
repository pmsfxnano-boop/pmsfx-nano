from __future__ import annotations

import os
from .config import settings
from .storage import Store
from .promotion import evaluate_promotion, CURRENT_BATCH10_EVIDENCE


def _promotion_status(decision: dict | None = None) -> str:
    # Promotion is derived from the validated research gate, not runtime configuration.
    decision = decision or evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    return str(decision.get("status", "BLOCKED"))


def build_control_state(store: Store | None = None) -> dict:
    store = store or Store()
    store.init()

    drift_rows = store.latest_drift(limit=200)
    latest = {}
    for row in drift_rows:
        key = (row["symbol"], row["field"])
        if key not in latest:
            latest[key] = row

    alerts = [
        row for row in latest.values()
        if row.get("status") in {"WARN", "ALERT"}
    ]
    rank = {"ALERT": 0, "WARN": 1}
    alerts.sort(key=lambda row: (rank.get(row.get("status"), 9), row.get("created_at", "")), reverse=False)

    promotion_decision = store.latest_promotion_decision()
    promotion_evaluation = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    promotion = _promotion_status(promotion_decision or promotion_evaluation)
    latest_learning = store.latest_learning(limit=1)
    return {
        "batch": 13,
        "runtime": {
            "mode": "RESEARCH",
            "storage": "postgres" if settings.database_url else "sqlite-fallback",
            "predictor_promotion": promotion,
        },
        "promotion_gate": {
            "status": promotion,
            "automatic_promotion": False,
            "reason": (
                (promotion_decision or {}).get("reasons")
                if promotion_decision
                else promotion_evaluation.get("reasons")
            ),
        },
        "monitoring": {
            "data_distribution_drift": "IMPLEMENTED",
            "prediction_drift": "NOT_IMPLEMENTED",
            "realized_vs_predicted": "NOT_IMPLEMENTED",
            "automatic_recalibration": "NOT_IMPLEMENTED",
            "automatic_kill_switch": "NOT_IMPLEMENTED",
            "shadow_ledger": "IMPLEMENTED",
            "continuous_learning": "IMPLEMENTED_AS_CANDIDATE_CYCLE",
            "continuous_learning_promotion": "BLOCKED_UNTIL_PROMOTION_GATE",
        },
        "source_health": store.health(),
        "drift": {
            "snapshots_seen": len(drift_rows),
            "latest_series": len(latest),
            "warnings_or_alerts": alerts,
        },
        "learning": {
            "latest_run": latest_learning[0] if latest_learning else None,
        },
    }
