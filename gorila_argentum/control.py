from __future__ import annotations

import os
from .config import settings
from .storage import Store


def _promotion_status() -> str:
    # Promotion is derived from the validated research gate, not from runtime configuration.
    return "BLOCKED"


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

    promotion = _promotion_status()
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
            "reason": os.getenv(
                "GORILA_PREDICTOR_PROMOTION_REASON",
                "Statistical promotion gate not cleared.",
            ),
        },
        "monitoring": {
            "data_distribution_drift": "IMPLEMENTED",
            "prediction_drift": "NOT_IMPLEMENTED",
            "realized_vs_predicted": "NOT_IMPLEMENTED",
            "automatic_recalibration": "NOT_IMPLEMENTED",
            "automatic_kill_switch": "NOT_IMPLEMENTED",
        },
        "source_health": store.health(),
        "drift": {
            "snapshots_seen": len(drift_rows),
            "latest_series": len(latest),
            "warnings_or_alerts": alerts,
        },
    }
