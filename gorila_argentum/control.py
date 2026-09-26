from __future__ import annotations

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

    storage_backend = "postgres" if store.pg else "sqlite-fallback"
    durability_ok = bool(store.pg)

    halt_reasons = []
    if not durability_ok:
        halt_reasons.append("NON_DURABLE_STORAGE")
    if any(row.get("status") == "ALERT" for row in alerts):
        halt_reasons.append("DRIFT_ALERT")
    if any(row.get("status") in {"ERROR", "FAILED", "STALE"} for row in store.health()):
        halt_reasons.append("SOURCE_HEALTH_FAILURE")

    if halt_reasons:
        circuit_status = "HALTED"
    elif alerts:
        circuit_status = "DEGRADED"
    else:
        circuit_status = "NORMAL"

    promotion_reasons = list(
        (
            (promotion_decision or {}).get("reasons")
            if promotion_decision
            else promotion_evaluation.get("reasons")
            or []
        )
    )
    if not durability_ok and "STORAGE_DURABILITY_FAILED" not in promotion_reasons:
        promotion_reasons.append("STORAGE_DURABILITY_FAILED")
    if circuit_status != "NORMAL" and "CIRCUIT_BREAKER_NOT_NORMAL" not in promotion_reasons:
        promotion_reasons.append("CIRCUIT_BREAKER_NOT_NORMAL")
    if promotion != "BLOCKED" and promotion_reasons:
        promotion = "BLOCKED"

    return {
        "batch": 13,
        "runtime": {
            "mode": "RESEARCH",
            "storage": storage_backend,
            "predictor_promotion": promotion,
            "storage_durable": durability_ok,
            "circuit_breaker": circuit_status,
            "circuit_breaker_reasons": halt_reasons,
            "promotion_operational_gate": "PASS" if durability_ok and circuit_status == "NORMAL" else "BLOCKED",
        },
        "promotion_gate": {
            "status": promotion,
            "automatic_promotion": False,
            "reason": promotion_reasons,
        },
        "monitoring": {
            "data_distribution_drift": "IMPLEMENTED",
            "prediction_drift": "NOT_IMPLEMENTED",
            "realized_vs_predicted": "NOT_IMPLEMENTED",
            "automatic_recalibration": "NOT_IMPLEMENTED",
            "automatic_kill_switch": "IMPLEMENTED_RESEARCH_CIRCUIT_BREAKER",
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
