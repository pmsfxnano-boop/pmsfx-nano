from __future__ import annotations

from .storage import Store
from .promotion import evaluate_promotion, evaluate_live_promotion, evaluate_predictive_promotion, CURRENT_BATCH10_EVIDENCE
from .drift import rolling_drift
from .calibration import build_recalibration_candidate


def _promotion_status(decision: dict | None = None) -> str:
    # Promotion is derived from the validated research gate, not runtime configuration.
    decision = decision or evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    return str(decision.get("status", "BLOCKED"))


def _shadow_diagnostics(rows: list[dict], *, current_size: int = 30, reference_size: int = 90) -> dict:
    settled = [
        row for row in rows
        if row.get("status") == "SETTLED"
        and row.get("realized_direction") in {"UP", "DOWN"}
        and row.get("probability_up") is not None
    ]
    settled.sort(key=lambda row: row.get("observed_at") or row.get("created_at") or "")
    needed = int(current_size) + int(reference_size)
    if len(settled) < needed:
        return {
            "status": "INSUFFICIENT_DATA",
            "samples": len(settled),
            "required_samples": needed,
            "prediction_drift": {"status": "INSUFFICIENT_DATA"},
            "realized_vs_predicted": {"status": "INSUFFICIENT_DATA"},
        }

    probs = [float(row["probability_up"]) for row in settled]
    prob_drift = rolling_drift(
        probs[-needed:],
        current_size=int(current_size),
        reference_size=int(reference_size),
    )

    split = len(settled) - int(current_size)
    reference = settled[split - int(reference_size):split]
    current = settled[split:]

    def label(row: dict) -> float:
        return 1.0 if row["realized_direction"] == "UP" else 0.0

    ref_residuals = [float(r["probability_up"]) - label(r) for r in reference]
    cur_residuals = [float(r["probability_up"]) - label(r) for r in current]
    ref_briers = [float(r["brier"]) for r in reference if r.get("brier") is not None]
    cur_briers = [float(r["brier"]) for r in current if r.get("brier") is not None]

    ref_gap = sum(ref_residuals) / len(ref_residuals)
    cur_gap = sum(cur_residuals) / len(cur_residuals)
    brier_delta = (
        (sum(cur_briers) / len(cur_briers)) - (sum(ref_briers) / len(ref_briers))
        if ref_briers and cur_briers
        else None
    )

    if abs(cur_gap) >= 0.15 or (brier_delta is not None and brier_delta >= 0.10):
        realized_status = "ALERT"
    elif abs(cur_gap) >= 0.08 or (brier_delta is not None and brier_delta >= 0.05):
        realized_status = "WARN"
    else:
        realized_status = "OK"

    return {
        "status": "ALERT" if prob_drift.get("status") == "ALERT" or realized_status == "ALERT"
                  else "WARN" if prob_drift.get("status") == "WARN" or realized_status == "WARN"
                  else "OK",
        "samples": len(settled),
        "prediction_drift": prob_drift,
        "realized_vs_predicted": {
            "status": realized_status,
            "reference_n": len(reference),
            "current_n": len(current),
            "reference_prediction_gap": ref_gap,
            "current_prediction_gap": cur_gap,
            "gap_delta": cur_gap - ref_gap,
            "brier_delta": brier_delta,
        },
    }


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
    predictive_evidence = evaluate_predictive_promotion(store)
    live_evidence = evaluate_live_promotion(store)
    historical_evaluation = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    promotion = _promotion_status(promotion_decision or live_evidence)
    latest_learning = store.latest_learning(limit=1)

    storage_backend = "postgres" if store.pg else "sqlite-fallback"
    durability_ok = bool(store.pg)

    halt_reasons = []
    if not durability_ok:
        halt_reasons.append("NON_DURABLE_STORAGE")
    if any(row.get("status") == "ALERT" for row in alerts):
        halt_reasons.append("DRIFT_ALERT")
    required_sources = {"BCRA/FX", "ArgentinaDatos/FX", "ArgentinaDatos/EMBI+"}
    required_failures = [
        row for row in store.health()
        if row.get("source") in required_sources
        and row.get("status") in {"ERROR", "FAILED", "STALE", "DEGRADED"}
    ]
    if required_failures:
        halt_reasons.append("REQUIRED_SOURCE_HEALTH_FAILURE")

    if halt_reasons:
        circuit_status = "HALTED"
    elif alerts:
        circuit_status = "DEGRADED"
    else:
        circuit_status = "NORMAL"

    shadow_rows = store.latest_shadow(status="SETTLED", limit=500)
    shadow_diagnostics = _shadow_diagnostics(shadow_rows)
    recalibration = build_recalibration_candidate(shadow_rows)

    diagnostic_statuses = {
        shadow_diagnostics["prediction_drift"].get("status"),
        shadow_diagnostics["realized_vs_predicted"].get("status"),
    }
    if "ALERT" in diagnostic_statuses:
        halt_reasons.append("MODEL_DRIFT_ALERT")
        circuit_status = "HALTED"
    elif "WARN" in diagnostic_statuses and circuit_status == "NORMAL":
        circuit_status = "DEGRADED"

    promotion_reasons = list(
        (
            (promotion_decision or {}).get("reasons")
            if promotion_decision
            else live_evidence.get("reasons")
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
            "predictive_evidence": predictive_evidence,
            "live_evidence": live_evidence,
            "historical_reference": historical_evaluation,
        },
        "monitoring": {
            "data_distribution_drift": "IMPLEMENTED",
            "prediction_drift": "IMPLEMENTED",
            "realized_vs_predicted": "IMPLEMENTED",
            "automatic_recalibration": "IMPLEMENTED_AS_GATED_CANDIDATE",
            "automatic_kill_switch": "IMPLEMENTED_RESEARCH_CIRCUIT_BREAKER",
            "shadow_ledger": "IMPLEMENTED",
            "continuous_learning": "IMPLEMENTED_AS_CANDIDATE_CYCLE",
            "continuous_learning_promotion": "BLOCKED_UNTIL_PROMOTION_GATE",
            "fresh_evidence_required": True,
            "historical_batch10_is_not_live_gate": True,
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
        "model_diagnostics": shadow_diagnostics,
        "recalibration": recalibration,
    }
