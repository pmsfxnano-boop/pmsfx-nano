from __future__ import annotations

import json
from typing import Any

from gorila_argentum.audit import build_audit_state
from gorila_argentum.calibration import build_recalibration_candidate
from gorila_argentum.config import settings
from gorila_argentum.learning import run_learning_cycle
from gorila_argentum.ingest import run_batch
from gorila_argentum.promotion import CURRENT_BATCH10_EVIDENCE, evaluate_promotion
from gorila_argentum.storage import Store


def run_tick(store: Store | None = None) -> dict[str, Any]:
    store = store or Store()
    store.init()
    if not store.pg:
        raise RuntimeError("durable_storage_required")

    ingestion = run_batch()
    learning = [
        run_learning_cycle(symbol, horizon_days=5, store=store)
        for symbol in settings.core_symbols
    ]

    settlement = store.settle_due_shadow_from_observations(
        max_lateness_seconds=3600,
        limit=100,
    )

    decision = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    persisted_promotion = store.save_promotion_decision(
        "multihorizon-meta-research-v1", "V2", decision
    )

    shadow_rows = store.latest_shadow(status="SETTLED", limit=500)
    recalibration = build_recalibration_candidate(shadow_rows)
    persisted_recalibration = store.save_calibration_run(
        "shadow-probability-v0", recalibration
    )

    audit = build_audit_state(store)
    return {
        "status": "COMPLETED",
        "ingestion": ingestion,
        "learning": learning,
        "settlement": settlement,
        "promotion": {
            "decision": decision,
            "persisted": persisted_promotion,
        },
        "recalibration": {
            "candidate": recalibration,
            "persisted": persisted_recalibration,
            "automatic_apply": False,
        },
        "audit": audit,
    }


def main() -> int:
    try:
        payload = run_tick()
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(payload, sort_keys=True, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
