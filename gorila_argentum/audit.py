from __future__ import annotations

from .control import build_control_state
from .promotion import evaluate_promotion, CURRENT_BATCH10_EVIDENCE
from .storage import Store


def build_audit_state(store: Store | None = None) -> dict:
    store = store or Store()
    store.init()
    control = build_control_state(store)
    promotion = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    shadow = store.shadow_summary()
    learning = store.latest_learning(limit=5)

    return {
        "service": "gorila-argentum",
        "mode": control["runtime"]["mode"],
        "storage": control["runtime"]["storage"],
        "promotion": {
            "status": promotion["status"],
            "eligible": promotion["eligible"],
            "automatic_promotion": False,
            "reasons": promotion["reasons"],
        },
        "shadow": shadow,
        "learning": {
            "runs": len(learning),
            "latest": learning[0] if learning else None,
        },
        "drift": control["drift"],
        "source_health": control["source_health"],
        "invariants": {
            "trading_execution": False,
            "automatic_promotion": False,
            "point_in_time_shadow_settlement": True,
        },
    }
