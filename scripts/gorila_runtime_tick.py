from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from gorila_argentum.audit import build_audit_state
from gorila_argentum.calibration import build_recalibration_candidate
from gorila_argentum.config import settings
from gorila_argentum.learning import run_learning_cycle
from gorila_argentum.ingest import run_batch
from gorila_argentum.promotion import CURRENT_BATCH10_EVIDENCE, evaluate_promotion
from gorila_argentum.storage import Store


def _create_learning_shadow_predictions(store: Store, learning_results: list[dict[str, Any]]) -> dict[str, Any]:
    created = []
    skipped = []
    for result in learning_results:
        probability = result.get("latest_probability_up")
        symbol = str(result.get("symbol") or "").strip().upper()
        if probability is None or not symbol:
            skipped.append({"symbol": symbol or None, "reason": "NO_LATEST_PROBABILITY"})
            continue

        series = store.recent_series(symbol, "close", limit=1)
        if not series:
            skipped.append({"symbol": symbol, "reason": "NO_ENTRY_PRICE"})
            continue

        event_time, entry_price = series[-1]
        dataset_hash = result.get("dataset_hash") or ""
        feature_hash = hashlib.sha256(
            f"{symbol}|{event_time}|{entry_price}|{probability}|{dataset_hash}".encode()
        ).hexdigest()

        latest = store.latest_shadow(symbol=symbol, limit=1)
        if latest and latest[0].get("feature_hash") == feature_hash:
            skipped.append({"symbol": symbol, "reason": "ALREADY_CAPTURED", "feature_hash": feature_hash})
            continue

        shadow = store.save_shadow_prediction(
            symbol=symbol,
            model_version="gorila-learning-5d-v1",
            probability_up=float(probability),
            horizon_seconds=5 * 24 * 3600,
            regime="LEARNING_5D",
            entry_price=float(entry_price),
            feature_hash=feature_hash,
            metadata={
                "source": "autonomous_learning_cycle",
                "dataset_hash": dataset_hash,
                "learning_status": result.get("status"),
                "source_event_time": event_time,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        created.append(
            {
                "symbol": symbol,
                "prediction_id": shadow["id"],
                "probability_up": float(probability),
                "horizon_seconds": 5 * 24 * 3600,
                "feature_hash": feature_hash,
                "source_event_time": event_time,
            }
        )
    return {"created": created, "created_count": len(created), "skipped": skipped}


def run_tick(store: Store | None = None) -> dict[str, Any]:
    store = store or Store()
    store.init()
    if not store.pg:
        raise RuntimeError("durable_storage_required")

    ingestion = run_batch()
    learning = []
    symbols = tuple(settings.core_symbols)
    max_workers = min(3, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                run_learning_cycle,
                symbol,
                horizon_days=5,
            ): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "status": "ERROR",
                    "symbol": symbol,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            learning.append(result)
    learning.sort(key=lambda row: str(row.get("symbol") or ""))

    shadow_capture = _create_learning_shadow_predictions(store, learning)

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
        "shadow_capture": shadow_capture,
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


def run_autonomous_tick(
    *,
    store: Store | None = None,
    run_id: str | None = None,
    kind: str = "autonomous",
) -> dict[str, Any]:
    store = store or Store()
    store.init()
    if not store.pg:
        raise RuntimeError("durable_storage_required")

    now = datetime.now(timezone.utc)
    if run_id is None:
        slot_seconds = max(60, int(__import__("os").getenv("GORILA_AUTONOMOUS_INTERVAL_SECONDS", "300")))
        slot = int(now.timestamp() // slot_seconds)
        run_id = f"{kind}-{slot}"

    if not store.claim_runtime_run(run_id, kind):
        return {
            "status": "SKIPPED_ALREADY_CLAIMED",
            "run_id": run_id,
            "kind": kind,
        }

    started = now.isoformat()
    try:
        payload = run_tick(store=store)
        payload = dict(payload)
        payload.update(
            {
                "run_id": run_id,
                "kind": kind,
                "started_at": started,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "status": payload.get("status", "COMPLETED"),
                "autonomous": True,
            }
        )
        store.finish_runtime_run(run_id, payload["status"], payload)
        return payload
    except Exception as exc:
        result = {
            "status": "FAILED",
            "run_id": run_id,
            "kind": kind,
            "started_at": started,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
            "autonomous": True,
        }
        store.finish_runtime_run(run_id, "FAILED", result)
        raise
