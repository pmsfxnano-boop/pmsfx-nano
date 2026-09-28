from __future__ import annotations

import hashlib
import json
import os
import uuid

import psycopg
from datetime import datetime, timezone
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from gorila_argentum.audit import build_audit_state
from gorila_argentum.calibration import build_recalibration_candidate
from gorila_argentum.config import settings
from gorila_argentum.learning import run_learning_cycle
from gorila_argentum.ingest import run_batch
from gorila_argentum.promotion import evaluate_live_promotion
from gorila_argentum.storage import Store
from gorila_argentum.canonical_data import canonical_daily_series
from gorila_argentum.shadow import compute_shadow_outcome
from gorila_argentum.evidence import persist_manifest, persist_v2_evidence
from quant.db import connection as quant_connection, record_model_registry


def _sync_pmsfx_shadow_ledger(store: Store, limit: int = 250) -> dict[str, Any]:
    created = 0
    settled = 0
    pending = 0
    errors = []
    forecast_rows = []

    try:
        with quant_connection() as conn:
            if conn is None:
                return {"status": "NO_QUANT_DB", "created": 0, "settled": 0, "pending": 0}
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, created_at, symbol, model_id, status, direction,
                           p_up, confidence, horizon_seconds,
                           COALESCE((bid + ask) / 2.0, last) AS entry_price
                    FROM forecasts
                    WHERE p_up IS NOT NULL
                      AND horizon_seconds IS NOT NULL
                      AND COALESCE((bid + ask) / 2.0, last) IS NOT NULL
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    (max(1, min(int(limit), 500)),),
                )
                forecast_rows = cur.fetchall()
    except Exception as exc:
        return {
            "status": "ERROR",
            "created": 0,
            "settled": 0,
            "pending": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }

    candidates = []
    for row in forecast_rows:
        forecast_id = int(row[0])
        candidates.append(
            {
                "symbol": str(row[2]).upper(),
                "model_version": "pmsfx-x-upstream-shadow-v1",
                "probability_up": float(row[6]),
                "horizon_seconds": int(row[8]),
                "regime": "PMSF_X_LIVE",
                "entry_price": float(row[9]),
                "feature_hash": f"pmsfx-forecast:{forecast_id}",
                "created_at": row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1]),
                "metadata": {
                    "source": "shared_quant_postgres",
                    "quant_forecast_id": forecast_id,
                    "upstream_model_id": row[3],
                    "upstream_status": row[4],
                    "upstream_direction": row[5],
                    "upstream_confidence": row[7],
                    "historical_backfill": True,
                },
                "forecast_id": forecast_id,
            }
        )

    existing_hashes = store.shadow_existing_feature_hashes(
        [row["feature_hash"] for row in candidates]
    )
    new_predictions = [
        {
            key: value
            for key, value in row.items()
            if key not in {"forecast_id"}
        }
        for row in candidates
        if row["feature_hash"] not in existing_hashes
    ]
    if new_predictions:
        bulk = store.save_shadow_predictions_bulk(new_predictions)
        created = int(bulk.get("created", 0))

    open_rows = [
        row for row in store.latest_shadow(status="OPEN", limit=500)
        if str(row.get("feature_hash") or "").startswith("pmsfx-forecast:")
    ]
    quant_ids = []
    for shadow in open_rows:
        try:
            quant_ids.append(int(str(shadow["feature_hash"]).split(":", 1)[1]))
        except (ValueError, KeyError):
            continue

    outcomes_by_forecast = {}
    if quant_ids:
        try:
            with quant_connection() as conn:
                if conn is not None:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT forecast_id, resolved_at, exit_price, realized_direction,
                                   realized_return_bps, prediction_correct, brier_loss
                            FROM forecast_outcomes
                            WHERE forecast_id = ANY(%s)
                            """,
                            (quant_ids,),
                        )
                        outcomes_by_forecast = {
                            int(row[0]): row for row in cur.fetchall()
                        }
        except Exception as exc:
            errors.append({"stage": "forecast_outcomes_query", "error": f"{type(exc).__name__}: {exc}"})

    for shadow in open_rows:
        feature_hash = str(shadow.get("feature_hash") or "")
        if not feature_hash.startswith("pmsfx-forecast:"):
            continue
        try:
            forecast_id = int(feature_hash.split(":", 1)[1])
        except ValueError:
            continue

        outcome_row = outcomes_by_forecast.get(forecast_id)
        if not outcome_row or outcome_row[1] is None or outcome_row[0] is None:
            pending += 1
            continue

        observed_at = outcome_row[1].isoformat() if hasattr(outcome_row[1], "isoformat") else str(outcome_row[1])
        observed_price_source = "exit_price"
        try:
            try:
                observed_price = float(outcome_row[2])
            except (TypeError, ValueError):
                realized_return_bps = float(outcome_row[4])
                entry_price = float(shadow["entry_price"])
                observed_price = entry_price * (1.0 + realized_return_bps / 10000.0)
                observed_price_source = "reconstructed_from_realized_return_bps"

            outcome = compute_shadow_outcome(
                float(shadow["probability_up"]),
                float(shadow["entry_price"]),
                observed_price,
            )
            store.settle_shadow_prediction(
                shadow["id"],
                outcome,
                observed_at,
                metadata={
                    "resolution": "shared_quant_forecast_outcome",
                    "quant_forecast_id": forecast_id,
                    "upstream_realized_direction": outcome_row[3],
                    "upstream_realized_return_bps": outcome_row[4],
                    "upstream_prediction_correct": outcome_row[5],
                    "upstream_brier_loss": outcome_row[6],
                    "observed_price_source": observed_price_source,
                },
            )
            settled += 1
        except Exception as exc:
            pending += 1
            errors.append(
                {
                    "prediction_id": shadow.get("id"),
                    "forecast_id": forecast_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    return {
        "status": "COMPLETED" if not errors else "PARTIAL",
        "created": created,
        "settled": settled,
        "pending": pending,
        "errors": errors[:20],
        "source_forecasts_seen": len(forecast_rows),
        "candidate_rows": len(candidates),
        "existing_feature_hashes": len(existing_hashes),
    }

def _persist_learning_registry(learning_results: list[dict[str, Any]]) -> dict[str, Any]:
    saved = 0
    errors = []
    for result in learning_results:
        validation = result.get("validation") or {}
        model_hash = str(result.get("model_hash") or "")
        learner_id = str(result.get("learner_id") or "unknown")
        try:
            record_model_registry({
                "registered_at": result.get("generated_at") or datetime.now(timezone.utc),
                "model_id": learner_id,
                "version": model_hash[:32] if model_hash else str(result.get("trainer_version") or "unknown"),
                "status": str(result.get("status") or "UNKNOWN"),
                "dataset_version": result.get("dataset_hash"),
                "features_version": hashlib.sha256(
                    json.dumps(result.get("feature_names") or [], sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "validation_type": "purged_walk_forward",
                "accuracy": validation.get("accuracy"),
                "brier": validation.get("brier"),
                "brier_skill": validation.get("brier_skill"),
                "calibration_status": "NOT_CALIBRATED",
                "regime": {"data_fabric": result.get("data_fabric")},
                "cpcv_status": "NOT_RUN",
                "pbo_status": "NOT_RUN",
                "dsr_status": "NOT_RUN",
                "selection_rule": "candidate_gate_only_no_auto_promotion",
                "metadata": {
                    "trainer_version": result.get("trainer_version"),
                    "samples": result.get("samples"),
                    "horizon_days": result.get("horizon_days"),
                    "candidate_policy": result.get("candidate_policy"),
                    "model_hash": model_hash,
                },
            })
            saved += 1
        except Exception as exc:
            errors.append({
                "symbol": result.get("symbol"),
                "error": f"{type(exc).__name__}: {exc}",
            })
    return {"status": "COMPLETED" if not errors else "PARTIAL", "saved": saved, "errors": errors[:20]}


def _create_learning_shadow_predictions(store: Store, learning_results: list[dict[str, Any]]) -> dict[str, Any]:
    created = []
    skipped = []
    for result in learning_results:
        symbol = str(result.get("symbol") or "").strip().upper()
        status = str(result.get("status") or "")
        probability = result.get("latest_probability_up")
        if not symbol:
            skipped.append({"symbol": None, "reason": "NO_SYMBOL"})
            continue
        # Rejected/insufficient candidates stay in research storage only. They
        # must never enter the shadow ledger as though they were validated.
        if status != "CANDIDATE_ELIGIBLE":
            skipped.append({"symbol": symbol, "reason": "CANDIDATE_NOT_ELIGIBLE", "status": status})
            continue
        if probability is None:
            skipped.append({"symbol": symbol, "reason": "NO_LATEST_PROBABILITY"})
            continue
        if result.get("data_fabric") != "CANONICAL_DAILY_V1":
            skipped.append({"symbol": symbol, "reason": "NON_CANONICAL_DATA_FABRIC"})
            continue

        series = canonical_daily_series(store, symbol, "close", limit=1)
        if not series:
            skipped.append({"symbol": symbol, "reason": "NO_CANONICAL_ENTRY_PRICE"})
            continue

        event_time, entry_price = series[-1]
        dataset_hash = result.get("dataset_hash") or ""
        model_hash = result.get("model_hash") or ""
        feature_hash = hashlib.sha256(
            f"{symbol}|{event_time}|{entry_price}|{probability}|{dataset_hash}|{model_hash}".encode()
        ).hexdigest()

        latest = store.latest_shadow(symbol=symbol, limit=1)
        if latest and latest[0].get("feature_hash") == feature_hash:
            skipped.append({"symbol": symbol, "reason": "ALREADY_CAPTURED", "feature_hash": feature_hash})
            continue

        shadow = store.save_shadow_prediction(
            symbol=symbol,
            model_version=result.get("learner_id", "gorila-learning-5d-v2"),
            probability_up=float(probability),
            horizon_seconds=5 * 24 * 3600,
            regime="LEARNING_5D",
            entry_price=float(entry_price),
            feature_hash=feature_hash,
            metadata={
                "source": "autonomous_learning_cycle",
                "learner_id": result.get("learner_id"),
                "trainer_version": result.get("trainer_version"),
                "data_fabric": result.get("data_fabric"),
                "feature_names": result.get("feature_names"),
                "dataset_hash": dataset_hash,
                "model_hash": model_hash,
                "learning_status": status,
                "validation": result.get("validation"),
                "candidate_policy": result.get("candidate_policy"),
                "source_session_date": event_time,
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
                "source_session_date": event_time,
                "model_hash": model_hash,
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
                store=Store(),
                initialize_store=False,
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

    learning_registry = _persist_learning_registry(learning)
    shadow_capture = _create_learning_shadow_predictions(store, learning)
    pmsfx_shadow = _sync_pmsfx_shadow_ledger(store)

    settlement = store.settle_due_shadow_from_observations(
        max_lateness_seconds=3600,
        limit=100,
    )

    manifest_path = os.getenv("GORILA_EVIDENCE_MANIFEST_PATH", "research/evidence_manifest.json")
    evidence_sync = {"status": "NOT_FOUND", "path": manifest_path}
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
        if manifest.get("status") == "COMPLETE":
            evidence_sync = {"status": "PERSISTED", **persist_manifest(store, manifest, source="runtime_tick")}
        else:
            evidence_sync = {"status": "SKIPPED", "reason": "MANIFEST_NOT_COMPLETE", "path": manifest_path}
    except FileNotFoundError:
        pass
    except Exception as exc:
        evidence_sync = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "path": manifest_path}

    v2_evidence_sync = {"status": "NOT_FOUND", "path": "research/gorila_v2_evidence.json"}
    try:
        with open("research/gorila_v2_evidence.json", "r", encoding="utf-8") as fh:
            v2_manifest = json.load(fh)
        if v2_manifest.get("status") == "COMPLETE":
            v2_evidence_sync = {"status": "PERSISTED", **persist_v2_evidence(store, v2_manifest)}
        else:
            v2_evidence_sync = {"status": "SKIPPED", "reason": "V2_MANIFEST_NOT_COMPLETE"}
    except FileNotFoundError:
        pass
    except Exception as exc:
        v2_evidence_sync = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}

    decision = evaluate_live_promotion(store)
    persisted_promotion = store.save_promotion_decision(
        "gorila-quantitative-v1", "V1", decision
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
        "learning_registry": learning_registry,
        "shadow_capture": shadow_capture,
        "pmsfx_shadow": pmsfx_shadow,
        "settlement": settlement,
        "evidence_sync": evidence_sync,
        "v2_evidence_sync": v2_evidence_sync,
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


def _acquire_autonomous_lock():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("durable_storage_required")
    conn = psycopg.connect(url, sslmode="require", connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (2147483647,))
            acquired = bool(cur.fetchone()[0])
        if not acquired:
            conn.close()
            return None
        return conn
    except Exception:
        conn.close()
        raise


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

    lock_conn = _acquire_autonomous_lock()
    if lock_conn is None:
        return {
            "status": "SKIPPED_ALREADY_RUNNING",
            "kind": kind,
            "autonomous": True,
        }

    now = datetime.now(timezone.utc)
    if run_id is None:
        run_id = f"{kind}-{int(now.timestamp())}-{uuid.uuid4().hex[:10]}"

    if not store.claim_runtime_run(run_id, kind):
        lock_conn.close()
        return {
            "status": "SKIPPED_ALREADY_CLAIMED",
            "run_id": run_id,
            "kind": kind,
            "autonomous": True,
        }

    started = now.isoformat()
    try:
        payload = dict(run_tick(store=store))
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
    finally:
        lock_conn.close()

