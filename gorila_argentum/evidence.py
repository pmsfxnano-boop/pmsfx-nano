from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from .storage import Store


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def manifest_digest(manifest: dict[str, Any]) -> str:
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def persist_manifest(store: Store, manifest: dict[str, Any], *, source: str = "research_ci") -> dict[str, Any]:
    store.init()
    if not store.pg:
        raise RuntimeError("durable_storage_required")
    run_id = str(manifest.get("run_id") or uuid.uuid4().hex)
    digest = str(manifest.get("manifest_sha256") or manifest_digest(manifest))
    rows = manifest.get("evidence") or []
    if not rows and manifest.get("runs"):
        rows = [item for run in manifest["runs"] for item in run.get("evidence", [])]
    with store.connect() as conn:
        with conn.cursor() as cur:
            for item in rows:
                cur.execute(
                    """
                    INSERT INTO research_evidence(
                      id,run_id,created_at,source,model_id,symbol,horizon_days,
                      dataset_sha256,sample_count,oos_samples,outer_folds,accuracy,
                      brier,baseline_brier,brier_skill,brier_skill_ci_low,brier_skill_ci_high,
                      logloss,rank_ic,net_return_50bps,placebo_accuracy_p95,pbo,dsr,
                      execution_delta_50bps,stress_pass,data_health,point_in_time,
                      validation_status,prediction_status,strategy_status,validation_reasons,prediction_reasons,strategy_reasons,manifest_sha256,metrics
                    )
                    VALUES(
                      %(id)s,%(run_id)s,%(created_at)s,%(source)s,%(model_id)s,%(symbol)s,%(horizon_days)s,
                      %(dataset_sha256)s,%(sample_count)s,%(oos_samples)s,%(outer_folds)s,%(accuracy)s,
                      %(brier)s,%(baseline_brier)s,%(brier_skill)s,%(ci_low)s,%(ci_high)s,
                      %(logloss)s,%(rank_ic)s,%(net_return_50bps)s,%(placebo_accuracy_p95)s,%(pbo)s,%(dsr)s,
                      %(execution_delta_50bps)s,%(stress_pass)s,%(data_health)s,%(point_in_time)s,
                      %(validation_status)s,%(prediction_status)s,%(strategy_status)s,%(validation_reasons)s,%(prediction_reasons)s,%(strategy_reasons)s,%(manifest_sha256)s,%(metrics)s
                    )
                    ON CONFLICT (run_id,symbol,horizon_days) DO UPDATE SET
                      created_at=EXCLUDED.created_at,
                      source=EXCLUDED.source,
                      model_id=EXCLUDED.model_id,
                      dataset_sha256=EXCLUDED.dataset_sha256,
                      validation_status=EXCLUDED.validation_status,
                      prediction_status=EXCLUDED.prediction_status,
                      strategy_status=EXCLUDED.strategy_status,
                      validation_reasons=EXCLUDED.validation_reasons,
                      prediction_reasons=EXCLUDED.prediction_reasons,
                      strategy_reasons=EXCLUDED.strategy_reasons,
                      manifest_sha256=EXCLUDED.manifest_sha256,
                      metrics=EXCLUDED.metrics
                    """,
                    {
                        "id": uuid.uuid4().hex,
                        "run_id": run_id,
                        "created_at": item.get("generated_at") or manifest.get("generated_at") or _utc(),
                        "source": source,
                        "model_id": manifest.get("method", "unknown"),
                        "symbol": item.get("symbol"),
                        "horizon_days": int(item.get("horizon_days")),
                        "dataset_sha256": item.get("dataset_sha256") or manifest.get("snapshot_sha256"),
                        "sample_count": int(item.get("dataset_samples", 0)),
                        "oos_samples": int(item.get("oos_samples", 0)),
                        "outer_folds": int(item.get("outer_folds", 0)),
                        "accuracy": item.get("accuracy"),
                        "brier": item.get("brier"),
                        "baseline_brier": item.get("baseline_brier"),
                        "brier_skill": item.get("brier_skill"),
                        "ci_low": (item.get("brier_skill_ci95") or [None, None])[0],
                        "ci_high": (item.get("brier_skill_ci95") or [None, None])[1],
                        "logloss": item.get("logloss"),
                        "rank_ic": item.get("rank_ic"),
                        "net_return_50bps": (item.get("strategy_costs", {}).get("50", {}) or {}).get("net_return"),
                        "placebo_accuracy_p95": item.get("placebo_accuracy_p95"),
                        "pbo": (item.get("pbo") or {}).get("pbo") if isinstance(item.get("pbo"), dict) else item.get("pbo"),
                        "dsr": item.get("dsr"),
                        "execution_delta_50bps": item.get("execution_delta_vs_flat_50bps"),
                        "stress_pass": int(item.get("stress_pass", item.get("validation_status") == "VALIDATED")),
                        "data_health": int(bool(item.get("data_health", True))),
                        "point_in_time": int(bool(item.get("point_in_time", True))),
                        "validation_status": item.get("validation_status", "BLOCKED"),
                        "prediction_status": item.get("prediction_status", "BLOCKED"),
                        "strategy_status": item.get("strategy_status", "BLOCKED"),
                        "validation_reasons": json.dumps(item.get("validation_reasons", []), sort_keys=True),
                        "prediction_reasons": json.dumps(item.get("prediction_reasons", []), sort_keys=True),
                        "strategy_reasons": json.dumps(item.get("strategy_reasons", []), sort_keys=True),
                        "manifest_sha256": digest,
                        "metrics": json.dumps(item, sort_keys=True, default=str),
                    },
                )
        conn.commit()
    return {"run_id": run_id, "manifest_sha256": digest, "rows_saved": len(rows)}


def latest_evidence(store: Store, symbol: str | None = None, horizon_days: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
    store.init()
    if not store.pg:
        return []
    clauses, params = [], []
    if symbol:
        clauses.append("symbol=%s")
        params.append(symbol)
    if horizon_days is not None:
        clauses.append("horizon_days=%s")
        params.append(int(horizon_days))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(max(1, min(500, int(limit))))
    with store.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT run_id,created_at,model_id,symbol,horizon_days,dataset_sha256,
                       sample_count,oos_samples,outer_folds,accuracy,brier,baseline_brier,
                       brier_skill,brier_skill_ci_low,brier_skill_ci_high,logloss,rank_ic,
                       net_return_50bps,placebo_accuracy_p95,pbo,dsr,execution_delta_50bps,
                       stress_pass,data_health,point_in_time,validation_status,prediction_status,strategy_status,
                       validation_reasons,prediction_reasons,strategy_reasons,manifest_sha256,metrics
                FROM research_evidence
                {where}
                ORDER BY created_at DESC,symbol,horizon_days
                LIMIT %s
                """,
                tuple(params),
            )
            columns = [d.name for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    for row in rows:
        try:
            row["validation_reasons"] = json.loads(row["validation_reasons"] or "[]")
            row["prediction_reasons"] = json.loads(row.get("prediction_reasons") or "[]")
            row["strategy_reasons"] = json.loads(row.get("strategy_reasons") or "[]")
        except Exception:
            pass
        try:
            row["metrics"] = json.loads(row["metrics"] or "{}")
        except Exception:
            pass
    return rows


def persist_v2_evidence(store: Store, manifest: dict[str, Any], *, source: str = "research_v2_ci") -> dict[str, Any]:
    store.init()
    if not store.pg:
        raise RuntimeError("durable_storage_required")
    digest = manifest_digest(manifest)
    generated = str(manifest.get("generated_at") or _utc())
    run_id = f"v2-{digest[:16]}"
    horizon = int((manifest.get("predictor") or {}).get("horizon_days") or 5)
    overall_validation = manifest.get("validation_status", "BLOCKED")
    trading_validation = manifest.get("trading_validation_status", "BLOCKED")
    common = {
        "id": None,
        "run_id": run_id,
        "created_at": generated,
        "source": source,
        "model_id": (manifest.get("predictor") or {}).get("model", "fixed-pooled-logit-v1"),
        "horizon_days": horizon,
        "dataset_sha256": (manifest.get("dataset") or {}).get("dataset_sha256"),
        "sample_count": int((manifest.get("dataset") or {}).get("common_dates", 0)),
        "oos_samples": int((manifest.get("oos") or {}).get("observations", 0)),
        "outer_folds": int((manifest.get("oos") or {}).get("folds", 0)),
        "accuracy": None,
        "brier": None,
        "baseline_brier": None,
        "brier_skill": None,
        "ci_low": None,
        "ci_high": None,
        "logloss": None,
        "rank_ic": (manifest.get("oos") or {}).get("mean_rank_ic"),
        "net_return_50bps": (manifest.get("economics") or {}).get("mean_net_return_per_rebalance"),
        "placebo_accuracy_p95": (manifest.get("placebo") or {}).get("rank_ic_mean_distribution_p95"),
        "pbo": 0.0,
        "dsr": ((manifest.get("economics") or {}).get("performance_audit") or {}).get("deflated_sharpe_probability"),
        "execution_delta_50bps": (manifest.get("economics") or {}).get("delta_vs_momentum_mean"),
        "stress_pass": int(overall_validation == "VALIDATED_RESEARCH"),
        "data_health": 1,
        "point_in_time": int(bool(manifest.get("point_in_time", False))),
        "validation_status": overall_validation,
        "validation_reasons": json.dumps(manifest.get("validation_reasons", []), sort_keys=True),
        "manifest_sha256": digest,
        "metrics": json.dumps({
            "schema": manifest.get("schema"),
            "predictor": manifest.get("predictor"),
            "economics": manifest.get("economics"),
            "oos": manifest.get("oos"),
            "placebo": manifest.get("placebo"),
            "stress": manifest.get("stress"),
            "trading_validation_status": trading_validation,
            "trading_validation_reasons": manifest.get("trading_validation_reasons", []),
        }, sort_keys=True, default=str),
    }
    per_symbol = ((manifest.get("oos") or {}).get("per_symbol") or {})
    saved = 0
    with store.connect() as conn:
        with conn.cursor() as cur:
            for symbol, symbol_metrics in per_symbol.items():
                values = dict(common)
                values["id"] = uuid.uuid5(uuid.NAMESPACE_URL, f"{run_id}:{symbol}:{horizon}").hex
                values["symbol"] = symbol
                values["metrics"] = json.dumps({
                    "symbol": symbol,
                    **symbol_metrics,
                    "aggregate": json.loads(common["metrics"]),
                }, sort_keys=True, default=str)
                cur.execute(
                    """
                    INSERT INTO research_evidence(
                      id,run_id,created_at,source,model_id,symbol,horizon_days,
                      dataset_sha256,sample_count,oos_samples,outer_folds,accuracy,
                      brier,baseline_brier,brier_skill,brier_skill_ci_low,brier_skill_ci_high,
                      logloss,rank_ic,net_return_50bps,placebo_accuracy_p95,pbo,dsr,
                      execution_delta_50bps,stress_pass,data_health,point_in_time,
                      validation_status,validation_reasons,manifest_sha256,metrics
                    )
                    VALUES(
                      %(id)s,%(run_id)s,%(created_at)s,%(source)s,%(model_id)s,%(symbol)s,%(horizon_days)s,
                      %(dataset_sha256)s,%(sample_count)s,%(oos_samples)s,%(outer_folds)s,%(accuracy)s,
                      %(brier)s,%(baseline_brier)s,%(brier_skill)s,%(ci_low)s,%(ci_high)s,
                      %(logloss)s,%(rank_ic)s,%(net_return_50bps)s,%(placebo_accuracy_p95)s,%(pbo)s,%(dsr)s,
                      %(execution_delta_50bps)s,%(stress_pass)s,%(data_health)s,%(point_in_time)s,
                      %(validation_status)s,%(validation_reasons)s,%(manifest_sha256)s,%(metrics)s
                    )
                    ON CONFLICT (run_id,symbol,horizon_days) DO UPDATE SET
                      created_at=EXCLUDED.created_at,
                      validation_status=EXCLUDED.validation_status,
                      validation_reasons=EXCLUDED.validation_reasons,
                      manifest_sha256=EXCLUDED.manifest_sha256,
                      metrics=EXCLUDED.metrics,
                      rank_ic=EXCLUDED.rank_ic,
                      net_return_50bps=EXCLUDED.net_return_50bps,
                      execution_delta_50bps=EXCLUDED.execution_delta_50bps
                    """,
                    values,
                )
                saved += 1
        conn.commit()
    return {
        "run_id": run_id,
        "manifest_sha256": digest,
        "rows_saved": saved,
        "validation_status": overall_validation,
        "trading_validation_status": trading_validation,
    }
