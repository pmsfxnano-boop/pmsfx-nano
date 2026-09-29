from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from gorila_argentum.storage import Store
from gorila_argentum.canonical_data import canonical_daily_series
from research.gorila_cross_sectional_predictor_v2 import (
    FEATURE_NAMES,
    HORIZON_DAYS,
    L2,
    _fit_logistic,
    _predict,
)


SYMBOLS = ("GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU")
EVIDENCE_PATH = Path(__file__).resolve().parents[1] / "research" / "gorila_v2_evidence.json"


def _date_key(value: Any) -> str:
    text = str(value)
    return text[:10]


def _feature(series: dict[str, dict[str, float]], symbol: str, dates: list[str], i: int) -> tuple[float, float, float]:
    p = series[symbol][dates[i]]
    return (
        math.log(p / series[symbol][dates[i - 1]]),
        math.log(p / series[symbol][dates[i - 3]]),
        math.log(p / series[symbol][dates[i - 5]]),
    )


def _series_from_store(store: Store, limit: int = 2500) -> dict[str, dict[str, float]]:
    """Read model inputs exclusively from the reconciled daily data fabric."""
    output: dict[str, dict[str, float]] = {}
    for symbol in SYMBOLS:
        rows = canonical_daily_series(store, symbol, "close", limit=limit)
        output[symbol] = {str(session_date): float(value) for session_date, value in rows}
    return output


def _dataset_identity(series: dict[str, dict[str, float]], common_dates: list[str]) -> str:
    payload = [
        [symbol, date, float(series[symbol][date])]
        for date in common_dates
        for symbol in SYMBOLS
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()


def _evidence() -> dict[str, Any]:
    try:
        return json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {
            "validation_status": "UNKNOWN",
            "validation_reasons": ["EVIDENCE_MANIFEST_UNAVAILABLE"],
        }


def score_universe(store: Store | None = None, limit: int = 2500) -> dict[str, Any]:
    store = store or Store()
    store.init()
    series = _series_from_store(store, limit=limit)
    missing_symbols = [symbol for symbol in SYMBOLS if not series.get(symbol)]
    latest_dates = {
        symbol: (max(series[symbol]) if series.get(symbol) else None)
        for symbol in SYMBOLS
    }
    if missing_symbols:
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": "LIVE_SYMBOL_HISTORY_MISSING",
            "missing_symbols": missing_symbols,
            "latest_dates": latest_dates,
            "required_symbols": list(SYMBOLS),
            "research_only": True,
            "no_execution_authority": True,
        }
    common_dates = sorted(
        set.intersection(*(set(series[s]) for s in SYMBOLS))
    )
    if len(common_dates) <= HORIZON_DAYS + 65:
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": "COMMON_DAILY_HISTORY_TOO_SHORT",
            "observations": len(common_dates),
            "latest_dates": latest_dates,
            "symbols": list(SYMBOLS),
            "research_only": True,
            "no_execution_authority": True,
        }

    # Build the training panel only from anchors for which every symbol has
    # both the anchor and the H-day-ahead observation. This is deliberately
    # defensive: the live store can contain sparse vendor histories or date
    # normalization collisions. A raw dict lookup must never turn that into
    # an HTTP 500 or silently contaminate the panel.
    rows = []
    skipped_anchors = 0
    candidate_anchors = range(65, len(common_dates) - HORIZON_DAYS)
    for i in candidate_anchors:
        anchor_date = common_dates[i]
        future_date = common_dates[i + HORIZON_DAYS]
        if any(
            anchor_date not in series[s]
            or future_date not in series[s]
            or series[s][anchor_date] <= 0
            or series[s][future_date] <= 0
            for s in SYMBOLS
        ):
            skipped_anchors += 1
            continue
        future = np.asarray(
            [
                math.log(series[s][future_date] / series[s][anchor_date])
                for s in SYMBOLS
            ],
            dtype=np.float64,
        )
        if not np.isfinite(future).all():
            skipped_anchors += 1
            continue
        median_future = float(np.median(future))
        for j, symbol in enumerate(SYMBOLS):
            features = _feature(series, symbol, common_dates, i)
            if not all(np.isfinite(x) for x in features):
                continue
            residual = float(future[j] - median_future)
            rows.append(
                {
                    "features": features,
                    "target_up": int(residual > 0.0),
                    "symbol": symbol,
                }
            )

    training_anchors = len({i for i in range(65, len(common_dates) - HORIZON_DAYS) if i < len(common_dates)})
    if len(rows) < max(120, len(SYMBOLS) * 20):
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": "VALID_TRAINING_PANEL_TOO_SHORT",
            "observations": len(common_dates),
            "training_rows": len(rows),
            "training_anchors": training_anchors - skipped_anchors,
            "skipped_anchors": skipped_anchors,
            "latest_dates": latest_dates,
            "symbols": list(SYMBOLS),
            "research_only": True,
            "no_execution_authority": True,
        }

    model_rows = [
        type(
            "PanelRowCompat",
            (),
            {
                "features": tuple(row["features"]),
                "target_up": row["target_up"],
                "target_residual_log_return": 0.0,
                "date_index": 0,
                "symbol": row["symbol"],
            },
        )()
        for row in rows
    ]
    model = _fit_logistic(model_rows)

    latest_index = len(common_dates) - 1
    latest_features = [_feature(series, s, common_dates, latest_index) for s in SYMBOLS]
    probabilities = _predict(model, latest_features)
    order = np.argsort(probabilities)

    ranks = {}
    for position, idx in enumerate(order):
        ranks[SYMBOLS[int(idx)]] = {
            "rank": position + 1,
            "percentile": float(position / max(1, len(SYMBOLS) - 1)),
        }

    evidence = _evidence()
    live_dataset_sha256 = _dataset_identity(series, common_dates)
    evidence_dataset_sha256 = (evidence.get("dataset") or {}).get("dataset_sha256")
    evidence_predictor = evidence.get("predictor") or {}
    evidence_model_identity_matches = (
        evidence_predictor.get("model") == "fixed-pooled-logit-v1"
        and tuple(evidence_predictor.get("features") or ()) == tuple(FEATURE_NAMES)
        and float(evidence_predictor.get("l2", L2)) == float(L2)
        and int(evidence_predictor.get("horizon_days", HORIZON_DAYS)) == int(HORIZON_DAYS)
    )
    evidence_dataset_identity_matches = bool(
        evidence_dataset_sha256 and evidence_dataset_sha256 == live_dataset_sha256
    )
    evidence_bound_to_live = bool(
        evidence.get("validation_status") == "VALIDATED_RESEARCH"
        and evidence_model_identity_matches
        and evidence_dataset_identity_matches
        and evidence.get("point_in_time") is True
    )
    generated_at = evidence.get("generated_at")
    evidence_age_hours = None
    if generated_at:
        try:
            stamp = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            evidence_age_hours = max(
                0.0,
                (datetime.now(timezone.utc) - stamp).total_seconds() / 3600.0,
            )
        except Exception:
            pass

    items = []
    for symbol, probability in zip(SYMBOLS, probabilities):
        alpha = float(probability - 0.5)
        percentile = ranks[symbol]["percentile"]
        if percentile >= 0.8:
            direction = "UP_RELATIVE"
        elif percentile <= 0.2:
            direction = "DOWN_RELATIVE"
        else:
            direction = "NEUTRAL_RELATIVE"
        items.append(
            {
                "symbol": symbol,
                "p_residual_up": float(probability),
                "relative_edge_pp": round(alpha * 100.0, 4),
                "probability_semantics": "RELATIVE_OUTPERFORMANCE_VS_CROSS_SECTIONAL_MEDIAN",
                "alpha_semantics": "P_RELATIVE_OUTPERFORMANCE_MINUS_50PP",
                "rank": ranks[symbol]["rank"],
                "percentile": percentile,
                "direction": direction,
                "horizon_days": HORIZON_DAYS,
                "validated_research": evidence_bound_to_live,
                "evidence_age_hours": evidence_age_hours,
                "evidence_binding": "BOUND_TO_LIVE_CANONICAL_DATASET" if evidence_bound_to_live else "NOT_BOUND_TO_LIVE_DATASET",
            }
        )

    items.sort(key=lambda x: x["p_residual_up"], reverse=True)
    return {
        "status": "READY",
        "research_score_status": "VALIDATED_RESEARCH_BOUND" if evidence_bound_to_live else "UNVALIDATED_LIVE_REFIT",
        "score_semantics": {
            "primary_forecast": False,
            "probability": "RELATIVE_OUTPERFORMANCE_VS_CROSS_SECTIONAL_MEDIAN",
            "alpha": "P_RELATIVE_OUTPERFORMANCE_MINUS_50PP",
            "display_rule": "NEVER_PRESENT_AS_ABSOLUTE_DIRECTIONAL_P_UP",
        },
        "model": "fixed-pooled-logit-v1",
        "data_fabric": "CANONICAL_DAILY_V1",
        "features": list(FEATURE_NAMES),
        "l2": L2,
        "horizon_days": HORIZON_DAYS,
        "point_in_time": True,
        "selection_trials": 1,
        "common_dates": len(common_dates),
        "latest_date": common_dates[-1],
        "training_rows": len(rows),
        "training_anchors": max(0, training_anchors - skipped_anchors),
        "skipped_anchors": skipped_anchors,
        "live_dataset_sha256": live_dataset_sha256,
        "latest_dates": latest_dates,
        "coverage": {
            "required_symbols": list(SYMBOLS),
            "missing_symbols": [],
            "common_dates": len(common_dates),
        },
        "items": items,
        "evidence": {
            "validation_status": evidence.get("validation_status"),
            "validation_reasons": evidence.get("validation_reasons", []),
            "trading_validation_status": evidence.get("trading_validation_status"),
            "generated_at": generated_at,
            "age_hours": evidence_age_hours,
            "bound_to_live_dataset": evidence_bound_to_live,
            "model_identity_matches": evidence_model_identity_matches,
            "dataset_identity_matches": evidence_dataset_identity_matches,
            "evidence_dataset_sha256": evidence_dataset_sha256,
            "live_dataset_sha256": live_dataset_sha256,
        },
        "research_only": True,
        "no_execution_authority": True,
    }
