from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from gorila_argentum.storage import Store
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
    output: dict[str, dict[str, float]] = {}
    for symbol in SYMBOLS:
        rows = store.recent_series(symbol, "close", limit=limit)
        values = {}
        for event_time, value in rows:
            values[_date_key(event_time)] = float(value)
        output[symbol] = values
    return output


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
    common_dates = sorted(
        set.intersection(*(set(series[s]) for s in SYMBOLS if series[s]))
    )
    if len(common_dates) <= HORIZON_DAYS + 65:
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": "COMMON_DAILY_HISTORY_TOO_SHORT",
            "observations": len(common_dates),
            "symbols": list(SYMBOLS),
            "research_only": True,
        }

    rows = []
    last_train_index = len(common_dates) - HORIZON_DAYS - 1
    for i in range(65, last_train_index + 1):
        future = np.asarray(
            [
                math.log(series[s][common_dates[i + HORIZON_DAYS]] / series[s][common_dates[i]])
                for s in SYMBOLS
            ],
            dtype=np.float64,
        )
        median_future = float(np.median(future))
        for j, symbol in enumerate(SYMBOLS):
            features = _feature(series, symbol, common_dates, i)
            residual = float(future[j] - median_future)
            rows.append(
                {
                    "features": features,
                    "target_up": int(residual > 0.0),
                    "symbol": symbol,
                }
            )

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
                "alpha": alpha,
                "rank": ranks[symbol]["rank"],
                "percentile": percentile,
                "direction": direction,
                "horizon_days": HORIZON_DAYS,
                "validated_research": evidence.get("validation_status") == "VALIDATED_RESEARCH",
                "evidence_age_hours": evidence_age_hours,
            }
        )

    items.sort(key=lambda x: x["p_residual_up"], reverse=True)
    return {
        "status": "READY",
        "model": "fixed-pooled-logit-v1",
        "features": list(FEATURE_NAMES),
        "l2": L2,
        "horizon_days": HORIZON_DAYS,
        "point_in_time": True,
        "selection_trials": 1,
        "common_dates": len(common_dates),
        "latest_date": common_dates[-1],
        "items": items,
        "evidence": {
            "validation_status": evidence.get("validation_status"),
            "validation_reasons": evidence.get("validation_reasons", []),
            "trading_validation_status": evidence.get("trading_validation_status"),
            "generated_at": generated_at,
            "age_hours": evidence_age_hours,
        },
        "research_only": True,
        "no_execution_authority": True,
    }
