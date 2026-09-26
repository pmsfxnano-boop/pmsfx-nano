from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .prediction import fit_logistic, predict
from .storage import Store
from .validation import purged_walk_forward


def build_training_dataset(store: Store, symbol: str, horizon_days: int = 5, limit: int = 5000):
    series = store.recent_series(symbol, "close", limit=limit)
    if len(series) < 140 + horizon_days:
        return {"status": "INSUFFICIENT_DATA", "samples": 0, "dataset_hash": None}

    values = [float(v) for _, v in series]
    times = [t for t, _ in series]
    rows = []
    for i in range(20, len(values) - horizon_days):
        past = values[: i + 1]
        if any(v <= 0 for v in past[-20:]) or values[i] <= 0 or values[i + horizon_days] <= 0:
            continue
        r1 = values[i] / values[i - 1] - 1.0
        r3 = values[i] / values[i - 3] - 1.0
        r5 = values[i] / values[i - 5] - 1.0
        w5 = past[-5:]
        w20 = past[-20:]
        mean5 = sum(w5) / 5
        mean20 = sum(w20) / 20
        vol5 = (sum(((w5[j] / w5[j - 1]) - 1.0) ** 2 for j in range(1, 5)) / 4.0) ** 0.5
        vol20 = (
            sum(((w20[j] / w20[j - 1]) - 1.0) ** 2 for j in range(1, 20)) / 19.0
        ) ** 0.5
        std20 = (
            sum((x - mean20) ** 2 for x in w20) / 20.0
        ) ** 0.5
        z20 = 0.0 if std20 <= 1e-12 else (values[i] - mean20) / std20
        label = int(values[i + horizon_days] > values[i])
        rows.append({
            "event_time": times[i],
            "label_end_time": times[i + horizon_days],
            "features": [r1, r3, r5, vol5, vol20, z20],
            "label": label,
        })

    canonical = json.dumps(
        [{"t": r["event_time"], "e": r["label_end_time"], "x": r["features"], "y": r["label"]} for r in rows],
        sort_keys=True, separators=(",", ":"),
    ).encode()
    return {
        "status": "READY" if len(rows) >= 100 else "INSUFFICIENT_DATA",
        "samples": len(rows),
        "dataset_hash": hashlib.sha256(canonical).hexdigest(),
        "rows": rows,
    }


def run_learning_cycle(
    symbol: str,
    *,
    horizon_days: int = 5,
    train_size: int = 80,
    test_size: int = 20,
    store: Store | None = None,
) -> dict[str, Any]:
    store = store or Store()
    store.init()

    dataset = build_training_dataset(store, symbol, horizon_days=horizon_days)
    if dataset["status"] != "READY":
        result = {
            "status": "INSUFFICIENT_DATA",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "dataset_hash": dataset["dataset_hash"],
            "samples": dataset["samples"],
        }
        store.save_learning_run(symbol, horizon_days, result)
        return result

    X = [row["features"] for row in dataset["rows"]]
    y = [row["label"] for row in dataset["rows"]]
    validation = purged_walk_forward(
        X, y, train_size=train_size, test_size=test_size, purge=horizon_days
    )
    if validation["status"] != "READY":
        result = {
            "status": "VALIDATION_INSUFFICIENT",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "dataset_hash": dataset["dataset_hash"],
            "samples": dataset["samples"],
            "validation": validation,
        }
        store.save_learning_run(symbol, horizon_days, result)
        return result

    model = fit_logistic(X, y)

    latest_values = [float(v) for _, v in store.recent_series(symbol, "close", limit=25)]
    if len(latest_values) < 21 or any(v <= 0 for v in latest_values[-21:]):
        result = {
            "status": "LATEST_STATE_INSUFFICIENT",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "dataset_hash": dataset["dataset_hash"],
            "samples": dataset["samples"],
            "validation": validation,
            "promotion": "BLOCKED",
        }
        store.save_learning_run(symbol, horizon_days, result)
        return result
    latest_r1 = latest_values[-1] / latest_values[-2] - 1.0
    latest_r3 = latest_values[-1] / latest_values[-4] - 1.0
    latest_r5 = latest_values[-1] / latest_values[-6] - 1.0
    latest5 = latest_values[-5:]
    latest20 = latest_values[-20:]
    latest_mean20 = sum(latest20) / 20.0
    latest_vol5 = (sum(((latest5[j] / latest5[j - 1]) - 1.0) ** 2 for j in range(1, 5)) / 4.0) ** 0.5
    latest_vol20 = (sum(((latest20[j] / latest20[j - 1]) - 1.0) ** 2 for j in range(1, 20)) / 19.0) ** 0.5
    latest_std20 = (sum((x - latest_mean20) ** 2 for x in latest20) / 20.0) ** 0.5
    latest_z20 = 0.0 if latest_std20 <= 1e-12 else (latest_values[-1] - latest_mean20) / latest_std20
    latest_features = [latest_r1, latest_r3, latest_r5, latest_vol5, latest_vol20, latest_z20]
    latest_probability = predict(model, latest_features)
    model_payload = {
        "mean": model.mean,
        "scale": model.scale,
        "weights": model.weights,
        "bias": model.bias,
    }
    candidate_pass = (
        validation["accuracy"] >= 0.55
        and validation["brier_skill"] is not None
        and validation["brier_skill"] >= 0.0
    )
    result = {
        "status": "CANDIDATE_ELIGIBLE" if candidate_pass else "CANDIDATE_REJECTED",
        "symbol": symbol,
        "horizon_days": horizon_days,
        "dataset_hash": dataset["dataset_hash"],
        "samples": dataset["samples"],
        "validation": validation,
        "latest_probability_up": latest_probability,
        "model": model_payload,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "promotion": "BLOCKED",
    }
    store.save_learning_run(symbol, horizon_days, result)
    return result
