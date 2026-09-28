from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .prediction import fit_logistic, predict
from .storage import Store
from .canonical_data import canonical_content_hash, canonical_content_hash_batch, canonical_daily_series

LEARNER_ID = "gorila-univariate-logit-candidate-v2"
TRAINER_VERSION = "2026-09-28"
DATA_FABRIC = "CANONICAL_DAILY_V1"
FEATURE_NAMES = ("r1", "r3", "r5", "vol5", "vol20", "z20")
LEARNING_SPEC = {
    "learner_id": LEARNER_ID,
    "trainer_version": TRAINER_VERSION,
    "feature_names": list(FEATURE_NAMES),
    "feature_horizon_days": 5,
    "default_train_size": 80,
    "default_test_size": 20,
    "gate": {
        "min_accuracy": 0.55,
        "min_brier_skill": 0.0,
        "min_oos_n": 40,
        "min_folds": 2,
    },
}
LEARNING_SPEC_HASH = hashlib.sha256(
    json.dumps(LEARNING_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

from .validation import purged_walk_forward


def build_training_dataset(store: Store, symbol: str, horizon_days: int = 5, limit: int = 5000):
    # The learner consumes only the model-facing canonical fabric. Raw
    # observations are intentionally unavailable to the training path.
    series = canonical_daily_series(store, symbol, "close", limit=limit)
    if len(series) < 140 + horizon_days:
        return {
            "status": "INSUFFICIENT_DATA",
            "samples": 0,
            "dataset_hash": None,
            "data_fabric": DATA_FABRIC,
        }

    values = [float(v) for _, v in series]
    times = [str(t) for t, _ in series]
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
        "data_fabric": DATA_FABRIC,
        "feature_names": list(FEATURE_NAMES),
        "rows": rows,
    }


def build_learning_context(
    store: Store,
    symbols: tuple[str, ...],
    *,
    horizon_days: int = 5,
) -> dict[str, dict[str, Any]]:
    """Prepare all learner cache keys with three indexed/batched reads."""
    symbols = tuple(str(symbol).upper() for symbol in symbols)
    canonical_hashes = canonical_content_hash_batch(store, symbols, "close")
    feedback = store.shadow_feedback_fingerprint_batch(
        model_version=LEARNER_ID,
        symbols=symbols,
        horizon_seconds=int(horizon_days * 24 * 3600),
    )
    latest = store.latest_learning_batch(symbols, limit_per_symbol=1)
    return {
        symbol: {
            "canonical_content_hash": canonical_hashes.get(symbol, ""),
            "feedback_state": feedback.get(
                symbol,
                {"hash": hashlib.sha256(b"").hexdigest(), "sample_count": 0, "last_observed_at": None},
            ),
            "previous": latest.get(symbol),
        }
        for symbol in symbols
    }


def run_learning_cycle(
    symbol: str,
    *,
    horizon_days: int = 5,
    train_size: int = 80,
    test_size: int = 20,
    store: Store | None = None,
    initialize_store: bool = True,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    store = store or Store()
    if initialize_store:
        store.init()

    state = context or {}
    canonical_hash = state.get("canonical_content_hash") or canonical_content_hash(store, symbol, "close")
    feedback_state = state.get("feedback_state") or store.shadow_feedback_fingerprint(
        model_version=LEARNER_ID,
        symbol=symbol,
        horizon_seconds=horizon_days * 24 * 3600,
    )
    feedback_hash = feedback_state["hash"]
    previous = state.get("previous")
    if context is None and previous is None:
        latest_runs = store.latest_learning(symbol=symbol, limit=1)
        previous = latest_runs[0] if latest_runs else None
    previous_result = (previous or {}).get("result") or {}
    if (
        previous
        and previous.get("dataset_hash")
        and previous_result.get("canonical_content_hash") == canonical_hash
        and previous_result.get("learning_spec_hash") == LEARNING_SPEC_HASH
        and previous_result.get("feedback_hash") == feedback_hash
    ):
        cached = dict(previous_result)
        cached.update({
            "status": cached.get("status") or "CANDIDATE_REJECTED",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "learner_id": LEARNER_ID,
            "trainer_version": TRAINER_VERSION,
            "data_fabric": DATA_FABRIC,
            "canonical_content_hash": canonical_hash,
            "learning_spec_hash": LEARNING_SPEC_HASH,
            "feedback_hash": feedback_hash,
            "feedback_sample_count": feedback_state["sample_count"],
            "feedback_last_observed_at": feedback_state["last_observed_at"],
            "reused": True,
            "reuse_reason": "CANONICAL_CONTENT_UNCHANGED",
            "generated_at": cached.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        })
        return cached

    dataset = build_training_dataset(store, symbol, horizon_days=horizon_days)
    if dataset["status"] != "READY":
        result = {
            "status": "INSUFFICIENT_DATA",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "learner_id": LEARNER_ID,
            "trainer_version": TRAINER_VERSION,
            "data_fabric": DATA_FABRIC,
            "learning_spec_hash": LEARNING_SPEC_HASH,
            "dataset_hash": dataset["dataset_hash"],
            "samples": dataset["samples"],
        }
        store.save_learning_run(symbol, horizon_days, result)
        return result

    if (
        previous
        and previous.get("dataset_hash") == dataset["dataset_hash"]
        and (previous.get("result") or {}).get("learning_spec_hash") == LEARNING_SPEC_HASH
        and (previous.get("result") or {}).get("feedback_hash") == feedback_hash
    ):
        cached = dict(previous.get("result") or {})
        cached.update({
            "status": cached.get("status") or "CANDIDATE_REJECTED",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "learner_id": LEARNER_ID,
            "trainer_version": TRAINER_VERSION,
            "data_fabric": DATA_FABRIC,
            "dataset_hash": dataset["dataset_hash"],
            "canonical_content_hash": canonical_hash,
            "learning_spec_hash": LEARNING_SPEC_HASH,
            "feedback_hash": feedback_hash,
            "feedback_sample_count": feedback_state["sample_count"],
            "feedback_last_observed_at": feedback_state["last_observed_at"],
            "reused": True,
            "reuse_reason": "DATASET_UNCHANGED",
            "generated_at": cached.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        })
        return cached

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
            "learner_id": LEARNER_ID,
            "trainer_version": TRAINER_VERSION,
            "data_fabric": DATA_FABRIC,
            "learning_spec_hash": LEARNING_SPEC_HASH,
            "dataset_hash": dataset["dataset_hash"],
            "samples": dataset["samples"],
            "validation": validation,
        }
        store.save_learning_run(symbol, horizon_days, result)
        return result

    model = fit_logistic(X, y)

    latest_series = canonical_daily_series(store, symbol, "close", limit=25)
    latest_values = [float(v) for _, v in latest_series]
    if len(latest_values) < 21 or any(v <= 0 for v in latest_values[-21:]):
        result = {
            "status": "LATEST_STATE_INSUFFICIENT",
            "symbol": symbol,
            "horizon_days": horizon_days,
            "learner_id": LEARNER_ID,
            "trainer_version": TRAINER_VERSION,
            "data_fabric": DATA_FABRIC,
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
    model_canonical = json.dumps(
        model_payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    model_hash = hashlib.sha256(model_canonical).hexdigest()
    candidate_pass = candidate_pass and (
        int(validation.get("oos_n", 0)) >= 40
        and len(validation.get("folds", [])) >= 2
    )
    result = {
        "status": "CANDIDATE_ELIGIBLE" if candidate_pass else "CANDIDATE_REJECTED",
        "symbol": symbol,
        "horizon_days": horizon_days,
        "learner_id": LEARNER_ID,
        "trainer_version": TRAINER_VERSION,
        "data_fabric": DATA_FABRIC,
        "feature_names": list(FEATURE_NAMES),
        "dataset_hash": dataset["dataset_hash"],
        "canonical_content_hash": canonical_hash,
        "learning_spec_hash": LEARNING_SPEC_HASH,
        "learning_spec": LEARNING_SPEC,
        "feedback_hash": feedback_hash,
        "feedback_sample_count": feedback_state["sample_count"],
        "feedback_last_observed_at": feedback_state["last_observed_at"],
        "samples": dataset["samples"],
        "validation": validation,
        "latest_probability_up": latest_probability,
        "model": model_payload,
        "model_hash": model_hash,
        "candidate_policy": {
            "min_accuracy": 0.55,
            "min_brier_skill": 0.0,
            "min_oos_n": 40,
            "min_folds": 2,
            "automatic_promotion": False,
            "serving_model_mutation": False,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "promotion": "BLOCKED",
    }
    store.save_learning_run(symbol, horizon_days, result)
    return result
