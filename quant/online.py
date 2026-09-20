"""PMSF-X Nano — Online predictive engine.

This module turns repeated live market snapshots into an auditable,
time-forward binary classifier. It is intentionally conservative:
the model never trains on a sample before its future outcome is known.

The engine is process-local. A persistent database can be added later;
the current goal is to make the full capture -> label -> train -> evaluate
-> forecast loop real and inspectable.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import math
import time
from typing import Any


HORIZON_SECONDS = 60.0
LABEL_THRESHOLD_BPS = 0.5
MIN_EXPERIMENTAL_SAMPLES = 30
MIN_VALIDATION_SAMPLES = 120
MAX_SAMPLES = 600
LEARNING_RATE = 0.08
TRAIN_STEPS = 180
L2 = 0.02


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def _log_loss(p: float, y: int) -> float:
    p = _clamp(p, 1e-6, 1 - 1e-6)
    return -(y * math.log(p) + (1 - y) * math.log(1 - p))


@dataclass
class PendingSample:
    created_at: float
    mid: float
    features: list[float]


@dataclass
class ResolvedSample:
    created_at: float
    features: list[float]
    label: int
    future_return_bps: float


class OnlineFlowEngine:
    def __init__(self) -> None:
        self._pending: dict[str, deque[PendingSample]] = defaultdict(
            lambda: deque(maxlen=MAX_SAMPLES)
        )
        self._resolved: dict[str, deque[ResolvedSample]] = defaultdict(
            lambda: deque(maxlen=MAX_SAMPLES)
        )
        self._previous: dict[str, tuple[float, float]] = {}
        self._models: dict[str, list[float]] = {}

    @staticmethod
    def _features(
        *,
        last: float,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        spread_bps: float,
        microprice: float | None,
        previous: tuple[float, float] | None,
    ) -> tuple[float, list[float], float]:
        mid = (bid + ask) / 2.0
        total = bid_size + ask_size
        imbalance = (bid_size - ask_size) / total if total > 0 else 0.0
        micro_delta_bps = (
            (microprice - mid) / mid * 10000.0 if microprice is not None else 0.0
        )
        last_delta_bps = (last - mid) / mid * 10000.0
        price_return_bps = 0.0
        imbalance_change = 0.0

        if previous is not None:
            previous_mid, previous_imbalance = previous
            if previous_mid > 0:
                price_return_bps = (mid - previous_mid) / previous_mid * 10000.0
            imbalance_change = imbalance - previous_imbalance

        x = [
            imbalance,
            _clamp(micro_delta_bps / 5.0, -2.0, 2.0),
            _clamp(last_delta_bps / 5.0, -2.0, 2.0),
            _clamp(spread_bps / 10.0, 0.0, 3.0),
            _clamp(price_return_bps / 5.0, -2.0, 2.0),
            _clamp(imbalance_change, -2.0, 2.0),
        ]
        return mid, x, imbalance

    @staticmethod
    def _fit(samples: list[ResolvedSample]) -> list[float]:
        weights = [0.0] * 7
        n = len(samples)
        if n == 0:
            return weights

        for _ in range(TRAIN_STEPS):
            grad = [0.0] * 7
            for sample in samples:
                z = weights[0]
                for j, feature in enumerate(sample.features, start=1):
                    z += weights[j] * feature
                p = _sigmoid(z)
                error = p - sample.label
                grad[0] += error
                for j, feature in enumerate(sample.features, start=1):
                    grad[j] += error * feature
            for j in range(7):
                grad[j] /= n
                if j > 0:
                    grad[j] += L2 * weights[j]
                weights[j] -= LEARNING_RATE * grad[j]
        return weights

    @staticmethod
    def _predict(weights: list[float], features: list[float]) -> float:
        z = weights[0]
        for j, feature in enumerate(features, start=1):
            z += weights[j] * feature
        return _sigmoid(z)

    def _evaluate(self, samples: list[ResolvedSample]) -> dict[str, Any]:
        n = len(samples)
        if n < MIN_VALIDATION_SAMPLES:
            return {
                "sample_count": n,
                "test_count": 0,
                "accuracy": None,
                "brier": None,
                "baseline_brier": None,
                "log_loss": None,
                "baseline_log_loss": None,
                "brier_skill": None,
                "validated": False,
                "validation_reason": "INSUFFICIENT_SAMPLES",
            }

        split = max(1, int(n * 0.8))
        train = samples[:split]
        test = samples[split:]
        if (
            len(train) < 2
            or len(test) < 24
            or len({s.label for s in train}) < 2
            or len({s.label for s in test}) < 2
        ):
            return {
                "sample_count": n,
                "test_count": len(test),
                "accuracy": None,
                "brier": None,
                "baseline_brier": None,
                "log_loss": None,
                "baseline_log_loss": None,
                "brier_skill": None,
                "validated": False,
                "validation_reason": "INVALID_TEMPORAL_SPLIT",
            }

        weights = self._fit(train)
        predictions = [self._predict(weights, s.features) for s in test]
        labels = [s.label for s in test]
        train_rate = sum(s.label for s in train) / len(train)
        baseline_predictions = [train_rate] * len(labels)

        accuracy = sum(
            (1 if p >= 0.5 else 0) == y
            for p, y in zip(predictions, labels)
        ) / len(test)
        brier = sum((p - y) ** 2 for p, y in zip(predictions, labels)) / len(test)
        baseline_brier = sum(
            (p - y) ** 2 for p, y in zip(baseline_predictions, labels)
        ) / len(test)
        log_loss = sum(
            _log_loss(p, y) for p, y in zip(predictions, labels)
        ) / len(test)
        baseline_log_loss = sum(
            _log_loss(p, y) for p, y in zip(baseline_predictions, labels)
        ) / len(test)
        brier_skill = 1.0 - (brier / baseline_brier) if baseline_brier > 0 else None

        validated = (
            n >= MIN_VALIDATION_SAMPLES
            and accuracy >= 0.55
            and brier < baseline_brier
        )

        return {
            "sample_count": n,
            "test_count": len(test),
            "accuracy": round(accuracy, 4),
            "brier": round(brier, 5),
            "baseline_brier": round(baseline_brier, 5),
            "log_loss": round(log_loss, 5),
            "baseline_log_loss": round(baseline_log_loss, 5),
            "brier_skill": round(brier_skill, 5) if brier_skill is not None else None,
            "train_base_rate": round(train_rate, 5),
            "validated": validated,
            "validation_reason": "PASS" if validated else "METRICS_BELOW_THRESHOLD",
        }

    def observe(
        self,
        *,
        symbol: str,
        last: Any,
        bid: Any,
        ask: Any,
        bid_size: Any,
        ask_size: Any,
        spread_bps: Any,
        microprice: Any,
    ) -> dict[str, Any]:
        last_f = _safe_float(last)
        bid_f = _safe_float(bid)
        ask_f = _safe_float(ask)
        bid_size_f = _safe_float(bid_size) or 0.0
        ask_size_f = _safe_float(ask_size) or 0.0
        spread_f = _safe_float(spread_bps)
        micro_f = _safe_float(microprice)

        empty_evaluation = self._evaluate(list(self._resolved[symbol]))
        if None in (last_f, bid_f, ask_f, spread_f) or bid_f <= 0 or ask_f <= 0:
            return self._response(
                symbol=symbol,
                status="INSUFFICIENT_DATA",
                forecast=None,
                evaluation=empty_evaluation,
            )

        now = time.time()
        mid, features, imbalance = self._features(
            last=last_f,
            bid=bid_f,
            ask=ask_f,
            bid_size=bid_size_f,
            ask_size=ask_size_f,
            spread_bps=spread_f,
            microprice=micro_f,
            previous=self._previous.get(symbol),
        )

        # Resolve old samples using a genuinely future observation.
        pending = self._pending[symbol]
        still_pending: deque[PendingSample] = deque(maxlen=MAX_SAMPLES)
        resolved = self._resolved[symbol]

        for sample in pending:
            if now - sample.created_at < HORIZON_SECONDS:
                still_pending.append(sample)
                continue

            future_return_bps = (mid - sample.mid) / sample.mid * 10000.0
            if abs(future_return_bps) >= LABEL_THRESHOLD_BPS:
                resolved.append(
                    ResolvedSample(
                        created_at=sample.created_at,
                        features=sample.features,
                        label=1 if future_return_bps > 0 else 0,
                        future_return_bps=future_return_bps,
                    )
                )
        self._pending[symbol] = still_pending

        self._previous[symbol] = (mid, imbalance)
        self._pending[symbol].append(
            PendingSample(created_at=now, mid=mid, features=features)
        )

        resolved_list = list(resolved)
        evaluation = self._evaluate(resolved_list)

        if len(resolved_list) < MIN_EXPERIMENTAL_SAMPLES:
            return self._response(
                symbol=symbol,
                status="WARMUP",
                forecast=None,
                evaluation=evaluation,
                extra={"pending_samples": len(self._pending[symbol])},
            )

        weights = self._fit(resolved_list)
        self._models[symbol] = weights
        p_up = self._predict(weights, features)
        p_down = 1.0 - p_up
        confidence = abs(p_up - 0.5) * 2.0

        if p_up >= 0.55:
            direction = "UP"
        elif p_down >= 0.55:
            direction = "DOWN"
        else:
            direction = "NEUTRAL"

        model_status = "VALIDATED" if evaluation["validated"] else "EXPERIMENTAL"
        armed = (
            evaluation["validated"]
            and confidence >= 0.20
            and spread_f <= 5.0
            and direction != "NEUTRAL"
        )

        forecast = {
            "direction": direction,
            "raw_probability_up": round(p_up, 4),
            "raw_probability_down": round(p_down, 4),
            "confidence_raw": round(confidence, 4),
            "model_id": "online-logit-v1",
            "status": model_status,
            "validated": bool(evaluation["validated"]),
            "calibrated": False,
            "horizon_seconds": int(HORIZON_SECONDS),
            "label_threshold_bps": LABEL_THRESHOLD_BPS,
            "features": {
                "imbalance": round(features[0], 4),
                "micro_delta_bps_scaled": round(features[1], 4),
                "last_delta_bps_scaled": round(features[2], 4),
                "spread_bps_scaled": round(features[3], 4),
                "return_bps_scaled": round(features[4], 4),
                "imbalance_change": round(features[5], 4),
            },
        }

        return self._response(
            symbol=symbol,
            status=model_status,
            forecast=forecast,
            evaluation=evaluation,
            extra={
                "pending_samples": len(self._pending[symbol]),
                "armed": armed,
            },
        )

    @staticmethod
    def _response(
        *,
        symbol: str,
        status: str,
        forecast: dict[str, Any] | None,
        evaluation: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "symbol": symbol,
            "status": status,
            "forecast": forecast,
            "evaluation": evaluation,
            "gatillazo": (
                "ARMED"
                if forecast is not None and extra and extra.get("armed")
                else "BLOCKED_VALIDATION"
            ),
        }
        if extra:
            payload.update(extra)
        return payload


ENGINE = OnlineFlowEngine()


def observe_online(**kwargs: Any) -> dict[str, Any]:
    return ENGINE.observe(**kwargs)


def get_online_summary(symbol: str) -> dict[str, Any]:
    samples = list(ENGINE._resolved[symbol])
    evaluation = ENGINE._evaluate(samples)
    return {
        "symbol": symbol,
        "resolved_samples": len(samples),
        "pending_samples": len(ENGINE._pending[symbol]),
        "evaluation": evaluation,
        "model_id": "online-logit-v1" if symbol in ENGINE._models else None,
    }
