"""Adaptive Opportunity Clock intelligence for the Crypto cleanroom.

The module turns the descriptive A6 clock into a learning system without making
the capture/runtime or promotion gate depend on the learner.

Design:
    immutable ingest-order events
        -> microstructure state
        -> leader shock / opportunity lifecycle
        -> discrete-time hazard learning
        -> probability-of-reaction curve + expected reaction time
        -> delayed online updates from realized outcomes

The learner is self-supervised: labels are generated only when future target
events become observable in the ledger. No retrospective provider data is used.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

MODEL_VERSION = "opportunity-clock-intelligence-v2"
HORIZONS_MS = (100, 250, 500, 1_000, 2_000, 5_000)
SHOCK_LOOKBACK_MS = 1_000
SHOCK_THRESHOLD_BPS = 5.0
REACTION_THRESHOLD_BPS = 2.0
CONVERGENCE_FRACTION = 0.70
MAX_OPPORTUNITY_MS = 5_000
MAX_TARGET_AGE_MS = 500.0
REFRACTORY_MS = 1_000
EWMA_ALPHA = 0.08
EPS = 1e-9
BASE_COST_BPS = 1.0
BASE_SLIPPAGE_BPS = 1.0
MAX_RESPONSE_BPS = 100.0
MIN_RESPONSE_STD_BPS = 0.25

FEATURE_NAMES = (
    "leader_return_bps",
    "leader_abs_return_bps",
    "leader_imbalance",
    "leader_spread_bps",
    "leader_microprice_gap_bps",
    "leader_trade_flow_z",
    "leader_trade_intensity",
    "leader_trade_excitation",
    "leader_quote_excitation",
    "leader_queue_pressure",
    "leader_depth_activity",
    "leader_realized_vol_bps",
    "leader_transport_age_ms",
    "target_imbalance",
    "target_spread_bps",
    "target_microprice_gap_bps",
    "target_trade_flow_z",
    "target_trade_intensity",
    "target_trade_excitation",
    "target_quote_excitation",
    "target_queue_pressure",
    "target_depth_activity",
    "target_realized_vol_bps",
    "target_transport_age_ms",
    "cross_imbalance_delta",
    "cross_flow_delta",
    "cross_trade_excitation_delta",
    "cross_quote_excitation_delta",
    "cross_queue_pressure_delta",
    "cross_depth_activity_delta",
    "cross_vol_delta",
    "transport_age_delta_ms",
)


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ms(later: datetime, earlier: datetime) -> float:
    return (later - earlier).total_seconds() * 1000.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _sigmoid(x: float) -> float:
    x = max(-30.0, min(30.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def _zscore(value: float, mean: float, var: float) -> float:
    return (value - mean) / math.sqrt(max(var, 1e-8))


class EWMA:
    def __init__(self, alpha: float = EWMA_ALPHA) -> None:
        self.alpha = alpha
        self.value = 0.0
        self.initialized = False

    def update(self, value: float) -> float:
        if not self.initialized:
            self.value = float(value)
            self.initialized = True
        else:
            self.value = self.alpha * float(value) + (1.0 - self.alpha) * self.value
        return self.value

    def state(self) -> dict[str, Any]:
        return {
            "alpha": self.alpha,
            "value": self.value,
            "initialized": self.initialized,
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "EWMA":
        obj = cls(float(payload.get("alpha", EWMA_ALPHA)))
        obj.value = float(payload.get("value", 0.0))
        obj.initialized = bool(payload.get("initialized", False))
        return obj


class OnlineMoments:
    """Exponentially weighted mean/variance for non-stationary streams."""

    def __init__(self, alpha: float = 0.02) -> None:
        self.alpha = alpha
        self.mean = 0.0
        self.var = 1.0
        self.initialized = False

    def update(self, value: float) -> tuple[float, float]:
        x = float(value)
        if not self.initialized:
            self.mean = x
            self.var = max(abs(x), 1e-6) ** 2
            self.initialized = True
            return self.mean, self.var
        delta = x - self.mean
        self.mean += self.alpha * delta
        self.var = (1.0 - self.alpha) * (self.var + self.alpha * delta * delta)
        return self.mean, max(self.var, 1e-8)

    def state(self) -> dict[str, Any]:
        return {
            "alpha": self.alpha,
            "mean": self.mean,
            "var": self.var,
            "initialized": self.initialized,
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "OnlineMoments":
        obj = cls(float(payload.get("alpha", 0.02)))
        obj.mean = float(payload.get("mean", 0.0))
        obj.var = max(1e-8, float(payload.get("var", 1.0)))
        obj.initialized = bool(payload.get("initialized", False))
        return obj


class DecayIntensity:
    """Exponentially decaying event intensity, updated online in receive time."""

    def __init__(self, tau_ms: float) -> None:
        self.tau_ms = float(tau_ms)
        self.value = 0.0
        self.last_time: datetime | None = None

    def update(self, received_time: datetime, mark: float = 1.0) -> float:
        if self.last_time is not None:
            dt = max(0.0, _ms(received_time, self.last_time))
            self.value *= math.exp(-dt / max(self.tau_ms, 1e-6))
        self.value += float(mark)
        self.last_time = received_time
        return self.value

    def state(self) -> dict[str, Any]:
        return {
            "tau_ms": self.tau_ms,
            "value": self.value,
            "last_time": self.last_time.isoformat() if self.last_time else None,
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "DecayIntensity":
        obj = cls(float(payload.get("tau_ms", 250.0)))
        obj.value = float(payload.get("value", 0.0))
        obj.last_time = _dt(payload["last_time"]) if payload.get("last_time") else None
        return obj


@dataclass
class SymbolMicrostructure:
    symbol: str
    bid: float | None = None
    ask: float | None = None
    bid_size: float = 0.0
    ask_size: float = 0.0
    last_mid: float | None = None
    last_trade_price: float | None = None
    last_trade_received: datetime | None = None
    last_book_received: datetime | None = None
    last_depth_received: datetime | None = None
    last_market_received: datetime | None = None
    signed_flow: EWMA = field(default_factory=EWMA)
    flow_z_signal: EWMA = field(default_factory=EWMA)
    trade_intensity: EWMA = field(default_factory=EWMA)
    trade_excitation: DecayIntensity = field(default_factory=lambda: DecayIntensity(250.0))
    quote_excitation: DecayIntensity = field(default_factory=lambda: DecayIntensity(150.0))
    queue_pressure: EWMA = field(default_factory=EWMA)
    realized_vol: EWMA = field(default_factory=EWMA)
    depth_activity: EWMA = field(default_factory=EWMA)
    flow_moments: OnlineMoments = field(default_factory=lambda: OnlineMoments(alpha=0.02))
    recent_trades: deque[tuple[datetime, float]] = field(default_factory=lambda: deque(maxlen=256))

    def feed(self, event_type: str, payload: Mapping[str, Any], received_time: datetime) -> None:
        if event_type in {"bookTicker", "depthUpdate"}:
            if event_type == "bookTicker":
                self.last_market_received = received_time
                previous_bid_size = self.bid_size
                previous_ask_size = self.ask_size
                previous_total = previous_bid_size + previous_ask_size
                bid = _safe_float(payload.get("b"), self.bid or 0.0)
                ask = _safe_float(payload.get("a"), self.ask or 0.0)
                self.bid_size = max(0.0, _safe_float(payload.get("B"), self.bid_size))
                self.ask_size = max(0.0, _safe_float(payload.get("A"), self.ask_size))
                if previous_total > 0:
                    queue_delta = (
                        (self.bid_size - previous_bid_size)
                        - (self.ask_size - previous_ask_size)
                    ) / previous_total
                    self.queue_pressure.update(max(-1.0, min(1.0, queue_delta)))
                self.quote_excitation.update(received_time)
            else:
                # Binance diff-depth is a delta stream, not a standalone L2 snapshot.
                # Do not pretend its first level is the best level. Track activity only
                # until a proper sequence-aware depth reducer is available.
                bid_deltas = payload.get("b") or payload.get("bids") or []
                ask_deltas = payload.get("a") or payload.get("asks") or []
                update_count = len(bid_deltas) + len(ask_deltas)
                self.depth_activity.update(float(update_count))
                self.last_depth_received = received_time
                return
            if bid > 0:
                self.bid = bid
            if ask > 0:
                self.ask = ask
            self.last_book_received = received_time
            self.last_market_received = received_time
            self._update_mid()

        elif event_type == "trade":
            price = _safe_float(payload.get("p"), 0.0)
            qty = max(0.0, _safe_float(payload.get("q"), 0.0))
            if price <= 0:
                return
            aggressor_sign = -1.0 if bool(payload.get("m")) else 1.0
            signed_notional = aggressor_sign * price * qty
            self.signed_flow.update(signed_notional)
            prior_mean = self.flow_moments.mean
            prior_var = self.flow_moments.var
            raw_flow_z = (
                _zscore(signed_notional, prior_mean, prior_var)
                if self.flow_moments.initialized
                else 0.0
            )
            self.flow_moments.update(signed_notional)
            self.flow_z_signal.update(max(-12.0, min(12.0, raw_flow_z)))
            self.trade_excitation.update(received_time)

            if self.last_trade_received is not None:
                dt_ms = max(0.001, _ms(received_time, self.last_trade_received))
                self.trade_intensity.update(1000.0 / dt_ms)

            if self.last_trade_price is not None and self.last_trade_price > 0:
                ret_bps = 10_000.0 * math.log(price / self.last_trade_price)
                self.realized_vol.update(ret_bps * ret_bps)
            self.recent_trades.append((received_time, price))

            self.last_trade_price = price
            self.last_trade_received = received_time
            self.last_market_received = received_time
            if self.bid is None or self.ask is None:
                self.bid = price
                self.ask = price
            self._update_mid()

    def return_over_ms(self, now: datetime, price: float, lookback_ms: float) -> float | None:
        cutoff = max(0.0, float(lookback_ms))
        reference_price: float | None = None
        for received_at, reference in reversed(self.recent_trades):
            if _ms(now, received_at) >= cutoff:
                reference_price = reference
                break
        if reference_price is None or reference_price <= 0 or price <= 0:
            return None
        return 10_000.0 * math.log(price / reference_price)

    def _update_mid(self) -> None:
        if self.bid is not None and self.ask is not None and self.bid > 0 and self.ask >= self.bid:
            self.last_mid = 0.5 * (self.bid + self.ask)
        elif self.bid and self.bid > 0:
            self.last_mid = self.bid
        elif self.ask and self.ask > 0:
            self.last_mid = self.ask

    def state(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "last_mid": self.last_mid,
            "last_trade_price": self.last_trade_price,
            "last_trade_received": self.last_trade_received.isoformat() if self.last_trade_received else None,
            "last_book_received": self.last_book_received.isoformat() if self.last_book_received else None,
            "last_depth_received": self.last_depth_received.isoformat() if self.last_depth_received else None,
            "last_market_received": self.last_market_received.isoformat() if self.last_market_received else None,
            "signed_flow": self.signed_flow.state(),
            "flow_z_signal": self.flow_z_signal.state(),
            "trade_intensity": self.trade_intensity.state(),
            "trade_excitation": self.trade_excitation.state(),
            "quote_excitation": self.quote_excitation.state(),
            "queue_pressure": self.queue_pressure.state(),
            "realized_vol": self.realized_vol.state(),
            "depth_activity": self.depth_activity.state(),
            "flow_moments": self.flow_moments.state(),
            "recent_trades": [
                [received.isoformat(), price] for received, price in self.recent_trades
            ],
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "SymbolMicrostructure":
        obj = cls(str(payload["symbol"]))
        obj.bid = payload.get("bid")
        obj.ask = payload.get("ask")
        obj.bid_size = float(payload.get("bid_size", 0.0))
        obj.ask_size = float(payload.get("ask_size", 0.0))
        obj.last_mid = payload.get("last_mid")
        obj.last_trade_price = payload.get("last_trade_price")
        obj.last_trade_received = _dt(payload["last_trade_received"]) if payload.get("last_trade_received") else None
        obj.last_book_received = _dt(payload["last_book_received"]) if payload.get("last_book_received") else None
        obj.last_depth_received = _dt(payload["last_depth_received"]) if payload.get("last_depth_received") else None
        obj.last_market_received = _dt(payload["last_market_received"]) if payload.get("last_market_received") else None
        obj.signed_flow = EWMA.from_state(payload.get("signed_flow") or {})
        obj.flow_z_signal = EWMA.from_state(payload.get("flow_z_signal") or {})
        obj.trade_intensity = EWMA.from_state(payload.get("trade_intensity") or {})
        obj.trade_excitation = DecayIntensity.from_state(
            payload.get("trade_excitation") or {"tau_ms": 250.0}
        )
        obj.quote_excitation = DecayIntensity.from_state(
            payload.get("quote_excitation") or {"tau_ms": 150.0}
        )
        obj.queue_pressure = EWMA.from_state(payload.get("queue_pressure") or {})
        obj.realized_vol = EWMA.from_state(payload.get("realized_vol") or {})
        obj.depth_activity = EWMA.from_state(payload.get("depth_activity") or {})
        obj.flow_moments = OnlineMoments.from_state(payload.get("flow_moments") or {})
        obj.recent_trades = deque(
            [(_dt(row[0]), float(row[1])) for row in payload.get("recent_trades") or []],
            maxlen=256,
        )
        return obj

    def snapshot(self, now: datetime) -> dict[str, float]:
        bid = self.bid or 0.0
        ask = self.ask or bid
        mid = self.last_mid or (0.5 * (bid + ask) if bid > 0 and ask > 0 else max(bid, ask, self.last_trade_price or 0.0))
        spread_bps = 10_000.0 * (ask - bid) / mid if mid > 0 and ask >= bid else 0.0
        total_depth = self.bid_size + self.ask_size
        imbalance = (self.bid_size - self.ask_size) / total_depth if total_depth > 0 else 0.0
        microprice = (
            (ask * self.bid_size + bid * self.ask_size) / total_depth
            if total_depth > 0 and ask >= bid
            else mid
        )
        microprice_gap_bps = 10_000.0 * (microprice - mid) / mid if mid > 0 else 0.0
        flow_z = self.flow_z_signal.value
        vol_bps = math.sqrt(max(self.realized_vol.value, 0.0))
        intensity = max(self.trade_intensity.value, 0.0)
        transport_age_ms = _ms(now, self.last_market_received) if self.last_market_received else float("inf")
        return {
            "imbalance": float(max(-1.0, min(1.0, imbalance))),
            "spread_bps": float(max(0.0, spread_bps)),
            "microprice_gap_bps": float(max(-1000.0, min(1000.0, microprice_gap_bps))),
            "trade_flow_z": float(max(-12.0, min(12.0, flow_z))),
            "trade_intensity": float(min(1e6, intensity)),
            "trade_excitation": float(min(1e6, self.trade_excitation.value)),
            "quote_excitation": float(min(1e6, self.quote_excitation.value)),
            "queue_pressure": float(max(-1.0, min(1.0, self.queue_pressure.value))),
            "depth_activity": float(min(1e6, self.depth_activity.value)),
            "realized_vol_bps": float(min(1e6, vol_bps)),
            "transport_age_ms": float(transport_age_ms if math.isfinite(transport_age_ms) else 1e9),
            "mid": float(mid),
        }


class MicrostructureBook:
    def __init__(self) -> None:
        self.symbols: dict[str, SymbolMicrostructure] = {}

    def feed(self, symbol: str, event_type: str, payload: Mapping[str, Any], received_time: datetime) -> None:
        state = self.symbols.setdefault(symbol.upper(), SymbolMicrostructure(symbol.upper()))
        state.feed(event_type, payload, received_time)

    def snapshot(self, symbol: str, now: datetime) -> dict[str, float]:
        return self.symbols.setdefault(symbol.upper(), SymbolMicrostructure(symbol.upper())).snapshot(now)

    def price(self, symbol: str) -> float | None:
        state = self.symbols.get(symbol.upper())
        if state is None:
            return None
        return state.last_trade_price or state.last_mid

    def state(self) -> dict[str, Any]:
        return {
            "symbols": {
                symbol: state.state() for symbol, state in self.symbols.items()
            }
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "MicrostructureBook":
        book = cls()
        book.symbols = {
            str(symbol): SymbolMicrostructure.from_state(state)
            for symbol, state in (payload.get("symbols") or {}).items()
        }
        return book


class OnlineLogistic:
    """Small diagonal-Adagrad logistic learner for fast delayed online updates."""

    def __init__(self, dimension: int, learning_rate: float = 0.08, l2: float = 1e-4) -> None:
        self.dimension = dimension
        self.learning_rate = learning_rate
        self.l2 = l2
        self.weights = [0.0] * dimension
        self.grad_sq = [1e-6] * dimension
        self.updates = 0

    def score(self, x: list[float]) -> float:
        return sum(w * v for w, v in zip(self.weights, x))

    def probability(self, x: list[float]) -> float:
        return _sigmoid(self.score(x))

    def update(self, x: list[float], label: int, weight: float = 1.0) -> float:
        p = self.probability(x)
        error = (p - float(label)) * weight
        for i, value in enumerate(x):
            grad = error * value + self.l2 * self.weights[i]
            self.grad_sq[i] += grad * grad
            self.weights[i] -= self.learning_rate * grad / math.sqrt(self.grad_sq[i] + EPS)
        self.updates += 1
        return p

    def state(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension,
            "learning_rate": self.learning_rate,
            "l2": self.l2,
            "weights": self.weights,
            "grad_sq": self.grad_sq,
            "updates": self.updates,
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "OnlineLogistic":
        model = cls(
            int(payload["dimension"]),
            float(payload.get("learning_rate", 0.08)),
            float(payload.get("l2", 1e-4)),
        )
        model.weights = [float(x) for x in payload["weights"]]
        model.grad_sq = [max(EPS, float(x)) for x in payload["grad_sq"]]
        model.updates = int(payload.get("updates", 0))
        return model


class OnlineLinear:
    """Fast online linear regression head with diagonal Adagrad and residual EWMA."""

    def __init__(self, dimension: int, learning_rate: float = 0.03, l2: float = 1e-4) -> None:
        self.dimension = dimension
        self.learning_rate = learning_rate
        self.l2 = l2
        self.weights = [0.0] * dimension
        self.grad_sq = [1e-6] * dimension
        self.residual_ewma = 4.0
        self.updates = 0

    def predict(self, x: list[float]) -> float:
        return sum(w * v for w, v in zip(self.weights, x))

    def update(self, x: list[float], target: float) -> float:
        y = max(-MAX_RESPONSE_BPS, min(MAX_RESPONSE_BPS, float(target)))
        prediction = self.predict(x)
        residual = y - prediction
        for i, value in enumerate(x):
            grad = -residual * value + self.l2 * self.weights[i]
            self.grad_sq[i] += grad * grad
            self.weights[i] -= self.learning_rate * grad / math.sqrt(self.grad_sq[i] + EPS)
        alpha = 0.02
        self.residual_ewma = (
            (1.0 - alpha) * self.residual_ewma
            + alpha * residual * residual
        )
        self.updates += 1
        return prediction

    def health(self) -> dict[str, float | int]:
        return {
            "updates": self.updates,
            "residual_std_bps": max(MIN_RESPONSE_STD_BPS, math.sqrt(max(0.0, self.residual_ewma))),
        }

    def state(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension,
            "learning_rate": self.learning_rate,
            "l2": self.l2,
            "weights": self.weights,
            "grad_sq": self.grad_sq,
            "residual_ewma": self.residual_ewma,
            "updates": self.updates,
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "OnlineLinear":
        obj = cls(
            int(payload["dimension"]),
            float(payload.get("learning_rate", 0.03)),
            float(payload.get("l2", 1e-4)),
        )
        obj.weights = [float(x) for x in payload["weights"]]
        obj.grad_sq = [max(EPS, float(x)) for x in payload["grad_sq"]]
        obj.residual_ewma = max(0.0, float(payload.get("residual_ewma", 4.0)))
        obj.updates = int(payload.get("updates", 0))
        return obj


class ResponseLearner:
    """Conditional signed response head for the same reaction-time grid."""

    def __init__(self, feature_dimension: int, horizons_ms: tuple[int, ...] = HORIZONS_MS) -> None:
        self.feature_dimension = feature_dimension
        self.horizons_ms = tuple(horizons_ms)
        self.models = {int(h): OnlineLinear(feature_dimension + 1) for h in self.horizons_ms}

    def _vector(self, features: list[float], horizon_ms: int) -> list[float]:
        scale = math.log1p(horizon_ms) / math.log1p(self.horizons_ms[-1])
        return [1.0, *features, scale]

    def update(self, features: list[float], response_bps: float, duration_ms: float) -> dict[int, float]:
        duration = max(0.0, float(duration_ms))
        outputs: dict[int, float] = {}
        for horizon in self.horizons_ms:
            if duration > horizon:
                continue
            outputs[horizon] = self.models[horizon].update(
                self._vector(features, horizon),
                response_bps,
            )
        return outputs

    def predict(self, features: list[float]) -> tuple[dict[int, float], dict[int, float]]:
        means: dict[int, float] = {}
        stds: dict[int, float] = {}
        for horizon in self.horizons_ms:
            model = self.models[horizon]
            means[horizon] = max(-MAX_RESPONSE_BPS, min(MAX_RESPONSE_BPS, model.predict(self._vector(features, horizon))))
            stds[horizon] = model.health()["residual_std_bps"]
        return means, stds

    def health(self) -> dict[str, Any]:
        return {
            "updates": {str(h): self.models[h].updates for h in self.horizons_ms},
            "residual_std_bps": {str(h): self.models[h].health()["residual_std_bps"] for h in self.horizons_ms},
        }

    def state(self) -> dict[str, Any]:
        return {
            "feature_dimension": self.feature_dimension,
            "horizons_ms": list(self.horizons_ms),
            "models": {str(h): self.models[h].state() for h in self.horizons_ms},
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "ResponseLearner":
        obj = cls(int(payload["feature_dimension"]), tuple(int(x) for x in payload["horizons_ms"]))
        obj.models = {int(h): OnlineLinear.from_state(payload["models"][str(h)]) for h in obj.horizons_ms}
        return obj


@dataclass
class HazardForecast:
    pair: str
    hazards: dict[int, float]
    probability_by_horizon: dict[int, float]
    survival_by_horizon: dict[int, float]
    expected_reaction_ms: float
    conditional_response_bps: dict[int, float]
    response_std_bps: dict[int, float]
    expected_net_bps: dict[int, float]
    risk_adjusted_net_bps: dict[int, float]
    execution_drag_bps: float
    model_version: str


class DiscreteHazardLearner:
    """Learns the full reaction-time distribution, not just a direction label."""

    def __init__(self, feature_dimension: int, horizons_ms: tuple[int, ...] = HORIZONS_MS) -> None:
        self.feature_dimension = feature_dimension
        self.horizons_ms = tuple(horizons_ms)
        self.models = {
            int(h): OnlineLogistic(feature_dimension + 1)
            for h in self.horizons_ms
        }
        self.brier_ewma = {int(h): 0.25 for h in self.horizons_ms}
        self.logloss_ewma = {int(h): math.log(2.0) for h in self.horizons_ms}
        self.examples_seen = {int(h): 0 for h in self.horizons_ms}

    def _vector(self, features: list[float], horizon_ms: int) -> list[float]:
        scale = math.log1p(horizon_ms) / math.log1p(self.horizons_ms[-1])
        return [1.0, *features, scale]

    def health(self) -> dict[str, Any]:
        return {
            "examples_seen": dict(self.examples_seen),
            "brier_ewma": dict(self.brier_ewma),
            "logloss_ewma": dict(self.logloss_ewma),
            "mature_examples": min(self.examples_seen.values()) if self.examples_seen else 0,
        }

    def predict(self, features: list[float], pair: str) -> HazardForecast:
        survival = 1.0
        hazards: dict[int, float] = {}
        cdf: dict[int, float] = {}
        expected = 0.0

        previous = 0
        for horizon in self.horizons_ms:
            hazard = max(1e-5, min(1.0 - 1e-5, self.models[horizon].probability(self._vector(features, horizon))))
            hazards[horizon] = hazard
            interval = max(1, horizon - previous)
            expected += survival * hazard * (previous + interval / 2.0)
            survival *= 1.0 - hazard
            cdf[horizon] = 1.0 - survival
            previous = horizon

        return HazardForecast(
            pair=pair,
            hazards=hazards,
            probability_by_horizon=cdf,
            survival_by_horizon={h: 1.0 - cdf[h] for h in self.horizons_ms},
            expected_reaction_ms=min(float(self.horizons_ms[-1]), expected),
            model_version=MODEL_VERSION,
        )

    def update(
        self,
        features: list[float],
        *,
        event_observed: bool,
        duration_ms: float,
    ) -> dict[int, float]:
        """Update only the bins that were actually at risk under PIT censoring."""
        duration = max(0.0, float(duration_ms))
        previous = 0
        outputs: dict[int, float] = {}
        for horizon in self.horizons_ms:
            if previous > duration:
                break
            event_in_bin = event_observed and previous < duration <= horizon
            label = 1 if event_in_bin else 0
            probability = self.models[horizon].update(
                self._vector(features, horizon), label
            )
            p = max(1e-6, min(1.0 - 1e-6, probability))
            alpha = 0.02
            self.brier_ewma[horizon] = (
                (1.0 - alpha) * self.brier_ewma[horizon]
                + alpha * (p - label) ** 2
            )
            self.logloss_ewma[horizon] = (
                (1.0 - alpha) * self.logloss_ewma[horizon]
                - alpha * (label * math.log(p) + (1 - label) * math.log(1.0 - p))
            )
            self.examples_seen[horizon] += 1
            outputs[horizon] = probability
            if event_in_bin:
                break
            previous = horizon
        return outputs

    def state(self) -> dict[str, Any]:
        return {
            "feature_dimension": self.feature_dimension,
            "horizons_ms": list(self.horizons_ms),
            "models": {str(h): self.models[h].state() for h in self.horizons_ms},
            "brier_ewma": self.brier_ewma,
            "logloss_ewma": self.logloss_ewma,
            "examples_seen": self.examples_seen,
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "DiscreteHazardLearner":
        model = cls(int(payload["feature_dimension"]), tuple(int(x) for x in payload["horizons_ms"]))
        model.models = {
            int(h): OnlineLogistic.from_state(payload["models"][str(h)])
            for h in model.horizons_ms
        }
        model.brier_ewma = {
            int(h): float(v) for h, v in (payload.get("brier_ewma") or {}).items()
        } or model.brier_ewma
        model.logloss_ewma = {
            int(h): float(v) for h, v in (payload.get("logloss_ewma") or {}).items()
        } or model.logloss_ewma
        model.examples_seen = {
            int(h): int(v) for h, v in (payload.get("examples_seen") or {}).items()
        } or model.examples_seen
        return model


@dataclass
class PendingOpportunity:
    opportunity_id: str
    leader_symbol: str
    target_symbol: str
    direction: int
    leader_return_bps: float
    detected_received_time: datetime
    baseline_target_price: float
    features: list[float]
    replay_fingerprint: str
    created_at: datetime


class AdaptiveOpportunityClock:
    """Hierarchical global + pair-specific learner with online regime awareness."""

    def __init__(
        self,
        *,
        symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        horizons_ms: tuple[int, ...] = HORIZONS_MS,
    ) -> None:
        self.symbols = tuple(s.upper() for s in symbols)
        self.horizons_ms = tuple(horizons_ms)
        self.book = MicrostructureBook()
        self.global_model = DiscreteHazardLearner(len(FEATURE_NAMES), self.horizons_ms)
        self.pair_models: dict[str, DiscreteHazardLearner] = {}
        self.global_response_model = ResponseLearner(len(FEATURE_NAMES), self.horizons_ms)
        self.pair_response_models: dict[str, ResponseLearner] = {}
        self.pending: dict[str, PendingOpportunity] = {}
        self.pending_by_pair: dict[str, str] = {}
        self.last_impulse_received: dict[str, datetime] = {}
        self.events_seen = 0
        self.opportunities_started = 0
        self.training_updates = 0
        self.resolved = 0
        self.censored = 0

    def _pair(self, leader: str, target: str) -> str:
        return f"{leader.upper()}->{target.upper()}"

    def _features(self, leader: str, target: str, now: datetime, leader_return_bps: float) -> list[float]:
        l = self.book.snapshot(leader, now)
        t = self.book.snapshot(target, now)
        values = [
            max(-100.0, min(100.0, leader_return_bps)),
            max(0.0, min(100.0, abs(leader_return_bps))),
            l["imbalance"],
            max(0.0, min(100.0, l["spread_bps"])),
            max(-50.0, min(50.0, l["microprice_gap_bps"])),
            max(-6.0, min(6.0, l["trade_flow_z"])),
            min(20.0, math.log1p(l["trade_intensity"])),
            min(20.0, math.log1p(l["trade_excitation"])),
            min(20.0, math.log1p(l["quote_excitation"])),
            l["queue_pressure"],
            min(20.0, math.log1p(l["depth_activity"])),
            min(20.0, math.log1p(l["realized_vol_bps"])),
            min(1_000.0, l["transport_age_ms"]),
            t["imbalance"],
            max(0.0, min(100.0, t["spread_bps"])),
            max(-50.0, min(50.0, t["microprice_gap_bps"])),
            max(-6.0, min(6.0, t["trade_flow_z"])),
            min(20.0, math.log1p(t["trade_intensity"])),
            min(20.0, math.log1p(t["trade_excitation"])),
            min(20.0, math.log1p(t["quote_excitation"])),
            t["queue_pressure"],
            min(20.0, math.log1p(t["depth_activity"])),
            min(20.0, math.log1p(t["realized_vol_bps"])),
            min(1_000.0, t["transport_age_ms"]),
            max(-2.0, min(2.0, l["imbalance"] - t["imbalance"])),
            max(-12.0, min(12.0, l["trade_flow_z"] - t["trade_flow_z"])),
            max(-20.0, min(20.0, math.log1p(l["trade_excitation"]) - math.log1p(t["trade_excitation"]))),
            max(-20.0, min(20.0, math.log1p(l["quote_excitation"]) - math.log1p(t["quote_excitation"]))),
            max(-2.0, min(2.0, l["queue_pressure"] - t["queue_pressure"])),
            max(-10.0, min(10.0, math.log1p(l["depth_activity"]) - math.log1p(t["depth_activity"]))),
            max(-10.0, min(10.0, math.log1p(l["realized_vol_bps"]) - math.log1p(t["realized_vol_bps"]))),
            max(-1_000.0, min(1_000.0, l["transport_age_ms"] - t["transport_age_ms"])),
        ]
        return [float(x) for x in values]

    def _response_model_for(self, pair: str) -> ResponseLearner:
        if pair not in self.pair_response_models:
            self.pair_response_models[pair] = ResponseLearner(len(FEATURE_NAMES), self.horizons_ms)
        return self.pair_response_models[pair]

    def _execution_drag_bps(self, features: list[float]) -> float:
        target_spread_index = FEATURE_NAMES.index("target_spread_bps")
        target_spread_bps = max(0.0, float(features[target_spread_index]))
        return target_spread_bps + 2.0 * (BASE_COST_BPS + BASE_SLIPPAGE_BPS)

    def _model_for(self, pair: str) -> DiscreteHazardLearner:
        if pair not in self.pair_models:
            self.pair_models[pair] = DiscreteHazardLearner(len(FEATURE_NAMES), self.horizons_ms)
        return self.pair_models[pair]

    def feed_event(
        self,
        *,
        symbol: str,
        event_type: str,
        payload: Mapping[str, Any],
        event_time: datetime,
        received_time: datetime,
        event_id: str,
        replay_fingerprint: str,
        learn: bool = True,
    ) -> list[dict[str, Any]]:
        """Consume exactly one ledger event in ingest order."""
        self.events_seen += 1
        self.book.feed(symbol, event_type, payload, received_time)

        outcomes: list[dict[str, Any]] = []

        if event_type == "trade":
            price = _safe_float(payload.get("p"), 0.0)
            if price <= 0:
                return outcomes

            # First resolve target observations using the state available at receipt.
            for opportunity_id, pending in list(self.pending.items()):
                if pending.target_symbol != symbol.upper():
                    continue
                if received_time < pending.detected_received_time:
                    continue
                if pending.baseline_target_price <= 0:
                    continue
                duration = _ms(received_time, pending.detected_received_time)
                # Reactions after the modeled opportunity window are censored,
                # never positive labels.
                if duration > MAX_OPPORTUNITY_MS:
                    continue
                signed = pending.direction * 10_000.0 * math.log(price / pending.baseline_target_price)
                if signed >= REACTION_THRESHOLD_BPS:
                    outcomes.append(
                        self._resolve(
                            pending,
                            duration,
                            event_id,
                            observed=True,
                            signed_return_bps=signed,
                            learn=learn,
                        )
                    )
                    self.pending.pop(opportunity_id, None)
                    self.pending_by_pair.pop(
                        self._pair(pending.leader_symbol, pending.target_symbol),
                        None,
                    )

            # Leader impulse detection uses only the current symbol's own received order.
            state = self.book.symbols.get(symbol.upper())
            if state is None or state.last_trade_received is None:
                return outcomes
            recent_bps = state.return_over_ms(received_time, price, SHOCK_LOOKBACK_MS)
            if recent_bps is None or abs(recent_bps) < SHOCK_THRESHOLD_BPS:
                return outcomes

            last_impulse = self.last_impulse_received.get(symbol.upper())
            if last_impulse and _ms(received_time, last_impulse) < REFRACTORY_MS:
                return outcomes

            direction = 1 if recent_bps > 0 else -1
            self.last_impulse_received[symbol.upper()] = received_time

            for target in self.symbols:
                if target == symbol.upper():
                    continue
                target_price = self.book.price(target)
                target_snapshot = self.book.snapshot(target, received_time)
                if (
                    target_price is None
                    or target_price <= 0
                    or target_snapshot["transport_age_ms"] > MAX_TARGET_AGE_MS
                ):
                    continue
                pair = self._pair(symbol, target)
                if pair in self.pending_by_pair:
                    continue
                features = self._features(symbol, target, received_time, recent_bps)
                opportunity_id = hashlib.sha256(
                    json.dumps(
                        {
                            "leader_event_id": event_id,
                            "pair": pair,
                            "replay_fingerprint": replay_fingerprint,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()[:32]
                self.pending[opportunity_id] = PendingOpportunity(
                    opportunity_id=opportunity_id,
                    leader_symbol=symbol.upper(),
                    target_symbol=target,
                    direction=direction,
                    leader_return_bps=recent_bps,
                    detected_received_time=received_time,
                    baseline_target_price=target_price,
                    features=features,
                    replay_fingerprint=replay_fingerprint,
                    created_at=received_time,
                )
                self.pending_by_pair[pair] = opportunity_id
                self.opportunities_started += 1

        # Expire/censor after processing this event, preserving PIT ordering.
        for opportunity_id, pending in list(self.pending.items()):
            age = _ms(received_time, pending.detected_received_time)
            if age > MAX_OPPORTUNITY_MS:
                outcomes.append(
                    self._resolve(
                        pending,
                        MAX_OPPORTUNITY_MS,
                        None,
                        observed=False,
                        signed_return_bps=0.0,
                        learn=learn,
                    )
                )
                self.pending.pop(opportunity_id, None)
                self.pending_by_pair.pop(
                    self._pair(pending.leader_symbol, pending.target_symbol),
                    None,
                )

        return outcomes

    def _resolve(
        self,
        pending: PendingOpportunity,
        duration_ms: float,
        event_id: str | None,
        *,
        observed: bool,
        signed_return_bps: float,
        learn: bool = True,
    ) -> dict[str, Any]:
        pair = self._pair(pending.leader_symbol, pending.target_symbol)
        global_out = {}
        pair_out = {}
        if learn:
            global_out = self.global_model.update(
                pending.features,
                event_observed=observed,
                duration_ms=duration_ms,
            )
            pair_out = self._model_for(pair).update(
                pending.features,
                event_observed=observed,
                duration_ms=duration_ms,
            )
            self.training_updates += 1
        if observed:
            self.resolved += 1
        else:
            self.censored += 1
        return {
            "opportunity_id": pending.opportunity_id,
            "pair": pair,
            "leader_symbol": pending.leader_symbol,
            "target_symbol": pending.target_symbol,
            "leader_return_bps": pending.leader_return_bps,
            "duration_ms": float(duration_ms),
            "event_observed": bool(observed),
            "reaction_event_id": event_id,
            "signed_reaction_bps": float(signed_return_bps),
            "replay_fingerprint": pending.replay_fingerprint,
            "features": dict(zip(FEATURE_NAMES, pending.features)),
            "global_update_bins": global_out,
            "pair_update_bins": pair_out,
            "model_version": MODEL_VERSION,
        }

    def predict(self, leader_symbol: str, target_symbol: str, features: list[float]) -> HazardForecast:
        pair = self._pair(leader_symbol, target_symbol)
        base = self.global_model.predict(features, pair)
        specific_model = self._model_for(pair)
        specific = specific_model.predict(features, pair)

        global_health = self.global_model.health()
        pair_health = specific_model.health()
        pair_n = min(pair_health["examples_seen"].values()) if pair_health["examples_seen"] else 0
        pair_brier = sum(pair_health["brier_ewma"].values()) / len(pair_health["brier_ewma"])
        global_brier = sum(global_health["brier_ewma"].values()) / len(global_health["brier_ewma"])
        sample_weight = pair_n / (pair_n + 200.0)
        calibration_delta = global_brier - pair_brier
        calibration_bonus = max(-0.20, min(0.20, 2.0 * calibration_delta))
        pair_weight = max(0.10, min(0.90, 0.10 + 0.70 * sample_weight + calibration_bonus))
        global_weight = 1.0 - pair_weight

        hazards = {
            h: max(1e-5, min(
                1.0 - 1e-5,
                pair_weight * specific.hazards[h] + global_weight * base.hazards[h],
            ))
            for h in self.horizons_ms
        }
        survival = 1.0
        cdf: dict[int, float] = {}
        expected = 0.0
        previous = 0
        for h in self.horizons_ms:
            interval = max(1, h - previous)
            expected += survival * hazards[h] * (previous + interval / 2.0)
            survival *= 1.0 - hazards[h]
            cdf[h] = 1.0 - survival
            previous = h

        return HazardForecast(
            pair=pair,
            hazards=hazards,
            probability_by_horizon=cdf,
            survival_by_horizon={h: 1.0 - cdf[h] for h in self.horizons_ms},
            expected_reaction_ms=min(float(self.horizons_ms[-1]), expected),
            model_version=MODEL_VERSION,
        )

    def _pair_blend_weight(self, pair: str) -> float:
        pair_model = self._model_for(pair)
        pair_health = pair_model.health()
        global_health = self.global_model.health()
        pair_n = min(pair_health["examples_seen"].values()) if pair_health["examples_seen"] else 0
        pair_brier = sum(pair_health["brier_ewma"].values()) / len(pair_health["brier_ewma"])
        global_brier = sum(global_health["brier_ewma"].values()) / len(global_health["brier_ewma"])
        sample_weight = pair_n / (pair_n + 200.0)
        calibration_bonus = max(-0.20, min(0.20, 2.0 * (global_brier - pair_brier)))
        return max(0.10, min(0.90, 0.10 + 0.70 * sample_weight + calibration_bonus))

    def snapshot(self, leader_symbol: str, target_symbol: str, now: datetime, leader_return_bps: float) -> dict[str, Any]:
        features = self._features(leader_symbol, target_symbol, now, leader_return_bps)
        forecast = self.predict(leader_symbol, target_symbol, features)
        return {
            "model_version": forecast.model_version,
            "pair": forecast.pair,
            "probability_by_horizon": forecast.probability_by_horizon,
            "survival_by_horizon": forecast.survival_by_horizon,
            "hazard_by_horizon": forecast.hazards,
            "expected_reaction_ms": forecast.expected_reaction_ms,
            "events_seen": self.events_seen,
            "opportunities_started": self.opportunities_started,
            "training_updates": self.training_updates,
            "resolved": self.resolved,
            "censored": self.censored,
            "pending": len(self.pending),
            "global_calibration": {
                "brier_ewma": self.global_model.brier_ewma,
                "logloss_ewma": self.global_model.logloss_ewma,
                "examples_seen": self.global_model.examples_seen,
            },
            "pair_calibration": self._model_for(forecast.pair).health(),
            "global_model_health": self.global_model.health(),
            "blend": {
                "pair_weight": self._pair_blend_weight(forecast.pair),
                "global_weight": 1.0 - self._pair_blend_weight(forecast.pair),
            },
            "feature_schema_hash": hashlib.sha256(
                json.dumps(FEATURE_NAMES, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        }

    def state(self) -> dict[str, Any]:
        return {
            "model_version": MODEL_VERSION,
            "symbols": list(self.symbols),
            "horizons_ms": list(self.horizons_ms),
            "global_model": self.global_model.state(),
            "pair_models": {pair: model.state() for pair, model in self.pair_models.items()},
            "microstructure": self.book.state(),
            "events_seen": self.events_seen,
            "opportunities_started": self.opportunities_started,
            "training_updates": self.training_updates,
            "resolved": self.resolved,
            "censored": self.censored,
            "pending_by_pair": dict(self.pending_by_pair),
            "last_impulse_received": {symbol: dt.isoformat() for symbol, dt in self.last_impulse_received.items()},
            "pending": [
                {
                    **asdict(pending),
                    "detected_received_time": pending.detected_received_time.isoformat(),
                    "created_at": pending.created_at.isoformat(),
                }
                for pending in self.pending.values()
            ],
        }

    @classmethod
    def from_state(cls, payload: Mapping[str, Any]) -> "AdaptiveOpportunityClock":
        engine = cls(
            symbols=tuple(str(x) for x in payload.get("symbols", ("BTCUSDT", "ETHUSDT", "SOLUSDT"))),
            horizons_ms=tuple(int(x) for x in payload.get("horizons_ms", HORIZONS_MS)),
        )
        engine.global_model = DiscreteHazardLearner.from_state(payload["global_model"])
        engine.book = MicrostructureBook.from_state(payload.get("microstructure") or {})
        engine.pair_models = {
            pair: DiscreteHazardLearner.from_state(model_state)
            for pair, model_state in (payload.get("pair_models") or {}).items()
        }
        engine.events_seen = int(payload.get("events_seen", 0))
        engine.opportunities_started = int(payload.get("opportunities_started", 0))
        engine.training_updates = int(payload.get("training_updates", 0))
        engine.resolved = int(payload.get("resolved", 0))
        engine.censored = int(payload.get("censored", 0))
        engine.last_impulse_received = {
            str(symbol): _dt(value)
            for symbol, value in (payload.get("last_impulse_received") or {}).items()
        }
        for row in payload.get("pending") or []:
            pending = PendingOpportunity(
                opportunity_id=str(row["opportunity_id"]),
                leader_symbol=str(row["leader_symbol"]),
                target_symbol=str(row["target_symbol"]),
                direction=int(row["direction"]),
                leader_return_bps=float(row["leader_return_bps"]),
                detected_received_time=_dt(row["detected_received_time"]),
                baseline_target_price=float(row["baseline_target_price"]),
                features=[float(x) for x in row["features"]],
                replay_fingerprint=str(row["replay_fingerprint"]),
                created_at=_dt(row["created_at"]),
            )
            engine.pending[pending.opportunity_id] = pending
        engine.pending_by_pair = {
            str(pair): str(opportunity_id)
            for pair, opportunity_id in (payload.get("pending_by_pair") or {}).items()
            if opportunity_id in engine.pending
        }
        return engine


@dataclass(frozen=True)
class IntelligenceManifest:
    model_version: str
    feature_schema_hash: str
    horizons_ms: tuple[int, ...]
    symbols: tuple[str, ...]


def manifest() -> IntelligenceManifest:
    return IntelligenceManifest(
        model_version=MODEL_VERSION,
        feature_schema_hash=hashlib.sha256(
            json.dumps(FEATURE_NAMES, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        horizons_ms=HORIZONS_MS,
        symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    )
