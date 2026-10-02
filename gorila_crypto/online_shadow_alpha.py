"""Online, PIT-safe alpha shadow plane for the Cryptonita research cohort.

This module never promotes a strategy and never touches the hot market API path.
It continuously records a sparse stream of candidate forecasts and resolves their
future outcomes from already-observed market events. Once enough historical labels
exist, it fits a ridge-logistic shadow model using only outcomes known before the
current prediction time.

The formal 7-day PIT/OOS promotion gate remains authoritative and independent.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .forecast import (
    DetectionFeatureSnapshot,
    FORECAST_SEMANTICS,
    MICROSTRUCTURE_FEATURES,
    FEATURE_SET_VERSION,
    build_microstructure_feature_vector,
    feature_set_hash,
)
from .market_cache import MARKET_CACHE
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .quant_store import QuantCryptoStore
from .validation import (
    ForecastDatasetRow,
    ForecastLabel,
    WalkForwardConfig,
    fit_ridge_logistic,
    predict_probability,
)


SOURCE = "gorila.crypto.online_shadow_alpha"
MODEL_ID = "crypto-online-microstructure-ridge-shadow-v1"
MAX_PENDING_LOOKBACK_SECONDS = 30.0
# Shadow-only economic dead zone. Returns whose absolute move does not
# overcome the preregistered base round-trip friction are neutral for online
# model training; the formal v6 PIT/OOS label contract remains unchanged.
SHADOW_NEUTRAL_BPS = (
    float(PREREGISTERED_CRYPTO_PROTOCOL.base_cost_bps)
    + float(PREREGISTERED_CRYPTO_PROTOCOL.base_slippage_bps)
)
SHADOW_MAX_P95_LATENCY_RATIO = 0.75
HORIZON_CYCLE_MS = PREREGISTERED_CRYPTO_PROTOCOL.forecast_horizons_ms


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bps(current: float, reference: float) -> float:
    if current <= 0 or reference <= 0:
        raise ValueError("prices must be positive")
    return 10_000.0 * math.log(current / reference)


def _event_map(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in events:
        result.setdefault(str(row["symbol"]).upper(), []).append(row)
    for rows in result.values():
        rows.sort(key=lambda item: (
            _dt(item["received_time"]),
            _dt(item["event_time"]),
            int(item.get("stream_seq") or 0),
        ))
    return result


def _latest_before(
    rows: list[dict[str, Any]],
    *,
    decision_event: datetime,
    decision_received: datetime,
) -> dict[str, Any] | None:
    candidate = None
    for row in rows:
        et = _dt(row["event_time"])
        rt = _dt(row["received_time"])
        if et <= decision_event and rt <= decision_received:
            candidate = row
    return candidate


def _prior_before(
    rows: list[dict[str, Any]],
    *,
    decision_event: datetime,
    decision_received: datetime,
    minimum_gap_seconds: float = 1.0,
) -> dict[str, Any] | None:
    cutoff = decision_event - timedelta(seconds=minimum_gap_seconds)
    candidate = None
    for row in rows:
        et = _dt(row["event_time"])
        rt = _dt(row["received_time"])
        if et <= cutoff and rt <= decision_received:
            candidate = row
    return candidate


def _flow_features(
    rows: list[dict[str, Any]],
    *,
    decision_event: datetime,
    decision_received: datetime,
) -> dict[str, float]:
    def window(seconds: float) -> tuple[float, float, int]:
        cutoff = decision_event - timedelta(seconds=seconds)
        signed = 0.0
        gross = 0.0
        count = 0
        for row in rows:
            et = _dt(row["event_time"])
            rt = _dt(row["received_time"])
            if et > cutoff and et <= decision_event and rt <= decision_received:
                quantity = float(row.get("quantity") or 0.0)
                signed += quantity if str(row.get("side")).upper() == "BUY" else -quantity
                gross += quantity
                count += 1
        return signed, gross, count

    signed_1s, gross_1s, count_1s = window(1.0)
    signed_5s, gross_5s, count_5s = window(5.0)
    return {
        "flow_imbalance_1s": signed_1s / gross_1s if gross_1s > 0 else 0.0,
        "flow_imbalance_5s": signed_5s / gross_5s if gross_5s > 0 else 0.0,
        "trade_intensity_1s": math.log1p(count_1s),
        "trade_intensity_5s": math.log1p(count_5s),
        "_gross_1s": gross_1s,
        "_gross_5s": gross_5s,
    }


def _book_features(
    book: dict[str, Any],
    *,
    decision_received: datetime,
) -> dict[str, float]:
    bid = float(book["bid"])
    ask = float(book["ask"])
    bid_qty = float(book.get("bid_qty") or 0.0)
    ask_qty = float(book.get("ask_qty") or 0.0)
    depth = bid_qty + ask_qty
    mid = 0.5 * (bid + ask)
    micro = (ask * bid_qty + bid * ask_qty) / depth if depth > 0 else mid
    age_ms = max(
        0.0,
        (decision_received - _dt(book["received_time"])).total_seconds() * 1000.0,
    )
    decay = max(float(PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds), 1.0)
    confidence = math.exp(-(age_ms / 1000.0) / decay)
    return {
        "queue_imbalance": (bid_qty - ask_qty) / depth if depth > 0 else 0.0,
        "spread_bps": ((ask / bid) - 1.0) * 10_000.0 if bid > 0 else 0.0,
        "microprice_gap_bps": ((micro / mid) - 1.0) * 10_000.0 if mid > 0 else 0.0,
        "age_ms": age_ms,
        "confidence": confidence,
    }



class OnlineShadowAlpha:
    def __init__(
        self,
        store: QuantCryptoStore,
        *,
        interval_seconds: float = 5.0,
        min_training_rows: int = 500,
        training_rows: int = 5000,
        training_interval_seconds: float = 30.0,
    ) -> None:
        self.store = store
        self.interval_seconds = float(interval_seconds)
        self.min_training_rows = int(min_training_rows)
        self.training_rows = int(training_rows)
        self.training_interval_seconds = float(training_interval_seconds)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._last_emit = 0.0
        self._last_train = 0.0
        self._horizon_index = 0
        self._sample_index = 0
        self._last_leader_event: dict[tuple[str, str], str] = {}
        self._models: dict[int, Any] = {}
        self._model_spec_hashes: dict[int, str] = {}
        self._training_rows_used: dict[int, int] = {}
        self._horizon_gates: dict[int, dict[str, Any]] = {}
        self._last_training_time: datetime | None = None
        self._last_outcome_resolve = 0.0
        self._last_session_resolve = 0.0
        self._session_id: str | None = None
        self._shadow_fingerprint = ""

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._loop,
            name="gorila-online-shadow-alpha",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=5.0)

    def _resolve_session(self) -> bool:
        if (
            self._session_id is not None
            and self._shadow_fingerprint
            and time.monotonic() - self._last_session_resolve < 15.0
        ):
            return True
        self._last_session_resolve = time.monotonic()
        self._session_id = self.store.active_capture_session(PREREGISTERED_CRYPTO_PROTOCOL.study_id)
        if not self._session_id:
            return False
        self._shadow_fingerprint = hashlib.sha256(
            f"shadow|{PREREGISTERED_CRYPTO_PROTOCOL.study_id}|{self._session_id}|{FEATURE_SET_VERSION}".encode("utf-8")
        ).hexdigest()
        return True

    def _build_candidate(self) -> dict[str, Any] | None:
        snap = MARKET_CACHE.research_snapshot(
            symbols=PREREGISTERED_CRYPTO_PROTOCOL.symbols,
            trade_limit=2000,
        )
        by_symbol = _event_map(snap["events"])
        books = {str(k).upper(): dict(v) for k, v in snap["latest_books"].items()}
        candidates: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        for leader in PREREGISTERED_CRYPTO_PROTOCOL.symbols:
            leader_rows = by_symbol.get(leader, [])
            leader_trade = leader_rows[-1] if leader_rows else None
            if leader_trade is None or leader not in books:
                continue
            decision_event = _dt(leader_trade["event_time"])
            decision_received = _dt(leader_trade["received_time"])
            if (now - decision_received).total_seconds() > 2.0:
                continue
            previous = _prior_before(
                leader_rows,
                decision_event=decision_event,
                decision_received=decision_received,
            )
            if previous is None:
                continue
            leader_return = _bps(
                float(leader_trade["price"]),
                float(previous["price"]),
            )
            leader_flow = _flow_features(
                leader_rows,
                decision_event=decision_event,
                decision_received=decision_received,
            )
            if leader_flow["_gross_1s"] <= 0.0 or leader_flow["_gross_5s"] <= 0.0:
                continue
            leader_book = _book_features(
                books[leader],
                decision_received=decision_received,
            )

            for target in PREREGISTERED_CRYPTO_PROTOCOL.symbols:
                if target == leader or target not in books:
                    continue
                target_rows = by_symbol.get(target, [])
                baseline = _latest_before(
                    target_rows,
                    decision_event=decision_event,
                    decision_received=decision_received,
                )
                target_previous = _prior_before(
                    target_rows,
                    decision_event=decision_event,
                    decision_received=decision_received,
                )
                if baseline is None or target_previous is None:
                    continue

                target_return = _bps(
                    float(baseline["price"]),
                    float(target_previous["price"]),
                )
                target_flow = _flow_features(
                    target_rows,
                    decision_event=decision_event,
                    decision_received=decision_received,
                )
                if target_flow["_gross_1s"] <= 0.0 or target_flow["_gross_5s"] <= 0.0:
                    continue
                target_book = _book_features(
                    books[target],
                    decision_received=decision_received,
                )

                direction = 1.0 if leader_return >= 0 else -1.0
                score = (
                    abs(leader_return)
                    * (0.5 + abs(leader_flow["flow_imbalance_1s"]))
                    * (0.5 + abs(leader_book["queue_imbalance"]))
                    * max(0.25, leader_book["confidence"])
                )

                source_ids = (
                    str(leader_trade["event_key"]),
                    str(previous["event_key"]),
                    str(baseline["event_key"]),
                    str(target_previous["event_key"]),
                    str(books[leader]["event_key"]),
                    str(books[target]["event_key"]),
                )

                features = build_microstructure_feature_vector(
                    leader_return_bps=leader_return,
                    leader_transport_latency_ms=max(
                        0.0,
                        (decision_received - decision_event).total_seconds() * 1000.0,
                    ),
                    target_return_bps_lookback=target_return,
                    target_information_age_ms=max(
                        0.0,
                        (decision_received - _dt(baseline["received_time"])).total_seconds() * 1000.0,
                    ),
                    target_market_age_ms=max(
                        0.0,
                        (decision_event - _dt(baseline["event_time"])).total_seconds() * 1000.0,
                    ),
                    leader_flow_imbalance_1s=leader_flow["flow_imbalance_1s"],
                    leader_flow_imbalance_5s=leader_flow["flow_imbalance_5s"],
                    leader_trade_intensity_1s=leader_flow["trade_intensity_1s"],
                    leader_trade_intensity_5s=leader_flow["trade_intensity_5s"],
                    target_flow_imbalance_1s=target_flow["flow_imbalance_1s"],
                    target_flow_imbalance_5s=target_flow["flow_imbalance_5s"],
                    target_trade_intensity_1s=target_flow["trade_intensity_1s"],
                    target_trade_intensity_5s=target_flow["trade_intensity_5s"],
                    leader_bid=float(books[leader]["bid"]),
                    leader_ask=float(books[leader]["ask"]),
                    leader_bid_qty=float(books[leader].get("bid_qty") or 0.0),
                    leader_ask_qty=float(books[leader].get("ask_qty") or 0.0),
                    target_bid=float(books[target]["bid"]),
                    target_ask=float(books[target]["ask"]),
                    target_bid_qty=float(books[target].get("bid_qty") or 0.0),
                    target_ask_qty=float(books[target].get("ask_qty") or 0.0),
                    leader_book_age_ms=leader_book["age_ms"],
                    target_book_age_ms=target_book["age_ms"],
                    bookticker_interval_seconds=float(
                        PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds
                    ),
                )
                if set(features) != set(MICROSTRUCTURE_FEATURES):
                    continue

                pair_key = (leader, target)
                previous_event_key = self._last_leader_event.get(pair_key)
                if previous_event_key == str(leader_trade["event_key"]):
                    continue

                candidates.append(
                    {
                        "leader": leader,
                        "target": target,
                        "decision_event": decision_event,
                        "decision_received": decision_received,
                        "leader_event_id": str(leader_trade["event_key"]),
                        "source_ids": source_ids,
                        "features": features,
                        "selection_score": float(direction * leader_return * (
                            leader_flow["flow_imbalance_1s"] + leader_book["queue_imbalance"]
                        )),
                        "absolute_score": float(score),
                    }
                )

        if not candidates:
            return None

        # Fixed round-robin pair sampling avoids selecting observations by their
        # realized attractiveness. This is the shadow collection rule, not an OOS
        # selection rule: formal promotion still replays the full preregistered ledger.
        pair_schedule = tuple(
            (leader, target)
            for leader in PREREGISTERED_CRYPTO_PROTOCOL.symbols
            for target in PREREGISTERED_CRYPTO_PROTOCOL.symbols
            if leader != target
        )
        by_pair = {(item["leader"], item["target"]): item for item in candidates}
        for offset in range(len(pair_schedule)):
            pair = pair_schedule[(self._sample_index + offset) % len(pair_schedule)]
            item = by_pair.get(pair)
            if item is not None:
                self._sample_index = (
                    self._sample_index + offset + 1
                ) % len(pair_schedule)
                return item
        return None

    def _training_dataset(self, cutoff: datetime, horizon_ms: int) -> list[ForecastDatasetRow]:
        conn = self.store.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.forecast_id,s.created_at,s.decision_event_time,s.decision_received_time,
                           s.horizon_ms,s.features_json,s.source_event_ids_json,
                           o.observed_at,o.observed_event_id,o.realized_signed_return_bps,o.realized_target
                    FROM crypto_forecast_shadow s
                    JOIN crypto_forecast_outcomes o ON o.forecast_id=s.forecast_id
                    WHERE o.status='RESOLVED'
                      AND o.realized_target IS NOT NULL
                      AND COALESCE((o.metadata::jsonb->>'shadow_neutral')::boolean, FALSE) = FALSE
                      AND o.observed_at::timestamptz <= %s::timestamptz
                      AND s.feature_set_hash IS NOT NULL
                      AND s.model_id=%s
                      AND s.replay_fingerprint=%s
                      AND s.horizon_ms=%s
                    ORDER BY s.decision_received_time DESC
                    LIMIT %s
                    """,
                    (
                        cutoff.isoformat(),
                        MODEL_ID,
                        self._shadow_fingerprint,
                        int(horizon_ms),
                        self.training_rows,
                    ),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        output: list[ForecastDatasetRow] = []
        for row in reversed(rows):
            try:
                features = {
                    str(k): float(v)
                    for k, v in json.loads(row[5]).items()
                }
                if set(features) != set(MICROSTRUCTURE_FEATURES):
                    continue
                decision_event = _dt(row[2])
                decision_received = _dt(row[3])
                label = ForecastLabel(
                    realized_target=int(row[10]),
                    realized_signed_return_bps=float(row[9]),
                    baseline_target_price=0.0,
                    future_target_price=0.0,
                    label_event_time=_dt(row[7]),
                    label_received_time=_dt(row[7]),
                    label_event_id=str(row[8] or ""),
                    horizon_ms=int(row[4]),
                )
                snapshot = DetectionFeatureSnapshot(
                    feature_set_version=FEATURE_SET_VERSION,
                    decision_event_time=decision_event,
                    decision_received_time=decision_received,
                    leader_symbol="SHADOW",
                    target_symbol="SHADOW",
                    leader_event_id=str(row[0]),
                    feature_values=features,
                    source_event_ids=tuple(json.loads(row[6])),
                    feature_set_hash="shadow-training",
                )
                output.append(ForecastDatasetRow(snapshot=snapshot, label=label))
            except Exception:
                continue
        return output

    @staticmethod
    def _percentile(values: list[float], q: float) -> float | None:
        if not values:
            return None
        ordered = sorted(float(v) for v in values)
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * max(0.0, min(100.0, q)) / 100.0
        lo = int(math.floor(rank))
        hi = int(math.ceil(rank))
        if lo == hi:
            return ordered[lo]
        weight = rank - lo
        return ordered[lo] * (1.0 - weight) + ordered[hi] * weight

    def _retrain_if_due(self, now: datetime) -> None:
        if time.monotonic() - self._last_train < self.training_interval_seconds:
            return
        self._last_train = time.monotonic()
        if not self._resolve_session():
            return

        self._horizon_gates = {}
        for horizon_ms in HORIZON_CYCLE_MS:
            dataset = self._training_dataset(now, int(horizon_ms))
            if len(dataset) < self.min_training_rows:
                self._horizon_gates[int(horizon_ms)] = {
                    "state": "WARMING",
                    "reason": "INSUFFICIENT_DIRECTIONAL_ROWS",
                    "directional_rows": len(dataset),
                    "required_rows": self.min_training_rows,
                }
                continue

            latencies = [
                float(row.snapshot.feature_values.get("leader_transport_latency_ms", 0.0))
                for row in dataset
            ]
            p95_latency = self._percentile(latencies, 95.0)
            max_latency = SHADOW_MAX_P95_LATENCY_RATIO * float(horizon_ms)
            if p95_latency is not None and p95_latency > max_latency:
                self._horizon_gates[int(horizon_ms)] = {
                    "state": "BLOCKED",
                    "reason": "SIGNAL_LATENCY_NOT_FEASIBLE",
                    "directional_rows": len(dataset),
                    "p95_transport_latency_ms": p95_latency,
                    "max_p95_transport_latency_ms": max_latency,
                    "max_p95_ratio": SHADOW_MAX_P95_LATENCY_RATIO,
                }
                self._models.pop(int(horizon_ms), None)
                self._model_spec_hashes.pop(int(horizon_ms), None)
                self._training_rows_used.pop(int(horizon_ms), None)
                continue

            labels = [row.label.realized_target for row in dataset]
            if len(set(labels)) < 2:
                self._horizon_gates[int(horizon_ms)] = {
                    "state": "WARMING",
                    "reason": "SINGLE_CLASS",
                    "directional_rows": len(dataset),
                }
                continue

            config = WalkForwardConfig(
                min_train_rows=1,
                test_rows=1,
                step_rows=1,
                purge_ms=5_000,
                embargo_ms=5_000,
                ridge_alpha=1.0,
                max_iterations=60,
                convergence_tol=1e-7,
            )
            model = fit_ridge_logistic(
                dataset,
                tuple(range(len(dataset))),
                MICROSTRUCTURE_FEATURES,
                config,
                model_id=f"{MODEL_ID}-h{horizon_ms}",
                version="online-v2",
            )
            self._models[int(horizon_ms)] = model
            self._model_spec_hashes[int(horizon_ms)] = model.spec_hash
            self._training_rows_used[int(horizon_ms)] = len(dataset)
            self._horizon_gates[int(horizon_ms)] = {
                "state": "READY",
                "reason": "PIT_SHADOW_HORIZON_FEASIBLE",
                "directional_rows": len(dataset),
                "p95_transport_latency_ms": p95_latency,
                "max_p95_transport_latency_ms": max_latency,
                "max_p95_ratio": SHADOW_MAX_P95_LATENCY_RATIO,
            }
        if self._models:
            self._last_training_time = now

    def _persist_forecast(
        self,
        candidate: dict[str, Any],
        horizon_ms: int,
        probability: float | None,
    ) -> str:
        forecast_id = str(uuid.uuid4())
        model_spec_hash = self._model_spec_hashes.get(int(horizon_ms))
        training_rows = self._training_rows_used.get(int(horizon_ms), 0)
        metadata = {
            "alpha_family": "microstructure",
            "feature_set_version": FEATURE_SET_VERSION,
            "selection_score": candidate["selection_score"],
            "absolute_selection_score": candidate["absolute_score"],
            "model_spec_hash": model_spec_hash,
            "training_rows": training_rows,
            "training_finished_at": self._last_training_time.isoformat() if self._last_training_time else None,
            "research_status": "SHADOW_ONLY",
        }
        conn = self.store.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO crypto_forecast_shadow(
                        forecast_id,created_at,replay_fingerprint,model_id,model_version,
                        symbol,target_symbol,leader_event_id,decision_event_time,
                        decision_received_time,horizon_ms,target_kind,semantics,
                        probability_response_positive,status,feature_set_hash,
                        features_json,source_event_ids_json,metadata
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s)
                    ON CONFLICT(forecast_id) DO NOTHING
                    """,
                    (
                        forecast_id,
                        candidate["decision_received"].isoformat(),
                        self._shadow_fingerprint,
                        MODEL_ID,
                        "online-v1" if probability is not None else "warmup-v1",
                        candidate["target"],
                        candidate["target"],
                        candidate["leader_event_id"],
                        candidate["decision_event"].isoformat(),
                        candidate["decision_received"].isoformat(),
                        int(horizon_ms),
                        "signed_return_positive",
                        FORECAST_SEMANTICS,
                        probability,
                        "SHADOW_SCORED" if probability is not None else "WARMUP",
                        feature_set_hash(
                            leader_symbol=candidate["leader"],
                            target_symbol=candidate["target"],
                            decision_event_time=candidate["decision_event"],
                            decision_received_time=candidate["decision_received"],
                            source_event_ids=tuple(candidate["source_ids"]),
                            feature_values=candidate["features"],
                        ),
                        json.dumps(candidate["features"], sort_keys=True, separators=(",", ":")),
                        json.dumps(candidate["source_ids"], separators=(",", ":")),
                        json.dumps(metadata, sort_keys=True),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        self._last_leader_event[(candidate["leader"], candidate["target"])] = candidate["leader_event_id"]
        return forecast_id

    def _resolve_outcomes(self, now: datetime) -> None:
        if time.monotonic() - self._last_outcome_resolve < 1.0:
            return
        self._last_outcome_resolve = time.monotonic()
        if not self._resolve_session():
            return
        snap = MARKET_CACHE.research_snapshot(
            symbols=PREREGISTERED_CRYPTO_PROTOCOL.symbols,
            trade_limit=2000,
        )
        by_symbol = _event_map(snap["events"])
        conn = self.store.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.forecast_id,s.target_symbol,s.leader_event_id,
                           s.decision_event_time,s.decision_received_time,s.horizon_ms,
                           s.features_json
                    FROM crypto_forecast_shadow s
                    LEFT JOIN crypto_forecast_outcomes o ON o.forecast_id=s.forecast_id
                    WHERE o.forecast_id IS NULL
                      AND s.replay_fingerprint=%s
                      AND s.created_at::timestamptz >= %s::timestamptz
                    ORDER BY s.created_at ASC
                    LIMIT 250
                    """,
                    (
                        self._shadow_fingerprint,
                        (now - timedelta(seconds=MAX_PENDING_LOOKBACK_SECONDS)).isoformat(),
                    ),
                )
                pending = cur.fetchall()

            resolved: list[tuple[str, dict[str, Any]]] = []
            for row in pending:
                forecast_id = str(row[0])
                target = str(row[1])
                decision_event = _dt(row[3])
                decision_received = _dt(row[4])
                horizon_ms = int(row[5])
                cutoff_event = decision_event + timedelta(milliseconds=horizon_ms)
                cutoff_received = decision_received + timedelta(milliseconds=horizon_ms)
                candidates = by_symbol.get(target, [])
                future = next(
                    (
                        event for event in candidates
                        if _dt(event["event_time"]) >= cutoff_event
                        and _dt(event["received_time"]) >= cutoff_received
                        and _dt(event["received_time"]) <= cutoff_received + timedelta(milliseconds=50)
                    ),
                    None,
                )
                if future is None:
                    continue
                features = json.loads(row[6])
                direction = 1.0 if float(features["leader_direction"]) >= 0 else -1.0
                baseline = _latest_before(
                    candidates,
                    decision_event=decision_event,
                    decision_received=decision_received,
                )
                if baseline is None:
                    continue
                raw_return = _bps(float(future["price"]), float(baseline["price"]))
                signed = direction * raw_return
                resolved.append(
                    (
                        forecast_id,
                        {
                            "observed_at": _dt(future["received_time"]).isoformat(),
                            "observed_event_id": str(future["event_key"]),
                            "observed_price": float(future["price"]),
                            "realized_signed_return_bps": float(signed),
                            "realized_target": 1 if signed > 0 else 0,
                            "transaction_cost_bps": float(PREREGISTERED_CRYPTO_PROTOCOL.base_cost_bps),
                            "slippage_bps": float(PREREGISTERED_CRYPTO_PROTOCOL.base_slippage_bps),
                            "status": "RESOLVED",
                            "metadata": {
                                "feature_set_version": FEATURE_SET_VERSION,
                                "actual_horizon_ms": int(
                                    round(
                                        (_dt(future["event_time"]) - decision_event).total_seconds() * 1000.0
                                    )
                                ),
                                "shadow_neutral": bool(abs(signed) < SHADOW_NEUTRAL_BPS),
                            "shadow_deadzone_bps": float(SHADOW_NEUTRAL_BPS),
                            "shadow_execution_proxy_bps": (
                                    float(PREREGISTERED_CRYPTO_PROTOCOL.base_cost_bps)
                                    + float(PREREGISTERED_CRYPTO_PROTOCOL.base_slippage_bps)
                                    + 0.5 * abs(float(features.get("target_spread_bps") or 0.0))
                                ),
                                "target_queue_imbalance_at_decision": float(
                                    features.get("target_queue_imbalance") or 0.0
                                ),
                                "target_book_confidence_at_decision": float(
                                    features.get("target_book_confidence") or 0.0
                                ),
                                "leader_transport_latency_ms": float(
                                    features.get("leader_transport_latency_ms") or 0.0
                                ),
                            },
                        },
                    )
                )

            if not resolved:
                return
            with conn.cursor() as cur:
                for forecast_id, outcome in resolved:
                    cur.execute(
                        """
                        INSERT INTO crypto_forecast_outcomes(
                            forecast_id,observed_at,observed_event_id,observed_price,
                            realized_signed_return_bps,realized_target,
                            transaction_cost_bps,slippage_bps,status,metadata
                        )
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(forecast_id) DO NOTHING
                        """,
                        (
                            forecast_id,
                            outcome["observed_at"],
                            outcome["observed_event_id"],
                            outcome["observed_price"],
                            outcome["realized_signed_return_bps"],
                            outcome["realized_target"],
                            outcome["transaction_cost_bps"],
                            outcome["slippage_bps"],
                            outcome["status"],
                            json.dumps(outcome["metadata"], sort_keys=True),
                        ),
                    )
            conn.commit()
        finally:
            conn.close()

    def snapshot(self) -> dict[str, Any]:
        return {
            "status": "READY" if self._models else "WARMING",
            "feature_set_version": FEATURE_SET_VERSION,
            "model_id": MODEL_ID,
            "model_spec_hashes": dict(self._model_spec_hashes),
            "training_rows": dict(self._training_rows_used),
            "horizon_gates": dict(self._horizon_gates),
            "last_training_time": self._last_training_time.isoformat() if self._last_training_time else None,
            "shadow_session_id": self._session_id,
            "shadow_fingerprint": self._shadow_fingerprint,
            "selection_rule": "fixed_round_robin_cross_asset_pair_schedule",
            "promotion": "SHADOW_ONLY",
        }

    def _loop_once(self) -> None:
        now = datetime.now(timezone.utc)
        self._resolve_session()
        self._resolve_outcomes(now)
        self._retrain_if_due(now)
        if time.monotonic() - self._last_emit < self.interval_seconds:
            return
        candidate = self._build_candidate()
        if candidate is None:
            return
        horizon = int(HORIZON_CYCLE_MS[self._horizon_index % len(HORIZON_CYCLE_MS)])
        self._horizon_index += 1
        probability = None
        if horizon in self._models:
            snapshot = DetectionFeatureSnapshot(
                feature_set_version=FEATURE_SET_VERSION,
                decision_event_time=candidate["decision_event"],
                decision_received_time=candidate["decision_received"],
                leader_symbol=candidate["leader"],
                target_symbol=candidate["target"],
                leader_event_id=candidate["leader_event_id"],
                feature_values=candidate["features"],
                source_event_ids=tuple(candidate["source_ids"]),
                feature_set_hash="online",
            )
            probability = predict_probability(self._models[horizon], snapshot)
        self._persist_forecast(candidate, horizon, probability)
        self._last_emit = time.monotonic()

    def _loop(self) -> None:
        self.store.register_study(PREREGISTERED_CRYPTO_PROTOCOL)
        while not self.stop_event.is_set():
            try:
                self._loop_once()
            except Exception as exc:
                try:
                    self.store.record_connection(
                        source=SOURCE,
                        status="ERROR",
                        reason=f"{type(exc).__name__}: {exc}",
                        metadata={
                            "feature_set_version": FEATURE_SET_VERSION,
                            "model_id": MODEL_ID,
                        },
                    )
                except Exception:
                    pass
            self.stop_event.wait(0.5)

