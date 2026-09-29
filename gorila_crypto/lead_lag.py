"""Shadow-only lead/lag and Opportunity Clock research engine.

This module consumes replayed ledger rows. It does not create forecasts, scores,
trade recommendations, or execution decisions. All calculations are descriptive
until later OOS/economic tests establish whether any relation survives costs and
non-stationarity.
"""

from __future__ import annotations

import hashlib
import json
import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any, Iterable, Mapping


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ms(a: datetime, b: datetime) -> float:
    return (a - b).total_seconds() * 1000.0


def _log_return_bps(current: float, reference: float) -> float:
    if current <= 0 or reference <= 0:
        raise ValueError("prices must be positive")
    return 10_000.0 * math.log(current / reference)


def _corr(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs)
    dy = sum((y - my) ** 2 for y in ys)
    if dx <= 0 or dy <= 0:
        return None
    return num / math.sqrt(dx * dy)


@dataclass(frozen=True)
class LeadLagConfig:
    lookback_seconds: float = 1.0
    shock_min_bps: float = 5.0
    response_delays_ms: tuple[int, ...] = (100, 250, 500, 1000, 2000, 5000)
    max_response_seconds: float = 10.0
    reaction_threshold_bps: float = 2.0
    convergence_fraction: float = 0.70

    def validate(self) -> None:
        if self.lookback_seconds <= 0:
            raise ValueError("lookback_seconds must be positive")
        if self.shock_min_bps <= 0:
            raise ValueError("shock_min_bps must be positive")
        if not self.response_delays_ms or any(x < 0 for x in self.response_delays_ms):
            raise ValueError("response_delays_ms must contain non-negative values")
        if self.max_response_seconds <= 0:
            raise ValueError("max_response_seconds must be positive")
        if self.reaction_threshold_bps <= 0:
            raise ValueError("reaction_threshold_bps must be positive")
        if not 0 < self.convergence_fraction <= 1:
            raise ValueError("convergence_fraction must be in (0,1]")


@dataclass(frozen=True)
class PricePoint:
    ledger_seq: int
    symbol: str
    event_time: datetime
    received_time: datetime
    price: float
    size: float | None
    event_id: str


@dataclass(frozen=True)
class LeadLagObservation:
    leader_symbol: str
    target_symbol: str
    leader_ledger_seq: int
    leader_event_time: datetime
    leader_received_time: datetime
    delay_ms: int
    leader_return_bps: float
    target_return_bps: float
    signed_target_response_bps: float
    market_lag_ms: float
    information_lag_ms: float


@dataclass(frozen=True)
class LeadLagSummary:
    leader_symbol: str
    target_symbol: str
    delay_ms: int
    n: int
    mean_signed_response_bps: float | None
    median_signed_response_bps: float | None
    correlation: float | None
    hit_rate: float | None
    median_market_lag_ms: float | None
    median_information_lag_ms: float | None


@dataclass(frozen=True)
class OpportunityClockResult:
    opportunity_id: str
    replay_fingerprint: str
    leader_symbol: str
    target_symbol: str
    leader_ledger_seq: int
    direction: int
    leader_return_bps: float
    detection_event_time: datetime
    detection_received_time: datetime
    baseline_target_price: float
    first_reaction_event_time: datetime | None
    first_reaction_received_time: datetime | None
    convergence_event_time: datetime | None
    convergence_received_time: datetime | None
    first_reaction_market_lag_ms: float | None
    first_reaction_information_lag_ms: float | None
    convergence_market_lag_ms: float | None
    convergence_information_lag_ms: float | None
    max_favorable_excursion_bps: float
    max_adverse_excursion_bps: float
    status: str


def _trade_price(row: Mapping[str, Any]) -> tuple[float, float | None] | None:
    if str(row.get("event_type") or "") != "trade":
        return None
    payload = row.get("payload") or {}
    try:
        price = float(payload["p"])
    except (KeyError, TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0:
        return None
    size = None
    if payload.get("q") is not None:
        try:
            candidate = float(payload["q"])
            if math.isfinite(candidate) and candidate >= 0:
                size = candidate
        except (TypeError, ValueError):
            pass
    return price, size


def build_price_points(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[PricePoint]]:
    """Build deterministic trade price series from ledger rows only."""
    points: dict[str, list[PricePoint]] = {}
    seen: set[str] = set()
    for row in rows:
        event_id = str(row.get("event_id") or "")
        if not event_id or event_id in seen:
            continue
        parsed = _trade_price(row)
        if parsed is None:
            continue
        try:
            point = PricePoint(
                ledger_seq=int(row["ledger_seq"]),
                symbol=str(row["symbol"]).upper(),
                event_time=_dt(row["event_time"]),
                received_time=_dt(row["received_time"]),
                price=parsed[0],
                size=parsed[1],
                event_id=event_id,
            )
        except (KeyError, TypeError, ValueError):
            continue
        seen.add(event_id)
        points.setdefault(point.symbol, []).append(point)
    for symbol, series in points.items():
        series.sort(key=lambda p: (p.event_time, p.received_time, p.ledger_seq))
    return points


def _past_available_price(
    series: list[PricePoint],
    event_time: datetime,
    received_cutoff: datetime,
    lookback_seconds: float,
) -> PricePoint | None:
    cutoff = event_time.timestamp() - lookback_seconds
    candidate: PricePoint | None = None
    for point in reversed(series):
        if point.received_time > received_cutoff:
            continue
        if point.event_time.timestamp() <= cutoff:
            candidate = point
            break
    return candidate


def _baseline_target(
    series: list[PricePoint],
    event_time: datetime,
    received_cutoff: datetime,
) -> PricePoint | None:
    candidate = None
    for point in reversed(series):
        if point.received_time > received_cutoff:
            continue
        if point.event_time <= event_time:
            candidate = point
            break
    return candidate


def _future_target(
    series: list[PricePoint],
    source_event_time: datetime,
    source_received_time: datetime,
    target_time: datetime,
) -> PricePoint | None:
    candidate = None
    for point in series:
        if point.event_time < target_time:
            continue
        if point.received_time < source_received_time:
            continue
        candidate = point
        break
    return candidate


def _target_events_after_detection(
    series: list[PricePoint],
    detection_event_time: datetime,
    detection_received_time: datetime,
    expiry_time: datetime,
) -> list[PricePoint]:
    return [
        point
        for point in series
        if point.received_time >= detection_received_time
        and point.event_time >= detection_event_time
        and point.event_time <= expiry_time
    ]


def detect_leader_impulses(
    series: list[PricePoint],
    config: LeadLagConfig,
) -> list[tuple[PricePoint, float]]:
    config.validate()
    impulses: list[tuple[PricePoint, float]] = []
    for point in series:
        reference = _past_available_price(
            series, point.event_time, point.received_time, config.lookback_seconds
        )
        if reference is None:
            continue
        leader_return = _log_return_bps(point.price, reference.price)
        if abs(leader_return) < config.shock_min_bps:
            continue
        impulses.append((point, leader_return))
    return impulses


def measure_lead_lag(
    leader: list[PricePoint],
    target: list[PricePoint],
    config: LeadLagConfig,
    *,
    leader_symbol: str | None = None,
    target_symbol: str | None = None,
) -> tuple[list[LeadLagObservation], list[LeadLagSummary]]:
    config.validate()
    leader_symbol = (leader_symbol or (leader[0].symbol if leader else "")).upper()
    target_symbol = (target_symbol or (target[0].symbol if target else "")).upper()
    observations: list[LeadLagObservation] = []
    impulses = detect_leader_impulses(leader, config)
    for point, leader_return in impulses:
        baseline = _baseline_target(target, point.event_time, point.received_time)
        if baseline is None:
            continue
        direction = 1 if leader_return > 0 else -1
        for delay_ms in config.response_delays_ms:
            target_time = point.event_time.timestamp() + delay_ms / 1000.0
            future = _future_target(
                target,
                point.event_time,
                point.received_time,
                datetime.fromtimestamp(target_time, tz=timezone.utc),
            )
            if future is None:
                continue
            target_return = _log_return_bps(future.price, baseline.price)
            observations.append(
                LeadLagObservation(
                    leader_symbol=leader_symbol,
                    target_symbol=target_symbol,
                    leader_ledger_seq=point.ledger_seq,
                    leader_event_time=point.event_time,
                    leader_received_time=point.received_time,
                    delay_ms=delay_ms,
                    leader_return_bps=leader_return,
                    target_return_bps=target_return,
                    signed_target_response_bps=direction * target_return,
                    market_lag_ms=_ms(future.event_time, point.event_time),
                    information_lag_ms=_ms(future.received_time, point.received_time),
                )
            )
    summaries: list[LeadLagSummary] = []
    by_delay: dict[int, list[LeadLagObservation]] = {}
    for obs in observations:
        by_delay.setdefault(obs.delay_ms, []).append(obs)
    for delay_ms in sorted(by_delay):
        batch = by_delay[delay_ms]
        xs = [x.leader_return_bps for x in batch]
        ys = [x.target_return_bps for x in batch]
        signed = [x.signed_target_response_bps for x in batch]
        summaries.append(
            LeadLagSummary(
                leader_symbol=leader_symbol,
                target_symbol=target_symbol,
                delay_ms=delay_ms,
                n=len(batch),
                mean_signed_response_bps=sum(signed) / len(signed),
                median_signed_response_bps=median(signed),
                correlation=_corr(xs, ys),
                hit_rate=sum(1 for x in signed if x > 0) / len(signed),
                median_market_lag_ms=median(x.market_lag_ms for x in batch),
                median_information_lag_ms=median(x.information_lag_ms for x in batch),
            )
        )
    return observations, summaries


def build_opportunity_clock(
    leader: list[PricePoint],
    target: list[PricePoint],
    config: LeadLagConfig,
    replay_fingerprint: str,
    *,
    leader_symbol: str | None = None,
    target_symbol: str | None = None,
) -> list[OpportunityClockResult]:
    config.validate()
    leader_symbol = (leader_symbol or (leader[0].symbol if leader else "")).upper()
    target_symbol = (target_symbol or (target[0].symbol if target else "")).upper()
    results: list[OpportunityClockResult] = []
    for point, leader_return in detect_leader_impulses(leader, config):
        baseline = _baseline_target(target, point.event_time, point.received_time)
        if baseline is None:
            continue
        direction = 1 if leader_return > 0 else -1
        expiry_time = point.event_time.timestamp() + config.max_response_seconds
        expiry = datetime.fromtimestamp(expiry_time, tz=timezone.utc)
        events = _target_events_after_detection(
            target, point.event_time, point.received_time, expiry
        )
        first_reaction = None
        convergence = None
        mfe = 0.0
        mae = 0.0
        convergence_target = abs(leader_return) * config.convergence_fraction
        for event in events:
            signed_target = direction * _log_return_bps(event.price, baseline.price)
            mfe = max(mfe, signed_target)
            mae = min(mae, signed_target)
            if first_reaction is None and signed_target >= config.reaction_threshold_bps:
                first_reaction = event
            if convergence is None and signed_target >= max(
                config.reaction_threshold_bps, convergence_target
            ):
                convergence = event
                break
        if convergence is not None:
            status = "CONVERGED"
        elif first_reaction is not None:
            status = "PARTIALLY_RESOLVED"
        else:
            status = "EXPIRED"
        raw_id = {
            "fingerprint": replay_fingerprint,
            "leader_symbol": leader_symbol,
            "target_symbol": target_symbol,
            "leader_ledger_seq": point.ledger_seq,
            "leader_event_time": point.event_time.isoformat(),
        }
        opportunity_id = hashlib.sha256(
            json.dumps(raw_id, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:32]
        results.append(
            OpportunityClockResult(
                opportunity_id=opportunity_id,
                replay_fingerprint=replay_fingerprint,
                leader_symbol=leader_symbol,
                target_symbol=target_symbol,
                leader_ledger_seq=point.ledger_seq,
                direction=direction,
                leader_return_bps=leader_return,
                detection_event_time=point.event_time,
                detection_received_time=point.received_time,
                baseline_target_price=baseline.price,
                first_reaction_event_time=first_reaction.event_time if first_reaction else None,
                first_reaction_received_time=first_reaction.received_time if first_reaction else None,
                convergence_event_time=convergence.event_time if convergence else None,
                convergence_received_time=convergence.received_time if convergence else None,
                first_reaction_market_lag_ms=_ms(first_reaction.event_time, point.event_time) if first_reaction else None,
                first_reaction_information_lag_ms=_ms(first_reaction.received_time, point.received_time) if first_reaction else None,
                convergence_market_lag_ms=_ms(convergence.event_time, point.event_time) if convergence else None,
                convergence_information_lag_ms=_ms(convergence.received_time, point.received_time) if convergence else None,
                max_favorable_excursion_bps=mfe,
                max_adverse_excursion_bps=mae,
                status=status,
            )
        )
    return results


def summarize_opportunity_clock(results: Iterable[OpportunityClockResult]) -> dict[str, Any]:
    rows = list(results)
    if not rows:
        return {"count": 0, "status": "NO_DATA"}
    convergence_times = [x.convergence_information_lag_ms for x in rows if x.convergence_information_lag_ms is not None]
    reaction_times = [x.first_reaction_information_lag_ms for x in rows if x.first_reaction_information_lag_ms is not None]
    return {
        "count": len(rows),
        "status": "DESCRIPTIVE_SHADOW",
        "converged": sum(x.status == "CONVERGED" for x in rows),
        "partially_resolved": sum(x.status == "PARTIALLY_RESOLVED" for x in rows),
        "expired": sum(x.status == "EXPIRED" for x in rows),
        "median_first_reaction_information_lag_ms": median(reaction_times) if reaction_times else None,
        "median_convergence_information_lag_ms": median(convergence_times) if convergence_times else None,
        "median_mae_bps": median(x.max_adverse_excursion_bps for x in rows),
        "median_mfe_bps": median(x.max_favorable_excursion_bps for x in rows),
    }

@dataclass(frozen=True)
class LeadLagShadowScan:
    replay_fingerprint: str
    symbols: tuple[str, ...]
    observation_count: int
    pair_summary_count: int
    opportunity_count: int
    summaries: tuple[LeadLagSummary, ...]
    opportunities: tuple[OpportunityClockResult, ...]


def run_lead_lag_shadow(
    rows: Iterable[Mapping[str, Any]],
    replay_fingerprint: str,
    config: LeadLagConfig,
    *,
    symbols: Iterable[str] | None = None,
) -> LeadLagShadowScan:
    """Run the descriptive pairwise scan for one exact replay slice."""
    points = build_price_points(rows)
    universe = tuple(
        sorted(
            {str(symbol).upper() for symbol in symbols}
            if symbols is not None
            else points.keys()
        )
    )
    summaries: list[LeadLagSummary] = []
    opportunities: list[OpportunityClockResult] = []
    observation_count = 0

    for leader_symbol in universe:
        leader = points.get(leader_symbol) or []
        if not leader:
            continue
        for target_symbol in universe:
            if target_symbol == leader_symbol:
                continue
            target = points.get(target_symbol) or []
            if not target:
                continue
            observations, pair_summaries = measure_lead_lag(
                leader,
                target,
                config,
                leader_symbol=leader_symbol,
                target_symbol=target_symbol,
            )
            observation_count += len(observations)
            summaries.extend(pair_summaries)
            opportunities.extend(
                build_opportunity_clock(
                    leader,
                    target,
                    config,
                    replay_fingerprint,
                    leader_symbol=leader_symbol,
                    target_symbol=target_symbol,
                )
            )

    return LeadLagShadowScan(
        replay_fingerprint=replay_fingerprint,
        symbols=universe,
        observation_count=observation_count,
        pair_summary_count=len(summaries),
        opportunity_count=len(opportunities),
        summaries=tuple(summaries),
        opportunities=tuple(opportunities),
    )
