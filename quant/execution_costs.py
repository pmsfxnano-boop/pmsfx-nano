"""PMSF-X Nano — conservative execution-cost model for research/backtests."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CostEstimate:
    spread_bps: float
    commission_bps: float
    slippage_bps: float
    impact_bps: float
    latency_bps: float
    total_bps: float
    executable: bool
    reason: str


def estimate_execution_cost(*, bid: float | None, ask: float | None, mid: float | None,
                            order_notional: float, adv_notional: float,
                            commission_bps: float = 0.0, slippage_bps: float = 1.0,
                            latency_bps: float = 0.5, max_spread_bps: float = 50.0) -> CostEstimate:
    if bid is None or ask is None or mid is None or bid <= 0 or ask <= 0 or mid <= 0 or ask < bid:
        return CostEstimate(0, commission_bps, slippage_bps, 0, latency_bps, 0, False, "INVALID_QUOTE")
    spread_bps = (ask - bid) / mid * 10000.0
    if spread_bps > max_spread_bps:
        return CostEstimate(spread_bps, commission_bps, slippage_bps, 0, latency_bps, spread_bps + commission_bps + slippage_bps + latency_bps, False, "SPREAD_TOO_WIDE")
    participation = order_notional / max(adv_notional, 1.0)
    impact_bps = 10.0 * (participation ** 0.5) if participation > 0 else 0.0
    total = spread_bps / 2.0 + commission_bps + slippage_bps + impact_bps + latency_bps
    return CostEstimate(spread_bps, commission_bps, slippage_bps, impact_bps, latency_bps, total, True, "OK")


def net_expected_move_bps(expected_move_bps: float, cost: CostEstimate, safety_multiplier: float = 1.25) -> dict:
    required = cost.total_bps * safety_multiplier
    return {"expected_move_bps": expected_move_bps, "estimated_cost_bps": round(cost.total_bps, 4), "required_move_bps": round(required, 4), "net_edge_bps": round(expected_move_bps - required, 4), "passes_cost_gate": bool(cost.executable and expected_move_bps > required)}
