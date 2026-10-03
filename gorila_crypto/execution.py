"""Execution-cost contract for quantitative validation.

The engine distinguishes a deterministic cost proxy from a validated execution model.
A proxy may be used for diagnostics, but it can never unlock production promotion.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Mapping


@dataclass(frozen=True)
class ExecutionCostModel:
    version: str = "execution_proxy_v2"
    fee_bps: float = 1.0
    slippage_bps: float = 1.0
    spread_multiplier: float = 1.0
    validated_execution_model: bool = False

    def validate(self) -> None:
        if not self.version.strip():
            raise ValueError("execution model version cannot be empty")
        if self.fee_bps < 0 or self.slippage_bps < 0:
            raise ValueError("fees/slippage must be non-negative")
        if self.spread_multiplier < 0:
            raise ValueError("spread multiplier must be non-negative")

    @property
    def model_hash(self) -> str:
        self.validate()
        payload = asdict(self)
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()


def execution_cost_bps(
    feature_values: Mapping[str, float],
    model: ExecutionCostModel,
) -> float:
    model.validate()
    spread = max(0.0, float(feature_values.get("target_spread_bps") or 0.0))
    # A round-trip market-order proxy charges one full quoted spread plus
    # explicit fees/slippage. This is deliberately conservative relative to
    # a mid-to-mid return and is not treated as a validated fill simulator.
    return (
        model.fee_bps
        + model.slippage_bps
        + model.spread_multiplier * spread
    )
