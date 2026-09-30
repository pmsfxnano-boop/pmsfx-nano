"""Immutable research protocol for the Crypto quantitative cleanroom.

The protocol is executable metadata, not documentation. Any empirical result intended
for promotion must bind to the exact protocol hash and runtime capture session.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class CryptoStudyProtocol:
    study_id: str = "crypto-binance-spot-prospective-v2"
    version: str = "2"
    provider: str = "binance"
    venue: str = "binance_spot"
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    streams: tuple[str, ...] = ("trade", "bookTicker")
    prospect_days: int = 7
    min_trade_rows_per_symbol: int = 100_000
    max_p99_transport_latency_ms: float = 5_000.0
    purge_ms: int = 5_000
    embargo_ms: int = 5_000
    long_threshold: float = 0.55
    short_threshold: float = 0.45
    base_cost_bps: float = 1.0
    base_slippage_bps: float = 1.0
    stress_scenarios: tuple[tuple[str, float, float], ...] = (
        ("base", 1.0, 1.0),
        ("stress_1", 2.0, 2.0),
        ("stress_2", 4.0, 4.0),
    )
    forecast_horizons_ms: tuple[int, ...] = (100, 250, 500, 1_000, 2_000, 5_000)
    horizon_alignment_tolerance_ms: int = 50
    refractory_seconds: float = 1.0
    placebo_block_size: int = 20
    placebo_iterations: int = 2_000
    multiple_testing_alpha: float = 0.05
    declared_hypothesis_family_size: int = 36
    candidate_ridge_alphas: tuple[float, ...] = (0.1, 1.0, 10.0)
    cscv_groups: int = 6
    cscv_test_groups: int = 3
    min_cscv_candidates: int = 2

    def validate(self) -> None:
        if self.provider != "binance" or self.venue != "binance_spot":
            raise ValueError("preregistered crypto study is Binance Spot only")
        if self.symbols != ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
            raise ValueError("study universe is immutable")
        if self.streams != ("trade", "bookTicker"):
            raise ValueError("study streams are immutable")
        if self.prospect_days < 1:
            raise ValueError("prospective duration must be positive")
        if self.min_trade_rows_per_symbol < 100_000:
            raise ValueError("minimum trade rows cannot be reduced")
        if self.max_p99_transport_latency_ms != 5_000.0:
            raise ValueError("p99 transport threshold is fixed at 5 seconds")
        if self.purge_ms != 5_000 or self.embargo_ms != 5_000:
            raise ValueError("purge/embargo are fixed at 5 seconds")
        if not 0.0 <= self.short_threshold < self.long_threshold <= 1.0:
            raise ValueError("invalid forecast thresholds")
        if self.base_cost_bps <= 0 or self.base_slippage_bps <= 0:
            raise ValueError("base friction must be explicit and positive")
        if len(self.stress_scenarios) != 3:
            raise ValueError("exactly three preregistered friction scenarios are required")
        if any(h <= 0 for h in self.forecast_horizons_ms):
            raise ValueError("forecast horizons must be positive")
        if self.horizon_alignment_tolerance_ms < 0:
            raise ValueError("horizon alignment tolerance must be non-negative")
        if self.refractory_seconds <= 0:
            raise ValueError("refractory period must be positive")
        if self.placebo_iterations < 1000:
            raise ValueError("placebo iterations are fixed at >=1000")
        if not 0.0 < self.multiple_testing_alpha < 1.0:
            raise ValueError("invalid multiple-testing alpha")
        if self.declared_hypothesis_family_size < 1:
            raise ValueError("hypothesis family size must be positive")
        if len(self.candidate_ridge_alphas) < self.min_cscv_candidates:
            raise ValueError("candidate family must contain at least two models")
        if self.cscv_groups < 4 or self.cscv_groups % 2:
            raise ValueError("CSCV group count must be even and >=4")
        if self.cscv_test_groups != self.cscv_groups // 2:
            raise ValueError("CSCV uses symmetric half-split evaluation")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    @property
    def protocol_hash(self) -> str:
        payload = json.dumps(
            self.canonical_dict(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def matches_runtime(
        self,
        *,
        provider: str,
        symbols: tuple[str, ...],
        streams: tuple[str, ...],
    ) -> bool:
        return (
            provider == self.provider
            and tuple(symbols) == self.symbols
            and tuple(streams) == self.streams
        )

    def quality_config(self) -> Mapping[str, Any]:
        self.validate()
        return {
            "min_rows_per_symbol": self.min_trade_rows_per_symbol,
            "min_duration_seconds": self.prospect_days * 24 * 3600,
            "max_p99_transport_latency_ms": self.max_p99_transport_latency_ms,
            "required_event_types": self.streams,
            "required_event_type_min_rows": {
                "trade": self.min_trade_rows_per_symbol,
                "bookTicker": 1,
            },
        }


PREREGISTERED_CRYPTO_PROTOCOL = CryptoStudyProtocol()
PREREGISTERED_CRYPTO_PROTOCOL.validate()
