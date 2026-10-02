"""Immutable research protocol for the Crypto quantitative cleanroom.

The protocol is executable metadata, not documentation. Any empirical result intended
for promotion must bind to the exact protocol hash and runtime capture session.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
import math
from typing import Any, Mapping


@dataclass(frozen=True)
class CryptoStudyProtocol:
    study_id: str = "crypto-binance-spot-prospective-v2"
    version: str = "2"
    provider: str = "binance"
    venue: str = "binance_spot"
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    streams: tuple[str, ...] = ("trade", "bookTicker")
    normalized_event_types: tuple[str, ...] = ()
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
    # v3 storage contract: the market hot plane remains lossless, while the
    # durable research ledger keeps a deterministic 5% trade sample plus
    # 1-second bookTicker snapshots. This is pre-registered to fit the 1 GB
    # free-tier Postgres envelope without changing PIT/OOS semantics.
    trade_persistence_sample_rate: float = 0.05
    bookticker_persistence_interval_seconds: float = 1.0
    persistence_contract_version: str = "deterministic_sample_v1"

    def validate(self) -> None:
        if self.provider not in {"binance", "kraken"}:
            raise ValueError(f"unsupported preregistered crypto provider: {self.provider}")
        if self.venue not in {"binance_spot", "kraken_spot"}:
            raise ValueError(f"unsupported crypto venue: {self.venue}")
        if not self.symbols or not self.streams:
            raise ValueError("study universe and streams cannot be empty")
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
        if not 0.0 < self.trade_persistence_sample_rate <= 1.0:
            raise ValueError("trade persistence sample rate must be in (0,1]")
        if self.bookticker_persistence_interval_seconds <= 0:
            raise ValueError("bookTicker persistence interval must be positive")
        if not self.persistence_contract_version.strip():
            raise ValueError("persistence contract version cannot be empty")

        if self.provider == "binance":
            if self.venue != "binance_spot":
                raise ValueError("Binance provider requires Binance Spot venue")
            if self.symbols != ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
                raise ValueError("Binance study universe is immutable")
            if self.streams != ("trade", "bookTicker"):
                raise ValueError("Binance study streams are immutable")
            if self.normalized_event_types not in {(), ("trade", "bookTicker")}:
                raise ValueError("invalid Binance normalized event types")
            if self.version in {"3", "4", "5"}:
                if self.trade_persistence_sample_rate != 0.05:
                    raise ValueError("Binance v3/v4/v5 trade persistence sampling is immutable at 5%")
            if self.version == "3":
                if self.bookticker_persistence_interval_seconds != 1.0:
                    raise ValueError("Binance v3 bookTicker persistence interval is immutable at 1s")
                if self.persistence_contract_version != "deterministic_sample_v1":
                    raise ValueError("Binance v3 persistence contract is immutable")
            if self.version == "4":
                if self.bookticker_persistence_interval_seconds != 5.0:
                    raise ValueError("Binance v4 bookTicker persistence interval is immutable at 5s")
                if self.persistence_contract_version != "deterministic_sample_v2_compact":
                    raise ValueError("Binance v4 persistence contract is immutable")
            if self.version == "5":
                if self.trade_persistence_sample_rate != 0.05:
                    raise ValueError("Binance v5 trade persistence sampling is immutable at 5%")
                if self.bookticker_persistence_interval_seconds != 5.0:
                    raise ValueError("Binance v5 bookTicker persistence interval is immutable at 5s")
                if self.persistence_contract_version != "typed_compact_v1":
                    raise ValueError("Binance v5 persistence contract is immutable")
        else:
            if self.venue != "kraken_spot":
                raise ValueError("Kraken provider requires Kraken Spot venue")
            if self.symbols != ("BTC/USD", "ETH/USD", "SOL/USD"):
                raise ValueError("Kraken study universe is immutable")
            if self.streams != ("trade", "bookTicker"):
                raise ValueError("Kraken study transport streams are immutable")
            if self.normalized_event_types != ("trade", "bookUpdate"):
                raise ValueError("Kraken normalized event types are immutable")

    def canonical_dict(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        # v2 was registered before normalized_event_types became an explicit
        # quality/runtime field. Preserve the immutable v2 study identity rather
        # than silently changing its protocol hash after data capture began.
        if self.version == "2":
            payload.pop("normalized_event_types", None)
            payload.pop("trade_persistence_sample_rate", None)
            payload.pop("bookticker_persistence_interval_seconds", None)
            payload.pop("persistence_contract_version", None)
        return payload

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
        event_types = self.normalized_event_types or self.streams
        persisted_trade_min = self.min_trade_rows_per_symbol
        if self.provider == "binance" and self.version in {"3", "4", "5"}:
            persisted_trade_min = max(
                1,
                math.ceil(
                    self.min_trade_rows_per_symbol
                    * self.trade_persistence_sample_rate
                ),
            )
        min_rows = {
            "trade": persisted_trade_min,
        }
        if "bookTicker" in event_types:
            min_rows["bookTicker"] = max(1, min_rows.get("bookTicker", 1))
        if "bookUpdate" in event_types:
            min_rows["bookUpdate"] = max(10_000, self.min_trade_rows_per_symbol // 10)
        return {
            # The durable v3/v4 ledger is a deterministic 5% sample of trades.
            # Quality operates on the persisted replay slice, so its per-symbol
            # row floor must be expressed in persisted rows while the immutable
            # protocol field remains the raw-observation floor.
            "min_rows_per_symbol": persisted_trade_min,
            "min_raw_rows_per_symbol": self.min_trade_rows_per_symbol,
            "min_duration_seconds": self.prospect_days * 24 * 3600,
            "max_p99_transport_latency_ms": self.max_p99_transport_latency_ms,
            "required_event_types": event_types,
            "required_event_type_min_rows": min_rows,
        }

BINANCE_CRYPTO_PROTOCOL = CryptoStudyProtocol(
    study_id="crypto-binance-spot-prospective-v2",
    version="2",
    provider="binance",
    venue="binance_spot",
    symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    streams=("trade", "bookTicker"),
    normalized_event_types=("trade", "bookTicker"),
)
BINANCE_CRYPTO_PROTOCOL.validate()



BINANCE_CRYPTO_PROTOCOL_V3 = CryptoStudyProtocol(
    study_id="crypto-binance-spot-prospective-v3",
    version="3",
    provider="binance",
    venue="binance_spot",
    symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    streams=("trade", "bookTicker"),
    normalized_event_types=("trade", "bookTicker"),
)
BINANCE_CRYPTO_PROTOCOL_V3.validate()

BINANCE_CRYPTO_PROTOCOL_V4 = CryptoStudyProtocol(
    study_id="crypto-binance-spot-prospective-v4",
    version="4",
    provider="binance",
    venue="binance_spot",
    symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    streams=("trade", "bookTicker"),
    normalized_event_types=("trade", "bookTicker"),
    bookticker_persistence_interval_seconds=5.0,
    persistence_contract_version="deterministic_sample_v2_compact",
)
BINANCE_CRYPTO_PROTOCOL_V4.validate()

# v5 keeps the v4 empirical universe and persistence rates unchanged, but moves
# the durable replay ledger to typed columns. This is a storage-layer change,
# not an alpha/data-selection change, and therefore gets a fresh prospective
# cohort so its exact storage contract is bound from t=0.
BINANCE_CRYPTO_PROTOCOL_V5 = CryptoStudyProtocol(
    study_id="crypto-binance-spot-prospective-v5",
    version="5",
    provider="binance",
    venue="binance_spot",
    symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
    streams=("trade", "bookTicker"),
    normalized_event_types=("trade", "bookTicker"),
    bookticker_persistence_interval_seconds=5.0,
    persistence_contract_version="typed_compact_v1",
)
BINANCE_CRYPTO_PROTOCOL_V5.validate()

KRAKEN_CRYPTO_PROTOCOL = CryptoStudyProtocol(
    study_id="crypto-kraken-spot-prospective-v1",
    version="1",
    provider="kraken",
    venue="kraken_spot",
    symbols=("BTC/USD", "ETH/USD", "SOL/USD"),
    streams=("trade", "bookTicker"),
    normalized_event_types=("trade", "bookUpdate"),
)
KRAKEN_CRYPTO_PROTOCOL.validate()

CRYPTO_PROTOCOLS: Mapping[str, CryptoStudyProtocol] = {
    "binance": BINANCE_CRYPTO_PROTOCOL_V5,
    "kraken": KRAKEN_CRYPTO_PROTOCOL,
}


def protocol_for(provider: str) -> CryptoStudyProtocol:
    try:
        return CRYPTO_PROTOCOLS[provider.strip().lower()]
    except KeyError as exc:
        raise ValueError(f"no preregistered crypto protocol for provider={provider!r}") from exc


PREREGISTERED_CRYPTO_PROTOCOL = BINANCE_CRYPTO_PROTOCOL_V5