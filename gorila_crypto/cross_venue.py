"""Cross-venue comparability guards for crypto market-data research.

The module refuses economically ambiguous comparisons. Symbol strings alone are
not enough: the base asset, quote currency, time basis and quote conversion path
must all be explicit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping


@dataclass(frozen=True)
class InstrumentSpec:
    venue: str
    symbol: str
    base: str
    quote: str

    def validate(self) -> None:
        for value in (self.venue, self.symbol, self.base, self.quote):
            if not str(value).strip():
                raise ValueError("instrument fields cannot be empty")


@dataclass(frozen=True)
class QuoteObservation:
    instrument: InstrumentSpec
    price: float
    received_time: datetime

    def validate(self) -> None:
        self.instrument.validate()
        if self.price <= 0:
            raise ValueError("quote price must be positive")


@dataclass(frozen=True)
class CrossVenueAuditResult:
    status: str
    reason: str
    venue_a: str
    venue_b: str
    symbol_a: str
    symbol_b: str
    time_delta_ms: float | None
    comparable_price_a: float | None
    comparable_price_b: float | None
    spread_bps: float | None


def audit_quote_observations(
    a: QuoteObservation,
    b: QuoteObservation,
    *,
    max_time_delta_ms: float = 250.0,
    quote_conversion_a_to_b: float | None = None,
) -> CrossVenueAuditResult:
    """Compare two observations only when their economic units are explicit."""
    a.validate()
    b.validate()
    if max_time_delta_ms < 0:
        raise ValueError("max_time_delta_ms must be non-negative")
    if a.instrument.base.upper() != b.instrument.base.upper():
        return CrossVenueAuditResult(
            "BLOCKED_INCOMPATIBLE_INSTRUMENT",
            "BASE_ASSET_MISMATCH",
            a.instrument.venue,
            b.instrument.venue,
            a.instrument.symbol,
            b.instrument.symbol,
            None,
            None,
            None,
            None,
        )

    quote_a = a.instrument.quote.upper()
    quote_b = b.instrument.quote.upper()
    if quote_a != quote_b and quote_conversion_a_to_b is None:
        return CrossVenueAuditResult(
            "BLOCKED_INCOMPATIBLE_QUOTE",
            f"QUOTE_MISMATCH:{quote_a}->{quote_b}:EXPLICIT_CONVERSION_REQUIRED",
            a.instrument.venue,
            b.instrument.venue,
            a.instrument.symbol,
            b.instrument.symbol,
            None,
            None,
            None,
            None,
        )

    ta = a.received_time.astimezone(timezone.utc)
    tb = b.received_time.astimezone(timezone.utc)
    delta_ms = abs((ta - tb).total_seconds()) * 1000.0
    if delta_ms > max_time_delta_ms:
        return CrossVenueAuditResult(
            "BLOCKED_TIME_MISALIGNMENT",
            f"RECEIVE_TIME_DELTA_MS:{delta_ms:.6f}>{max_time_delta_ms:.6f}",
            a.instrument.venue,
            b.instrument.venue,
            a.instrument.symbol,
            b.instrument.symbol,
            delta_ms,
            None,
            None,
            None,
        )

    comparable_a = float(a.price)
    comparable_b = float(b.price)
    if quote_a != quote_b:
        rate = float(quote_conversion_a_to_b)
        if rate <= 0:
            raise ValueError("quote conversion must be positive")
        comparable_a = comparable_a * rate

    spread_bps = (comparable_a / comparable_b - 1.0) * 10000.0
    return CrossVenueAuditResult(
        "COMPARABLE",
        "EXPLICIT_UNIT_AND_TIME_ALIGNMENT",
        a.instrument.venue,
        b.instrument.venue,
        a.instrument.symbol,
        b.instrument.symbol,
        delta_ms,
        comparable_a,
        comparable_b,
        spread_bps,
    )
