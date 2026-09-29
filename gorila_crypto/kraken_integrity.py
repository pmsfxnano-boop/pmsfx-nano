"""Deterministic Kraken Spot v2 L2 book integrity primitives.

Kraken v2 book messages carry no sequence number. The venue integrity mechanism is
an unsigned CRC32 over the ten best asks followed by the ten best bids, rendered
at the pair's instrument precision with decimal points removed. The local book
must therefore be reconstructed exactly before the checksum is evaluated.
"""

from __future__ import annotations

import binascii
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


KRAKEN_CHECKSUM_DEPTH = 10


class KrakenIntegrityError(RuntimeError):
    """Base class for deterministic Kraken book-integrity failures."""


class KrakenChecksumMismatch(KrakenIntegrityError):
    """Raised when a reconstructed Kraken book disagrees with venue CRC32."""


@dataclass(frozen=True)
class KrakenPrecision:
    price: int
    qty: int

    def validate(self) -> None:
        if self.price < 0 or self.qty < 0:
            raise ValueError("Kraken precision values must be non-negative")


@dataclass
class KrakenBookState:
    symbol: str
    depth: int = 10
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)

    def validate(self) -> None:
        if self.depth not in {10, 25, 100, 500, 1000}:
            raise ValueError("Kraken depth must be one of 10,25,100,500,1000")

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def top_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.bids:
            return None
        price = max(self.bids)
        return price, self.bids[price]

    def top_ask(self) -> tuple[Decimal, Decimal] | None:
        if not self.asks:
            return None
        price = min(self.asks)
        return price, self.asks[price]


@dataclass(frozen=True)
class KrakenIntegrityResult:
    expected_checksum: int | None
    computed_checksum: int | None
    verified: bool
    status: str
    reason: str
    checksum_payload: str | None
    derived_l1: Mapping[str, Mapping[str, str]]


def _decimal(value: Any, field_name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise KrakenIntegrityError(f"{field_name} must be numeric") from exc
    if not result.is_finite():
        raise KrakenIntegrityError(f"{field_name} must be finite")
    return result


def _fixed(value: Decimal, places: int, field_name: str) -> str:
    value = _decimal(value, field_name)
    if value < 0:
        raise KrakenIntegrityError(f"{field_name} cannot be negative")
    rendered = format(value, f".{places}f")
    if _decimal(rendered, field_name) != value:
        raise KrakenIntegrityError(
            f"{field_name} has more precision than the instrument allows"
        )
    return rendered


def _checksum_component(value: Decimal, places: int, field_name: str) -> str:
    rendered = _fixed(value, places, field_name)
    digits = rendered.replace(".", "").lstrip("0")
    return digits or "0"


def kraken_checksum(
    book: KrakenBookState,
    precision: KrakenPrecision,
) -> tuple[int, str]:
    """Compute Kraken's checksum from the resulting book state."""
    precision.validate()
    book.validate()

    parts: list[str] = []
    for price, qty in sorted(book.asks.items(), key=lambda item: item[0])[:KRAKEN_CHECKSUM_DEPTH]:
        parts.append(_checksum_component(price, precision.price, "ask.price"))
        parts.append(_checksum_component(qty, precision.qty, "ask.qty"))
    for price, qty in sorted(book.bids.items(), key=lambda item: item[0], reverse=True)[:KRAKEN_CHECKSUM_DEPTH]:
        parts.append(_checksum_component(price, precision.price, "bid.price"))
        parts.append(_checksum_component(qty, precision.qty, "bid.qty"))

    payload = "".join(parts)
    return binascii.crc32(payload.encode("ascii")) & 0xFFFFFFFF, payload


def _apply_levels(
    target: dict[Decimal, Decimal],
    levels: Sequence[Mapping[str, Any]],
    *,
    side: str,
) -> None:
    for row in levels:
        if not isinstance(row, Mapping):
            raise KrakenIntegrityError(f"{side} level must be an object")
        price = _decimal(row.get("price"), f"{side}.price")
        qty = _decimal(row.get("qty"), f"{side}.qty")
        if qty < 0:
            raise KrakenIntegrityError(f"{side}.qty cannot be negative")
        if qty == 0:
            target.pop(price, None)
        else:
            target[price] = qty


def _trim_depth(book: KrakenBookState) -> None:
    if len(book.bids) > book.depth:
        keep = sorted(book.bids, key=lambda price: price, reverse=True)[: book.depth]
        book.bids = {price: book.bids[price] for price in keep}
    if len(book.asks) > book.depth:
        keep = sorted(book.asks)[: book.depth]
        book.asks = {price: book.asks[price] for price in keep}


def _derived_l1(book: KrakenBookState) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    bid = book.top_bid()
    ask = book.top_ask()
    if bid is not None:
        result["bid"] = {"price": str(bid[0]), "qty": str(bid[1])}
    if ask is not None:
        result["ask"] = {"price": str(ask[0]), "qty": str(ask[1])}
    return result


def apply_and_verify(
    book: KrakenBookState,
    row: Mapping[str, Any],
    *,
    message_type: str,
    precision: KrakenPrecision | None,
) -> KrakenIntegrityResult:
    """Apply one complete Kraken book row, then verify its resulting checksum."""
    book.validate()
    if message_type == "snapshot":
        book.reset()

    bids = row.get("bids")
    asks = row.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise KrakenIntegrityError("book bids/asks must be arrays")

    _apply_levels(book.bids, bids, side="bid")
    _apply_levels(book.asks, asks, side="ask")
    _trim_depth(book)

    expected_raw = row.get("checksum")
    expected = None if expected_raw is None else int(expected_raw)
    derived = _derived_l1(book)

    if precision is None:
        return KrakenIntegrityResult(
            expected_checksum=expected,
            computed_checksum=None,
            verified=False,
            status="INTEGRITY_UNVERIFIED",
            reason="MISSING_INSTRUMENT_PRECISION",
            checksum_payload=None,
            derived_l1=derived,
        )
    if expected is None:
        return KrakenIntegrityResult(
            expected_checksum=None,
            computed_checksum=None,
            verified=False,
            status="INTEGRITY_UNVERIFIED",
            reason="MISSING_PROVIDER_CHECKSUM",
            checksum_payload=None,
            derived_l1=derived,
        )

    computed, checksum_payload = kraken_checksum(book, precision)
    if computed != expected:
        return KrakenIntegrityResult(
            expected_checksum=expected,
            computed_checksum=computed,
            verified=False,
            status="INTEGRITY_CHECKSUM_FAIL",
            reason="CHECKSUM_MISMATCH",
            checksum_payload=checksum_payload,
            derived_l1=derived,
        )

    return KrakenIntegrityResult(
        expected_checksum=expected,
        computed_checksum=computed,
        verified=True,
        status="INTEGRITY_VERIFIED",
        reason="MATCH",
        checksum_payload=checksum_payload,
        derived_l1=derived,
    )
