from __future__ import annotations

from gorila_crypto.kraken_integrity import (
    KrakenBookState,
    KrakenPrecision,
    apply_and_verify,
    kraken_checksum,
)


def test_kraken_checksum_is_ascending_asks_then_descending_bids() -> None:
    book = KrakenBookState(
        symbol="BTC/USD",
        depth=10,
        asks={60001: 1.1, 60002: 2.2},
        bids={60000: 1.2, 59999: 3.4},
    )
    checksum, payload = kraken_checksum(book, KrakenPrecision(price=0, qty=1))
    assert payload == "60001:1.1,60002:2.2,60000:1.2,59999:3.4"
    assert checksum == 507378030


def test_kraken_integrity_marks_matching_checksum_verified() -> None:
    row = {
        "symbol": "BTC/USD",
        "bids": [{"price": 60000, "qty": 1.2}],
        "asks": [{"price": 60001, "qty": 1.1}],
    }
    book = KrakenBookState(symbol="BTC/USD", depth=10)
    checksum, _ = kraken_checksum(
        KrakenBookState(
            symbol="BTC/USD",
            depth=10,
            bids={60000: 1.2},
            asks={60001: 1.1},
        ),
        KrakenPrecision(price=0, qty=1),
    )
    row["checksum"] = checksum
    result = apply_and_verify(
        book,
        row,
        message_type="snapshot",
        precision=KrakenPrecision(price=0, qty=1),
    )
    assert result.status == "INTEGRITY_VERIFIED"
    assert result.verified is True
    assert result.computed_checksum == checksum
    assert result.derived_l1["bid"]["price"] == "60000"


def test_kraken_integrity_rejects_checksum_drift_without_guessing() -> None:
    book = KrakenBookState(symbol="BTC/USD", depth=10)
    result = apply_and_verify(
        book,
        {
            "symbol": "BTC/USD",
            "bids": [{"price": 60000, "qty": 1.2}],
            "asks": [{"price": 60001, "qty": 1.1}],
            "checksum": 123,
        },
        message_type="snapshot",
        precision=KrakenPrecision(price=0, qty=1),
    )
    assert result.status == "INTEGRITY_CHECKSUM_FAIL"
    assert result.reason == "CHECKSUM_MISMATCH"
    assert result.expected_checksum == 123
    assert result.computed_checksum is not None


def test_kraken_checksum_updates_remove_zero_and_trim_depth() -> None:
    book = KrakenBookState(
        symbol="BTC/USD",
        depth=10,
        bids={60000: 1},
        asks={60001: 1},
    )
    checksum, _ = kraken_checksum(book, KrakenPrecision(price=0, qty=0))
    row = {
        "bids": [{"price": 60000, "qty": 0}, {"price": 59999, "qty": 2}],
        "asks": [{"price": 60001, "qty": 0}, {"price": 60002, "qty": 3}],
        "checksum": checksum,
    }
    result = apply_and_verify(
        book,
        row,
        message_type="update",
        precision=KrakenPrecision(price=0, qty=0),
    )
    assert result.status == "INTEGRITY_CHECKSUM_FAIL"
    assert 60000 not in book.bids
    assert 59999 in book.bids
    assert 60001 not in book.asks
    assert 60002 in book.asks


def test_kraken_checksum_preserves_fixed_precision_digits() -> None:
    book = KrakenBookState(
        symbol="BTC/USD",
        depth=10,
        asks={60001.1: 1.234},
        bids={60000.0: 0.010},
    )
    checksum, payload = kraken_checksum(
        book,
        KrakenPrecision(price=2, qty=3),
    )
    assert payload == "60001.10:1.234,60000.00:0.010"
    assert checksum == 1799580579
