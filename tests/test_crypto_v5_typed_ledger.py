"""Binance v5 typed compact ledger tests."""

from __future__ import annotations

from pathlib import Path

from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.quant_store import QuantCryptoStore


def _store(tmp_path: Path) -> QuantCryptoStore:
    return QuantCryptoStore(sqlite_path=str(tmp_path / "v5.sqlite3"))


def test_v5_study_is_typed_compact_protocol() -> None:
    assert PREREGISTERED_CRYPTO_PROTOCOL.version == "5"
    assert PREREGISTERED_CRYPTO_PROTOCOL.persistence_contract_version == "typed_compact_v1"
    assert PREREGISTERED_CRYPTO_PROTOCOL.trade_persistence_sample_rate == 0.05
    assert PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds == 5.0


def test_v5_trade_and_bookticker_roundtrip_is_idempotent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    study = PREREGISTERED_CRYPTO_PROTOCOL.study_id
    session = "11111111-1111-1111-1111-111111111111"

    trade = {
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": "2026-10-01T23:00:00+00:00",
        "received_time": "2026-10-01T23:00:00.250000+00:00",
        "provider_time": "2026-10-01T23:00:00+00:00",
        "source": "binance.websocket.trade",
        "sequence_start": 100,
        "sequence_end": 100,
        "quality": "OK",
        "metadata": {"ingest_epoch": 7},
        "payload": {"e": "trade", "s": "BTCUSDT", "t": 100, "p": "60000.0", "q": "0.01", "m": False},
    }
    book = {
        "symbol": "BTCUSDT",
        "event_type": "bookTicker",
        "event_time": "2026-10-01T23:00:00.100000+00:00",
        "received_time": "2026-10-01T23:00:00.150000+00:00",
        "provider_time": None,
        "source": "binance.websocket.bookTicker",
        "sequence_start": 200,
        "sequence_end": 200,
        "quality": "TRANSPORT_TIME_ONLY",
        "metadata": {"ingest_epoch": 7},
        "payload": {
            "e": "bookTicker",
            "s": "BTCUSDT",
            "u": 200,
            "b": "59999.9",
            "B": "1.2",
            "a": "60000.1",
            "A": "0.8",
        },
    }

    first = store.append_scoped_events(
        study_id=study,
        capture_session_id=session,
        events=[trade, book],
    )
    second = store.append_scoped_events(
        study_id=study,
        capture_session_id=session,
        events=[trade, book],
    )

    assert len(first) == len(second) == 2
    assert all(item["inserted"] for item in first)
    assert all(not item["inserted"] for item in second)
    assert [item["ledger_seq"] for item in first] == [item["ledger_seq"] for item in second]

    rows = store.read_scoped_events(
        study_id=study,
        capture_session_id=session,
        order="ingest",
        limit=10,
    )
    assert len(rows) == 2
    trade_row = next(row for row in rows if row["event_type"] == "trade")
    book_row = next(row for row in rows if row["event_type"] == "bookTicker")
    assert trade_row["payload"]["p"] == "60000.0"
    assert trade_row["sequence_start"] == 100
    assert book_row["payload"]["b"] == "59999.9"
    assert book_row["payload"]["A"] == "0.8"

    conn = store.connect()
    try:
        typed_rows = conn.execute("SELECT COUNT(*) FROM crypto_events_v5").fetchone()[0]
        generic_rows = conn.execute("SELECT COUNT(*) FROM crypto_events").fetchone()[0]
    finally:
        conn.close()

    assert typed_rows == 2
    assert generic_rows == 0
