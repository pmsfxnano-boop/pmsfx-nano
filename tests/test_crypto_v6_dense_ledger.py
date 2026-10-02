"""Binance v6 dense replay ledger tests."""

from __future__ import annotations

from pathlib import Path

from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.quant_store import QuantCryptoStore


def test_v6_protocol_contract() -> None:
    protocol = PREREGISTERED_CRYPTO_PROTOCOL
    assert protocol.version == "6"
    assert protocol.study_id == "crypto-binance-spot-prospective-v6"
    assert protocol.trade_persistence_sample_rate == 0.01
    assert protocol.bookticker_persistence_interval_seconds == 5.0
    assert protocol.persistence_contract_version == "typed_dense_v1"


def test_v6_dense_ledger_roundtrip_is_idempotent(tmp_path: Path) -> None:
    store = QuantCryptoStore(sqlite_path=str(tmp_path / "v6.sqlite3"))
    study = PREREGISTERED_CRYPTO_PROTOCOL.study_id
    session = "22222222-2222-2222-2222-222222222222"

    trade = {
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": "2026-10-02T00:00:00+00:00",
        "received_time": "2026-10-02T00:00:00.050000+00:00",
        "provider_time": None,
        "source": "binance.websocket.trade",
        "sequence_start": 1000,
        "sequence_end": 1000,
        "quality": "OK",
        "metadata": {"ingest_epoch": 3},
        "payload": {"e": "trade", "s": "BTCUSDT", "t": 1000, "p": "60000.0", "q": "0.01", "m": False},
    }
    book = {
        "symbol": "BTCUSDT",
        "event_type": "bookTicker",
        "event_time": "2026-10-02T00:00:00.100000+00:00",
        "received_time": "2026-10-02T00:00:00.120000+00:00",
        "provider_time": None,
        "source": "binance.websocket.bookTicker",
        "sequence_start": 2000,
        "sequence_end": 2000,
        "quality": "TRANSPORT_TIME_ONLY",
        "metadata": {"ingest_epoch": 3},
        "payload": {
            "e": "bookTicker", "s": "BTCUSDT", "u": 2000,
            "b": "59999.9", "B": "1.2", "a": "60000.1", "A": "0.8",
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

    assert [row["inserted"] for row in first] == [True, True]
    assert [row["inserted"] for row in second] == [False, False]
    assert [row["ledger_seq"] for row in first] == [row["ledger_seq"] for row in second]

    rows = store.read_scoped_events(
        study_id=study,
        capture_session_id=session,
        order="ingest",
        limit=10,
    )
    assert len(rows) == 2
    assert rows[0]["symbol"] == "BTCUSDT"
    assert {row["event_type"] for row in rows} == {"trade", "bookTicker"}

    conn = store.connect()
    try:
        v6_rows = conn.execute("SELECT COUNT(*) FROM crypto_events_v6").fetchone()[0]
        v5_rows = conn.execute("SELECT COUNT(*) FROM crypto_events_v5").fetchone()[0]
        generic_rows = conn.execute("SELECT COUNT(*) FROM crypto_events").fetchone()[0]
    finally:
        conn.close()

    assert v6_rows == 2
    assert v5_rows == 0
    assert generic_rows == 0
