from __future__ import annotations

import json

import pytest

from gorila_crypto.evidence_spool import EvidenceSpool


def _row(symbol: str = "BTCUSDT") -> dict:
    return {
        "symbol": symbol,
        "event_type": "trade",
        "event_time": "2026-10-01T08:20:00+00:00",
        "received_time": "2026-10-01T08:20:00.010000+00:00",
        "source": "binance.websocket.trade",
        "payload": {"p": "100.0", "q": "0.1", "m": False},
        "metadata": {"event_key": "unit-test"},
    }


def test_evidence_spool_is_ordered_and_replayable(tmp_path) -> None:
    spool = EvidenceSpool(
        path=str(tmp_path / "spool.sqlite3"),
        max_bytes=2 * 1024 * 1024,
        max_batches=10,
    )

    first = spool.append([_row()])
    second = spool.append([_row("ETHUSDT")])

    assert first == 1
    assert second == 2

    batch = spool.peek()
    assert batch is not None
    assert batch.batch_id == 1
    assert batch.rows[0]["symbol"] == "BTCUSDT"

    spool.delete(batch.batch_id)
    next_batch = spool.peek()
    assert next_batch is not None
    assert next_batch.batch_id == 2
    assert next_batch.rows[0]["symbol"] == "ETHUSDT"


def test_evidence_spool_enforces_capacity(tmp_path) -> None:
    row = _row()
    encoded_size = len(
        json.dumps([row], sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    )
    spool = EvidenceSpool(
        path=str(tmp_path / "spool.sqlite3"),
        max_bytes=encoded_size,
        max_batches=1,
    )

    spool.append([row])
    with pytest.raises(OverflowError, match="evidence_spool_capacity_exceeded"):
        spool.append([row])


def test_evidence_spool_compresses_large_payload_and_roundtrips(tmp_path) -> None:
    row = _row()
    row["payload"]["padding"] = "x" * 20_000
    raw_size = len(
        json.dumps([row], sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    )
    spool = EvidenceSpool(
        path=str(tmp_path / "compressed.sqlite3"),
        max_bytes=2 * 1024 * 1024,
        max_batches=10,
    )

    spool.append([row])

    batch = spool.peek()
    assert batch is not None
    assert batch.rows == [row]
    assert batch.byte_count < raw_size
    assert spool.stats()["bytes"] == batch.byte_count


def test_evidence_spool_migrates_legacy_uncompressed_rows(tmp_path) -> None:
    import sqlite3

    path = tmp_path / "legacy.sqlite3"
    row = _row()
    payload_json = json.dumps(
        [row], sort_keys=True, separators=(",", ":"), default=str
    )
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            """
            CREATE TABLE evidence_spool_batches (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                byte_count INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO evidence_spool_batches(
                created_at,payload_json,byte_count
            ) VALUES(?,?,?)
            """,
            ("2026-10-01T13:00:00+00:00", payload_json, len(payload_json.encode("utf-8"))),
        )
        conn.commit()
    finally:
        conn.close()

    spool = EvidenceSpool(
        path=str(path),
        max_bytes=2 * 1024 * 1024,
        max_batches=10,
    )

    batch = spool.peek()
    assert batch is not None
    assert batch.batch_id == 1
    assert batch.rows == [row]
    assert batch.byte_count < len(payload_json.encode("utf-8"))

    conn = sqlite3.connect(path)
    try:
        migrated = conn.execute(
            "SELECT payload_json,payload_zlib,byte_count FROM evidence_spool_batches WHERE batch_id=1"
        ).fetchone()
        assert migrated[0] == ""
        assert migrated[1] is not None
        assert migrated[2] == batch.byte_count
    finally:
        conn.close()
