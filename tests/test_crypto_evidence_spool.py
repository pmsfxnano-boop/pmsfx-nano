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


def test_evidence_spool_state_accounting_survives_reopen(tmp_path) -> None:
    path = tmp_path / "state.sqlite3"
    spool = EvidenceSpool(
        path=str(path),
        max_bytes=2 * 1024 * 1024,
        max_batches=10,
    )
    first = spool.append([_row()])
    second = spool.append([_row("SOLUSDT")])
    assert first == 1
    assert second == 2
    stats = spool.stats()
    assert stats["batches"] == 2
    assert stats["bytes"] > 0
    spool.close()

    reopened = EvidenceSpool(
        path=str(path),
        max_bytes=2 * 1024 * 1024,
        max_batches=10,
    )
    assert reopened.stats()["batches"] == 2
    first_batch = reopened.peek()
    assert first_batch is not None
    reopened.delete(first_batch.batch_id)
    assert reopened.stats()["batches"] == 1
    reopened.close()
