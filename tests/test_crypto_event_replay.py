"""A5 tests for immutable event ledger and deterministic replay."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from gorila_crypto.ledger import ReplaySpec, replay, replay_fingerprint, replay_manifest
from gorila_crypto.storage import CryptoStore


def _store(tmp_path: Path) -> CryptoStore:
    return CryptoStore(sqlite_path=str(tmp_path / "ledger.sqlite3"))


def _append(
    store: CryptoStore,
    *,
    symbol: str,
    event_time: str,
    received_time: str,
    sequence: int,
    price: str,
) -> dict:
    return store.append_event(
        symbol=symbol,
        event_type="trade",
        event_time=event_time,
        received_time=received_time,
        source="binance.websocket.trade",
        sequence_start=sequence,
        sequence_end=sequence,
        provider_time=event_time,
        payload={
            "e": "trade",
            "s": symbol,
            "t": sequence,
            "p": price,
            "q": "0.01",
        },
    )


def test_event_payload_is_persisted_for_offline_replay(tmp_path: Path) -> None:
    store = _store(tmp_path)
    result = _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:00.010000+00:00",
        sequence=1,
        price="60000.0",
    )
    assert result["inserted"] is True

    rows = store.read_events()
    assert len(rows) == 1
    assert rows[0]["payload"]["p"] == "60000.0"
    assert rows[0]["payload_hash"]


def test_duplicate_delivery_is_idempotent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:00.010000+00:00",
        sequence=10,
        price="60000.0",
    )
    second = _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:01.010000+00:00",
        sequence=10,
        price="60000.0",
    )

    assert first["inserted"] is True
    assert second["inserted"] is False
    assert second["ledger_seq"] == first["ledger_seq"]
    assert second["event_id"] == first["event_id"]
    assert len(store.read_events()) == 1


def test_ingest_order_reconstructs_arrival_information_set(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:02+00:00",
        received_time="2026-09-29T15:00:02.010000+00:00",
        sequence=2,
        price="60002.0",
    )
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:01+00:00",
        received_time="2026-09-29T15:00:03.010000+00:00",
        sequence=1,
        price="60001.0",
    )

    rows = store.read_events(order="ingest")
    assert [row["sequence_start"] for row in rows] == [2, 1]


def test_event_time_replay_is_explicitly_separate_from_ingest_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:02+00:00",
        received_time="2026-09-29T15:00:02.010000+00:00",
        sequence=2,
        price="60002.0",
    )
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:01+00:00",
        received_time="2026-09-29T15:00:03.010000+00:00",
        sequence=1,
        price="60001.0",
    )

    result = replay(
        store,
        ReplaySpec(symbol="BTCUSDT", order="event_time"),
    )
    assert [row["sequence_start"] for row in result.rows] == [1, 2]


def test_received_time_cutoff_is_point_in_time(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:01+00:00",
        received_time="2026-09-29T15:00:01+00:00",
        sequence=1,
        price="60001.0",
    )
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:02+00:00",
        received_time="2026-09-29T15:00:03+00:00",
        sequence=2,
        price="60002.0",
    )

    result = replay(
        store,
        ReplaySpec(
            symbol="BTCUSDT",
            order="ingest",
            end_received_time="2026-09-29T15:00:02+00:00",
        ),
    )
    assert [row["sequence_start"] for row in result.rows] == [1]


def test_replay_fingerprint_is_stable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _append(
        store,
        symbol="ETHUSDT",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:00+00:00",
        sequence=100,
        price="2500.0",
    )
    one = replay(store, ReplaySpec(symbol="ETHUSDT"))
    two = replay(store, ReplaySpec(symbol="ETHUSDT"))

    assert one.fingerprint == two.fingerprint
    assert one.fingerprint == replay_fingerprint(one.rows)


def test_replay_reducer_state_is_reproducible(tmp_path: Path) -> None:
    store = _store(tmp_path)
    for sequence, price in ((1, "100.0"), (2, "101.5"), (3, "99.5")):
        _append(
            store,
            symbol="SOLUSDT",
            event_time=f"2026-09-29T15:00:0{sequence}+00:00",
            received_time=f"2026-09-29T15:00:0{sequence}+00:00",
            sequence=sequence,
            price=price,
        )

    reducer = lambda state, row: (state or []) + [row["payload"]["p"]]
    result = replay(store, ReplaySpec(symbol="SOLUSDT"), reducer=reducer, initial_state=[])

    assert result.final_state == ["100.0", "101.5", "99.5"]


def test_replay_manifest_binds_policy_to_fingerprint(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _append(
        store,
        symbol="BTCUSDT",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:00+00:00",
        sequence=1,
        price="60000.0",
    )
    spec = ReplaySpec(symbol="BTCUSDT", end_received_time="2026-09-29T15:01:00+00:00")
    result = replay(store, spec)
    manifest = replay_manifest(spec, result)

    assert manifest["row_count"] == 1
    assert manifest["fingerprint_sha256"] == result.fingerprint
    assert manifest["end_received_time"] == spec.end_received_time
    assert manifest["order"] == "ingest"
