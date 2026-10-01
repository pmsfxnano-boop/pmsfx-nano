"""A3 storage isolation tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from gorila_crypto.storage import CryptoStore


LEGACY_TABLES = {
    "observations",
    "source_health",
    "shadow_predictions",
    "shadow_outcomes",
    "promotion_decisions",
    "learning_runs",
    "calibration_runs",
    "runtime_heartbeats",
    "runtime_runs",
    "research_evidence",
    "canonical_daily",
}


def _tables(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()


def test_crypto_sqlite_creates_only_crypto_namespace(tmp_path: Path) -> None:
    db = tmp_path / "crypto.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    store.init()

    tables = _tables(db)

    assert tables
    assert not (tables & LEGACY_TABLES)
    assert "crypto_events" in tables
    assert "crypto_connection_events" in tables
    assert "crypto_data_gaps" in tables
    assert "crypto_runtime_runs" in tables
    assert "crypto_source_health" in tables


def test_crypto_event_persistence_stays_in_crypto_events(tmp_path: Path) -> None:
    db = tmp_path / "crypto.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    event_id = store.record_event(
        symbol="BTCUSDT",
        event_type="trade",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:00.050000+00:00",
        provider_time="2026-09-29T15:00:00+00:00",
        source="binance.websocket",
        sequence_start=100,
        sequence_end=100,
        payload={"p": "60000.0", "q": "0.01"},
    )

    assert event_id
    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT symbol,event_type,source,sequence_start,sequence_end "
            "FROM crypto_events WHERE event_id=?",
            (event_id,),
        ).fetchone()
        assert row == ("BTCUSDT", "trade", "binance.websocket", 100, 100)
        for table in sorted(LEGACY_TABLES):
            assert not conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
    finally:
        conn.close()


def test_crypto_batch_append_is_atomic_and_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "batch.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    events = []
    for trade_id in range(1, 6):
        events.append(
            {
                "symbol": "BTCUSDT",
                "event_type": "trade",
                "event_time": f"2026-09-29T15:00:0{trade_id}+00:00",
                "received_time": f"2026-09-29T15:00:0{trade_id}.010000+00:00",
                "provider_time": f"2026-09-29T15:00:0{trade_id}+00:00",
                "source": "binance.websocket.trade",
                "sequence_start": trade_id,
                "sequence_end": trade_id,
                "payload": {"p": str(60000 + trade_id), "q": "0.01"},
            }
        )

    first = store.append_events(events)
    second = store.append_events(events)

    assert len(first) == 5
    assert all(row["inserted"] for row in first)
    assert len(second) == 5
    assert all(not row["inserted"] for row in second)
    assert [row["ledger_seq"] for row in first] == [row["ledger_seq"] for row in second]

    conn = store.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM crypto_events").fetchone()[0] == 5
    finally:
        conn.close()


def test_crypto_batch_append_rejects_provider_identity_payload_conflict(tmp_path: Path) -> None:
    db = tmp_path / "batch-conflict.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    base = {
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": "2026-09-29T15:00:00+00:00",
        "received_time": "2026-09-29T15:00:00.010000+00:00",
        "provider_time": "2026-09-29T15:00:00+00:00",
        "source": "binance.websocket.trade",
        "sequence_start": 100,
        "sequence_end": 100,
        "payload": {"p": "60000.0", "q": "0.01"},
    }
    store.append_events([base])
    conflict = dict(base, payload={"p": "60001.0", "q": "0.01"})
    with pytest.raises(Exception, match="provider_identity_conflict"):
        store.append_events([conflict])


def test_crypto_storage_ignores_legacy_database_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://legacy.example/db")
    store = CryptoStore(sqlite_path=":memory:")
    assert store.backend == "sqlite"
    assert store.database_url == ""


def test_crypto_storage_rewrites_cross_region_postgres_endpoint_with_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "GORILA_CRYPTO_DATABASE_HOST_OVERRIDE",
        "dpg-example-a.oregon-postgres.render.com",
    )
    store = CryptoStore(
        database_url="postgresql://user:pa%40ss@internal-host:5432/dbname"
    )
    assert (
        store.database_url
        == "postgresql://user:pa%40ss@dpg-example-a.oregon-postgres.render.com:5432/dbname?sslmode=require"
    )


def test_crypto_read_events_can_scope_source_and_skip_payload(tmp_path: Path) -> None:
    db = tmp_path / "crypto.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    store.record_event(
        symbol="BTCUSDT",
        event_type="trade",
        event_time="2026-09-29T15:00:00+00:00",
        received_time="2026-09-29T15:00:00.050000+00:00",
        provider_time="2026-09-29T15:00:00+00:00",
        source="binance.websocket.trade",
        sequence_start=1,
        sequence_end=1,
        payload={"p": "60000.0"},
    )
    store.record_event(
        symbol="BTC/USD",
        event_type="trade",
        event_time="2026-09-29T15:00:01+00:00",
        received_time="2026-09-29T15:00:01.050000+00:00",
        provider_time="2026-09-29T15:00:01+00:00",
        source="kraken.websocket.trade",
        sequence_start=2,
        sequence_end=2,
        payload={"p": "60000.1"},
    )

    rows = store.read_events(
        source_prefix="binance.websocket.",
        include_payload=False,
    )

    assert len(rows) == 1
    assert rows[0]["source"] == "binance.websocket.trade"
    assert rows[0]["payload"] is None


def test_crypto_provider_scoping_excludes_other_venue_rows_and_gaps(tmp_path: Path) -> None:
    db = tmp_path / "crypto.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    for source, symbol in (
        ("binance.websocket.trade", "BTCUSDT"),
        ("kraken.websocket.trade", "BTC/USD"),
    ):
        store.record_event(
            symbol=symbol,
            event_type="trade",
            event_time="2026-09-29T15:00:00+00:00",
            received_time="2026-09-29T15:00:00.050000+00:00",
            provider_time="2026-09-29T15:00:00+00:00",
            source=source,
            sequence_start=1,
            sequence_end=1,
            payload={"source": source},
        )

    store.record_gap(
        symbol="BTCUSDT",
        source="binance.websocket.depth",
        expected_sequence=10,
        observed_sequence=20,
        status="GAP_DETECTED",
    )
    store.record_gap(
        symbol="BTC/USD",
        source="kraken.websocket.book",
        expected_sequence=30,
        observed_sequence=40,
        status="GAP_DETECTED",
    )

    store.upsert_source_health(
        source="binance.websocket.trade",
        status="LIVE",
        last_event_time="2026-09-29T15:00:00+00:00",
        last_received_time="2026-09-29T15:00:00.050000+00:00",
        event_age_seconds=0.0,
        transport_age_seconds=0.05,
        rows_last_batch=1,
    )
    store.upsert_source_health(
        source="kraken.websocket.trade",
        status="DELAYED",
        last_event_time="2026-09-29T14:00:00+00:00",
        last_received_time="2026-09-29T15:05:00+00:00",
        event_age_seconds=3600.0,
        transport_age_seconds=0.0,
        rows_last_batch=1,
    )

    stats = store.prospective_stats(source_prefix="binance.websocket.")
    gaps = store.read_data_gaps(source_prefix="binance.websocket.")
    health = store.health(source_prefix="binance.websocket.")

    assert {row["symbol"] for row in stats["event_counts"]} == {"BTCUSDT"}
    assert stats["gap_count"] == 1
    assert len(gaps) == 1
    assert gaps[0]["source"] == "binance.websocket.depth"
    assert [row["source"] for row in health] == ["binance.websocket.trade"]


def test_crypto_storage_rejects_legacy_sqlite_path_reuse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / "legacy.sqlite3"
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(legacy_path))
    with pytest.raises(ValueError, match="crypto_sqlite_path_matches_legacy_storage"):
        CryptoStore(sqlite_path=str(legacy_path))
