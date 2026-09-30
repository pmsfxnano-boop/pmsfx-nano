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


def test_crypto_storage_rejects_legacy_sqlite_path_reuse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / "legacy.sqlite3"
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(legacy_path))
    with pytest.raises(ValueError, match="crypto_sqlite_path_matches_legacy_storage"):
        CryptoStore(sqlite_path=str(legacy_path))
