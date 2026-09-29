"""Explicitly isolated persistence for the Crypto cleanroom.

The Crypto runtime never reuses gorila_argentum.storage.Store and never reads the
legacy DATABASE_URL / GORILA_DB_SCHEMA settings. PostgreSQL uses a dedicated
schema; SQLite uses a dedicated file. Both paths create only crypto_* tables.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

CRYPTO_DB_SCHEMA = os.getenv("GORILA_CRYPTO_DB_SCHEMA", "gorila_crypto").strip()
if not CRYPTO_DB_SCHEMA.replace("_", "").isalnum():
    raise ValueError("invalid_crypto_database_schema")

CRYPTO_SQLITE_PATH = os.getenv("GORILA_CRYPTO_SQLITE_PATH", "/tmp/gorila_crypto.sqlite3").strip()
CRYPTO_DATABASE_URL = os.getenv("GORILA_CRYPTO_DATABASE_URL", "").strip()

SCHEMA = """
CREATE TABLE IF NOT EXISTS crypto_events (
    event_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    received_time TEXT NOT NULL,
    provider_time TEXT,
    source TEXT NOT NULL,
    sequence_start BIGINT,
    sequence_end BIGINT,
    payload_hash TEXT NOT NULL,
    quality TEXT NOT NULL DEFAULT 'OK',
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_crypto_events_symbol_time
    ON crypto_events(symbol, event_time);
CREATE INDEX IF NOT EXISTS idx_crypto_events_source_time
    ON crypto_events(source, event_time);
CREATE INDEX IF NOT EXISTS idx_crypto_events_sequence
    ON crypto_events(symbol, sequence_start, sequence_end);

CREATE TABLE IF NOT EXISTS crypto_connection_events (
    connection_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_crypto_connection_events_time
    ON crypto_connection_events(created_at);

CREATE TABLE IF NOT EXISTS crypto_data_gaps (
    gap_id TEXT PRIMARY KEY,
    detected_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL,
    expected_sequence BIGINT,
    observed_sequence BIGINT,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_crypto_data_gaps_symbol_time
    ON crypto_data_gaps(symbol, detected_at);

CREATE TABLE IF NOT EXISTS crypto_runtime_runs (
    run_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    completed_at TEXT,
    result TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_crypto_runtime_runs_time
    ON crypto_runtime_runs(created_at);

CREATE TABLE IF NOT EXISTS crypto_source_health (
    source TEXT PRIMARY KEY,
    updated_at TEXT NOT NULL,
    status TEXT NOT NULL,
    last_event_time TEXT,
    last_received_time TEXT,
    event_age_seconds DOUBLE PRECISION,
    transport_age_seconds DOUBLE PRECISION,
    rows_last_batch INTEGER NOT NULL DEFAULT 0,
    error TEXT
);
"""

_SQLITE_SCHEMA = (
    SCHEMA
    .replace("BIGINT", "INTEGER")
    .replace("DOUBLE PRECISION", "REAL")
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _pg_identifier(value: str) -> str:
    return value.replace('"', '""')


class CryptoStore:
    """Storage boundary that can only address the Crypto persistence namespace."""

    _schema_lock = threading.Lock()
    _schema_ready = False

    def __init__(
        self,
        *,
        database_url: str | None = None,
        sqlite_path: str | None = None,
    ) -> None:
        self.database_url = (
            database_url if database_url is not None else CRYPTO_DATABASE_URL
        ).strip()
        self.sqlite_path = (
            sqlite_path if sqlite_path is not None else CRYPTO_SQLITE_PATH
        ).strip() or ":memory:"
        self._pg = bool(self.database_url)

    @property
    def backend(self) -> str:
        return "postgres" if self._pg else "sqlite"

    def connect(self):
        if self._pg:
            import psycopg

            conn = psycopg.connect(self.database_url)
            schema = _pg_identifier(CRYPTO_DB_SCHEMA)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext('gorila_crypto_schema_v1'))"
                )
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                cur.execute(f'SET search_path TO "{schema}"')
            return conn

        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        if self._pg:
            with self._schema_lock:
                if self._schema_ready:
                    return
                conn = self.connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute(SCHEMA)
                    conn.commit()
                    self.__class__._schema_ready = True
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
            return

        conn = self.connect()
        try:
            conn.executescript(_SQLITE_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def ping(self) -> bool:
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            else:
                conn.execute("SELECT 1").fetchone()
            return True
        finally:
            conn.close()

    def record_event(
        self,
        *,
        symbol: str,
        event_type: str,
        event_time: str,
        received_time: str,
        source: str,
        payload: dict[str, Any],
        provider_time: str | None = None,
        sequence_start: int | None = None,
        sequence_end: int | None = None,
        quality: str = "OK",
        metadata: dict[str, Any] | None = None,
        event_id: str | None = None,
    ) -> str:
        self.init()
        event_id = event_id or str(uuid.uuid4())
        payload_hash = hashlib.sha256(
            _json(payload).encode("utf-8")
        ).hexdigest()
        values = (
            event_id,
            symbol.upper(),
            event_type,
            event_time,
            received_time,
            provider_time,
            source,
            sequence_start,
            sequence_end,
            payload_hash,
            quality,
            _json(metadata or {}),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_events
                        (event_id,symbol,event_type,event_time,received_time,provider_time,
                         source,sequence_start,sequence_end,payload_hash,quality,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(event_id) DO NOTHING""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO crypto_events
                    (event_id,symbol,event_type,event_time,received_time,provider_time,
                     source,sequence_start,sequence_end,payload_hash,quality,metadata)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return event_id
        finally:
            conn.close()

    def record_connection(
        self,
        *,
        source: str,
        status: str,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
        connection_id: str | None = None,
    ) -> str:
        self.init()
        connection_id = connection_id or str(uuid.uuid4())
        values = (
            connection_id,
            _utc_now(),
            source,
            status,
            reason,
            _json(metadata or {}),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_connection_events
                        (connection_id,created_at,source,status,reason,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s)""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT INTO crypto_connection_events
                    (connection_id,created_at,source,status,reason,metadata)
                    VALUES (?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return connection_id
        finally:
            conn.close()

    def health(self) -> list[dict[str, Any]]:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT source,status,updated_at,last_event_time,last_received_time,"
                        "event_age_seconds,transport_age_seconds,rows_last_batch,error "
                        "FROM crypto_source_health ORDER BY source"
                    )
                    rows = cur.fetchall()
                    return [
                        dict(zip(
                            [
                                "source","status","updated_at","last_event_time",
                                "last_received_time","event_age_seconds",
                                "transport_age_seconds","rows_last_batch","error"
                            ],
                            row,
                        ))
                        for row in rows
                    ]

            rows = conn.execute(
                "SELECT source,status,updated_at,last_event_time,last_received_time,"
                "event_age_seconds,transport_age_seconds,rows_last_batch,error "
                "FROM crypto_source_health ORDER BY source"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
