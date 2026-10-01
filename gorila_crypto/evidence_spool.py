"""Session-local transactional evidence spool.

This is a loss-containment layer for prolonged durable-store outages.
It is not a substitute for durable storage: the filesystem may disappear
on a Render restart/redeploy. Its purpose is to bound RAM growth, preserve
batch ordering during a database outage, replay batches idempotently when
the durable ledger returns, and expose explicit pending/overflow state.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass(frozen=True)
class SpoolBatch:
    batch_id: int
    created_at: str
    rows: list[dict[str, Any]]
    byte_count: int


class EvidenceSpool:
    def __init__(
        self,
        *,
        path: str,
        max_bytes: int,
        max_batches: int,
    ) -> None:
        if not path.strip():
            raise ValueError("spool path cannot be empty")
        if max_bytes < 1024 * 1024:
            raise ValueError("spool max bytes must be >= 1 MiB")
        if max_batches < 1:
            raise ValueError("spool max batches must be positive")
        self.path = os.path.abspath(path)
        self.max_bytes = int(max_bytes)
        self.max_batches = int(max_batches)
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        conn = sqlite3.connect(
            self.path,
            timeout=5.0,
            isolation_level="IMMEDIATE",
        )
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS evidence_spool_batches (
                        batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        byte_count INTEGER NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_evidence_spool_created
                    ON evidence_spool_batches(created_at, batch_id)
                    """
                )
                conn.commit()
            finally:
                conn.close()

    @staticmethod
    def _encode(rows: Iterable[dict[str, Any]]) -> tuple[str, int]:
        payload = json.dumps(
            [dict(row) for row in rows],
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return payload, len(payload.encode("utf-8"))

    def append(self, rows: Iterable[dict[str, Any]]) -> int | None:
        materialized = [dict(row) for row in rows]
        if not materialized:
            return None
        payload, byte_count = self._encode(materialized)
        with self._lock:
            conn = self._connect()
            try:
                stats = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(byte_count), 0) AS queued_bytes,
                        COUNT(*) AS queued_batches
                    FROM evidence_spool_batches
                    """
                ).fetchone()
                queued_bytes = int(stats["queued_bytes"])
                queued_batches = int(stats["queued_batches"])
                if (
                    queued_bytes + byte_count > self.max_bytes
                    or queued_batches + 1 > self.max_batches
                ):
                    raise OverflowError("evidence_spool_capacity_exceeded")

                cursor = conn.execute(
                    """
                    INSERT INTO evidence_spool_batches(
                        created_at, payload_json, byte_count
                    )
                    VALUES(?,?,?)
                    """,
                    (
                        datetime.now(timezone.utc).isoformat(),
                        payload,
                        byte_count,
                    ),
                )
                conn.commit()
                return int(cursor.lastrowid)
            finally:
                conn.close()

    def peek(self) -> SpoolBatch | None:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    """
                    SELECT batch_id,created_at,payload_json,byte_count
                    FROM evidence_spool_batches
                    ORDER BY batch_id
                    LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    return None
                return SpoolBatch(
                    batch_id=int(row["batch_id"]),
                    created_at=str(row["created_at"]),
                    rows=list(json.loads(row["payload_json"])),
                    byte_count=int(row["byte_count"]),
                )
            finally:
                conn.close()

    def delete(self, batch_id: int) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "DELETE FROM evidence_spool_batches WHERE batch_id=?",
                    (int(batch_id),),
                )
                conn.commit()
            finally:
                conn.close()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS batches,
                        COALESCE(SUM(byte_count), 0) AS bytes,
                        MIN(created_at) AS oldest_created_at
                    FROM evidence_spool_batches
                    """
                ).fetchone()
                return {
                    "path": self.path,
                    "batches": int(row["batches"]),
                    "bytes": int(row["bytes"]),
                    "max_bytes": self.max_bytes,
                    "max_batches": self.max_batches,
                    "oldest_created_at": (
                        str(row["oldest_created_at"])
                        if row["oldest_created_at"] is not None
                        else None
                    ),
                }
            finally:
                conn.close()
