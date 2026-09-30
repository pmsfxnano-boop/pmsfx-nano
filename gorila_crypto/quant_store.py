"""Quantitative-research persistence wrapper.

Adds immutable study/session scope and crash-safe runtime leases without changing
the legacy CryptoStore schema contract.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

from .protocol import CryptoStudyProtocol
from .storage import CryptoStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False


class QuantCryptoStore(CryptoStore):
    """CryptoStore with experiment scoping and runtime lease semantics."""

    def init(self) -> None:
        global _SCHEMA_READY
        if _SCHEMA_READY:
            return
        with _SCHEMA_LOCK:
            if _SCHEMA_READY:
                return
            super().init()
            conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS crypto_studies (
                            study_id TEXT PRIMARY KEY,
                            protocol_hash TEXT NOT NULL UNIQUE,
                            created_at TEXT NOT NULL,
                            status TEXT NOT NULL,
                            protocol_json TEXT NOT NULL
                        );
                        CREATE TABLE IF NOT EXISTS crypto_capture_sessions (
                            session_id TEXT PRIMARY KEY,
                            study_id TEXT NOT NULL,
                            provider TEXT NOT NULL,
                            venue TEXT NOT NULL,
                            region TEXT,
                            instance_id TEXT,
                            code_version TEXT,
                            symbols_json TEXT NOT NULL,
                            streams_json TEXT NOT NULL,
                            protocol_hash TEXT NOT NULL,
                            started_at TEXT NOT NULL,
                            ended_at TEXT,
                            status TEXT NOT NULL,
                            metadata TEXT NOT NULL DEFAULT '{}'
                        );
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_active_study
                            ON crypto_capture_sessions(study_id)
                            WHERE status IN ('STARTING','RUNNING');
                        CREATE TABLE IF NOT EXISTS crypto_runtime_leases (
                            run_id TEXT PRIMARY KEY,
                            session_id TEXT,
                            started_at TEXT NOT NULL,
                            heartbeat_at TEXT NOT NULL,
                            status TEXT NOT NULL
                        );
                        CREATE INDEX IF NOT EXISTS idx_crypto_runtime_lease_heartbeat
                            ON crypto_runtime_leases(heartbeat_at,status);
                        """
                    )
                conn.commit()
            else:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS crypto_studies (
                        study_id TEXT PRIMARY KEY,
                        protocol_hash TEXT NOT NULL UNIQUE,
                        created_at TEXT NOT NULL,
                        status TEXT NOT NULL,
                        protocol_json TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS crypto_capture_sessions (
                        session_id TEXT PRIMARY KEY,
                        study_id TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        region TEXT,
                        instance_id TEXT,
                        code_version TEXT,
                        symbols_json TEXT NOT NULL,
                        streams_json TEXT NOT NULL,
                        protocol_hash TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        ended_at TEXT,
                        status TEXT NOT NULL,
                        metadata TEXT NOT NULL DEFAULT '{}'
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_active_study
                        ON crypto_capture_sessions(study_id)
                        WHERE status IN ('STARTING','RUNNING');
                    CREATE TABLE IF NOT EXISTS crypto_runtime_leases (
                        run_id TEXT PRIMARY KEY,
                        session_id TEXT,
                        started_at TEXT NOT NULL,
                        heartbeat_at TEXT NOT NULL,
                        status TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_crypto_runtime_lease_heartbeat
                        ON crypto_runtime_leases(heartbeat_at,status);
                    """
                )
                conn.commit()
        finally:
            conn.close()
        _SCHEMA_READY = True

    def register_study(self, protocol: CryptoStudyProtocol) -> str:
        protocol.validate()
        self.init()
        payload = json.dumps(protocol.canonical_dict(), sort_keys=True, separators=(",", ":"))
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO crypto_studies(study_id,protocol_hash,created_at,status,protocol_json)
                        VALUES(%s,%s,%s,'REGISTERED',%s)
                        ON CONFLICT(study_id) DO NOTHING
                        """,
                        (protocol.study_id, protocol.protocol_hash, _utc_now(), payload),
                    )
                    cur.execute(
                        "SELECT protocol_hash FROM crypto_studies WHERE study_id=%s",
                        (protocol.study_id,),
                    )
                    row = cur.fetchone()
                    if row is None or str(row[0]) != protocol.protocol_hash:
                        raise RuntimeError("protocol_hash_conflict")
            else:
                conn.execute(
                    """
                    INSERT INTO crypto_studies(study_id,protocol_hash,created_at,status,protocol_json)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(study_id) DO NOTHING
                    """,
                    (protocol.study_id, protocol.protocol_hash, _utc_now(), "REGISTERED", payload),
                )
                row = conn.execute(
                    "SELECT protocol_hash FROM crypto_studies WHERE study_id=?",