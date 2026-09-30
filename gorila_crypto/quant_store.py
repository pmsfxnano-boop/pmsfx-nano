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
        # The legacy CryptoStore schema is already idempotent. The Quant layer
        # must also be idempotent per database target: a process can exercise
        # multiple isolated SQLite files in tests or offline research.
        with _SCHEMA_LOCK:
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
                    (protocol.study_id,),
                ).fetchone()
                if row is None or str(row[0]) != protocol.protocol_hash:
                    raise RuntimeError("protocol_hash_conflict")
            conn.commit()
        finally:
            conn.close()
        return protocol.study_id

    def fence_active_study_session(
        self,
        *,
        study_id: str,
        reason: str = "REPLACED_BY_NEW_WORKER",
    ) -> int:
        """Atomically revoke the current single-writer cohort before takeover."""
        self.init()
        now = _utc_now()
        conn = self.connect()
        revoked = 0
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE crypto_runtime_leases
                        SET status='ABORTED_REPLACED', heartbeat_at=%s
                        WHERE status='RUNNING'
                          AND session_id IN (
                              SELECT session_id
                              FROM crypto_capture_sessions
                              WHERE study_id=%s AND status='RUNNING'
                          )
                        """,
                        (now, study_id),
                    )
                    revoked = cur.rowcount
                    cur.execute(
                        """
                        UPDATE crypto_runtime_runs
                        SET status='ABORTED_REPLACED',
                            completed_at=%s,
                            result=%s
                        WHERE status='RUNNING'
                          AND run_id IN (
                              SELECT l.run_id
                              FROM crypto_runtime_leases l
                              JOIN crypto_capture_sessions s
                                ON s.session_id=l.session_id
                              WHERE s.study_id=%s
                                AND l.status='ABORTED_REPLACED'
                          )
                        """,
                        (now, json.dumps({"reason": reason}, sort_keys=True)),
                    )
                    cur.execute(
                        """
                        UPDATE crypto_capture_sessions
                        SET status='ABORTED_REPLACED', ended_at=%s
                        WHERE study_id=%s AND status='RUNNING'
                        """,
                        (now, study_id),
                    )
            else:
                cur = conn.execute(
                    """
                    UPDATE crypto_runtime_leases
                    SET status='ABORTED_REPLACED', heartbeat_at=?
                    WHERE status='RUNNING'
                      AND session_id IN (
                          SELECT session_id
                          FROM crypto_capture_sessions
                          WHERE study_id=? AND status='RUNNING'
                      )
                    """,
                    (now, study_id),
                )
                revoked = cur.rowcount
                conn.execute(
                    """
                    UPDATE crypto_runtime_runs
                    SET status='ABORTED_REPLACED',
                        completed_at=?,
                        result=?
                    WHERE status='RUNNING'
                      AND run_id IN (
                          SELECT l.run_id
                          FROM crypto_runtime_leases l
                          JOIN crypto_capture_sessions s
                            ON s.session_id=l.session_id
                          WHERE s.study_id=?
                            AND l.status='ABORTED_REPLACED'
                      )
                    """,
                    (now, json.dumps({"reason": reason}, sort_keys=True)),
                )
                conn.execute(
                    """
                    UPDATE crypto_capture_sessions
                    SET status='ABORTED_REPLACED', ended_at=?
                    WHERE study_id=? AND status='RUNNING'
                    """,
                    (now, study_id),
                )
            conn.commit()
            return int(revoked)
        finally:
            conn.close()

    def start_capture_session(
        self,
        *,
        study_id: str,
        protocol_hash: str,
        provider: str,
        venue: str,
        symbols: tuple[str, ...],
        streams: tuple[str, ...],
        region: str | None,
        instance_id: str | None,
        code_version: str | None,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT protocol_hash FROM crypto_studies WHERE study_id=%s",
                        (study_id,),
                    )
                    study_row = cur.fetchone()
            else:
                study_row = conn.execute(
                    "SELECT protocol_hash FROM crypto_studies WHERE study_id=?",
                    (study_id,),
                ).fetchone()
            if study_row is None:
                raise RuntimeError("study_not_registered")
            if str(study_row[0]) != protocol_hash:
                raise RuntimeError("protocol_hash_conflict")
        finally:
            conn.close()
        session_id = str(uuid.uuid4())
        values = (
            session_id,
            study_id,
            provider,
            venue,
            region,
            instance_id,
            code_version,
            json.dumps(list(symbols), sort_keys=True),
            json.dumps(list(streams), sort_keys=True),
            protocol_hash,
            _utc_now(),
            "STARTING",
            json.dumps(dict(metadata or {}), sort_keys=True, default=str),
        )
        conn = self.connect()
        try:
            try:
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO crypto_capture_sessions(
                                session_id,study_id,provider,venue,region,instance_id,
                                code_version,symbols_json,streams_json,protocol_hash,
                                started_at,status,metadata)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            values,
                        )
                else:
                    conn.execute(
                        """
                        INSERT INTO crypto_capture_sessions(
                            session_id,study_id,provider,venue,region,instance_id,
                            code_version,symbols_json,streams_json,protocol_hash,
                            started_at,status,metadata)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        values,
                    )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                # A preregistered study is single-writer. A second active
                # capture is not allowed because it would contaminate the cohort.
                raise RuntimeError(
                    f"active_capture_session_exists_or_session_creation_failed:{type(exc).__name__}:{exc}"
                ) from exc
        finally:
            conn.close()
        self.set_capture_session_status(session_id, "RUNNING")
        return session_id

    def set_capture_session_status(self, session_id: str, status: str) -> None:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE crypto_capture_sessions SET status=%s, ended_at=%s WHERE session_id=%s AND status IN ('STARTING','RUNNING')",
                        (
                            status,
                            _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                            session_id,
                        ),
                    )
            else:
                conn.execute(
                    "UPDATE crypto_capture_sessions SET status=?, ended_at=? WHERE session_id=? AND status IN ('STARTING','RUNNING')",
                    (
                        status,
                        _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                        session_id,
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def append_scoped_event(self, *, study_id: str, capture_session_id: str, **kwargs: Any) -> dict[str, Any]:
        metadata = dict(kwargs.pop("metadata", None) or {})
        metadata.update(
            {
                "crypto_study_id": study_id,
                "capture_session_id": capture_session_id,
            }
        )
        return super().append_event(metadata=metadata, **kwargs)

    def start_runtime_run_scoped(self, *, kind: str, session_id: str | None = None) -> str:
        run_id = super().start_runtime_run(kind=kind)
        self.init()
        now = _utc_now()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO crypto_runtime_leases(run_id,session_id,started_at,heartbeat_at,status)
                        VALUES(%s,%s,%s,%s,'RUNNING')
                        ON CONFLICT(run_id) DO UPDATE SET
                            session_id=EXCLUDED.session_id,
                            heartbeat_at=EXCLUDED.heartbeat_at,
                            status='RUNNING'
                        """,
                        (run_id, session_id, now, now),
                    )
            else:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO crypto_runtime_leases(
                        run_id,session_id,started_at,heartbeat_at,status)
                    VALUES(?,?,?,?,?)
                    """,
                    (run_id, session_id, now, now, "RUNNING"),
                )
            conn.commit()
        finally:
            conn.close()
        return run_id

    def heartbeat_runtime_run(self, run_id: str) -> bool:
        self.init()
        heartbeat = _utc_now()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE crypto_runtime_leases SET heartbeat_at=%s WHERE run_id=%s AND status='RUNNING'",
                        (heartbeat, run_id),
                    )
                    alive = cur.rowcount == 1
            else:
                cur = conn.execute(
                    "UPDATE crypto_runtime_leases SET heartbeat_at=? WHERE run_id=? AND status='RUNNING'",
                    (heartbeat, run_id),
                )
                alive = cur.rowcount == 1
            conn.commit()
            return bool(alive)
        finally:
            conn.close()

    def finish_runtime_run_scoped(
        self,
        *,
        run_id: str,
        session_id: str | None,
        status: str,
        result: Mapping[str, Any],
    ) -> None:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status FROM crypto_runtime_leases WHERE run_id=%s",
                        (run_id,),
                    )
                    lease = cur.fetchone()
            else:
                lease = conn.execute(
                    "SELECT status FROM crypto_runtime_leases WHERE run_id=?",
                    (run_id,),
                ).fetchone()
            if lease is not None and str(lease[0]) != "RUNNING":
                conn.close()
                return
            super().finish_runtime_run(run_id=run_id, status=status, result=dict(result))

            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE crypto_runtime_leases SET status=%s,heartbeat_at=%s,session_id=COALESCE(session_id,%s) WHERE run_id=%s",
                        (status, _utc_now(), session_id, run_id),
                    )
            else:
                conn.execute(
                    "UPDATE crypto_runtime_leases SET status=?,heartbeat_at=?,session_id=COALESCE(session_id,?) WHERE run_id=?",
                    (status, _utc_now(), session_id, run_id),
                )
            conn.commit()
        finally:
            conn.close()

    def reconcile_stale_runtime_runs(self, stale_after_seconds: float = 120.0) -> int:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.init()
        cutoff = datetime.fromtimestamp(
            time.time() - stale_after_seconds,
            tz=timezone.utc,
        ).isoformat()
        conn = self.connect()
        count = 0
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE crypto_runtime_runs r
                        SET status='ABORTED_STALE',
                            completed_at=%s,
                            result=%s
                        WHERE r.status='RUNNING'
                          AND COALESCE(
                              (SELECT l.heartbeat_at
                               FROM crypto_runtime_leases l
                               WHERE l.run_id=r.run_id),
                              r.created_at
                          ) < %s
                        """,
                        (_utc_now(), json.dumps({"reason": "stale_runtime_lease"}), cutoff),
                    )
                    count = cur.rowcount
                    cur.execute(
                        """
                        UPDATE crypto_runtime_leases
                        SET status='ABORTED_STALE'
                        WHERE status='RUNNING' AND heartbeat_at < %s
                        """,
                        (cutoff,),
                    )
                    cur.execute(
                        """
                        UPDATE crypto_capture_sessions s
                        SET status='ABORTED_STALE', ended_at=%s
                        WHERE s.status='RUNNING'
                          AND (
                              s.started_at < %s
                              OR EXISTS (
                                  SELECT 1
                                  FROM crypto_runtime_leases l
                                  WHERE l.session_id=s.session_id
                                    AND l.status='ABORTED_STALE'
                              )
                          )
                        """,
                        (_utc_now(), cutoff),
                    )
            else:
                cur = conn.execute(
                    """
                    UPDATE crypto_runtime_runs
                    SET status='ABORTED_STALE',
                        completed_at=?,
                        result=?
                    WHERE status='RUNNING'