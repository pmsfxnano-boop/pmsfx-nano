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
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .storage import CryptoStore


_SCHEMA_LOCK = threading.Lock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class QuantCryptoStore(CryptoStore):
    """CryptoStore with experiment scoping and runtime lease semantics."""

    _quant_schema_lock = threading.Lock()
    _quant_schema_ready_urls: set[str] = set()

    def init(self) -> None:
        super().init()
        if self._pg:
            if self.database_url in self._quant_schema_ready_urls:
                return
            with self._quant_schema_lock:
                if self.database_url in self._quant_schema_ready_urls:
                    return
                conn = self.connect()
                try:
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
                            CREATE TABLE IF NOT EXISTS crypto_events_v5 (
                                ledger_seq BIGSERIAL PRIMARY KEY,
                                event_id UUID NOT NULL,
                                study_id TEXT NOT NULL,
                                capture_session_id UUID NOT NULL,
                                symbol TEXT NOT NULL,
                                event_type TEXT NOT NULL,
                                event_time TIMESTAMPTZ NOT NULL,
                                received_time TIMESTAMPTZ NOT NULL,
                                provider_time TIMESTAMPTZ,
                                source TEXT NOT NULL,
                                sequence_start BIGINT NOT NULL,
                                sequence_end BIGINT NOT NULL,
                                payload_hash BYTEA NOT NULL,
                                quality TEXT NOT NULL,
                                ingest_epoch BIGINT,
                                price DOUBLE PRECISION,
                                quantity DOUBLE PRECISION,
                                buyer_maker BOOLEAN,
                                bid DOUBLE PRECISION,
                                bid_qty DOUBLE PRECISION,
                                ask DOUBLE PRECISION,
                                ask_qty DOUBLE PRECISION,
                                recorded_at TIMESTAMPTZ NOT NULL,
                                UNIQUE(symbol,event_type,sequence_start)
                            );
                            CREATE INDEX IF NOT EXISTS idx_crypto_events_v5_scope_time
                                ON crypto_events_v5(study_id,capture_session_id,symbol,event_type,event_time,received_time,ledger_seq);
                            CREATE INDEX IF NOT EXISTS idx_crypto_events_v5_scope_seq
                                ON crypto_events_v5(study_id,capture_session_id,ledger_seq);
                            CREATE OR REPLACE VIEW crypto_events_v5_replay AS
                            SELECT
                                e.ledger_seq,
                                e.event_id::text AS event_id,
                                md5(
                                    e.source || '|' || e.symbol || '|' || e.event_type || '|' ||
                                    e.sequence_start::text || '|' || e.sequence_end::text
                                ) AS event_key,
                                e.symbol,
                                e.event_type,
                                e.event_time,
                                e.received_time,
                                e.provider_time,
                                e.source,
                                e.sequence_start,
                                e.sequence_end,
                                encode(e.payload_hash,'hex') AS payload_hash,
                                CASE
                                    WHEN e.event_type='trade' THEN jsonb_build_object(
                                        'e','trade','s',e.symbol,'t',e.sequence_start,
                                        'p',e.price::text,'q',e.quantity::text,'m',e.buyer_maker
                                    )
                                    ELSE jsonb_build_object(
                                        'e','bookTicker','s',e.symbol,'u',e.sequence_end,
                                        'b',e.bid::text,'B',e.bid_qty::text,
                                        'a',e.ask::text,'A',e.ask_qty::text
                                    )
                                END AS payload_json,
                                e.quality,
                                jsonb_build_object(
                                    'crypto_study_id',e.study_id,
                                    'capture_session_id',e.capture_session_id::text,
                                    'ingest_epoch',e.ingest_epoch
                                ) AS metadata,
                                e.recorded_at
                            FROM crypto_events_v5 e;
                            CREATE TABLE IF NOT EXISTS crypto_storage_actions (
                                action_key TEXT PRIMARY KEY,
                                completed_at TEXT NOT NULL,
                                details TEXT NOT NULL DEFAULT '{}'
                            );
                            """
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                self._quant_schema_ready_urls.add(self.database_url)
            return

        conn = self.connect()
        try:
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
                CREATE TABLE IF NOT EXISTS crypto_events_v5 (
                    ledger_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    study_id TEXT NOT NULL,
                    capture_session_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    received_time TEXT NOT NULL,
                    provider_time TEXT,
                    source TEXT NOT NULL,
                    sequence_start INTEGER NOT NULL,
                    sequence_end INTEGER NOT NULL,
                    payload_hash BLOB NOT NULL,
                    quality TEXT NOT NULL,
                    ingest_epoch INTEGER,
                    price REAL,
                    quantity REAL,
                    buyer_maker INTEGER,
                    bid REAL,
                    bid_qty REAL,
                    ask REAL,
                    ask_qty REAL,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(symbol,event_type,sequence_start)
                );
                CREATE INDEX IF NOT EXISTS idx_crypto_events_v5_scope_time
                    ON crypto_events_v5(study_id,capture_session_id,symbol,event_type,event_time,received_time,ledger_seq);
                CREATE INDEX IF NOT EXISTS idx_crypto_events_v5_scope_seq
                    ON crypto_events_v5(study_id,capture_session_id,ledger_seq);
                """
            )
            conn.commit()
        finally:
            conn.close()

    def emergency_disk_relief(self, *, keep_session_id: str | None) -> dict[str, int | bool | str]:
        """Reclaim invalid cohort storage and optionally reset an unvalidated ledger."""
        self.init()
        if not self._pg:
            return {"deleted_rows": 0, "dropped_indexes": 0, "reset": False}

        reset_requested = os.getenv(
            "GORILA_CRYPTO_RESET_EVENT_LEDGER", "false"
        ).strip().lower() in {"1", "true", "yes", "on"}

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crypto_storage_actions (
                        action_key TEXT PRIMARY KEY,
                        completed_at TEXT NOT NULL,
                        details TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
                cur.execute(
                    "SELECT 1 FROM crypto_storage_actions WHERE action_key=%s",
                    ("RESET_EVENT_LEDGER_V1",),
                )
                already_reset = cur.fetchone() is not None
        finally:
            conn.commit()
            conn.close()

        if reset_requested and not already_reset:
            # Never destroy a study that already has validated evidence.
            conn = self.connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM crypto_validation_runs")
                    validation_runs = int(cur.fetchone()[0] or 0)
                    cur.execute(
                        "SELECT COUNT(*) FROM crypto_research_runs WHERE status='COMPLETE'"
                    )
                    completed_research = int(cur.fetchone()[0] or 0)
                conn.commit()
            finally:
                conn.close()
            if validation_runs or completed_research:
                raise RuntimeError(
                    "refusing_event_ledger_reset_after_validated_research"
                )

            self.fence_active_study_session(
                study_id=PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                reason="EMERGENCY_LEDGER_RESET_NO_VALIDATED_RESEARCH",
            )
            conn = self.connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE crypto_events RESTART IDENTITY")
                    cur.execute("TRUNCATE TABLE crypto_data_gaps")
                    cur.execute(
                        """
                        INSERT INTO crypto_storage_actions(action_key,completed_at,details)
                        VALUES(%s,%s,%s)
                        ON CONFLICT(action_key) DO NOTHING
                        """,
                        (
                            "RESET_EVENT_LEDGER_V1",
                            _utc_now(),
                            json.dumps(
                                {
                                    "reason": "free_tier_disk_pressure",
                                    "validated_research_present": False,
                                },
                                sort_keys=True,
                            ),
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
            return {"deleted_rows": 0, "dropped_indexes": 0, "reset": True}

        deleted_rows = 0
        dropped_indexes = 0
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute('DROP INDEX IF EXISTS "idx_crypto_events_symbol_time"')
                if cur.rowcount:
                    dropped_indexes += 1
            conn.commit()
        finally:
            conn.close()

        conn = self.connect()
        try:
            while True:
                with conn.cursor() as cur:
                    if keep_session_id:
                        cur.execute(
                            """
                            WITH doomed AS (
                                SELECT e.ctid
                                FROM crypto_events e
                                WHERE (e.metadata::jsonb->>'capture_session_id') IN (
                                    SELECT s.session_id
                                    FROM crypto_capture_sessions s
                                    WHERE s.status NOT IN ('STARTING','RUNNING')
                                )
                                AND (e.metadata::jsonb->>'capture_session_id') <> %s
                                LIMIT 5000
                            )
                            DELETE FROM crypto_events e
                            USING doomed d
                            WHERE e.ctid = d.ctid
                            """,
                            (keep_session_id,),
                        )
                    else:
                        cur.execute(
                            """
                            WITH doomed AS (
                                SELECT e.ctid
                                FROM crypto_events e
                                WHERE (e.metadata::jsonb->>'capture_session_id') IN (
                                    SELECT s.session_id
                                    FROM crypto_capture_sessions s
                                    WHERE s.status NOT IN ('STARTING','RUNNING')
                                )
                                LIMIT 5000
                            )
                            DELETE FROM crypto_events e
                            USING doomed d
                            WHERE e.ctid = d.ctid
                            """
                        )
                    batch = int(cur.rowcount or 0)
                conn.commit()
                deleted_rows += batch
                if batch == 0:
                    break
        finally:
            conn.close()

        vacuum_conn = self.connect()
        try:
            vacuum_conn.commit()
            vacuum_conn.autocommit = True
            with vacuum_conn.cursor() as cur:
                cur.execute('VACUUM (ANALYZE) "gorila_crypto"."crypto_events"')
        finally:
            vacuum_conn.close()
        return {"deleted_rows": deleted_rows, "dropped_indexes": dropped_indexes, "reset": False}

    def purge_legacy_unvalidated_events(self, *, keep_study_id: str) -> dict[str, int]:
        """Remove pre-v3 raw capture rows only when no validated research exists."""
        self.init()
        if not self._pg:
            return {"deleted_rows": 0}
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM crypto_validation_runs")
                validation_runs = int(cur.fetchone()[0] or 0)
                cur.execute(
                    "SELECT COUNT(*) FROM crypto_research_runs WHERE status='COMPLETE'"
                )
                completed_research = int(cur.fetchone()[0] or 0)
                if validation_runs or completed_research:
                    raise RuntimeError("refusing_legacy_event_purge_after_validated_research")
                cur.execute(
                    """
                    WITH doomed AS (
                        SELECT e.ctid
                        FROM crypto_events e
                        WHERE COALESCE(e.metadata::jsonb->>'crypto_study_id','') <> %s
                    )
                    DELETE FROM crypto_events e
                    USING doomed d
                    WHERE e.ctid=d.ctid
                    """,
                    (keep_study_id,),
                )
                deleted = int(cur.rowcount or 0)
            conn.commit()
        finally:
            conn.close()

        if deleted:
            vacuum = self.connect()
            try:
                vacuum.rollback()
                vacuum.autocommit = True
                with vacuum.cursor() as cur:
                    # The free Postgres tier is capacity-constrained. FULL
                    # rewrites this intentionally tiny research ledger after
                    # a guarded legacy purge and rebuilds its indexes without
                    # carrying dead pages from aborted cohorts forward.
                    cur.execute('VACUUM (FULL, ANALYZE) "gorila_crypto"."crypto_events"')
            finally:
                vacuum.close()
        return {"deleted_rows": deleted}

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
                        "SELECT protocol_hash, protocol_json FROM crypto_studies WHERE study_id=%s",
                        (protocol.study_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("protocol_not_registered_after_insert")
                    if str(row[0]) != protocol.protocol_hash:
                        if json.loads(str(row[1])) != json.loads(payload):
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
                    "SELECT protocol_hash, protocol_json FROM crypto_studies WHERE study_id=?",
                    (protocol.study_id,),
                ).fetchone()
                if row is None:
                    raise RuntimeError("protocol_not_registered_after_insert")
                if str(row[0]) != protocol.protocol_hash:
                    if json.loads(str(row[1])) != json.loads(payload):
                        raise RuntimeError("protocol_hash_conflict")
            conn.commit()
        finally:
            conn.close()
        return protocol.study_id


    def get_study_protocol_hash(self, study_id: str) -> str:
        """Return the persisted protocol identity for an already-registered study."""
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT protocol_hash FROM crypto_studies WHERE study_id=%s",
                        (study_id,),
                    )
                    row = cur.fetchone()
            else:
                row = conn.execute(
                    "SELECT protocol_hash FROM crypto_studies WHERE study_id=?",
                    (study_id,),
                ).fetchone()
            if row is None:
                raise RuntimeError("study_not_registered")
            return str(row[0])
        finally:
            conn.close()


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
                        (now, json.dumps({"reason": reason}, sort_keys=True), study_id),
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
                    (now, json.dumps({"reason": reason}, sort_keys=True), study_id),
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
        stale_after_seconds = 120.0
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.session_id,
                               s.status,
                               s.started_at,
                               COALESCE(MAX(r.heartbeat_at), '') AS heartbeat_at
                        FROM crypto_capture_sessions s
                        LEFT JOIN crypto_runtime_leases r
                          ON r.session_id = s.session_id
                        WHERE s.study_id=%s
                          AND s.status IN ('STARTING','RUNNING')
                        GROUP BY s.session_id,s.status,s.started_at
                        """,
                        (study_id,),
                    )
                    active = cur.fetchone()
                    if active is not None:
                        active_session_id, active_status, started_at, heartbeat_at = active
                        age_reference = heartbeat_at or started_at
                        cur.execute(
                            "SELECT EXTRACT(EPOCH FROM (NOW() - %s::timestamptz))",
                            (age_reference,),
                        )
                        age_seconds = float(cur.fetchone()[0] or 0.0)
                        if age_seconds > stale_after_seconds:
                            cur.execute(
                                """
                                UPDATE crypto_capture_sessions
                                SET status='ABORTED_STALE',ended_at=NOW()::text
                                WHERE session_id=%s
                                  AND status IN ('STARTING','RUNNING')
                                """,
                                (active_session_id,),
                            )
                            conn.commit()
                        else:
                            conn.rollback()
                            raise RuntimeError(
                                "active_capture_session_exists:"
                                f"session={active_session_id}:status={active_status}:"
                                f"age_seconds={age_seconds:.1f}"
                            )
                    else:
                        conn.rollback()
            else:
                conn.rollback()
        finally:
            conn.close()

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
                        "UPDATE crypto_capture_sessions SET status=%s, ended_at=%s WHERE session_id=%s",
                        (
                            status,
                            _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                            session_id,
                        ),
                    )
            else:
                conn.execute(
                    "UPDATE crypto_capture_sessions SET status=?, ended_at=? WHERE session_id=?",
                    (
                        status,
                        _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                        session_id,
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _is_v5_study(study_id: str) -> bool:
        return str(study_id) == str(PREREGISTERED_CRYPTO_PROTOCOL.study_id) and PREREGISTERED_CRYPTO_PROTOCOL.version == "5"

    @staticmethod
    def _v5_prepare_row(*, study_id: str, capture_session_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(row.get("payload") or {})
        event_type = str(row["event_type"])
        symbol = str(row["symbol"]).upper()
        sequence_start = int(row["sequence_start"])
        sequence_end = int(row["sequence_end"])
        identity = f"{row['source']}|{symbol}|{event_type}|{sequence_start}|{sequence_end}"
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
        import hashlib
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).digest()
        typed = {
            "price": None,
            "quantity": None,
            "buyer_maker": None,
            "bid": None,
            "bid_qty": None,
            "ask": None,
            "ask_qty": None,
        }
        if event_type == "trade":
            typed["price"] = float(payload.get("p"))
            typed["quantity"] = float(payload.get("q"))
            typed["buyer_maker"] = payload.get("m")
        elif event_type == "bookTicker":
            typed["bid"] = float(payload.get("b"))
            typed["bid_qty"] = float(payload.get("B"))
            typed["ask"] = float(payload.get("a"))
            typed["ask_qty"] = float(payload.get("A"))
        else:
            raise ValueError(f"v5 persistence does not support event_type={event_type}")
        metadata = dict(row.get("metadata") or {})
        ingest_epoch = metadata.get("ingest_epoch")
        return {
            "event_id": event_id,
            "study_id": study_id,
            "capture_session_id": capture_session_id,
            "symbol": symbol,
            "event_type": event_type,
            "event_time": str(row["event_time"]),
            "received_time": str(row["received_time"]),
            "provider_time": row.get("provider_time"),
            "source": str(row["source"]),
            "sequence_start": sequence_start,
            "sequence_end": sequence_end,
            "payload_hash": payload_hash,
            "quality": str(row.get("quality") or "OK"),
            "ingest_epoch": int(ingest_epoch) if ingest_epoch is not None else None,
            **typed,
            "recorded_at": str(row.get("recorded_at") or _utc_now()),
        }

    def _append_scoped_events_v5(
        self,
        *,
        study_id: str,
        capture_session_id: str,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        self.init()
        if not events:
            return []
        prepared = [
            self._v5_prepare_row(
                study_id=study_id,
                capture_session_id=capture_session_id,
                row=item,
            )
            for item in events
        ]
        keys = list(dict.fromkeys(
            (row["symbol"], row["event_type"], row["sequence_start"])
            for row in prepared
        ))
        conn = self._write_connection()
        try:
            found: dict[tuple[str, str, int], tuple[int, str, bytes]] = {}
            if self._pg:
                with conn.cursor() as cur:
                    sql = """
                        INSERT INTO crypto_events_v5(
                            event_id,study_id,capture_session_id,symbol,event_type,
                            event_time,received_time,provider_time,source,
                            sequence_start,sequence_end,payload_hash,quality,ingest_epoch,
                            price,quantity,buyer_maker,bid,bid_qty,ask,ask_qty,recorded_at
                        ) VALUES (
                            %s,%s,%s::uuid,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s
                        )
                        ON CONFLICT(symbol,event_type,sequence_start) DO NOTHING
                        RETURNING symbol,event_type,sequence_start,ledger_seq,event_id,payload_hash
                    """
                    flat = []
                    for row in prepared:
                        flat.extend([
                            row["event_id"],row["study_id"],row["capture_session_id"],row["symbol"],
                            row["event_type"],row["event_time"],row["received_time"],row["provider_time"],
                            row["source"],row["sequence_start"],row["sequence_end"],row["payload_hash"],
                            row["quality"],row["ingest_epoch"],row["price"],row["quantity"],
                            row["buyer_maker"],row["bid"],row["bid_qty"],row["ask"],row["ask_qty"],row["recorded_at"]
                        ])
                    cur.execute(sql, flat)
                    for symbol,event_type,seq,ledger_seq,event_id,payload_hash in cur.fetchall():
                        found[(str(symbol),str(event_type),int(seq))] = (int(ledger_seq),str(event_id),bytes(payload_hash))
                    missing=[key for key in keys if key not in found]
                    if missing:
                        cur.execute(
                            "SELECT symbol,event_type,sequence_start,ledger_seq,event_id,payload_hash "
                            "FROM crypto_events_v5 WHERE "
                            + " OR ".join(["(symbol=%s AND event_type=%s AND sequence_start=%s)"]*len(missing)),
                            [value for key in missing for value in key],
                        )
                        for symbol,event_type,seq,ledger_seq,event_id,payload_hash in cur.fetchall():
                            found[(str(symbol),str(event_type),int(seq))]=(int(ledger_seq),str(event_id),bytes(payload_hash))
            else:
                for row in prepared:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO crypto_events_v5(
                            event_id,study_id,capture_session_id,symbol,event_type,event_time,received_time,
                            provider_time,source,sequence_start,sequence_end,payload_hash,quality,ingest_epoch,
                            price,quantity,buyer_maker,bid,bid_qty,ask,ask_qty,recorded_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            row["event_id"],row["study_id"],row["capture_session_id"],row["symbol"],
                            row["event_type"],row["event_time"],row["received_time"],row["provider_time"],
                            row["source"],row["sequence_start"],row["sequence_end"],row["payload_hash"],
                            row["quality"],row["ingest_epoch"],row["price"],row["quantity"],
                            row["buyer_maker"],row["bid"],row["bid_qty"],row["ask"],row["ask_qty"],row["recorded_at"]
                        ),
                    )
                placeholders=",".join(["(?,?,?)"]*len(missing := keys))
                conn_rows=conn.execute(
                    "SELECT symbol,event_type,sequence_start,ledger_seq,event_id,payload_hash "
                    "FROM crypto_events_v5 WHERE " + " OR ".join(["(symbol=? AND event_type=? AND sequence_start=?)"]*len(missing)),
                    [value for key in missing for value in key],
                ).fetchall()
                for symbol,event_type,seq,ledger_seq,event_id,payload_hash in conn_rows:
                    found[(str(symbol),str(event_type),int(seq))]=(int(ledger_seq),str(event_id),bytes(payload_hash))
            if len(found) != len(keys):
                raise RuntimeError("v5_event_batch_resolution_failed")
            results=[]
            for row in prepared:
                key=(row["symbol"],row["event_type"],row["sequence_start"])
                ledger_seq,event_id,payload_hash=found[key]
                if payload_hash != row["payload_hash"]:
                    raise RuntimeError("provider_identity_conflict: v5 payload hash differs")
                results.append({
                    "inserted": str(event_id) == str(row["event_id"]),
                    "ledger_seq": ledger_seq,
                    "event_id": event_id,
                    "event_key": hashlib.md5(
                        f"{row['source']}|{row['symbol']}|{row['event_type']}|{row['sequence_start']}|{row['sequence_end']}".encode()
                    ).hexdigest(),
                })
            conn.commit()
            return results
        except Exception:
            try:
                conn.rollback()
            finally:
                self.close()
            raise

    def append_scoped_event(self, *, study_id: str, capture_session_id: str, **kwargs: Any) -> dict[str, Any]:
        if self._is_v5_study(study_id):
            return self._append_scoped_events_v5(
                study_id=study_id,
                capture_session_id=capture_session_id,
                events=[kwargs],
            )[0]
        metadata = dict(kwargs.pop("metadata", None) or {})
        metadata.update(
            {
                "crypto_study_id": study_id,
                "capture_session_id": capture_session_id,
            }
        )
        return super().append_event(metadata=metadata, **kwargs)

    def append_scoped_events(
        self,
        *,
        study_id: str,
        capture_session_id: str,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if self._is_v5_study(study_id):
            return self._append_scoped_events_v5(
                study_id=study_id,
                capture_session_id=capture_session_id,
                events=events,
            )
        scoped: list[dict[str, Any]] = []
        for item in events:
            row = dict(item)
            metadata = dict(row.pop("metadata", None) or {})
            metadata.update(
                {
                    "crypto_study_id": study_id,
                    "capture_session_id": capture_session_id,
                }
            )
            row["metadata"] = metadata
            scoped.append(row)
        return super().append_events(scoped)

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

    def heartbeat_runtime_run(self, run_id: str) -> None:
        self.init()
        heartbeat = _utc_now()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE crypto_runtime_leases SET heartbeat_at=%s,status='RUNNING' WHERE run_id=%s",
                        (heartbeat, run_id),
                    )
            else:
                conn.execute(
                    "UPDATE crypto_runtime_leases SET heartbeat_at=?,status='RUNNING' WHERE run_id=?",
                    (heartbeat, run_id),
                )
            conn.commit()
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
        super().finish_runtime_run(run_id=run_id, status=status, result=dict(result))
        self.init()
        conn = self.connect()
        try:
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
        """Best-effort cleanup of stale leases; never block capture takeover on a deploy race."""
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.init()
        cutoff = datetime.fromtimestamp(
            time.time() - stale_after_seconds,
            tz=timezone.utc,
        ).isoformat()

        for attempt in range(3):
            conn = self.connect()
            try:
                if self._pg:
                    try:
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
                            count = int(cur.rowcount)
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
                        conn.commit()
                        return count
                    except Exception as exc:
                        conn.rollback()
                        if type(exc).__name__ != "DeadlockDetected" or attempt == 2:
                            # This cleanup is advisory. The atomic session-creation
                            # fence below remains authoritative for takeover.
                            print(
                                "GORILA_STALE_RECONCILE_DEFERRED "
                                + json.dumps(
                                    {"attempt": attempt + 1, "error": f"{type(exc).__name__}: {exc}"},
                                    sort_keys=True,
                                ),
                                flush=True,
                            )
                            return 0
                        time.sleep(0.25 * (attempt + 1))
                        continue

                cur = conn.execute(
                    """
                    UPDATE crypto_runtime_runs
                    SET status='ABORTED_STALE',
                        completed_at=?,
                        result=?
                    WHERE status='RUNNING'
                      AND COALESCE(
                          (SELECT l.heartbeat_at
                           FROM crypto_runtime_leases l
                           WHERE l.run_id=crypto_runtime_runs.run_id),
                          created_at
                      ) < ?
                    """,
                    (_utc_now(), json.dumps({"reason": "stale_runtime_lease"}), cutoff),
                )
                count = int(cur.rowcount)
                conn.execute(
                    """
                    UPDATE crypto_runtime_leases
                    SET status='ABORTED_STALE'
                    WHERE status='RUNNING' AND heartbeat_at < ?
                    """,
                    (cutoff,),
                )
                conn.execute(
                    """
                    UPDATE crypto_capture_sessions
                    SET status='ABORTED_STALE', ended_at=?
                    WHERE status='RUNNING'
                      AND (
                          started_at < ?
                          OR session_id IN (
                              SELECT session_id
                              FROM crypto_runtime_leases
                              WHERE status='ABORTED_STALE'
                          )
                      )
                    """,
                    (_utc_now(), cutoff),
                )
                conn.commit()
                return count
            finally:
                conn.close()

        return 0

    def read_scoped_data_gaps(
        self,
        *,
        study_id: str,
        capture_session_id: str | None = None,
        source_prefix: str | None = None,
        limit: int = 10_000,
    ) -> list[dict[str, Any]]:
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        params: list[Any] = [study_id]
        placeholder = "%s" if self._pg else "?"
        clauses = [
            "metadata::jsonb->>'crypto_study_id'=%s"
            if self._pg
            else "json_extract(metadata, '$.crypto_study_id')=?"
        ]
        if capture_session_id is not None:
            clauses.append(
                "metadata::jsonb->>'capture_session_id'=%s"
                if self._pg
                else "json_extract(metadata, '$.capture_session_id')=?"
            )
            params.append(capture_session_id)
        if source_prefix is not None:
            clauses.append(f"source LIKE {placeholder}")
            params.append(source_prefix.rstrip("%") + "%")
        where = " AND ".join(clauses)
        query = (
            "SELECT gap_id,detected_at,symbol,source,expected_sequence,"
            f"observed_sequence,status,metadata FROM crypto_data_gaps WHERE {where} "
            "ORDER BY detected_at DESC LIMIT " + str(int(limit))
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                keys = [
                    "gap_id","detected_at","symbol","source",
                    "expected_sequence","observed_sequence","status","metadata"
                ]
                return [
                    {
                        key: (json.loads(value) if key == "metadata" else value)
                        for key, value in zip(keys, row)
                    }
                    for row in rows
                ]
            rows = conn.execute(query, params).fetchall()
            return [
                {**dict(row), "metadata": json.loads(row["metadata"] or "{}")}
                for row in rows
            ]
        finally:
            conn.close()

    def active_capture_session(self, study_id: str) -> str | None:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT session_id FROM crypto_capture_sessions "
                        "WHERE study_id=%s AND status='RUNNING' ORDER BY started_at DESC LIMIT 1",
                        (study_id,),
                    )
                    row = cur.fetchone()
            else:
                row = conn.execute(
                    "SELECT session_id FROM crypto_capture_sessions "
                    "WHERE study_id=? AND status='RUNNING' ORDER BY started_at DESC LIMIT 1",
                    (study_id,),
                ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()

    def scoped_stats(self, *, study_id: str, capture_session_id: str | None = None) -> dict[str, Any]:
        self.init()
        params: list[Any] = [study_id]
        placeholder = "%s" if self._pg else "?"
        clauses = ["e.metadata::jsonb->>'crypto_study_id'=%s"] if self._pg else [
            "json_extract(e.metadata, '$.crypto_study_id')=?"
        ]
        if capture_session_id is not None:
            clauses.append(
                "e.metadata::jsonb->>'capture_session_id'=%s"
                if self._pg
                else "json_extract(e.metadata, '$.capture_session_id')=?"
            )
            params.append(capture_session_id)
        where = " AND ".join(clauses)
        query = (
            "SELECT e.symbol,e.event_type,count(*) AS rows,"
            "min(e.event_time) AS first_event,max(e.event_time) AS last_event "
            f"FROM crypto_events e WHERE {where} "
            "GROUP BY e.symbol,e.event_type ORDER BY e.symbol,e.event_type"
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
            else:
                rows = conn.execute(query, params).fetchall()
            grouped = [
                {
                    "symbol": str(row[0]),
                    "event_type": str(row[1]),
                    "rows": int(row[2]),
                    "first_event": str(row[3]),
                    "last_event": str(row[4]),
                }
                for row in rows
            ]
            return {
                "study_id": study_id,
                "capture_session_id": capture_session_id,
                "backend": self.backend,
                "event_counts": grouped,
                "total_rows": sum(item["rows"] for item in grouped),
            }
        finally:
            conn.close()

    def _read_scoped_events_v5(
        self,
        *,
        study_id: str,
        capture_session_id: str | None = None,
        symbol: str | None = None,
        source: str | None = None,
        start_received_time: str | None = None,
        end_received_time: str | None = None,
        start_event_time: str | None = None,
        end_event_time: str | None = None,
        order: str = "ingest",
        limit: int = 100_000,
        include_payload: bool = True,
    ) -> list[dict[str, Any]]:
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        clauses = ["study_id=%s"] if self._pg else ["study_id=?"]
        params: list[Any] = [study_id]
        if capture_session_id is not None:
            clauses.append("capture_session_id=%s" if self._pg else "capture_session_id=?")
            params.append(capture_session_id)
        if symbol is not None:
            clauses.append("symbol=%s" if self._pg else "symbol=?")
            params.append(symbol.upper())
        if source is not None:
            clauses.append("source=%s" if self._pg else "source=?")
            params.append(source)
        if start_received_time is not None:
            clauses.append("received_time>=%s" if self._pg else "received_time>=?")
            params.append(start_received_time)
        if end_received_time is not None:
            clauses.append("received_time<=%s" if self._pg else "received_time<=?")
            params.append(end_received_time)
        if start_event_time is not None:
            clauses.append("event_time>=%s" if self._pg else "event_time>=?")
            params.append(start_event_time)
        if end_event_time is not None:
            clauses.append("event_time<=%s" if self._pg else "event_time<=?")
            params.append(end_event_time)
        where=" AND ".join(clauses)
        order_by={
            "ingest":"ledger_seq ASC",
            "event_time":"event_time ASC,received_time ASC,ledger_seq ASC",
        }.get(order)
        if order_by is None:
            raise ValueError("invalid replay order")
        payload_sql = "" if include_payload else ""
        query=f"""
            SELECT ledger_seq,event_id,study_id,capture_session_id,symbol,event_type,
                   event_time,received_time,provider_time,source,sequence_start,sequence_end,
                   encode(payload_hash,'hex') AS payload_hash,quality,ingest_epoch,
                   price,quantity,buyer_maker,bid,bid_qty,ask,ask_qty,recorded_at
            FROM crypto_events_v5
            WHERE {where}
            ORDER BY {order_by}
            LIMIT {int(limit)}
        """
        if not self._pg:
            query=query.replace("encode(payload_hash,'hex')","lower(hex(payload_hash))")
        conn=self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query,params); rows=cur.fetchall()
            else:
                rows=conn.execute(query,params).fetchall()
        finally:
            conn.close()
        result=[]
        for row in rows:
            if self._pg:
                vals=list(row)
            else:
                vals=list(row)
            (ledger_seq,event_id,row_study,row_session,symbol,event_type,event_time,received_time,
             provider_time,source,seq_start,seq_end,payload_hash,quality,ingest_epoch,price,quantity,
             buyer_maker,bid,bid_qty,ask,ask_qty,recorded_at)=vals
            if event_type=="trade":
                payload={"e":"trade","s":symbol,"t":int(seq_start),"p":str(price),"q":str(quantity),"m":buyer_maker}
            else:
                payload={"e":"bookTicker","s":symbol,"u":int(seq_end),"b":str(bid),"B":str(bid_qty),"a":str(ask),"A":str(ask_qty)}
            if not include_payload:
                payload=None
            event_key=__import__("hashlib").md5(
                f"{source}|{symbol}|{event_type}|{seq_start}|{seq_end}".encode()
            ).hexdigest()
            result.append({
                "ledger_seq":int(ledger_seq),
                "event_id":str(event_id),
                "event_key":event_key,
                "symbol":str(symbol),
                "event_type":str(event_type),
                "event_time":str(event_time),
                "received_time":str(received_time),
                "provider_time":str(provider_time) if provider_time is not None else None,
                "source":str(source),
                "sequence_start":int(seq_start),
                "sequence_end":int(seq_end),
                "payload_hash":str(payload_hash),
                "payload":payload,
                "quality":str(quality),
                "metadata":{
                    "crypto_study_id":str(row_study),
                    "capture_session_id":str(row_session),
                    "ingest_epoch":int(ingest_epoch) if ingest_epoch is not None else None,
                },
                "recorded_at":str(recorded_at),
            })
        return result

    def read_scoped_events(
        self,
        *,
        study_id: str,
        capture_session_id: str | None = None,
        symbol: str | None = None,
        source: str | None = None,
        source_prefix: str | None = None,
        start_received_time: str | None = None,
        end_received_time: str | None = None,
        start_event_time: str | None = None,
        end_event_time: str | None = None,
        order: str = "ingest",
        limit: int = 100_000,
        include_payload: bool = True,
    ) -> list[dict[str, Any]]:
        """Read exactly one study/session cohort using the immutable metadata scope."""
        if self._is_v5_study(study_id):
            return self._read_scoped_events_v5(
                study_id=study_id,
                capture_session_id=capture_session_id,
                symbol=symbol,
                source=source,
                start_received_time=start_received_time,
                end_received_time=end_received_time,
                start_event_time=start_event_time,
                end_event_time=end_event_time,
                order=order,
                limit=limit,
                include_payload=include_payload,
            )
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        order_by = {
            "ingest": "e.ledger_seq ASC",
            "ingest_desc": "e.ledger_seq DESC",
            "event_time": "e.event_time ASC, e.received_time ASC, e.ledger_seq ASC",
        }.get(order)
        if order_by is None:
            raise ValueError("invalid replay order")
        params: list[Any] = [study_id]
        placeholder = "%s" if self._pg else "?"
        clauses = ["e.metadata::jsonb->>'crypto_study_id'=%s"] if self._pg else [
            "json_extract(e.metadata, '$.crypto_study_id')=?"
        ]
        if capture_session_id is not None:
            clauses.append(
                "e.metadata::jsonb->>'capture_session_id'=%s"
                if self._pg
                else "json_extract(e.metadata, '$.capture_session_id')=?"
            )
            params.append(capture_session_id)
        if symbol is not None:
            clauses.append(f"e.symbol={placeholder}")
            params.append(symbol.upper())
        if source is not None:
            clauses.append(f"e.source={placeholder}")
            params.append(source)
        if source_prefix:
            clauses.append(f"e.source LIKE {placeholder}")
            params.append(source_prefix.rstrip("%") + "%")
        if start_received_time is not None:
            clauses.append(f"e.received_time>={placeholder}")
            params.append(start_received_time)
        if end_received_time is not None:
            clauses.append(f"e.received_time<={placeholder}")
            params.append(end_received_time)
        if start_event_time is not None:
            clauses.append(f"e.event_time>={placeholder}")
            params.append(start_event_time)
        if end_event_time is not None:
            clauses.append(f"e.event_time<={placeholder}")
            params.append(end_event_time)
        where = " AND ".join(clauses)
        payload_column = "e.payload_json" if include_payload else "NULL AS payload_json"
        query = (
            "SELECT e.ledger_seq,e.event_id,e.event_key,e.symbol,e.event_type,"
            "e.event_time,e.received_time,e.provider_time,e.source,e.sequence_start,"
            "e.sequence_end,e.payload_hash,"
            f"{payload_column},e.quality,e.metadata,e.recorded_at "
            "FROM crypto_events e WHERE "
            f"{where} ORDER BY {order_by} LIMIT {int(limit)}"
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    return self._rows_to_dict(rows)
            rows = conn.execute(query, params).fetchall()
            return self._rows_to_dict(rows, sqlite=True)
        finally:
            conn.close()

    @staticmethod
    def _rows_to_dict(rows: Any, sqlite: bool = False) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in rows:
            if sqlite:
                values = [row[key] for key in (
                    "ledger_seq","event_id","event_key","symbol","event_type",
                    "event_time","received_time","provider_time","source",
                    "sequence_start","sequence_end","payload_hash","payload_json",
                    "quality","metadata","recorded_at"
                )]
            else:
                values = list(row)
            output.append(
                {
                    "ledger_seq": int(values[0]),
                    "event_id": str(values[1]),
                    "event_key": str(values[2]),
                    "symbol": str(values[3]),
                    "event_type": str(values[4]),
                    "event_time": str(values[5]),
                    "received_time": str(values[6]),
                    "provider_time": values[7],
                    "source": str(values[8]),
                    "sequence_start": values[9],
                    "sequence_end": values[10],
                    "payload_hash": str(values[11]),
                    "payload": json.loads(values[12]) if values[12] is not None else None,
                    "quality": str(values[13]),
                    "metadata": json.loads(values[14]),
                    "recorded_at": str(values[15]),
                }
            )
        return output