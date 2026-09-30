"""Quantitative-research persistence wrapper.

Adds immutable study/session scope and crash-safe runtime leases without changing
the legacy CryptoStore schema contract.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

from .protocol import CryptoStudyProtocol
from .storage import CryptoStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class QuantCryptoStore(CryptoStore):
    """CryptoStore with experiment scoping and runtime lease semantics."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._quant_schema_ready = False

    def init(self) -> None:
        if self._quant_schema_ready:
            return
        super().init()
        with self._schema_lock:
            if self._quant_schema_ready:
                return
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
            self._quant_schema_ready = True

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
                      AND COALESCE(
                          (SELECT l.heartbeat_at
                           FROM crypto_runtime_leases l
                           WHERE l.run_id=crypto_runtime_runs.run_id),
                          created_at
                      ) < ?
                    """,
                    (_utc_now(), json.dumps({"reason": "stale_runtime_lease"}), cutoff),
                )
                count = cur.rowcount
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
        finally:
            conn.close()
        return int(count)

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
            f"ORDER BY detected_at DESC LIMIT {int(limit)}"
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                keys = (
                    "gap_id","detected_at","symbol","source",
                    "expected_sequence","observed_sequence","status","metadata"
                )
                return [
                    {
                        key: json.loads(value) if key == "metadata" else value
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
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        order_by = {
            "ingest": "e.ledger_seq ASC",
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