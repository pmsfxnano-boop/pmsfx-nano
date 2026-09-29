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
CRYPTO_SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS crypto_events (
    ledger_seq BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_key TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    received_time TEXT NOT NULL,
    provider_time TEXT,
    source TEXT NOT NULL,
    sequence_start BIGINT,
    sequence_end BIGINT,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    quality TEXT NOT NULL DEFAULT 'OK',
    metadata TEXT NOT NULL DEFAULT '{}',
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_events_symbol_time
    ON crypto_events(symbol, event_time, ledger_seq);
CREATE INDEX IF NOT EXISTS idx_crypto_events_source_time
    ON crypto_events(source, event_time, ledger_seq);
CREATE INDEX IF NOT EXISTS idx_crypto_events_sequence
    ON crypto_events(symbol, sequence_start, sequence_end);
CREATE INDEX IF NOT EXISTS idx_crypto_events_received
    ON crypto_events(received_time, ledger_seq);

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

CREATE TABLE IF NOT EXISTS crypto_replay_manifests (
    manifest_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    replay_version TEXT NOT NULL,
    order_mode TEXT NOT NULL,
    symbol TEXT,
    source TEXT,
    start_received_time TEXT,
    end_received_time TEXT,
    start_event_time TEXT,
    end_event_time TEXT,
    row_count INTEGER NOT NULL,
    first_ledger_seq BIGINT,
    last_ledger_seq BIGINT,
    fingerprint_sha256 TEXT NOT NULL,
    manifest_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_replay_manifest_time
    ON crypto_replay_manifests(created_at);
CREATE INDEX IF NOT EXISTS idx_crypto_replay_manifest_fingerprint
    ON crypto_replay_manifests(fingerprint_sha256);

CREATE TABLE IF NOT EXISTS crypto_lead_lag_shadow (
    observation_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    replay_fingerprint TEXT NOT NULL,
    leader_symbol TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    leader_ledger_seq BIGINT NOT NULL,
    leader_event_time TEXT NOT NULL,
    leader_received_time TEXT NOT NULL,
    delay_ms INTEGER NOT NULL,
    target_ledger_seq BIGINT NOT NULL,
    target_event_time TEXT NOT NULL,
    target_received_time TEXT NOT NULL,
    leader_return_bps DOUBLE PRECISION NOT NULL,
    target_return_bps DOUBLE PRECISION NOT NULL,
    signed_target_response_bps DOUBLE PRECISION NOT NULL,
    market_lag_ms DOUBLE PRECISION NOT NULL,
    information_lag_ms DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_lead_lag_pair
    ON crypto_lead_lag_shadow(leader_symbol,target_symbol,delay_ms,created_at);
CREATE INDEX IF NOT EXISTS idx_crypto_lead_lag_fingerprint
    ON crypto_lead_lag_shadow(replay_fingerprint);

CREATE TABLE IF NOT EXISTS crypto_opportunity_shadow (
    opportunity_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    replay_fingerprint TEXT NOT NULL,
    leader_symbol TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    leader_ledger_seq BIGINT NOT NULL,
    direction INTEGER NOT NULL,
    leader_return_bps DOUBLE PRECISION NOT NULL,
    detection_event_time TEXT NOT NULL,
    detection_received_time TEXT NOT NULL,
    baseline_target_price DOUBLE PRECISION NOT NULL,
    first_reaction_ledger_seq BIGINT,
    first_reaction_event_time TEXT,
    first_reaction_received_time TEXT,
    convergence_ledger_seq BIGINT,
    convergence_event_time TEXT,
    convergence_received_time TEXT,
    first_reaction_market_lag_ms DOUBLE PRECISION,
    first_reaction_information_lag_ms DOUBLE PRECISION,
    convergence_market_lag_ms DOUBLE PRECISION,
    convergence_information_lag_ms DOUBLE PRECISION,
    max_favorable_excursion_bps DOUBLE PRECISION NOT NULL,
    max_adverse_excursion_bps DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_crypto_opportunity_pair
    ON crypto_opportunity_shadow(leader_symbol,target_symbol,created_at);
CREATE INDEX IF NOT EXISTS idx_crypto_opportunity_status
    ON crypto_opportunity_shadow(status,created_at);
CREATE INDEX IF NOT EXISTS idx_crypto_opportunity_fingerprint
    ON crypto_opportunity_shadow(replay_fingerprint);

CREATE TABLE IF NOT EXISTS crypto_forecast_shadow (
    forecast_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    replay_fingerprint TEXT NOT NULL,
    model_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    symbol TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    leader_event_id TEXT NOT NULL,
    decision_event_time TEXT NOT NULL,
    decision_received_time TEXT NOT NULL,
    horizon_ms INTEGER NOT NULL,
    target_kind TEXT NOT NULL,
    semantics TEXT NOT NULL,
    probability_response_positive DOUBLE PRECISION,
    status TEXT NOT NULL,
    feature_set_hash TEXT NOT NULL,
    features_json TEXT NOT NULL,
    source_event_ids_json TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_crypto_forecast_pair_time
    ON crypto_forecast_shadow(symbol,target_symbol,decision_received_time);
CREATE INDEX IF NOT EXISTS idx_crypto_forecast_model
    ON crypto_forecast_shadow(model_id,model_version,decision_received_time);
CREATE INDEX IF NOT EXISTS idx_crypto_forecast_fingerprint
    ON crypto_forecast_shadow(replay_fingerprint);

CREATE TABLE IF NOT EXISTS crypto_forecast_outcomes (
    forecast_id TEXT PRIMARY KEY,
    observed_at TEXT NOT NULL,
    observed_event_id TEXT,
    observed_price DOUBLE PRECISION,
    realized_signed_return_bps DOUBLE PRECISION,
    realized_target INTEGER,
    transaction_cost_bps DOUBLE PRECISION,
    slippage_bps DOUBLE PRECISION,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}'
);

"""

_SQLITE_SCHEMA = (
    SCHEMA
    .replace("BIGSERIAL", "INTEGER").replace("BIGINT", "INTEGER")
    .replace("DOUBLE PRECISION", "REAL")
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _pg_identifier(value: str) -> str:
    return value.replace('"', '""')


class LedgerIntegrityError(RuntimeError):
    """Raised when the same provider identity is delivered with different content."""


class CryptoStore:
    """Storage boundary that can only address the Crypto persistence namespace."""

    _schema_lock = threading.Lock()

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
        ).strip() or "/tmp/gorila_crypto.sqlite3"
        legacy_sqlite_path = os.getenv("GORILA_SQLITE_PATH", "").strip()
        if (
            not self.database_url
            and legacy_sqlite_path
            and self.sqlite_path == legacy_sqlite_path
        ):
            raise ValueError("crypto_sqlite_path_matches_legacy_storage")
        self._pg = bool(self.database_url)
        self._schema_ready = False

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
                    self._schema_ready = True
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
        self.init()
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

    def append_event(
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
        event_key: str | None = None,
    ) -> dict[str, Any]:
        """Append one immutable event idempotently and return ledger metadata.

        Duplicate provider deliveries are detected by event_key. The payload is
        stored canonically so later replay does not depend on the live provider.
        """
        self.init()
        symbol = symbol.upper()
        payload_json = _json(payload)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        identity = {
            "source": source,
            "symbol": symbol,
            "event_type": event_type,
            "sequence_start": sequence_start,
            "sequence_end": sequence_end,
        }
        if sequence_start is None or sequence_end is None:
            identity.update(
                {
                    "event_time": event_time,
                    "provider_time": provider_time,
                    "payload_hash": payload_hash,
                }
            )
        event_key = event_key or hashlib.sha256(
            _json(identity).encode("utf-8")
        ).hexdigest()
        event_id = event_id or str(uuid.uuid4())
        recorded_at = _utc_now()
        values = (
            event_id,
            event_key,
            symbol,
            event_type,
            event_time,
            received_time,
            provider_time,
            source,
            sequence_start,
            sequence_end,
            payload_hash,
            payload_json,
            quality,
            _json(metadata or {}),
            recorded_at,
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_events
                        (event_id,event_key,symbol,event_type,event_time,received_time,
                         provider_time,source,sequence_start,sequence_end,payload_hash,
                         payload_json,quality,metadata,recorded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(event_key) DO NOTHING
                        RETURNING ledger_seq,event_id,event_key,false AS duplicate""",
                        values,
                    )
                    row = cur.fetchone()
                    if row is None:
                        cur.execute(
                            "SELECT ledger_seq,event_id,event_key,payload_hash "
                            "FROM crypto_events WHERE event_key=%s",
                            (event_key,),
                        )
                        existing = cur.fetchone()
                        if existing is None:
                            raise RuntimeError("event_deduplication_lookup_failed")
                        if str(existing[3]) != payload_hash:
                            raise LedgerIntegrityError(
                                "provider_identity_conflict: existing payload hash differs"
                            )
                        result = {
                            "inserted": False,
                            "ledger_seq": int(existing[0]),
                            "event_id": str(existing[1]),
                            "event_key": str(existing[2]),
                        }
                    else:
                        result = {
                            "inserted": True,
                            "ledger_seq": int(row[0]),
                            "event_id": str(row[1]),
                            "event_key": str(row[2]),
                        }
            else:
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO crypto_events
                    (event_id,event_key,symbol,event_type,event_time,received_time,
                     provider_time,source,sequence_start,sequence_end,payload_hash,
                     payload_json,quality,metadata,recorded_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
                if cursor.rowcount == 1:
                    result = {
                        "inserted": True,
                        "ledger_seq": int(cursor.lastrowid),
                        "event_id": event_id,
                        "event_key": event_key,
                    }
                else:
                    row = conn.execute(
                        "SELECT ledger_seq,event_id,event_key,payload_hash "
                        "FROM crypto_events WHERE event_key=?",
                        (event_key,),
                    ).fetchone()
                    if row is None:
                        raise RuntimeError("event_deduplication_lookup_failed")
                    if str(row[3]) != payload_hash:
                        raise LedgerIntegrityError(
                            "provider_identity_conflict: existing payload hash differs"
                        )
                    result = {
                        "inserted": False,
                        "ledger_seq": int(row[0]),
                        "event_id": str(row[1]),
                        "event_key": str(row[2]),
                    }
            conn.commit()
            return result
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
        result = self.append_event(
            symbol=symbol,
            event_type=event_type,
            event_time=event_time,
            received_time=received_time,
            source=source,
            payload=payload,
            provider_time=provider_time,
            sequence_start=sequence_start,
            sequence_end=sequence_end,
            quality=quality,
            metadata=metadata,
            event_id=event_id,
        )
        return str(result["event_id"])

    def read_events(
        self,
        *,
        symbol: str | None = None,
        source: str | None = None,
        start_received_time: str | None = None,
        end_received_time: str | None = None,
        start_event_time: str | None = None,
        end_event_time: str | None = None,
        order: str = "ingest",
        limit: int = 100000,
    ) -> list[dict[str, Any]]:
        """Read immutable ledger rows using an explicit deterministic ordering."""
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        order_by = {
            "ingest": "ledger_seq ASC",
            "event_time": "event_time ASC, received_time ASC, ledger_seq ASC",
        }.get(order)
        if order_by is None:
            raise ValueError("order must be 'ingest' or 'event_time'")

        clauses: list[str] = []
        params: list[Any] = []
        placeholder = "%s" if self._pg else "?"

        if symbol is not None:
            clauses.append(f"symbol={placeholder}")
            params.append(symbol.upper())
        if source is not None:
            clauses.append(f"source={placeholder}")
            params.append(source)
        if start_received_time is not None:
            clauses.append(f"received_time>={placeholder}")
            params.append(start_received_time)
        if end_received_time is not None:
            clauses.append(f"received_time<={placeholder}")
            params.append(end_received_time)
        if start_event_time is not None:
            clauses.append(f"event_time>={placeholder}")
            params.append(start_event_time)
        if end_event_time is not None:
            clauses.append(f"event_time<={placeholder}")
            params.append(end_event_time)

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        limit_sql = f" LIMIT {int(limit)}"
        conn = self.connect()
        try:
            query = (
                "SELECT ledger_seq,event_id,event_key,symbol,event_type,event_time,"
                "received_time,provider_time,source,sequence_start,sequence_end,"
                "payload_hash,payload_json,quality,metadata,recorded_at "
                f"FROM crypto_events{where} ORDER BY {order_by}{limit_sql}"
            )
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    return [
                        {
                            "ledger_seq": int(row[0]),
                            "event_id": str(row[1]),
                            "event_key": str(row[2]),
                            "symbol": str(row[3]),
                            "event_type": str(row[4]),
                            "event_time": str(row[5]),
                            "received_time": str(row[6]),
                            "provider_time": row[7],
                            "source": str(row[8]),
                            "sequence_start": row[9],
                            "sequence_end": row[10],
                            "payload_hash": str(row[11]),
                            "payload": json.loads(row[12]),
                            "quality": str(row[13]),
                            "metadata": json.loads(row[14]),
                            "recorded_at": str(row[15]),
                        }
                        for row in rows
                    ]

            rows = conn.execute(query, params).fetchall()
            return [
                {
                    "ledger_seq": int(row["ledger_seq"]),
                    "event_id": str(row["event_id"]),
                    "event_key": str(row["event_key"]),
                    "symbol": str(row["symbol"]),
                    "event_type": str(row["event_type"]),
                    "event_time": str(row["event_time"]),
                    "received_time": str(row["received_time"]),
                    "provider_time": row["provider_time"],
                    "source": str(row["source"]),
                    "sequence_start": row["sequence_start"],
                    "sequence_end": row["sequence_end"],
                    "payload_hash": str(row["payload_hash"]),
                    "payload": json.loads(row["payload_json"]),
                    "quality": str(row["quality"]),
                    "metadata": json.loads(row["metadata"]),
                    "recorded_at": str(row["recorded_at"]),
                }
                for row in rows
            ]
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

    def save_replay_manifest(self, manifest: dict[str, Any]) -> str:
        self.init()
        manifest_id = str(uuid.uuid4())
        values = (
            manifest_id,
            _utc_now(),
            str(manifest.get("replay_version", "1")),
            str(manifest.get("order", "ingest")),
            manifest.get("symbol"),
            manifest.get("source"),
            manifest.get("start_received_time"),
            manifest.get("end_received_time"),
            manifest.get("start_event_time"),
            manifest.get("end_event_time"),
            int(manifest.get("row_count", 0)),
            manifest.get("first_ledger_seq"),
            manifest.get("last_ledger_seq"),
            str(manifest["fingerprint_sha256"]),
            _json(manifest),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_replay_manifests
                        (manifest_id,created_at,replay_version,order_mode,symbol,source,
                         start_received_time,end_received_time,start_event_time,end_event_time,
                         row_count,first_ledger_seq,last_ledger_seq,fingerprint_sha256,manifest_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT INTO crypto_replay_manifests
                    (manifest_id,created_at,replay_version,order_mode,symbol,source,
                     start_received_time,end_received_time,start_event_time,end_event_time,
                     row_count,first_ledger_seq,last_ledger_seq,fingerprint_sha256,manifest_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return manifest_id
        finally:
            conn.close()

    def save_lead_lag_observations(
        self,
        observations: list[dict[str, Any]],
    ) -> int:
        self.init()
        if not observations:
            return 0
        conn = self.connect()
        inserted = 0
        try:
            for row in observations:
                observation_id = str(
                    hashlib.sha256(
                        _json(
                            {
                                "fingerprint": row["replay_fingerprint"],
                                "leader_symbol": row["leader_symbol"],
                                "target_symbol": row["target_symbol"],
                                "leader_ledger_seq": row["leader_ledger_seq"],
                                "delay_ms": row["delay_ms"],
                            }
                        ).encode("utf-8")
                    ).hexdigest()[:32]
                )
                values = (
                    observation_id,
                    _utc_now(),
                    row["replay_fingerprint"],
                    row["leader_symbol"].upper(),
                    row["target_symbol"].upper(),
                    int(row["leader_ledger_seq"]),
                    row["leader_event_time"],
                    row["leader_received_time"],
                    int(row["delay_ms"]),
                    int(row["target_ledger_seq"]),
                    row["target_event_time"],
                    row["target_received_time"],
                    float(row["leader_return_bps"]),
                    float(row["target_return_bps"]),
                    float(row["signed_target_response_bps"]),
                    float(row["market_lag_ms"]),
                    float(row["information_lag_ms"]),
                )
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO crypto_lead_lag_shadow
                            (observation_id,created_at,replay_fingerprint,leader_symbol,
                             target_symbol,leader_ledger_seq,leader_event_time,
                             leader_received_time,delay_ms,target_ledger_seq,
                             target_event_time,target_received_time,leader_return_bps,
                             target_return_bps,signed_target_response_bps,
                             market_lag_ms,information_lag_ms)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(observation_id) DO NOTHING""",
                            values,
                        )
                        inserted += max(cur.rowcount, 0)
                else:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO crypto_lead_lag_shadow
                        (observation_id,created_at,replay_fingerprint,leader_symbol,
                         target_symbol,leader_ledger_seq,leader_event_time,
                         leader_received_time,delay_ms,target_ledger_seq,
                         target_event_time,target_received_time,leader_return_bps,
                         target_return_bps,signed_target_response_bps,
                         market_lag_ms,information_lag_ms)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        values,
                    )
                    inserted += max(cur.rowcount, 0)
            conn.commit()
            return inserted
        finally:
            conn.close()

    def save_opportunity_clocks(
        self,
        opportunities: list[dict[str, Any]],
    ) -> int:
        self.init()
        if not opportunities:
            return 0
        conn = self.connect()
        inserted = 0
        try:
            for row in opportunities:
                values = (
                    row["opportunity_id"],
                    _utc_now(),
                    row["replay_fingerprint"],
                    row["leader_symbol"].upper(),
                    row["target_symbol"].upper(),
                    int(row["leader_ledger_seq"]),
                    int(row["direction"]),
                    float(row["leader_return_bps"]),
                    row["detection_event_time"],
                    row["detection_received_time"],
                    float(row["baseline_target_price"]),
                    row.get("first_reaction_ledger_seq"),
                    row.get("first_reaction_event_time"),
                    row.get("first_reaction_received_time"),
                    row.get("convergence_ledger_seq"),
                    row.get("convergence_event_time"),
                    row.get("convergence_received_time"),
                    row.get("first_reaction_market_lag_ms"),
                    row.get("first_reaction_information_lag_ms"),
                    row.get("convergence_market_lag_ms"),
                    row.get("convergence_information_lag_ms"),
                    float(row["max_favorable_excursion_bps"]),
                    float(row["max_adverse_excursion_bps"]),
                    row["status"],
                    _json(row.get("metadata") or {}),
                )
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO crypto_opportunity_shadow
                            (opportunity_id,created_at,replay_fingerprint,leader_symbol,
                             target_symbol,leader_ledger_seq,direction,leader_return_bps,
                             detection_event_time,detection_received_time,
                             baseline_target_price,first_reaction_ledger_seq,
                             first_reaction_event_time,first_reaction_received_time,
                             convergence_ledger_seq,convergence_event_time,
                             convergence_received_time,first_reaction_market_lag_ms,
                             first_reaction_information_lag_ms,convergence_market_lag_ms,
                             convergence_information_lag_ms,max_favorable_excursion_bps,
                             max_adverse_excursion_bps,status,metadata)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(opportunity_id) DO NOTHING""",
                            values,
                        )
                        inserted += max(cur.rowcount, 0)
                else:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO crypto_opportunity_shadow
                        (opportunity_id,created_at,replay_fingerprint,leader_symbol,
                         target_symbol,leader_ledger_seq,direction,leader_return_bps,
                         detection_event_time,detection_received_time,
                         baseline_target_price,first_reaction_ledger_seq,
                         first_reaction_event_time,first_reaction_received_time,
                         convergence_ledger_seq,convergence_event_time,
                         convergence_received_time,first_reaction_market_lag_ms,
                         first_reaction_information_lag_ms,convergence_market_lag_ms,
                         convergence_information_lag_ms,max_favorable_excursion_bps,
                         max_adverse_excursion_bps,status,metadata)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        values,
                    )
                    inserted += max(cur.rowcount, 0)
            conn.commit()
            return inserted
        finally:
            conn.close()

    def save_forecast_shadow(self, row: dict[str, Any]) -> str:
        self.init()
        forecast_id = str(row["forecast_id"])
        values = (
            forecast_id,
            _utc_now(),
            row["replay_fingerprint"],
            row["model_id"],
            row["model_version"],
            row["symbol"].upper(),
            row["target_symbol"].upper(),
            row["leader_event_id"],
            row["decision_event_time"],
            row["decision_received_time"],
            int(row["horizon_ms"]),
            row["target_kind"],
            row["semantics"],
            row.get("probability_response_positive"),
            row["status"],
            row["feature_set_hash"],
            _json(row.get("features") or {}),
            _json(row.get("source_event_ids") or []),
            _json(row.get("metadata") or {}),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_forecast_shadow
                        (forecast_id,created_at,replay_fingerprint,model_id,model_version,
                         symbol,target_symbol,leader_event_id,decision_event_time,
                         decision_received_time,horizon_ms,target_kind,semantics,
                         probability_response_positive,status,feature_set_hash,
                         features_json,source_event_ids_json,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(forecast_id) DO NOTHING""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO crypto_forecast_shadow
                    (forecast_id,created_at,replay_fingerprint,model_id,model_version,
                     symbol,target_symbol,leader_event_id,decision_event_time,
                     decision_received_time,horizon_ms,target_kind,semantics,
                     probability_response_positive,status,feature_set_hash,
                     features_json,source_event_ids_json,metadata)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return forecast_id
        finally:
            conn.close()

    def save_forecast_outcome(self, row: dict[str, Any]) -> str:
        self.init()
        forecast_id = str(row["forecast_id"])
        values = (
            forecast_id,
            row["observed_at"],
            row.get("observed_event_id"),
            row.get("observed_price"),
            row.get("realized_signed_return_bps"),
            row.get("realized_target"),
            row.get("transaction_cost_bps"),
            row.get("slippage_bps"),
            row["status"],
            _json(row.get("metadata") or {}),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_forecast_outcomes
                        (forecast_id,observed_at,observed_event_id,observed_price,
                         realized_signed_return_bps,realized_target,
                         transaction_cost_bps,slippage_bps,status,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(forecast_id) DO NOTHING""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO crypto_forecast_outcomes
                    (forecast_id,observed_at,observed_event_id,observed_price,
                     realized_signed_return_bps,realized_target,
                     transaction_cost_bps,slippage_bps,status,metadata)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return forecast_id
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
