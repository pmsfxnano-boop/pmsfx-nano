"""Explicitly isolated persistence for the Crypto cleanroom.

The Crypto runtime has its own persistence contract and ignores legacy storage
configuration. PostgreSQL uses a dedicated schema; SQLite uses a dedicated file.
Both paths create only crypto_* tables.
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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

CRYPTO_DB_SCHEMA = os.getenv("GORILA_CRYPTO_DB_SCHEMA", "gorila_crypto").strip()
if not CRYPTO_DB_SCHEMA.replace("_", "").isalnum():
    raise ValueError("invalid_crypto_database_schema")

CRYPTO_SQLITE_PATH = os.getenv("GORILA_CRYPTO_SQLITE_PATH", "/tmp/gorila_crypto.sqlite3").strip()
CRYPTO_DATABASE_URL = (
    os.getenv("GORILA_CRYPTO_DATABASE_URL", "").strip()
    or os.getenv("DATABASE_URL", "").strip()
)
CRYPTO_SCHEMA_VERSION = 3

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


CREATE TABLE IF NOT EXISTS crypto_validation_runs (
    run_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    replay_fingerprint TEXT NOT NULL,
    model_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    horizon_ms INTEGER NOT NULL,
    status TEXT NOT NULL,
    placebo_p_value DOUBLE PRECISION,
    placebo_iterations INTEGER NOT NULL,
    promotion_eligible INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    aggregate_metrics_json TEXT NOT NULL,
    stability_json TEXT NOT NULL,
    stress_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_validation_runs_replay
    ON crypto_validation_runs(replay_fingerprint);

CREATE TABLE IF NOT EXISTS crypto_validation_folds (
    run_id TEXT NOT NULL,
    fold_id INTEGER NOT NULL,
    train_start TEXT NOT NULL,
    train_end TEXT NOT NULL,
    test_start TEXT NOT NULL,
    test_end TEXT NOT NULL,
    train_rows INTEGER NOT NULL,
    test_rows INTEGER NOT NULL,
    model_spec_hash TEXT NOT NULL,
    probabilistic_json TEXT NOT NULL,
    baseline_fifty_json TEXT NOT NULL,
    baseline_prevalence_json TEXT NOT NULL,
    economic_json TEXT NOT NULL,
    PRIMARY KEY(run_id, fold_id)
);

CREATE TABLE IF NOT EXISTS crypto_validation_oos (
    run_id TEXT NOT NULL,
    fold_id INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    leader_event_id TEXT NOT NULL,
    target_symbol TEXT NOT NULL,
    horizon_ms INTEGER NOT NULL,
    decision_event_time TEXT NOT NULL,
    decision_received_time TEXT NOT NULL,
    label_event_time TEXT NOT NULL,
    label_received_time TEXT NOT NULL,
    probability DOUBLE PRECISION NOT NULL,
    realized_target INTEGER NOT NULL,
    realized_signed_return_bps DOUBLE PRECISION NOT NULL,
    net_return_bps DOUBLE PRECISION NOT NULL,
    PRIMARY KEY(run_id, fold_id, row_index)
);
CREATE INDEX IF NOT EXISTS idx_crypto_validation_oos_group
    ON crypto_validation_oos(target_symbol,horizon_ms,decision_received_time);
CREATE INDEX IF NOT EXISTS idx_crypto_validation_oos_run
    ON crypto_validation_oos(run_id,fold_id,row_index);

CREATE TABLE IF NOT EXISTS crypto_validation_lineage (
    run_id TEXT NOT NULL,
    fold_id INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    feature_set_hash TEXT NOT NULL,
    source_event_ids_json TEXT NOT NULL,
    PRIMARY KEY(run_id, fold_id, row_index)
);
CREATE INDEX IF NOT EXISTS idx_crypto_validation_lineage_feature
    ON crypto_validation_lineage(feature_set_hash);

CREATE TABLE IF NOT EXISTS crypto_quality_reports (
    report_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    replay_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    report_hash TEXT NOT NULL,
    report_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_quality_reports_replay
    ON crypto_quality_reports(replay_fingerprint);
CREATE INDEX IF NOT EXISTS idx_crypto_quality_reports_status
    ON crypto_quality_reports(status);

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


def _rewrite_database_url_for_external_host(
    database_url: str,
    external_host: str | None = None,
) -> str:
    """Rewrite only the network endpoint for controlled cross-region Postgres use.

    Render's `fromDatabase.connectionString` is private-network scoped. A Frankfurt
    consumer must use the database's external endpoint with TLS. Credentials remain
    entirely inside the original URL/env var and are never written to source control.
    """
    override = (external_host or os.getenv("GORILA_CRYPTO_DATABASE_HOST_OVERRIDE", "")).strip()
    if not override:
        return database_url
    parsed = urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.netloc:
        raise ValueError("invalid_postgres_database_url_for_external_host_override")
    userinfo = parsed.netloc.rsplit("@", 1)[0] if "@" in parsed.netloc else ""
    port = parsed.port or 5432
    netloc = f"{userinfo}@{override}:{port}" if userinfo else f"{override}:{port}"
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["sslmode"] = "require"
    return urlunsplit((parsed.scheme, netloc, parsed.path, urlencode(query), parsed.fragment))


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
        require_durable: bool = False,
    ) -> None:
        raw_database_url = (
            database_url if database_url is not None else CRYPTO_DATABASE_URL
        ).strip()
        self.database_url = _rewrite_database_url_for_external_host(raw_database_url)
        self.require_durable = bool(require_durable)
        if self.require_durable and not self.database_url:
            raise RuntimeError(
                "durable_storage_required: GORILA_CRYPTO_DATABASE_URL or DATABASE_URL is required"
            )
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
        self._write_conn = None

    @property
    def backend(self) -> str:
        return "postgres" if self._pg else "sqlite"

    @property
    def durable(self) -> bool:
        return self._pg

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

    def _write_connection(self):
        """Return a reusable writer connection for the hot ingestion path."""
        self.init()
        if self._write_conn is None:
            self._write_conn = self.connect()
        return self._write_conn

    def close(self) -> None:
        conn = self._write_conn
        self._write_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

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
        conn = self._write_connection()
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
        except Exception:
            try:
                conn.rollback()
            finally:
                self.close()
            raise


    def append_events(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Append a batch of immutable events in one transaction.

        Event identity and payload integrity use the exact same rules as append_event.
        The batch path exists to prevent Postgres transaction latency from throttling
        the live market-data transport. Results preserve input order.
        """
        self.init()
        if not events:
            return []

        prepared: list[dict[str, Any]] = []
        for raw in events:
            item = dict(raw)
            symbol = str(item["symbol"]).upper()
            event_type = str(item["event_type"])
            payload = dict(item["payload"])
            payload_json = _json(payload)
            payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
            sequence_start = item.get("sequence_start")
            sequence_end = item.get("sequence_end")
            source = str(item["source"])
            event_time = str(item["event_time"])
            provider_time = item.get("provider_time")
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
            event_key = str(item.get("event_key") or hashlib.sha256(
                _json(identity).encode("utf-8")
            ).hexdigest())
            event_id = str(item.get("event_id") or uuid.uuid4())
            prepared.append(
                {
                    "event_id": event_id,
                    "event_key": event_key,
                    "symbol": symbol,
                    "event_type": event_type,
                    "event_time": event_time,
                    "received_time": str(item["received_time"]),
                    "provider_time": provider_time,
                    "source": source,
                    "sequence_start": sequence_start,
                    "sequence_end": sequence_end,
                    "payload_hash": payload_hash,
                    "payload_json": payload_json,
                    "quality": str(item.get("quality") or "OK"),
                    "metadata": _json(item.get("metadata") or {}),
                    "recorded_at": str(item.get("recorded_at") or _utc_now()),
                }
            )

        keys = [row["event_key"] for row in prepared]
        conn = self._write_connection()
        try:
            existing: dict[str, tuple[int, str, str]] = {}
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT event_key,ledger_seq,event_id,payload_hash "
                        "FROM crypto_events WHERE event_key = ANY(%s)",
                        (keys,),
                    )
                    for key, ledger_seq, event_id, payload_hash in cur.fetchall():
                        existing[str(key)] = (int(ledger_seq), str(event_id), str(payload_hash))

                    sql = """
                        INSERT INTO crypto_events(
                            event_id,event_key,symbol,event_type,event_time,received_time,
                            provider_time,source,sequence_start,sequence_end,payload_hash,
                            payload_json,quality,metadata,recorded_at
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(event_key) DO NOTHING
                    """
                    values = [
                        (
                            row["event_id"], row["event_key"], row["symbol"], row["event_type"],
                            row["event_time"], row["received_time"], row["provider_time"],
                            row["source"], row["sequence_start"], row["sequence_end"],
                            row["payload_hash"], row["payload_json"], row["quality"],
                            row["metadata"], row["recorded_at"],
                        )
                        for row in prepared
                    ]
                    cur.executemany(sql, values)
                    cur.execute(
                        "SELECT event_key,ledger_seq,event_id,payload_hash "
                        "FROM crypto_events WHERE event_key = ANY(%s)",
                        (keys,),
                    )
                    rows = cur.fetchall()
                    found = {
                        str(key): (int(ledger_seq), str(event_id), str(payload_hash))
                        for key, ledger_seq, event_id, payload_hash in rows
                    }
            else:
                placeholders = ",".join("?" for _ in keys)
                rows = conn.execute(
                    "SELECT event_key,ledger_seq,event_id,payload_hash "
                    f"FROM crypto_events WHERE event_key IN ({placeholders})",
                    keys,
                ).fetchall()
                for row in rows:
                    existing[str(row[0])] = (int(row[1]), str(row[2]), str(row[3]))

                sql = """
                    INSERT OR IGNORE INTO crypto_events(
                        event_id,event_key,symbol,event_type,event_time,received_time,
                        provider_time,source,sequence_start,sequence_end,payload_hash,
                        payload_json,quality,metadata,recorded_at
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
                values = [
                    (
                        row["event_id"], row["event_key"], row["symbol"], row["event_type"],
                        row["event_time"], row["received_time"], row["provider_time"],
                        row["source"], row["sequence_start"], row["sequence_end"],
                        row["payload_hash"], row["payload_json"], row["quality"],
                        row["metadata"], row["recorded_at"],
                    )
                    for row in prepared
                ]
                conn.executemany(sql, values)
                placeholders = ",".join("?" for _ in keys)
                rows = conn.execute(
                    "SELECT event_key,ledger_seq,event_id,payload_hash "
                    f"FROM crypto_events WHERE event_key IN ({placeholders})",
                    keys,
                ).fetchall()
                found = {
                    str(row[0]): (int(row[1]), str(row[2]), str(row[3]))
                    for row in rows
                }

            if len(found) != len(set(keys)):
                raise RuntimeError("event_batch_resolution_failed")

            results: list[dict[str, Any]] = []
            for row in prepared:
                resolved = found[row["event_key"]]
                if resolved[2] != row["payload_hash"]:
                    raise LedgerIntegrityError(
                        "provider_identity_conflict: existing payload hash differs"
                    )
                was_existing = row["event_key"] in existing
                results.append(
                    {
                        "inserted": not was_existing,
                        "ledger_seq": resolved[0],
                        "event_id": resolved[1],
                        "event_key": row["event_key"],
                    }
                )

            conn.commit()
            return results
        except Exception:
            try:
                conn.rollback()
            finally:
                self.close()
            raise

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
        source_prefix: str | None = None,
        order: str = "ingest",
        limit: int = 100000,
        include_payload: bool = True,
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
        if source_prefix is not None:
            clauses.append(f"source LIKE {placeholder}")
            params.append(source_prefix.rstrip("%") + "%")
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
            payload_column = "payload_json" if include_payload else "NULL AS payload_json"
            query = (
                "SELECT ledger_seq,event_id,event_key,symbol,event_type,event_time,"
                "received_time,provider_time,source,sequence_start,sequence_end,"
                f"payload_hash,{payload_column},quality,metadata,recorded_at "
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
                            "payload": json.loads(row[12]) if row[12] is not None else None,
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
                    "payload": json.loads(row["payload_json"]) if row["payload_json"] is not None else None,
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
        conn = self._write_connection()
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
        except Exception:
            try:
                conn.rollback()
            finally:
                self.close()
            raise

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

    def save_validation_run(self, row: dict[str, Any]) -> str:
        self.init()
        run_id = str(row["run_id"])
        values = (
            run_id,
            _utc_now(),
            row["replay_fingerprint"],
            row["model_id"],
            row["model_version"],
            row["target_kind"],
            int(row["horizon_ms"]),
            row["status"],
            row.get("placebo_p_value"),
            int(row.get("placebo_iterations", 0)),
            1 if row.get("promotion_eligible") else 0,
            _json(row.get("config") or {}),
            _json(row.get("aggregate_metrics") or {}),
            _json(row.get("stability") or {}),
            _json(row.get("stress") or {}),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_validation_runs
                        (run_id,created_at,replay_fingerprint,model_id,model_version,
                         target_kind,horizon_ms,status,placebo_p_value,
                         placebo_iterations,promotion_eligible,config_json,
                         aggregate_metrics_json,stability_json,stress_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(run_id) DO UPDATE SET
                          status=EXCLUDED.status,
                          placebo_p_value=EXCLUDED.placebo_p_value,
                          placebo_iterations=EXCLUDED.placebo_iterations,
                          promotion_eligible=EXCLUDED.promotion_eligible,
                          config_json=EXCLUDED.config_json,
                          aggregate_metrics_json=EXCLUDED.aggregate_metrics_json,
                          stability_json=EXCLUDED.stability_json,
                          stress_json=EXCLUDED.stress_json""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT INTO crypto_validation_runs
                    (run_id,created_at,replay_fingerprint,model_id,model_version,
                     target_kind,horizon_ms,status,placebo_p_value,
                     placebo_iterations,promotion_eligible,config_json,
                     aggregate_metrics_json,stability_json,stress_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(run_id) DO UPDATE SET
                      status=excluded.status,
                      placebo_p_value=excluded.placebo_p_value,
                      placebo_iterations=excluded.placebo_iterations,
                      promotion_eligible=excluded.promotion_eligible,
                      config_json=excluded.config_json,
                      aggregate_metrics_json=excluded.aggregate_metrics_json,
                      stability_json=excluded.stability_json,
                      stress_json=excluded.stress_json""",
                    values,
                )
            conn.commit()
            return run_id
        finally:
            conn.close()

    def save_validation_folds(self, rows: list[dict[str, Any]]) -> int:
        self.init()
        if not rows:
            return 0
        conn = self.connect()
        inserted = 0
        try:
            for row in rows:
                values = (
                    row["run_id"],
                    int(row["fold_id"]),
                    row["train_start"],
                    row["train_end"],
                    row["test_start"],
                    row["test_end"],
                    int(row["train_rows"]),
                    int(row["test_rows"]),
                    row["model_spec_hash"],
                    _json(row["probabilistic"]),
                    _json(row["baseline_fifty"]),
                    _json(row["baseline_prevalence"]),
                    _json(row["economic"]),
                )
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO crypto_validation_folds
                            (run_id,fold_id,train_start,train_end,test_start,test_end,
                             train_rows,test_rows,model_spec_hash,probabilistic_json,
                             baseline_fifty_json,baseline_prevalence_json,economic_json)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(run_id,fold_id) DO UPDATE SET
                              train_start=EXCLUDED.train_start,
                              train_end=EXCLUDED.train_end,
                              test_start=EXCLUDED.test_start,
                              test_end=EXCLUDED.test_end,
                              train_rows=EXCLUDED.train_rows,
                              test_rows=EXCLUDED.test_rows,
                              model_spec_hash=EXCLUDED.model_spec_hash,
                              probabilistic_json=EXCLUDED.probabilistic_json,
                              baseline_fifty_json=EXCLUDED.baseline_fifty_json,
                              baseline_prevalence_json=EXCLUDED.baseline_prevalence_json,
                              economic_json=EXCLUDED.economic_json""",
                            values,
                        )
                        inserted += max(cur.rowcount, 0)
                else:
                    cur = conn.execute(
                        """INSERT INTO crypto_validation_folds
                        (run_id,fold_id,train_start,train_end,test_start,test_end,
                         train_rows,test_rows,model_spec_hash,probabilistic_json,
                         baseline_fifty_json,baseline_prevalence_json,economic_json)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(run_id,fold_id) DO UPDATE SET
                          train_start=excluded.train_start,
                          train_end=excluded.train_end,
                          test_start=excluded.test_start,
                          test_end=excluded.test_end,
                          train_rows=excluded.train_rows,
                          test_rows=excluded.test_rows,
                          model_spec_hash=excluded.model_spec_hash,
                          probabilistic_json=excluded.probabilistic_json,
                          baseline_fifty_json=excluded.baseline_fifty_json,
                          baseline_prevalence_json=excluded.baseline_prevalence_json,
                          economic_json=excluded.economic_json""",
                        values,
                    )
                    inserted += max(cur.rowcount, 0)
            conn.commit()
            return inserted
        finally:
            conn.close()

    def save_validation_oos(self, rows: list[dict[str, Any]]) -> int:
        self.init()
        if not rows:
            return 0
        conn = self.connect()
        inserted = 0
        try:
            for row in rows:
                values = (
                    row["run_id"],
                    int(row["fold_id"]),
                    int(row["row_index"]),
                    row["leader_event_id"],
                    row["target_symbol"].upper(),
                    int(row["horizon_ms"]),
                    row["decision_event_time"],
                    row["decision_received_time"],
                    row["label_event_time"],
                    row["label_received_time"],
                    float(row["probability"]),
                    int(row["realized_target"]),
                    float(row["realized_signed_return_bps"]),
                    float(row["net_return_bps"]),
                )
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO crypto_validation_oos
                            (run_id,fold_id,row_index,leader_event_id,target_symbol,
                             horizon_ms,decision_event_time,decision_received_time,
                             label_event_time,label_received_time,probability,
                             realized_target,realized_signed_return_bps,net_return_bps)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT(run_id,fold_id,row_index) DO UPDATE SET
                              leader_event_id=EXCLUDED.leader_event_id,
                              target_symbol=EXCLUDED.target_symbol,
                              horizon_ms=EXCLUDED.horizon_ms,
                              decision_event_time=EXCLUDED.decision_event_time,
                              decision_received_time=EXCLUDED.decision_received_time,
                              label_event_time=EXCLUDED.label_event_time,
                              label_received_time=EXCLUDED.label_received_time,
                              probability=EXCLUDED.probability,
                              realized_target=EXCLUDED.realized_target,
                              realized_signed_return_bps=EXCLUDED.realized_signed_return_bps,
                              net_return_bps=EXCLUDED.net_return_bps""",
                            values,
                        )
                        inserted += max(cur.rowcount, 0)
                else:
                    cur = conn.execute(
                        """INSERT INTO crypto_validation_oos
                        (run_id,fold_id,row_index,leader_event_id,target_symbol,
                         horizon_ms,decision_event_time,decision_received_time,
                         label_event_time,label_received_time,probability,
                         realized_target,realized_signed_return_bps,net_return_bps)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(run_id,fold_id,row_index) DO UPDATE SET
                          leader_event_id=excluded.leader_event_id,
                          target_symbol=excluded.target_symbol,
                          horizon_ms=excluded.horizon_ms,
                          decision_event_time=excluded.decision_event_time,
                          decision_received_time=excluded.decision_received_time,
                          label_event_time=excluded.label_event_time,
                          label_received_time=excluded.label_received_time,
                          probability=excluded.probability,
                          realized_target=excluded.realized_target,
                          realized_signed_return_bps=excluded.realized_signed_return_bps,
                          net_return_bps=excluded.net_return_bps""",
                        values,
                    )
                    inserted += max(cur.rowcount, 0)
            conn.commit()
            return inserted
        finally:
            conn.close()

    def record_gap(
        self,
        *,
        symbol: str,
        source: str,
        expected_sequence: int | None,
        observed_sequence: int | None,
        status: str,
        metadata: dict[str, Any] | None = None,
        gap_id: str | None = None,
    ) -> str:
        self.init()
        identity = {
            "symbol": symbol.upper(),
            "source": source,
            "expected_sequence": expected_sequence,
            "observed_sequence": observed_sequence,
            "status": status,
        }
        gap_id = gap_id or hashlib.sha256(
            _json(identity).encode("utf-8")
        ).hexdigest()[:32]
        values = (
            gap_id,
            _utc_now(),
            symbol.upper(),
            source,
            expected_sequence,
            observed_sequence,
            status,
            _json(metadata or {}),
        )
        conn = self._write_connection()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_data_gaps
                        (gap_id,detected_at,symbol,source,expected_sequence,
                         observed_sequence,status,metadata)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(gap_id) DO NOTHING""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO crypto_data_gaps
                    (gap_id,detected_at,symbol,source,expected_sequence,
                     observed_sequence,status,metadata)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return gap_id
        except Exception:
            try:
                conn.rollback()
            finally:
                self.close()
            raise

    def upsert_source_health(
        self,
        *,
        source: str,
        status: str,
        last_event_time: str | None,
        last_received_time: str | None,
        event_age_seconds: float | None,
        transport_age_seconds: float | None,
        rows_last_batch: int = 0,
        error: str | None = None,
    ) -> None:
        self.init()
        values = (
            source,
            _utc_now(),
            status,
            last_event_time,
            last_received_time,
            event_age_seconds,
            transport_age_seconds,
            int(rows_last_batch),
            error,
        )
        conn = self._write_connection()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_source_health
                        (source,updated_at,status,last_event_time,last_received_time,
                         event_age_seconds,transport_age_seconds,rows_last_batch,error)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(source) DO UPDATE SET
                          updated_at=EXCLUDED.updated_at,
                          status=EXCLUDED.status,
                          last_event_time=EXCLUDED.last_event_time,
                          last_received_time=EXCLUDED.last_received_time,
                          event_age_seconds=EXCLUDED.event_age_seconds,
                          transport_age_seconds=EXCLUDED.transport_age_seconds,
                          rows_last_batch=EXCLUDED.rows_last_batch,
                          error=EXCLUDED.error""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT INTO crypto_source_health
                    (source,updated_at,status,last_event_time,last_received_time,
                     event_age_seconds,transport_age_seconds,rows_last_batch,error)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(source) DO UPDATE SET
                      updated_at=excluded.updated_at,
                      status=excluded.status,
                      last_event_time=excluded.last_event_time,
                      last_received_time=excluded.last_received_time,
                      event_age_seconds=excluded.event_age_seconds,
                      transport_age_seconds=excluded.transport_age_seconds,
                      rows_last_batch=excluded.rows_last_batch,
                      error=excluded.error""",
                    values,
                )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            finally:
                self.close()
            raise

    def start_runtime_run(self, *, kind: str, run_id: str | None = None) -> str:
        self.init()
        run_id = run_id or str(uuid.uuid4())
        values = (run_id, _utc_now(), kind, "RUNNING", None, _json({}))
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_runtime_runs
                        (run_id,created_at,kind,status,completed_at,result)
                        VALUES (%s,%s,%s,%s,%s,%s)""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT INTO crypto_runtime_runs
                    (run_id,created_at,kind,status,completed_at,result)
                    VALUES (?,?,?,?,?,?)""",
                    values,
                )
            conn.commit()
            return run_id
        finally:
            conn.close()

    def finish_runtime_run(
        self,
        *,
        run_id: str,
        status: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        self.init()
        values = (_utc_now(), status, _json(result or {}), run_id)
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE crypto_runtime_runs
                        SET completed_at=%s,status=%s,result=%s
                        WHERE run_id=%s""",
                        values,
                    )
            else:
                conn.execute(
                    """UPDATE crypto_runtime_runs
                    SET completed_at=?,status=?,result=?
                    WHERE run_id=?""",
                    values,
                )
            conn.commit()
        finally:
            conn.close()

    def save_validation_lineage(self, rows: list[dict[str, Any]]) -> int:
        self.init()
        if not rows:
            return 0
        conn = self.connect()
        inserted = 0
        try:
            for row in rows:
                values = (
                    row["run_id"],
                    int(row["fold_id"]),
                    int(row["row_index"]),
                    row["feature_set_hash"],
                    _json(row.get("source_event_ids") or []),
                )
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO crypto_validation_lineage
                            (run_id,fold_id,row_index,feature_set_hash,source_event_ids_json)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT(run_id,fold_id,row_index) DO UPDATE SET
                              feature_set_hash=EXCLUDED.feature_set_hash,
                              source_event_ids_json=EXCLUDED.source_event_ids_json""",
                            values,
                        )
                        inserted += max(cur.rowcount, 0)
                else:
                    cur = conn.execute(
                        """INSERT INTO crypto_validation_lineage
                        (run_id,fold_id,row_index,feature_set_hash,source_event_ids_json)
                        VALUES (?,?,?,?,?)
                        ON CONFLICT(run_id,fold_id,row_index) DO UPDATE SET
                          feature_set_hash=excluded.feature_set_hash,
                          source_event_ids_json=excluded.source_event_ids_json""",
                        values,
                    )
                    inserted += max(cur.rowcount, 0)
            conn.commit()
            return inserted
        finally:
            conn.close()

    def save_quality_report(self, report: dict[str, Any], report_hash: str) -> str:
        self.init()
        report_id = hashlib.sha256(
            _json({
                "replay_fingerprint": report["replay_fingerprint"],
                "report_hash": report_hash,
            }).encode("utf-8")
        ).hexdigest()[:32]
        values = (
            report_id,
            _utc_now(),
            report["replay_fingerprint"],
            report["status"],
            report_hash,
            _json(report),
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO crypto_quality_reports
                        (report_id,created_at,replay_fingerprint,status,report_hash,report_json)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(report_id) DO UPDATE SET
                          status=EXCLUDED.status,
                          report_hash=EXCLUDED.report_hash,
                          report_json=EXCLUDED.report_json""",
                        values,
                    )
            else:
                conn.execute(
                    """INSERT INTO crypto_quality_reports
                    (report_id,created_at,replay_fingerprint,status,report_hash,report_json)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT(report_id) DO UPDATE SET
                      status=excluded.status,
                      report_hash=excluded.report_hash,
                      report_json=excluded.report_json""",
                    values,
                )
            conn.commit()
            return report_id
        finally:
            conn.close()

    def read_data_gaps(
        self,
        *,
        source_prefix: str | None = None,
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        conn = self.connect()
        try:
            placeholder = "%s" if self._pg else "?"
            params: list[Any] = []
            where = ""
            if source_prefix is not None:
                where = f" WHERE source LIKE {placeholder}"
                params.append(source_prefix.rstrip("%") + "%")
            query = (
                "SELECT gap_id,detected_at,symbol,source,expected_sequence,"
                "observed_sequence,status,metadata FROM crypto_data_gaps"
                + where
                + " ORDER BY detected_at DESC LIMIT "
                + str(int(limit))
            )
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    keys = [
                        "gap_id","detected_at","symbol","source",
                        "expected_sequence","observed_sequence","status","metadata"
                    ]
                    return [
                        {key: (json.loads(value) if key == "metadata" else value)
                         for key, value in zip(keys, row)}
                        for row in rows
                    ]
            rows = conn.execute(query, params).fetchall()
            return [
                {
                    **dict(row),
                    "metadata": json.loads(row["metadata"] or "{}"),
                }
                for row in rows
            ]
        finally:
            conn.close()

    def prospective_stats(
        self,
        *,
        source_prefix: str | None = None,
    ) -> dict[str, Any]:
        self.init()
        conn = self.connect()
        try:
            placeholder = "%s" if self._pg else "?"
            params: list[Any] = []
            event_where = ""
            if source_prefix is not None:
                event_where = f" WHERE source LIKE {placeholder}"
                params.append(source_prefix.rstrip("%") + "%")
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT symbol,event_type,COUNT(*) AS rows,
                           MIN(event_time) AS first_event_time,
                           MAX(event_time) AS last_event_time,
                           MIN(received_time) AS first_received_time,
                           MAX(received_time) AS last_received_time
                        FROM crypto_events"""
                        + event_where
                        + """ GROUP BY symbol,event_type
                           ORDER BY symbol,event_type""",
                        params,
                    )
                    counts = [
                        {
                            "symbol": row[0],
                            "event_type": row[1],
                            "rows": int(row[2]),
                            "first_event_time": row[3],
                            "last_event_time": row[4],
                            "first_received_time": row[5],
                            "last_received_time": row[6],
                        }
                        for row in cur.fetchall()
                    ]
                    if source_prefix is not None:
                        cur.execute(
                            "SELECT COUNT(*) FROM crypto_data_gaps WHERE source LIKE " + placeholder,
                            [source_prefix.rstrip("%") + "%"],
                        )
                    else:
                        cur.execute("SELECT COUNT(*) FROM crypto_data_gaps")
                    gap_count = int(cur.fetchone()[0])
                    cur.execute(
                        "SELECT status,created_at,result FROM crypto_runtime_runs "
                        "ORDER BY created_at DESC LIMIT 1"
                    )
                    runtime_row = cur.fetchone()
            else:
                counts = [
                    dict(row)
                    for row in conn.execute(
                        """SELECT symbol,event_type,COUNT(*) AS rows,
                           MIN(event_time) AS first_event_time,
                           MAX(event_time) AS last_event_time,
                           MIN(received_time) AS first_received_time,
                           MAX(received_time) AS last_received_time
                        FROM crypto_events"""
                        + event_where
                        + """ GROUP BY symbol,event_type
                           ORDER BY symbol,event_type""",
                        params,
                    ).fetchall()
                ]
                gap_query = (
                    "SELECT COUNT(*) FROM crypto_data_gaps WHERE source LIKE ?"
                    if source_prefix is not None
                    else "SELECT COUNT(*) FROM crypto_data_gaps"
                )
                gap_count = int(
                    conn.execute(
                        gap_query,
                        [source_prefix.rstrip("%") + "%"] if source_prefix is not None else [],
                    ).fetchone()[0]
                )
                runtime_row = conn.execute(
                    "SELECT status,created_at,result FROM crypto_runtime_runs "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()

            runtime = None
            if runtime_row is not None:
                runtime = {
                    "status": runtime_row[0],
                    "created_at": runtime_row[1],
                    "result": json.loads(runtime_row[2] or "{}"),
                }
            return {
                "backend": self.backend,
                "event_counts": counts,
                "gap_count": gap_count,
                "runtime": runtime,
            }
        finally:
            conn.close()

    def health(self, *, source_prefix: str | None = None) -> list[dict[str, Any]]:
        self.init()
        conn = self.connect()
        try:
            placeholder = "%s" if self._pg else "?"
            where = ""
            params: list[Any] = []
            if source_prefix is not None:
                where = f" WHERE source LIKE {placeholder}"
                params.append(source_prefix.rstrip("%") + "%")
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT source,status,updated_at,last_event_time,last_received_time,"
                        "event_age_seconds,transport_age_seconds,rows_last_batch,error "
                        "FROM crypto_source_health"
                        + where
                        + " ORDER BY source",
                        params,
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
                "FROM crypto_source_health"
                + where
                + " ORDER BY source",
                params,
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
