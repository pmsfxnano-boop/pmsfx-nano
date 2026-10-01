"""Durable state boundary for the adaptive Opportunity Clock learner."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Mapping

SCHEMA = """
CREATE TABLE IF NOT EXISTS crypto_opportunity_intelligence_state (
    state_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    capture_session_id TEXT NOT NULL,
    last_ledger_seq BIGINT NOT NULL,
    updated_at TEXT NOT NULL,
    state_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crypto_intelligence_state_session
    ON crypto_opportunity_intelligence_state(capture_session_id, last_ledger_seq);

CREATE TABLE IF NOT EXISTS crypto_opportunity_intelligence_events (
    training_event_id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    capture_session_id TEXT NOT NULL,
    pair TEXT NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    event_observed INTEGER NOT NULL,
    leader_return_bps DOUBLE PRECISION NOT NULL,
    signed_reaction_bps DOUBLE PRECISION NOT NULL,
    reaction_event_id TEXT,
    replay_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    features_json TEXT NOT NULL,
    UNIQUE(opportunity_id, model_version, capture_session_id)
);
CREATE INDEX IF NOT EXISTS idx_crypto_intelligence_events_pair
    ON crypto_opportunity_intelligence_events(capture_session_id, pair, created_at);
"""

class IntelligenceStore:
    _lock = threading.Lock()

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = (database_url or os.getenv("GORILA_CRYPTO_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
        if not self.database_url:
            raise RuntimeError("intelligence_database_required")

    def connect(self):
        import psycopg
        conn = psycopg.connect(self.database_url)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO gorila_crypto")
        return conn

    def init(self) -> None:
        with self._lock:
            conn = self.connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(SCHEMA)
                conn.commit()
            finally:
                conn.close()

    def load_state(self, state_id: str) -> tuple[int, dict[str, Any]] | None:
        self.init()
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_ledger_seq,state_json FROM crypto_opportunity_intelligence_state WHERE state_id=%s",
                    (state_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return int(row[0]), json.loads(row[1])
        finally:
            conn.close()

    def save_state(
        self,
        *,
        state_id: str,
        model_version: str,
        capture_session_id: str,
        last_ledger_seq: int,
        state: Mapping[str, Any],
    ) -> None:
        self.init()
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO crypto_opportunity_intelligence_state
                    (state_id,model_version,capture_session_id,last_ledger_seq,updated_at,state_json)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(state_id) DO UPDATE SET
                      model_version=EXCLUDED.model_version,
                      capture_session_id=EXCLUDED.capture_session_id,
                      last_ledger_seq=EXCLUDED.last_ledger_seq,
                      updated_at=EXCLUDED.updated_at,
                      state_json=EXCLUDED.state_json
                    """,
                    (state_id, model_version, capture_session_id, int(last_ledger_seq), now, payload),
                )
            conn.commit()
        finally:
            conn.close()


    def commit_state_and_training_events(
        self,
        *,
        state_id: str,
        model_version: str,
        capture_session_id: str,
        last_ledger_seq: int,
        state: Mapping[str, Any],
        training_rows: list[Mapping[str, Any]],
    ) -> bool:
        """Atomically persist learner state and its training examples.

        The ledger sequence and model state advance in the same transaction as
        the examples that caused the update. A crash cannot leave the audit
        ledger ahead of the model state.
        """
        self.init()
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                for row in training_rows:
                    training_event_id = hashlib.sha256(
                        f"{row['opportunity_id']}|{row['model_version']}|{capture_session_id}".encode("utf-8")
                    ).hexdigest()
                    cur.execute(
                        """
                        INSERT INTO crypto_opportunity_intelligence_events
                        (training_event_id,opportunity_id,model_version,capture_session_id,pair,duration_ms,
                         event_observed,leader_return_bps,signed_reaction_bps,reaction_event_id,
                         replay_fingerprint,created_at,features_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(training_event_id) DO NOTHING
                        """,
                        (
                            training_event_id,
                            row["opportunity_id"],
                            row["model_version"],
                            capture_session_id,
                            row["pair"],
                            float(row["duration_ms"]),
                            int(bool(row["event_observed"])),
                            float(row["leader_return_bps"]),
                            float(row["signed_reaction_bps"]),
                            row.get("reaction_event_id"),
                            row["replay_fingerprint"],
                            now,
                            json.dumps(row["features"], sort_keys=True, separators=(",", ":")),
                        ),
                    )
                cur.execute(
                    """
                    INSERT INTO crypto_opportunity_intelligence_state
                    (state_id,model_version,capture_session_id,last_ledger_seq,updated_at,state_json)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(state_id) DO UPDATE SET
                      model_version=EXCLUDED.model_version,
                      capture_session_id=EXCLUDED.capture_session_id,
                      last_ledger_seq=EXCLUDED.last_ledger_seq,
                      updated_at=EXCLUDED.updated_at,
                      state_json=EXCLUDED.state_json
                    WHERE crypto_opportunity_intelligence_state.last_ledger_seq
                          <= EXCLUDED.last_ledger_seq
                    """,
                    (state_id, model_version, capture_session_id, int(last_ledger_seq), now, payload),
                )
                committed = cur.rowcount > 0
            conn.commit()
            return committed
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_training_event(self, *, capture_session_id: str, row: Mapping[str, Any]) -> bool:
        self.init()
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                training_event_id = hashlib.sha256(
                    f"{row['opportunity_id']}|{row['model_version']}|{capture_session_id}".encode("utf-8")
                ).hexdigest()
                cur.execute(
                    """
                    INSERT INTO crypto_opportunity_intelligence_events
                    (training_event_id,opportunity_id,model_version,capture_session_id,pair,duration_ms,
                     event_observed,leader_return_bps,signed_reaction_bps,reaction_event_id,
                     replay_fingerprint,created_at,features_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(training_event_id) DO NOTHING
                    """,
                    (
                        training_event_id,
                        row["opportunity_id"],
                        row["model_version"],
                        capture_session_id,
                        row["pair"],
                        float(row["duration_ms"]),
                        int(bool(row["event_observed"])),
                        float(row["leader_return_bps"]),
                        float(row["signed_reaction_bps"]),
                        row.get("reaction_event_id"),
                        row["replay_fingerprint"],
                        datetime.now(timezone.utc).isoformat(),
                        json.dumps(row["features"], sort_keys=True, separators=(",", ":")),
                    ),
                )
                inserted = cur.rowcount > 0
            conn.commit()
            return inserted
        finally:
            conn.close()
