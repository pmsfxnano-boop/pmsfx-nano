"""Continuous PIT learner for the adaptive Opportunity Clock.

This is deliberately separate from capture. Deploying this worker does not
restart the canonical capture cohort and does not change promotion gates.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from .intelligence_store import IntelligenceStore
from .opportunity_intelligence import AdaptiveOpportunityClock, MODEL_VERSION

BATCH_SIZE = 2_000
WARM_REPLAY_ROWS = 20_000
POLL_SECONDS = 0.25
STATE_ID = "binance-live-opportunity-clock-v2"


def _dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return json.loads(value)


def _active_session(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT capture_session_id
            FROM crypto_capture_sessions
            WHERE status='RUNNING'
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return str(row[0]) if row else None


def _fetch_events(conn, session_id: str, after_seq: int, limit: int) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ledger_seq,event_id,symbol,event_type,event_time,received_time,payload_json
            FROM crypto_events
            WHERE ledger_seq > %s
              AND metadata::jsonb->>'capture_session_id' = %s
            ORDER BY ledger_seq
            LIMIT %s
            """,
            (int(after_seq), session_id, int(limit)),
        )
        return list(cur.fetchall())


def _fetch_warm_events(conn, session_id: str, before_seq: int, limit: int) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ledger_seq,event_id,symbol,event_type,event_time,received_time,payload_json
            FROM (
                SELECT ledger_seq,event_id,symbol,event_type,event_time,received_time,payload_json
                FROM crypto_events
                WHERE ledger_seq < %s
                  AND metadata::jsonb->>'capture_session_id' = %s
                ORDER BY ledger_seq DESC
                LIMIT %s
            ) q
            ORDER BY ledger_seq
            """,
            (int(before_seq), session_id, int(limit)),
        )
        return list(cur.fetchall())


def process_event_stream(
    engine: AdaptiveOpportunityClock,
    store: IntelligenceStore,
    events: list[tuple[Any, ...]],
    session_id: str,
    *,
    learn: bool,
    fingerprint: str,
) -> int:
    last_seq = 0
    for ledger_seq, event_id, symbol, event_type, event_time, received_time, payload_json in events:
        received = _dt(str(received_time))
        outcomes = engine.feed_event(
            symbol=str(symbol),
            event_type=str(event_type),
            payload=_payload(payload_json),
            event_time=_dt(str(event_time)),
            received_time=received,
            event_id=str(event_id),
            replay_fingerprint=fingerprint,
            learn=learn,
        )
        for outcome in outcomes:
            if learn:
                store.save_training_event(capture_session_id=session_id, row=outcome)
        last_seq = max(last_seq, int(ledger_seq))
    return last_seq


def run_once(store: IntelligenceStore) -> dict[str, Any]:
    conn = store.connect()
    try:
        session_id = _active_session(conn)
        if not session_id:
            return {"status": "NO_RUNNING_CAPTURE_SESSION"}

        loaded = store.load_state(STATE_ID)
        if loaded and loaded[1].get("model_version") == MODEL_VERSION and loaded[1].get("capture_session_id") == session_id:
            last_seq, payload = loaded
            engine = AdaptiveOpportunityClock.from_state(payload)
            warm = _fetch_warm_events(conn, session_id, last_seq + 1, WARM_REPLAY_ROWS)
            process_event_stream(
                engine, store, warm, session_id, learn=False,
                fingerprint=f"warm:{session_id}",
            )
        else:
            last_seq = 0
            engine = AdaptiveOpportunityClock()

        events = _fetch_events(conn, session_id, last_seq, BATCH_SIZE)
        if events:
            latest = process_event_stream(
                engine, store, events, session_id, learn=True,
                fingerprint=f"live:{session_id}",
            )
            last_seq = latest

        state = engine.state()
        state["capture_session_id"] = session_id
        state["last_ledger_seq"] = last_seq
        store.save_state(
            state_id=STATE_ID,
            model_version=MODEL_VERSION,
            capture_session_id=session_id,
            last_ledger_seq=last_seq,
            state=state,
        )
        return {
            "status": "RUNNING",
            "capture_session_id": session_id,
            "last_ledger_seq": last_seq,
            "batch_events": len(events),
            "events_seen": engine.events_seen,
            "opportunities_started": engine.opportunities_started,
            "training_updates": engine.training_updates,
            "resolved": engine.resolved,
            "censored": engine.censored,
            "pending": len(engine.pending),
        }
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    store = IntelligenceStore()
    store.init()

    while True:
        result = run_once(store)
        print("GORILA_OPPORTUNITY_CLOCK", json.dumps(result, sort_keys=True), flush=True)
        if args.once:
            return 0
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
