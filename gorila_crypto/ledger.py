"""Deterministic replay layer for the immutable Crypto event ledger.

Two notions are intentionally exposed:
- ingest order: reconstructs what the system knew and when it received it;
- event-time order: reconstructs market chronology without pretending it was the
  order in which observations arrived.

Every replay produces a cryptographic fingerprint so the same ledger slice and
replay policy can be reproduced and compared later.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping
from datetime import datetime, timezone

from .storage import CryptoStore, CRYPTO_SCHEMA_VERSION


@dataclass(frozen=True)
class ReplaySpec:
    study_id: str | None = None
    capture_session_id: str | None = None
    symbol: str | None = None
    source: str | None = None
    start_received_time: str | None = None
    end_received_time: str | None = None
    start_event_time: str | None = None
    end_event_time: str | None = None
    order: str = "ingest"
    limit: int = 100000

    def validate(self) -> None:
        if self.order not in {"ingest", "event_time"}:
            raise ValueError("replay order must be 'ingest' or 'event_time'")
        if self.limit < 1:
            raise ValueError("replay limit must be positive")


@dataclass(frozen=True)
class ReplayResult:
    rows: tuple[dict[str, Any], ...]
    fingerprint: str
    first_ledger_seq: int | None
    last_ledger_seq: int | None
    final_state: Any = None


def canonical_replay_row(row: Mapping[str, Any]) -> str:
    return json.dumps(
        {
            "ledger_seq": row["ledger_seq"],
            "event_id": row["event_id"],
            "event_key": row["event_key"],
            "symbol": row["symbol"],
            "event_type": row["event_type"],
            "event_time": row["event_time"],
            "received_time": row["received_time"],
            "provider_time": row["provider_time"],
            "source": row["source"],
            "sequence_start": row["sequence_start"],
            "sequence_end": row["sequence_end"],
            "payload_hash": row["payload_hash"],
            "payload": row["payload"],
            "quality": row["quality"],
            "metadata": row["metadata"],
            "recorded_at": row["recorded_at"],
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def replay_fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        encoded = canonical_replay_row(row).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def replay(
    store: CryptoStore,
    spec: ReplaySpec,
    reducer: Callable[[Any, Mapping[str, Any]], Any] | None = None,
    initial_state: Any = None,
) -> ReplayResult:
    """Replay a deterministic ledger slice.

    Filtering by received_time provides an explicit point-in-time information set.
    Filtering by event_time is available for market chronology, but it does not
    imply the information was available to the strategy at that time.
    """
    spec.validate()
    if spec.study_id is not None and hasattr(store, "read_scoped_events"):
        rows = store.read_scoped_events(
            study_id=spec.study_id,
            capture_session_id=spec.capture_session_id,
            symbol=spec.symbol,
            source=spec.source,
            start_received_time=spec.start_received_time,
            end_received_time=spec.end_received_time,
            start_event_time=spec.start_event_time,
            end_event_time=spec.end_event_time,
            order=spec.order,
            limit=spec.limit,
        )
    else:
        rows = store.read_events(
            symbol=spec.symbol,
            source=spec.source,
            start_received_time=spec.start_received_time,
            end_received_time=spec.end_received_time,
            start_event_time=spec.start_event_time,
            end_event_time=spec.end_event_time,
            order=spec.order,
            limit=spec.limit,
        )
    state = initial_state
    if reducer is not None:
        for row in rows:
            state = reducer(state, row)

    fingerprint = replay_fingerprint(rows)
    result = ReplayResult(
        rows=tuple(rows),
        fingerprint=fingerprint,
        first_ledger_seq=int(rows[0]["ledger_seq"]) if rows else None,
        last_ledger_seq=int(rows[-1]["ledger_seq"]) if rows else None,
        final_state=None,
    )
    if reducer is not None:
        return ReplayResult(
            rows=result.rows,
            fingerprint=result.fingerprint,
            first_ledger_seq=result.first_ledger_seq,
            last_ledger_seq=result.last_ledger_seq,
            final_state=state,
        )
    return result


def replay_manifest(spec: ReplaySpec, result: ReplayResult) -> dict[str, Any]:
    """Create a persisted research manifest for an exact replay slice."""
    return {
        "replay_version": "2",
        "ledger_schema_version": CRYPTO_SCHEMA_VERSION,
        "code_version": os.getenv("RENDER_GIT_COMMIT") or os.getenv("GORILA_CRYPTO_CODE_VERSION") or "unknown",
        "manifest_created_at": datetime.now(timezone.utc).isoformat(),
        "order": spec.order,
        "study_id": spec.study_id,
        "capture_session_id": spec.capture_session_id,
        "symbol": spec.symbol,
        "source": spec.source,
        "start_received_time": spec.start_received_time,
        "end_received_time": spec.end_received_time,
        "start_event_time": spec.start_event_time,
        "end_event_time": spec.end_event_time,
        "limit": spec.limit,
        "row_count": len(result.rows),
        "first_ledger_seq": result.first_ledger_seq,
        "last_ledger_seq": result.last_ledger_seq,
        "fingerprint_sha256": result.fingerprint,
    }