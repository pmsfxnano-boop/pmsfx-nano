from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .storage import Store

SESSION_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
DEFAULT_MAX_REL_SPREAD = float(os.getenv("GORILA_CANONICAL_MAX_REL_SPREAD", "0.0025"))
DEFAULT_FRESHNESS_HOURS = float(os.getenv("GORILA_CANONICAL_FRESHNESS_HOURS", "12"))

# Raw vendor rows remain immutable. Only explicitly admitted daily source
# families can enter the model-facing canonical layer.
_SOURCE_PRIORITY = {
    "BYMADATA": 10,
    "Rava": 20,
    "Investing": 30,
    "TwelveData": 40,
}
_EXCLUDED_PREFIXES = ("YahooChart", "Yahoo")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_daily (
    symbol TEXT NOT NULL,
    field TEXT NOT NULL,
    session_date TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    max_relative_spread DOUBLE PRECISION,
    reconciled_at TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(symbol, field, session_date)
);
CREATE INDEX IF NOT EXISTS idx_canonical_daily_symbol_date
    ON canonical_daily(symbol, field, session_date);
CREATE INDEX IF NOT EXISTS idx_canonical_daily_status_date
    ON canonical_daily(status, session_date);

CREATE TABLE IF NOT EXISTS canonical_daily_quarantine (
    symbol TEXT NOT NULL,
    field TEXT NOT NULL,
    session_date TEXT NOT NULL,
    reason TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    max_relative_spread DOUBLE PRECISION,
    observed_at TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(symbol, field, session_date)
);
CREATE INDEX IF NOT EXISTS idx_canonical_quarantine_date
    ON canonical_daily_quarantine(session_date);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_time(value: Any) -> datetime:
    text = str(value).strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _source_family(source: str) -> str | None:
    text = str(source or "").strip()
    for family in _SOURCE_PRIORITY:
        if text.startswith(f"{family}/"):
            return family
    if text.startswith("RavaPublic/"):
        return "Rava"
    if text.startswith(_EXCLUDED_PREFIXES):
        return None
    return None


def _candidate_hash(candidates: list[dict[str, Any]]) -> str:
    payload = json.dumps(candidates, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ensure_canonical_schema(store: Store) -> None:
    conn = store.connect()
    try:
        if store.pg:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA)
        else:
            conn.executescript(
                _SCHEMA
                .replace("DOUBLE PRECISION", "REAL")
            )
        conn.commit()
    finally:
        conn.close()
        store.conn = None


def _fetch_raw(store: Store, symbol: str, field: str) -> list[tuple]:
    conn = store.connect()
    try:
        if store.pg:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT symbol,field,value,event_time,received_time,source,quality,metadata
                       FROM observations
                       WHERE symbol=%s AND field=%s AND value IS NOT NULL
                       ORDER BY event_time ASC, received_time ASC, source ASC""",
                    (symbol, field),
                )
                return list(cur.fetchall())
        rows = conn.execute(
            """SELECT symbol,field,value,event_time,received_time,source,quality,metadata
               FROM observations
               WHERE symbol=? AND field=? AND value IS NOT NULL
               ORDER BY event_time ASC, received_time ASC, source ASC""",
            (symbol, field),
        ).fetchall()
        return list(rows)
    finally:
        conn.close()
        store.conn = None


def _collapse_session_candidates(rows: list[tuple]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        symbol, field, value, event_time, received_time, source, quality, metadata = row
        family = _source_family(str(source))
        if family is None or str(quality or "OK").upper() not in {"OK", "VALIDATED"}:
            continue
        try:
            numeric = float(value)
            if not math.isfinite(numeric) or numeric <= 0:
                continue
            event_dt = _parse_time(event_time)
            received_dt = _parse_time(received_time)
        except (TypeError, ValueError, OverflowError):
            continue
        session = event_dt.astimezone(SESSION_TZ).date().isoformat()
        try:
            metadata_obj = json.loads(metadata or "{}")
        except Exception:
            metadata_obj = {}
        grouped[session][family].append(
            {
                "family": family,
                "source": str(source),
                "value": numeric,
                "event_time": event_dt.isoformat(),
                "received_time": received_dt.isoformat(),
                "quality": str(quality or "OK"),
                "metadata": metadata_obj,
            }
        )

    collapsed: dict[str, list[dict[str, Any]]] = {}
    for session, by_family in grouped.items():
        candidates: list[dict[str, Any]] = []
        for family, family_rows in by_family.items():
            family_rows.sort(
                key=lambda item: (
                    _parse_time(item["event_time"]),
                    _parse_time(item["received_time"]),
                    item["source"],
                ),
                reverse=True,
            )
            chosen = dict(family_rows[0])
            chosen["family_rows"] = len(family_rows)
            candidates.append(chosen)
        collapsed[session] = candidates
    return collapsed


def _resolve_session(
    candidates: list[dict[str, Any]],
    *,
    max_rel_spread: float,
    freshness_hours: float,
) -> tuple[str, dict[str, Any] | None]:
    if not candidates:
        return "NO_SUPPORTED_SOURCE", None

    candidates = sorted(
        candidates,
        key=lambda item: (
            _SOURCE_PRIORITY[item["family"]],
            -_parse_time(item["event_time"]).timestamp(),
            -_parse_time(item["received_time"]).timestamp(),
            item["source"],
        ),
    )
    values = [float(item["value"]) for item in candidates]
    median = float(sorted(values)[len(values) // 2]) if len(values) % 2 else float(
        (sorted(values)[len(values) // 2 - 1] + sorted(values)[len(values) // 2]) / 2.0
    )
    spread = 0.0 if median <= 0 else max(abs(v - median) / median for v in values)
    latest_event = max(_parse_time(item["event_time"]) for item in candidates)
    preferred = candidates[0]
    preferred_age = (latest_event - _parse_time(preferred["event_time"])).total_seconds() / 3600.0
    if preferred_age > freshness_hours:
        preferred = max(
            candidates,
            key=lambda item: (
                _parse_time(item["event_time"]),
                _parse_time(item["received_time"]),
                -_SOURCE_PRIORITY[item["family"]],
                item["source"],
            ),
        )

    if len(candidates) == 1:
        status = "ACCEPTED_SINGLE_SOURCE"
    elif spread <= max_rel_spread:
        status = "ACCEPTED_RECONCILED"
    else:
        return "QUARANTINED_SOURCE_DISAGREEMENT", {
            "spread": spread,
            "median": median,
            "candidates": candidates,
            "candidate_hash": _candidate_hash(candidates),
        }

    return status, {
        "value": float(preferred["value"]),
        "source": preferred["source"],
        "spread": spread,
        "median": median,
        "candidates": candidates,
        "candidate_hash": _candidate_hash(candidates),
        "freshness_override": preferred["source"] != candidates[0]["source"],
    }


def reconcile_daily_symbol(
    store: Store,
    symbol: str,
    field: str = "close",
    *,
    max_rel_spread: float = DEFAULT_MAX_REL_SPREAD,
    freshness_hours: float = DEFAULT_FRESHNESS_HOURS,
    limit_sessions: int | None = None,
) -> dict[str, Any]:
    ensure_canonical_schema(store)
    rows = _fetch_raw(store, symbol, field)
    grouped = _collapse_session_candidates(rows)
    sessions = sorted(grouped)
    if limit_sessions is not None and limit_sessions > 0:
        sessions = sessions[-int(limit_sessions):]

    now = _now_iso()
    accepted: list[tuple] = []
    quarantined: list[tuple] = []
    for session in sessions:
        status, resolution = _resolve_session(
            grouped[session],
            max_rel_spread=max_rel_spread,
            freshness_hours=freshness_hours,
        )
        if resolution is None:
            continue
        candidates = resolution["candidates"]
        audit = {
            "max_rel_spread": max_rel_spread,
            "freshness_hours": freshness_hours,
            "median_value": resolution.get("median"),
            "candidates": candidates,
            "candidate_hash": resolution["candidate_hash"],
            "freshness_override": resolution.get("freshness_override", False),
        }
        if status.startswith("ACCEPTED_"):
            accepted.append(
                (
                    symbol,
                    field,
                    session,
                    resolution["value"],
                    resolution["source"],
                    status,
                    len(candidates),
                    float(resolution["spread"]),
                    now,
                    json.dumps(audit, sort_keys=True, default=str),
                )
            )
        else:
            quarantined.append(
                (
                    symbol,
                    field,
                    session,
                    status,
                    len(candidates),
                    float(resolution.get("spread", math.nan)),
                    now,
                    json.dumps(audit, sort_keys=True, default=str),
                )
            )

    conn = store.connect()
    try:
        if store.pg:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM canonical_daily WHERE symbol=%s AND field=%s",
                    (symbol, field),
                )
                cur.execute(
                    "DELETE FROM canonical_daily_quarantine WHERE symbol=%s AND field=%s",
                    (symbol, field),
                )
                if accepted:
                    cur.executemany(
                        """INSERT INTO canonical_daily(
                           symbol,field,session_date,value,source,status,candidate_count,
                           max_relative_spread,reconciled_at,metadata)
                           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        accepted,
                    )
                if quarantined:
                    cur.executemany(
                        """INSERT INTO canonical_daily_quarantine(
                           symbol,field,session_date,reason,candidate_count,
                           max_relative_spread,observed_at,metadata)
                           VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                        quarantined,
                    )
        else:
            conn.execute(
                "DELETE FROM canonical_daily WHERE symbol=? AND field=?",
                (symbol, field),
            )
            conn.execute(
                "DELETE FROM canonical_daily_quarantine WHERE symbol=? AND field=?",
                (symbol, field),
            )
            if accepted:
                conn.executemany(
                    """INSERT INTO canonical_daily(
                       symbol,field,session_date,value,source,status,candidate_count,
                       max_relative_spread,reconciled_at,metadata)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    accepted,
                )
            if quarantined:
                conn.executemany(
                    """INSERT INTO canonical_daily_quarantine(
                       symbol,field,session_date,reason,candidate_count,
                       max_relative_spread,observed_at,metadata)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    quarantined,
                )
        conn.commit()
    finally:
        conn.close()
        store.conn = None

    status_counts = defaultdict(int)
    for row in accepted:
        status_counts[row[5]] += 1
    return {
        "symbol": symbol,
        "field": field,
        "raw_rows": len(rows),
        "sessions_considered": len(sessions),
        "accepted": len(accepted),
        "quarantined": len(quarantined),
        "status_counts": dict(sorted(status_counts.items())),
        "latest_session": sessions[-1] if sessions else None,
        "latest_accepted_session": max((row[2] for row in accepted), default=None),
        "max_rel_spread": max_rel_spread,
    }


def reconcile_all(
    store: Store,
    symbols: tuple[str, ...],
    field: str = "close",
    *,
    limit_sessions: int | None = None,
    max_rel_spread: float = DEFAULT_MAX_REL_SPREAD,
    freshness_hours: float = DEFAULT_FRESHNESS_HOURS,
) -> list[dict[str, Any]]:
    ensure_canonical_schema(store)
    return [
        reconcile_daily_symbol(
            store,
            symbol,
            field,
            limit_sessions=limit_sessions,
            max_rel_spread=max_rel_spread,
            freshness_hours=freshness_hours,
        )
        for symbol in symbols
    ]


def canonical_content_hash(
    store: Store,
    symbol: str,
    field: str = "close",
) -> str:
    """Stable content fingerprint for the accepted model-facing canonical series."""
    ensure_canonical_schema(store)
    conn = store.connect()
    try:
        if store.pg:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT session_date,value,source,status,max_relative_spread
                       FROM canonical_daily
                       WHERE symbol=%s AND field=%s AND status LIKE 'ACCEPTED%%'
                       ORDER BY session_date ASC""",
                    (symbol, field),
                )
                rows = cur.fetchall()
        else:
            rows = conn.execute(
                """SELECT session_date,value,source,status,max_relative_spread
                   FROM canonical_daily
                   WHERE symbol=? AND field=? AND status LIKE 'ACCEPTED%'
                   ORDER BY session_date ASC""",
                (symbol, field),
            ).fetchall()
    finally:
        conn.close()
        store.conn = None
    payload = [
        [str(row[0]), float(row[1]), str(row[2]), str(row[3]), None if row[4] is None else float(row[4])]
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()


def canonical_content_hash_batch(
    store: Store,
    symbols: tuple[str, ...],
    field: str = "close",
) -> dict[str, str]:
    """Hash accepted canonical content for multiple symbols in one database query."""
    symbols = tuple(str(symbol).upper() for symbol in symbols)
    if not symbols:
        return {}
    ensure_canonical_schema(store)
    conn = store.connect()
    if store.pg:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT symbol,session_date,value,source,status,max_relative_spread
                   FROM canonical_daily
                   WHERE symbol = ANY(%s)
                     AND field=%s
                     AND status LIKE 'ACCEPTED%%'
                   ORDER BY symbol,session_date ASC""",
                (list(symbols), field),
            )
            rows = cur.fetchall()
    else:
        placeholders = ",".join(["?"] * len(symbols))
        rows = conn.execute(
            f"""SELECT symbol,session_date,value,source,status,max_relative_spread
                FROM canonical_daily
                WHERE symbol IN ({placeholders})
                  AND field=?
                  AND status LIKE 'ACCEPTED%'
                ORDER BY symbol,session_date ASC""",
            (*symbols, field),
        ).fetchall()
    conn.close(); store.conn=None

    grouped = {symbol: [] for symbol in symbols}
    for row in rows:
        grouped[str(row[0]).upper()].append([
            str(row[1]),
            float(row[2]),
            str(row[3]),
            str(row[4]),
            None if row[5] is None else float(row[5]),
        ])
    return {
        symbol: hashlib.sha256(
            json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
        ).hexdigest()
        for symbol, payload in grouped.items()
    }


def canonical_daily_series(store: Store, symbol: str, field: str = "close", limit: int = 2500) -> list[tuple[str, float]]:
    ensure_canonical_schema(store)
    limit = max(1, int(limit))
    conn = store.connect()
    try:
        if store.pg:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT session_date,value
                       FROM canonical_daily
                       WHERE symbol=%s AND field=%s AND status LIKE 'ACCEPTED%%'
                       ORDER BY session_date DESC
                       LIMIT %s""",
                    (symbol, field, limit),
                )
                rows = cur.fetchall()
        else:
            rows = conn.execute(
                """SELECT session_date,value
                   FROM canonical_daily
                   WHERE symbol=? AND field=? AND status LIKE 'ACCEPTED%'
                   ORDER BY session_date DESC
                   LIMIT ?""",
                (symbol, field, limit),
            ).fetchall()
    finally:
        conn.close()
        store.conn = None
    return [(str(row[0]), float(row[1])) for row in reversed(rows)]
