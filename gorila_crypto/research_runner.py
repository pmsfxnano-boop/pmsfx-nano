"""Autonomous PIT / walk-forward / OOS runner for the Crypto cleanroom.

The runner operates only on a complete preregistered prospective capture session.
It does not synthesize data, stitch sessions, use retrospective provider history,
or promote a model automatically.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .forecast import DetectionFeatureSnapshot, ForecastTargetSpec, MICROSTRUCTURE_FEATURES
from .lead_lag import LeadLagConfig
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .quality import (
    DataQualityConfig,
    DataQualityReport,
    QualitySymbolStats,
    quality_fingerprint,
)
from .research_gates import deflated_sharpe_p_value
from .validation import (
    EconomicPolicySpec,
    ForecastDatasetRow,
    ForecastLabel,
    StressScenario,
    WalkForwardConfig,
    persist_validation_report,
    run_quality_gated_walk_forward,
)
from .ledger import canonical_replay_row


RESEARCH_STATUS_SOURCE = "gorila.crypto.research_runner"
FEATURE_SET_VERSION = "crypto_microstructure_alpha_v2"
DEFAULT_BATCH = 5000


class ResearchRunBlocked(RuntimeError):
    """Expected, non-fatal blocker for research progression."""


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _price(payload: Any) -> float:
    if isinstance(payload, str):
        payload = json.loads(payload)
    value = float(payload["p"])
    if not value > 0:
        raise ValueError("trade price must be positive")
    return value


def _feature_hash(
    *,
    leader_symbol: str,
    target_symbol: str,
    decision_event_time: datetime,
    decision_received_time: datetime,
    source_event_ids: tuple[str, ...],
    feature_values: dict[str, float],
) -> str:
    canonical = {
        "version": FEATURE_SET_VERSION,
        "leader_symbol": leader_symbol.upper(),
        "target_symbol": target_symbol.upper(),
        "decision_event_time": decision_event_time.isoformat(),
        "decision_received_time": decision_received_time.isoformat(),
        "source_event_ids": source_event_ids,
        "features": dict(sorted(feature_values.items())),
    }
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _cohort_window(session_row: dict[str, Any], *, now: datetime) -> tuple[datetime, datetime]:
    started = _dt(session_row["started_at"])
    cohort_end = started + timedelta(days=PREREGISTERED_CRYPTO_PROTOCOL.prospect_days)
    ended = _dt(session_row["ended_at"]) if session_row.get("ended_at") else None
    if cohort_end > now:
        raise ResearchRunBlocked("PROSPECTIVE_SESSION_NOT_MATURE")
    if ended is not None and ended < cohort_end:
        raise ResearchRunBlocked("CAPTURE_SESSION_ENDED_BEFORE_PROTOCOL_WINDOW")
    return started, cohort_end


def _find_mature_session(store, *, now: datetime) -> dict[str, Any] | None:
    conn = store.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id,study_id,provider,venue,started_at,ended_at,status,
                       protocol_hash,symbols_json,streams_json,code_version
                FROM crypto_capture_sessions
                WHERE study_id=%s
                  AND provider=%s
                  AND venue=%s
                ORDER BY started_at ASC
                """,
                (
                    PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                    PREREGISTERED_CRYPTO_PROTOCOL.provider,
                    PREREGISTERED_CRYPTO_PROTOCOL.venue,
                ),
            )
            columns = [desc.name for desc in cur.description]
            for row in cur.fetchall():
                item = dict(zip(columns, row))
                try:
                    _cohort_window(item, now=now)
                except ResearchRunBlocked:
                    continue
                if str(item.get("protocol_hash")) != PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash:
                    continue
                try:
                    symbols = tuple(json.loads(item["symbols_json"]))
                    streams = tuple(json.loads(item["streams_json"]))
                except Exception:
                    continue
                if symbols != PREREGISTERED_CRYPTO_PROTOCOL.symbols:
                    continue
                if streams != PREREGISTERED_CRYPTO_PROTOCOL.streams:
                    continue
                return item
        return None
    finally:
        conn.close()


def _scope_where() -> str:
    return (
        "e.metadata->>'crypto_study_id' = %s "
        "AND e.metadata->>'capture_session_id' = %s "
        "AND e.source LIKE 'binance.websocket.%' "
        "AND e.event_time::timestamptz >= %s "
        "AND e.event_time::timestamptz < %s "
        "AND e.received_time::timestamptz >= %s "
        "AND e.received_time::timestamptz < %s"
    )


def _stream_replay_rows(store, *, session_id: str, start: datetime, end: datetime):
    conn = store.connect()
    try:
        query = f"""
            SELECT e.ledger_seq,e.event_id,e.event_key,e.symbol,e.event_type,
                   e.event_time,e.received_time,e.provider_time,e.source,
                   e.sequence_start,e.sequence_end,e.payload_hash,e.payload_json,
                   e.quality,e.metadata,e.recorded_at
            FROM crypto_events e
            WHERE {_scope_where()}
            ORDER BY e.ledger_seq ASC
        """
        with conn.cursor(name="crypto_research_replay") as cur:
            cur.execute(
                query,
                (
                    PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                    session_id,
                    start.isoformat(),
                    end.isoformat(),
                    start.isoformat(),
                    end.isoformat(),
                ),
            )
            while True:
                batch = cur.fetchmany(DEFAULT_BATCH)
                if not batch:
                    break
                columns = [desc.name for desc in cur.description]
                for raw in batch:
                    row = dict(zip(columns, raw))
                    payload = row["payload_json"]
                    metadata = row["metadata"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if isinstance(metadata, str):
                        metadata = json.loads(metadata)
                    yield {
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
                        "payload": payload,
                        "quality": str(row["quality"]),
                        "metadata": metadata,
                        "recorded_at": str(row["recorded_at"]),
                    }
    finally:
        conn.close()


def _replay_fingerprint_and_manifest(
    store,
    *,
    session_id: str,
    start: datetime,
    end: datetime,
) -> tuple[str, int, int | None, int | None]:
    digest = hashlib.sha256()
    count = 0
    first_seq = None
    last_seq = None
    for row in _stream_replay_rows(store, session_id=session_id, start=start, end=end):
        encoded = canonical_replay_row(row).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        count += 1
        first_seq = first_seq if first_seq is not None else int(row["ledger_seq"])
        last_seq = int(row["ledger_seq"])
    return digest.hexdigest(), count, first_seq, last_seq


def _sql_quality_report(
    store,
    *,
    session_id: str,
    start: datetime,
    end: datetime,
    replay_fingerprint: str,
) -> DataQualityReport:
    protocol = PREREGISTERED_CRYPTO_PROTOCOL
    quality_map = protocol.quality_config()
    config = DataQualityConfig(
        min_rows_per_symbol=int(quality_map["min_rows_per_symbol"]),
        min_duration_seconds=float(quality_map["min_duration_seconds"]),
        max_p99_transport_latency_ms=float(quality_map["max_p99_transport_latency_ms"]),
        required_event_types=tuple(quality_map["required_event_types"]),
        required_event_type_min_rows=dict(quality_map["required_event_type_min_rows"]),
    )
    conn = store.connect()
    try:
        common = (
            protocol.study_id,
            session_id,
            start.isoformat(),
            end.isoformat(),
            start.isoformat(),
            end.isoformat(),
        )
        summary_sql = f"""
            WITH scoped AS (
                SELECT
                    e.event_id,
                    e.symbol,
                    e.event_time::timestamptz AS event_time,
                    e.received_time::timestamptz AS received_time,
                    EXTRACT(EPOCH FROM (
                        e.received_time::timestamptz - e.event_time::timestamptz
                    )) * 1000.0 AS latency_ms
                FROM crypto_events e
                WHERE {_scope_where()}
            ),
            sampled_quantiles AS (
                SELECT
                    s.symbol,
                    percentile_cont(0.50) WITHIN GROUP (ORDER BY GREATEST(0.0, s.latency_ms)) AS p50_latency_ms,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY GREATEST(0.0, s.latency_ms)) AS p95_latency_ms,
                    percentile_cont(0.99) WITHIN GROUP (ORDER BY GREATEST(0.0, s.latency_ms)) AS p99_latency_ms
                FROM scoped s
                WHERE MOD(ABS(hashtext(s.event_id)), 20) = 0
                GROUP BY s.symbol
            )
            SELECT s.symbol,
                   COUNT(*) AS rows,
                   MIN(s.event_time) AS first_event,
                   MAX(s.event_time) AS last_event,
                   q.p50_latency_ms,
                   q.p95_latency_ms,
                   q.p99_latency_ms,
                   COUNT(*) FILTER (
                       WHERE s.event_time > %s::timestamptz
                   ) AS future_events,
                   COUNT(*) FILTER (
                       WHERE s.received_time > %s::timestamptz
                   ) AS future_received,
                   COUNT(*) FILTER (
                       WHERE s.received_time < s.event_time
                   ) AS negative_latency,
                   COUNT(*) FILTER (
                       WHERE s.latency_ms > 5000.0
                   ) AS latency_over_5s
            FROM scoped s
            LEFT JOIN sampled_quantiles q ON q.symbol=s.symbol
            GROUP BY s.symbol,q.p50_latency_ms,q.p95_latency_ms,q.p99_latency_ms
            ORDER BY s.symbol
        """
        with conn.cursor() as cur:
            cur.execute(
                summary_sql,
                (
                    *common,
                    end.isoformat(),
                    end.isoformat(),
                ),
            )
            summary = cur.fetchall()
            summary_cols = [d.name for d in cur.description]

            cur.execute(
                f"""
                SELECT e.event_type, COUNT(*) AS n
                FROM crypto_events e
                WHERE {_scope_where()}
                GROUP BY e.event_type
                ORDER BY e.event_type
                """,
                common,
            )
            event_type_counts = {
                str(r[0]): int(r[1]) for r in cur.fetchall()
            }

            cur.execute(
                f"""
                SELECT e.event_type,e.quality,COUNT(*) AS n
                FROM crypto_events e
                WHERE {_scope_where()}
                GROUP BY e.event_type,e.quality
                ORDER BY e.event_type,e.quality
                """,
                common,
            )
            event_quality_counts: dict[str, dict[str, int]] = {}
            for event_type, quality, n in cur.fetchall():
                event_quality_counts.setdefault(str(event_type), {})[str(quality)] = int(n)

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT e.received_time::timestamptz AS received_time,
                           LAG(e.received_time::timestamptz) OVER (
                               PARTITION BY e.symbol,e.source,
                                   COALESCE(e.metadata->>'ingest_epoch','0')
                               ORDER BY e.ledger_seq
                           ) AS previous_received
                    FROM crypto_events e
                    WHERE {_scope_where()}
                      AND e.quality <> 'TRANSPORT_TIME_ONLY'
                ) x
                WHERE x.previous_received IS NOT NULL
                  AND x.received_time < x.previous_received
                """,
                common,
            )
            receive_reversals = int(cur.fetchone()[0] or 0)

            cur.execute(
                """
                SELECT COUNT(*)
                FROM crypto_data_gaps g
                WHERE g.symbol IS NOT NULL
                  AND (g.metadata->>'capture_session_id') = %s
                  AND (g.metadata->>'crypto_study_id') = %s
                  AND g.source LIKE 'binance.websocket.%'
                  AND COALESCE(
                      (g.metadata->>'event_type'),
                      ''
                  ) = ANY(%s)
                """,
                (
                    session_id,
                    protocol.study_id,
                    list(config.required_event_types),
                ),
            )
            required_source_gap_count = int(cur.fetchone()[0] or 0)

    finally:
        conn.close()

    reasons: list[str] = []
    symbol_stats: dict[str, QualitySymbolStats] = {}
    total_rows = 0
    future_events = 0
    future_received = 0
    negative_latency = 0
    for raw in summary:
        item = dict(zip(summary_cols, raw))
        symbol = str(item["symbol"]).upper()
        total_rows += int(item["rows"])
        future_events += int(item["future_events"] or 0)
        future_received += int(item["future_received"] or 0)
        negative_latency += int(item["negative_latency"] or 0)
        first_event = _dt(item["first_event"])
        last_event = _dt(item["last_event"])
        duration = (
            (last_event - first_event).total_seconds()
            if first_event and last_event
            else 0.0
        )
        p99 = float(item["p99_latency_ms"]) if item["p99_latency_ms"] is not None else None
        symbol_stats[symbol] = QualitySymbolStats(
            symbol=symbol,
            rows=int(item["rows"]),
            duration_seconds=duration,
            transport_latency_p50_ms=(
                float(item["p50_latency_ms"]) if item["p50_latency_ms"] is not None else None
            ),
            transport_latency_p95_ms=(
                float(item["p95_latency_ms"]) if item["p95_latency_ms"] is not None else None
            ),
            transport_latency_p99_ms=p99,
        )
        if int(item["rows"]) < config.min_rows_per_symbol:
            reasons.append(f"{symbol}:INSUFFICIENT_ROWS")
        if duration < config.min_duration_seconds:
            reasons.append(f"{symbol}:INSUFFICIENT_DURATION")
        latency_over_5s = int(item.get("latency_over_5s") or 0)
        # Exact tail-count gate: avoid a 1M-row PostgreSQL sort while preserving
        # the protocol meaning of p99 <= 5000ms.
        if int(item["rows"]) > 0 and latency_over_5s > int(item["rows"]) * 0.01:
            reasons.append(f"{symbol}:P99_TRANSPORT_LATENCY")
        elif p99 is not None and p99 > config.max_p99_transport_latency_ms:
            reasons.append(f"{symbol}:P99_TRANSPORT_LATENCY")

    for symbol in protocol.symbols:
        if symbol not in symbol_stats:
            reasons.append(f"{symbol}:MISSING_SYMBOL")

    if future_events:
        reasons.append("FUTURE_EVENTS")
    if future_received:
        reasons.append("FUTURE_RECEIVED_TIMES")
    if negative_latency:
        reasons.append("NEGATIVE_TRANSPORT_LATENCY")
    if receive_reversals:
        reasons.append("RECEIVE_TIME_REVERSAL")
    if required_source_gap_count:
        reasons.append("REQUIRED_SOURCE_GAPS")
    for event_type in config.required_event_types:
        observed = int(event_type_counts.get(event_type, 0))
        minimum = int(config.required_event_type_min_rows.get(event_type, 1))
        if observed < minimum:
            reasons.append(f"EVENT_TYPE_INSUFFICIENT:{event_type}:{observed}<{minimum}")

    status = "PASS" if not reasons else "FAIL"
    return DataQualityReport(
        status=status,
        replay_fingerprint=replay_fingerprint,
        reference_time=end.isoformat(),
        config_hash=hashlib.sha256(
            json.dumps(asdict(config), sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        rows=total_rows,
        symbols=tuple(sorted(symbol_stats)),
        invalid_timestamp_count=0,
        future_event_count=future_events,
        receive_time_reversal_count=receive_reversals,
        future_received_count=future_received,
        negative_transport_latency_count=negative_latency,
        required_source_gap_count=required_source_gap_count,
        event_type_counts=dict(sorted(event_type_counts.items())),
        event_quality_counts={
            key: dict(sorted(value.items()))
            for key, value in sorted(event_quality_counts.items())
        },
        symbol_stats=symbol_stats,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def _dataset_rows(
    store,
    *,
    session_id: str,
    start: datetime,
    end: datetime,
    leader_symbol: str,
    target_symbol: str,
    target_spec: ForecastTargetSpec,
) -> tuple[ForecastDatasetRow, ...]:
    """Build PIT-safe forecast rows with a frozen microstructure alpha feature family.

    All features are sourced only from observations whose receive time is no later
    than the decision receive time.  BookTicker is used only as a top-of-book state;
    no future depth or trade information enters the feature vector.
    """
    tol_ms = int(target_spec.alignment_tolerance_ms)
    horizon_ms = int(target_spec.horizon_ms)
    trade_sample_weight = 1.0 / float(PREREGISTERED_CRYPTO_PROTOCOL.trade_persistence_sample_rate)
    conn = store.connect()
    try:
        common_scope = (
            PREREGISTERED_CRYPTO_PROTOCOL.study_id,
            session_id,
            start.isoformat(),
            end.isoformat(),
            start.isoformat(),
            end.isoformat(),
        )
        sql = f"""
        WITH leader_base AS (
            SELECT e.ledger_seq,
                   e.event_id,
                   e.event_time::timestamptz AS et,
                   e.received_time::timestamptz AS rt,
                   (e.payload_json::jsonb->>'p')::double precision AS price,
                   (e.payload_json::jsonb->>'q')::double precision AS quantity,
                   (e.payload_json::jsonb->>'m')::boolean AS buyer_maker
            FROM crypto_events e
            WHERE e.metadata->>'crypto_study_id' = %s
              AND e.metadata->>'capture_session_id' = %s
              AND e.source = 'binance.websocket.trade'
              AND e.symbol = %s
              AND e.event_type = 'trade'
              AND e.event_time::timestamptz >= %s
              AND e.event_time::timestamptz < %s
              AND e.received_time::timestamptz >= %s
              AND e.received_time::timestamptz < %s
        ),
        target_base AS (
            SELECT e.ledger_seq,
                   e.event_id,
                   e.event_time::timestamptz AS et,
                   e.received_time::timestamptz AS rt,
                   (e.payload_json::jsonb->>'p')::double precision AS price,
                   (e.payload_json::jsonb->>'q')::double precision AS quantity,
                   (e.payload_json::jsonb->>'m')::boolean AS buyer_maker
            FROM crypto_events e
            WHERE e.metadata->>'crypto_study_id' = %s
              AND e.metadata->>'capture_session_id' = %s
              AND e.source = 'binance.websocket.trade'
              AND e.symbol = %s
              AND e.event_type = 'trade'
              AND e.event_time::timestamptz >= %s
              AND e.event_time::timestamptz < %s
              AND e.received_time::timestamptz >= %s
              AND e.received_time::timestamptz < %s
        ),
        book_base AS (
            SELECT e.ledger_seq,
                   e.event_id,
                   e.symbol,
                   e.received_time::timestamptz AS rt,
                   (e.payload_json::jsonb->>'b')::double precision AS bid,
                   (e.payload_json::jsonb->>'B')::double precision AS bid_qty,
                   (e.payload_json::jsonb->>'a')::double precision AS ask,
                   (e.payload_json::jsonb->>'A')::double precision AS ask_qty
            FROM crypto_events e
            WHERE e.metadata->>'crypto_study_id' = %s
              AND e.metadata->>'capture_session_id' = %s
              AND e.source = 'binance.websocket.bookTicker'
              AND e.event_type = 'bookTicker'
              AND e.symbol IN (%s,%s)
              AND e.received_time::timestamptz >= %s
              AND e.received_time::timestamptz < %s
        ),
        leader_candidates AS (
            SELECT l.*,
                   prev.event_id AS prev_leader_event_id,
                   prev.price AS prev_leader_price,
                   (10000.0 * ln(l.price / prev.price)) AS leader_return_bps
            FROM leader_base l
            CROSS JOIN LATERAL (
                SELECT p.event_id,p.price
                FROM leader_base p
                WHERE p.et <= l.et - interval '1 second'
                  AND p.rt <= l.rt
                ORDER BY p.et DESC,p.rt DESC,p.ledger_seq DESC
                LIMIT 1
            ) prev
            WHERE abs(10000.0 * ln(l.price / prev.price)) >= 5.0
        ),
        candidate_gap AS (
            SELECT c.*,
                   LAG(c.et) OVER (
                       ORDER BY c.et,c.rt,c.ledger_seq
                   ) AS previous_candidate_time
            FROM leader_candidates c
        ),
        impulses AS (
            SELECT *
            FROM candidate_gap
            WHERE previous_candidate_time IS NULL
               OR et - previous_candidate_time >= interval '1 second'
        )
        SELECT i.ledger_seq AS leader_ledger_seq,
               i.event_id AS leader_event_id,
               i.et AS leader_event_time,
               i.rt AS leader_received_time,
               i.price AS leader_price,
               i.leader_return_bps,
               baseline.event_id AS baseline_event_id,
               baseline.et AS baseline_event_time,
               baseline.rt AS baseline_received_time,
               baseline.price AS baseline_price,
               prior_point.event_id AS prior_event_id,
               prior_point.et AS prior_event_time,
               prior_point.rt AS prior_received_time,
               prior_point.price AS prior_price,
               future_point.event_id AS label_event_id,
               future_point.et AS label_event_time,
               future_point.rt AS label_received_time,
               future_point.price AS future_price,
               lb.event_id AS leader_book_event_id,
               lb.rt AS leader_book_received_time,
               lb.bid AS leader_bid,
               lb.bid_qty AS leader_bid_qty,
               lb.ask AS leader_ask,
               lb.ask_qty AS leader_ask_qty,
               tb.event_id AS target_book_event_id,
               tb.rt AS target_book_received_time,
               tb.bid AS target_bid,
               tb.bid_qty AS target_bid_qty,
               tb.ask AS target_ask,
               tb.ask_qty AS target_ask_qty,
               lf.signed_qty_1s / NULLIF(lf.gross_qty_1s, 0.0) AS leader_flow_imbalance_1s,
               lf.signed_qty_5s / NULLIF(lf.gross_qty_5s, 0.0) AS leader_flow_imbalance_5s,
               LN(1.0 + lf.count_1s * trade_sample_weight) AS leader_trade_intensity_1s,
               LN(1.0 + lf.count_5s * trade_sample_weight) AS leader_trade_intensity_5s,
               tf.signed_qty_1s / NULLIF(tf.gross_qty_1s, 0.0) AS target_flow_imbalance_1s,
               tf.signed_qty_5s / NULLIF(tf.gross_qty_5s, 0.0) AS target_flow_imbalance_5s,
               LN(1.0 + tf.count_1s * trade_sample_weight) AS target_trade_intensity_1s,
               LN(1.0 + tf.count_5s * trade_sample_weight) AS target_trade_intensity_5s
        FROM impulses i
        CROSS JOIN LATERAL (
            SELECT e.event_id,e.et,e.rt,e.price
            FROM target_base e
            WHERE e.et <= i.et
              AND e.rt <= i.rt
            ORDER BY e.et DESC,e.rt DESC,e.ledger_seq DESC
            LIMIT 1
        ) baseline
        CROSS JOIN LATERAL (
            SELECT e.event_id,e.et,e.rt,e.price
            FROM target_base e
            WHERE e.et <= i.et - interval '1 second'
              AND e.rt <= i.rt
            ORDER BY e.et DESC,e.rt DESC,e.ledger_seq DESC
            LIMIT 1
        ) prior_point
        CROSS JOIN LATERAL (
            SELECT e.event_id,e.et,e.rt,e.price
            FROM target_base e
            WHERE e.et >= GREATEST(
                      i.et + (%s || ' milliseconds')::interval,
                      i.rt + (%s || ' milliseconds')::interval
                  )
              AND e.et <= GREATEST(
                      i.et + (%s || ' milliseconds')::interval,
                      i.rt + (%s || ' milliseconds')::interval
                  ) + (%s || ' milliseconds')::interval
              AND e.rt >= i.rt
              AND e.rt <= i.rt + (%s || ' milliseconds')::interval + (%s || ' milliseconds')::interval
            ORDER BY e.et ASC,e.rt ASC,e.ledger_seq ASC
            LIMIT 1
        ) future_point
        CROSS JOIN LATERAL (
            SELECT b.*
            FROM book_base b
            WHERE b.symbol = %s
              AND b.rt <= i.rt
            ORDER BY b.rt DESC,b.ledger_seq DESC
            LIMIT 1
        ) lb
        CROSS JOIN LATERAL (
            SELECT b.*
            FROM book_base b
            WHERE b.symbol = %s
              AND b.rt <= i.rt
            ORDER BY b.rt DESC,b.ledger_seq DESC
            LIMIT 1
        ) tb
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN e.buyer_maker IS TRUE THEN -1.0 * COALESCE(e.quantity,0.0)
                        WHEN e.buyer_maker IS FALSE THEN 1.0 * COALESCE(e.quantity,0.0)
                        ELSE 0.0
                    END
                ) FILTER (WHERE e.et > i.et - interval '1 second'), 0.0) AS signed_qty_1s,
                COALESCE(SUM(COALESCE(e.quantity,0.0)) FILTER (WHERE e.et > i.et - interval '1 second'), 0.0) AS gross_qty_1s,
                COUNT(*) FILTER (WHERE e.et > i.et - interval '1 second') AS count_1s,
                COALESCE(SUM(
                    CASE
                        WHEN e.buyer_maker IS TRUE THEN -1.0 * COALESCE(e.quantity,0.0)
                        WHEN e.buyer_maker IS FALSE THEN 1.0 * COALESCE(e.quantity,0.0)
                        ELSE 0.0
                    END
                ) FILTER (WHERE e.et > i.et - interval '5 seconds'), 0.0) AS signed_qty_5s,
                COALESCE(SUM(COALESCE(e.quantity,0.0)) FILTER (WHERE e.et > i.et - interval '5 seconds'), 0.0) AS gross_qty_5s,
                COUNT(*) FILTER (WHERE e.et > i.et - interval '5 seconds') AS count_5s
            FROM leader_base e
            WHERE e.et <= i.et
              AND e.rt <= i.rt
        ) lf
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN e.buyer_maker IS TRUE THEN -1.0 * COALESCE(e.quantity,0.0)
                        WHEN e.buyer_maker IS FALSE THEN 1.0 * COALESCE(e.quantity,0.0)
                        ELSE 0.0
                    END
                ) FILTER (WHERE e.et > i.et - interval '1 second'), 0.0) AS signed_qty_1s,
                COALESCE(SUM(COALESCE(e.quantity,0.0)) FILTER (WHERE e.et > i.et - interval '1 second'), 0.0) AS gross_qty_1s,
                COUNT(*) FILTER (WHERE e.et > i.et - interval '1 second') AS count_1s,
                COALESCE(SUM(
                    CASE
                        WHEN e.buyer_maker IS TRUE THEN -1.0 * COALESCE(e.quantity,0.0)
                        WHEN e.buyer_maker IS FALSE THEN 1.0 * COALESCE(e.quantity,0.0)
                        ELSE 0.0
                    END
                ) FILTER (WHERE e.et > i.et - interval '5 seconds'), 0.0) AS signed_qty_5s,
                COALESCE(SUM(COALESCE(e.quantity,0.0)) FILTER (WHERE e.et > i.et - interval '5 seconds'), 0.0) AS gross_qty_5s,
                COUNT(*) FILTER (WHERE e.et > i.et - interval '5 seconds') AS count_5s
            FROM target_base e
            WHERE e.et <= i.et
              AND e.rt <= i.rt
        ) tf
        WHERE baseline.event_id <> prior_point.event_id
          AND lb.event_id IS NOT NULL
          AND tb.event_id IS NOT NULL
          AND lf.gross_qty_1s > 0
          AND lf.gross_qty_5s > 0
          AND tf.gross_qty_1s > 0
          AND tf.gross_qty_5s > 0
        """
        params = (
            *common_scope,
            PREREGISTERED_CRYPTO_PROTOCOL.study_id,
            session_id,
            target_symbol,
            *common_scope[2:],
            PREREGISTERED_CRYPTO_PROTOCOL.study_id,
            session_id,
            leader_symbol,
            target_symbol,
            start.isoformat(),
            end.isoformat(),
            horizon_ms,
            horizon_ms,
            horizon_ms,
            horizon_ms,
            tol_ms,
            horizon_ms,
            tol_ms,
            leader_symbol,
            target_symbol,
        )
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    finally:
        conn.close()

    import math

    output: list[ForecastDatasetRow] = []
    for raw in rows:
        item = dict(zip(cols, raw))
        leader_return = float(item["leader_return_bps"])
        baseline_price = float(item["baseline_price"])
        prior_price = float(item["prior_price"])
        future_price = float(item["future_price"])
        if baseline_price <= 0 or prior_price <= 0 or future_price <= 0:
            continue

        target_return = 10000.0 * math.log(baseline_price / prior_price)
        realized_raw = 10000.0 * math.log(future_price / baseline_price)
        direction_multiplier = 1.0 if leader_return >= 0 else -1.0
        signed = direction_multiplier * realized_raw

        def _book_features(prefix: str) -> dict[str, float]:
            bid = float(item[f"{prefix}_bid"])
            ask = float(item[f"{prefix}_ask"])
            bid_qty = float(item[f"{prefix}_bid_qty"])
            ask_qty = float(item[f"{prefix}_ask_qty"])
            depth = bid_qty + ask_qty
            mid = 0.5 * (bid + ask)
            micro = (
                (ask * bid_qty + bid * ask_qty) / depth
                if depth > 0
                else mid
            )
            return {
                f"{prefix}_queue_imbalance": (bid_qty - ask_qty) / depth if depth > 0 else 0.0,
                f"{prefix}_spread_bps": ((ask / bid) - 1.0) * 10_000.0 if bid > 0 else 0.0,
                f"{prefix}_microprice_gap_bps": ((micro / mid) - 1.0) * 10_000.0 if mid > 0 else 0.0,
            }

        leader_book = _book_features("leader")
        target_book = _book_features("target")
        leader_flow_1s = float(item["leader_flow_imbalance_1s"])
        leader_flow_5s = float(item["leader_flow_imbalance_5s"])
        target_flow_1s = float(item["target_flow_imbalance_1s"])
        target_flow_5s = float(item["target_flow_imbalance_5s"])

        features = {
            "leader_return_bps": leader_return,
            "leader_abs_return_bps": abs(leader_return),
            "leader_direction": 1.0 if leader_return > 0 else -1.0,
            "leader_transport_latency_ms": max(
                0.0,
                (item["leader_received_time"] - item["leader_event_time"]).total_seconds() * 1000.0,
            ),
            "target_return_bps_lookback": target_return,
            "target_abs_return_bps_lookback": abs(target_return),
            "target_information_age_ms": max(
                0.0,
                (item["leader_received_time"] - item["baseline_received_time"]).total_seconds() * 1000.0,
            ),
            "target_market_age_ms": max(
                0.0,
                (item["leader_event_time"] - item["baseline_event_time"]).total_seconds() * 1000.0,
            ),
            "leader_flow_imbalance_1s": leader_flow_1s,
            "leader_flow_imbalance_5s": leader_flow_5s,
            "leader_trade_intensity_1s": float(item["leader_trade_intensity_1s"]),
            "leader_trade_intensity_5s": float(item["leader_trade_intensity_5s"]),
            "target_flow_imbalance_1s": target_flow_1s,
            "target_flow_imbalance_5s": target_flow_5s,
            "target_trade_intensity_1s": float(item["target_trade_intensity_1s"]),
            "target_trade_intensity_5s": float(item["target_trade_intensity_5s"]),
            **leader_book,
            **target_book,
            "leader_flow_x_shock": leader_return * leader_flow_1s,
            "relative_flow_pressure": leader_flow_1s - target_flow_1s,
            "leader_flow_persistence": leader_flow_1s - leader_flow_5s,
            "target_flow_persistence": target_flow_1s - target_flow_5s,
            "leader_flow_x_queue": leader_flow_1s * leader_book["leader_queue_imbalance"],
            "target_flow_x_queue": target_flow_1s * target_book["target_queue_imbalance"],
            "target_adverse_selection_pressure": target_flow_1s * target_book["target_microprice_gap_bps"],
            "cross_asset_dislocation_bps": leader_return - target_return,
            "leader_book_age_ms": max(
                0.0,
                (item["leader_received_time"] - item["leader_book_received_time"]).total_seconds() * 1000.0,
            ),
            "target_book_age_ms": max(
                0.0,
                (item["leader_received_time"] - item["target_book_received_time"]).total_seconds() * 1000.0,
            ),
            "book_age_gap_ms": (
                max(0.0, (item["leader_received_time"] - item["leader_book_received_time"]).total_seconds() * 1000.0)
                - max(0.0, (item["leader_received_time"] - item["target_book_received_time"]).total_seconds() * 1000.0)
            ),
            "leader_book_confidence": math.exp(
                -max(0.0, (item["leader_received_time"] - item["leader_book_received_time"]).total_seconds())
                / max(float(PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds), 1.0)
            ),
            "target_book_confidence": math.exp(
                -max(0.0, (item["leader_received_time"] - item["target_book_received_time"]).total_seconds())
                / max(float(PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds), 1.0)
            ),
            "leader_flow_x_book_confidence": leader_flow_1s * math.exp(
                -max(0.0, (item["leader_received_time"] - item["leader_book_received_time"]).total_seconds())
                / max(float(PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds), 1.0)
            ),
            "target_flow_x_book_confidence": target_flow_1s * math.exp(
                -max(0.0, (item["leader_received_time"] - item["target_book_received_time"]).total_seconds())
                / max(float(PREREGISTERED_CRYPTO_PROTOCOL.bookticker_persistence_interval_seconds), 1.0)
            ),
        }

        source_ids = (
            str(item["leader_event_id"]),
            str(item["baseline_event_id"]),
            str(item["prior_event_id"]),
            str(item["leader_book_event_id"]),
            str(item["target_book_event_id"]),
        )
        feature_hash = _feature_hash(
            leader_symbol=leader_symbol,
            target_symbol=target_symbol,
            decision_event_time=item["leader_event_time"],
            decision_received_time=item["leader_received_time"],
            source_event_ids=source_ids,
            feature_values=features,
        )
        snapshot = DetectionFeatureSnapshot(
            feature_set_version=FEATURE_SET_VERSION,
            decision_event_time=_dt(item["leader_event_time"]),
            decision_received_time=_dt(item["leader_received_time"]),
            leader_symbol=leader_symbol.upper(),
            target_symbol=target_symbol.upper(),
            leader_event_id=str(item["leader_event_id"]),
            feature_values=features,
            source_event_ids=source_ids,
            feature_set_hash=feature_hash,
        )
        actual_event_horizon_ms = int(
            round((item["label_event_time"] - item["leader_event_time"]).total_seconds() * 1000.0)
        )
        actual_receive_horizon_ms = int(
            round((item["label_received_time"] - item["leader_received_time"]).total_seconds() * 1000.0)
        )
        label = ForecastLabel(
            realized_target=1 if signed > 0 else 0,
            realized_signed_return_bps=float(signed),
            baseline_target_price=baseline_price,
            future_target_price=future_price,
            label_event_time=_dt(item["label_event_time"]),
            label_received_time=_dt(item["label_received_time"]),
            label_event_id=str(item["label_event_id"]),
            horizon_ms=horizon_ms,
            actual_event_horizon_ms=actual_event_horizon_ms,
            actual_receive_horizon_ms=actual_receive_horizon_ms,
        )
        output.append(ForecastDatasetRow(snapshot=snapshot, label=label))

    output.sort(
        key=lambda row: (
            row.snapshot.decision_received_time,
            row.snapshot.decision_event_time,
            row.snapshot.leader_event_id,
        )
    )
    return tuple(output)

def _research_identity(session_id: str, replay_fingerprint: str) -> str:
    return hashlib.sha256(
        f"{PREREGISTERED_CRYPTO_PROTOCOL.study_id}|{session_id}|{replay_fingerprint}".encode("utf-8")
    ).hexdigest()[:32]


def _claim_research_run(
    store,
    *,
    session_id: str,
    start: datetime,
    end: datetime,
    replay_fingerprint: str,
    now: datetime,
) -> tuple[str, str]:
    run_id = _research_identity(session_id, replay_fingerprint)
    conn = store.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status,started_at
                FROM crypto_research_runs
                WHERE capture_session_id=%s AND replay_fingerprint=%s
                """,
                (session_id, replay_fingerprint),
            )
            existing = cur.fetchone()
            if existing is not None:
                status = str(existing[0])
                if status == "COMPLETE":
                    return run_id, "ALREADY_COMPLETE"
                cur.execute(
                    """
                    UPDATE crypto_research_runs
                    SET started_at=%s,status='RUNNING',finished_at=NULL,result_json=%s
                    WHERE capture_session_id=%s AND replay_fingerprint=%s
                    """,
                    (
                        now.isoformat(),
                        json.dumps({"resumed_at": now.isoformat()}, sort_keys=True),
                        session_id,
                        replay_fingerprint,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO crypto_research_runs(
                        research_run_id,created_at,started_at,study_id,capture_session_id,
                        cohort_start,cohort_end,replay_fingerprint,status,result_json
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING',%s)
                    ON CONFLICT(capture_session_id,replay_fingerprint) DO NOTHING
                    """,
                    (
                        run_id,
                        now.isoformat(),
                        now.isoformat(),
                        PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                        session_id,
                        start.isoformat(),
                        end.isoformat(),
                        replay_fingerprint,
                        json.dumps({"started_at": now.isoformat()}, sort_keys=True),
                    ),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        SELECT status FROM crypto_research_runs
                        WHERE capture_session_id=%s AND replay_fingerprint=%s
                        """,
                        (session_id, replay_fingerprint),
                    )
                    row = cur.fetchone()
                    if row is not None and str(row[0]) == "COMPLETE":
                        conn.rollback()
                        return run_id, "ALREADY_COMPLETE"
            conn.commit()
            return run_id, "CLAIMED"
    finally:
        conn.close()


def _finish_research_run(
    store,
    *,
    session_id: str,
    replay_fingerprint: str,
    status: str,
    result: dict[str, Any],
) -> None:
    conn = store.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE crypto_research_runs
                SET finished_at=%s,status=%s,result_json=%s
                WHERE capture_session_id=%s AND replay_fingerprint=%s
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    status,
                    json.dumps(result, sort_keys=True, default=str),
                    session_id,
                    replay_fingerprint,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def run_crypto_research_once(store) -> dict[str, Any]:
    """Execute one complete research attempt, or return a hard blocker."""
    now = datetime.now(timezone.utc)
    session = _find_mature_session(store, now=now)
    if session is None:
        store.record_connection(
            source=RESEARCH_STATUS_SOURCE,
            status="BLOCKED",
            reason="NO_MATURE_7D_SESSION",
            metadata={"study_id": PREREGISTERED_CRYPTO_PROTOCOL.study_id},
        )
        return {"status": "BLOCKED", "reason": "NO_MATURE_7D_SESSION"}

    session_id = str(session["session_id"])
    start, end = _cohort_window(session, now=now)

    replay_fp, row_count, first_seq, last_seq = _replay_fingerprint_and_manifest(
        store,
        session_id=session_id,
        start=start,
        end=end,
    )
    manifest = {
        "replay_version": "3-deterministic-sample",
        "ledger_schema_version": "crypto",
        "code_version": os.getenv("RENDER_GIT_COMMIT") or "unknown",
        "manifest_created_at": now.isoformat(),
        "order": "ingest",
        "study_id": PREREGISTERED_CRYPTO_PROTOCOL.study_id,
        "capture_session_id": session_id,
        "start_received_time": start.isoformat(),
        "end_received_time": end.isoformat(),
        "start_event_time": start.isoformat(),
        "end_event_time": end.isoformat(),
        "row_count": row_count,
        "first_ledger_seq": first_seq,
        "last_ledger_seq": last_seq,
        "fingerprint_sha256": replay_fp,
    }
    manifest_id = store.save_replay_manifest(manifest)
    research_run_id, claim_status = _claim_research_run(
        store,
        session_id=session_id,
        start=start,
        end=end,
        replay_fingerprint=replay_fp,
        now=now,
    )
    if claim_status == "ALREADY_COMPLETE":
        return {
            "status": "ALREADY_COMPLETE",
            "capture_session_id": session_id,
            "replay_fingerprint": replay_fp,
            "manifest_id": manifest_id,
            "research_run_id": research_run_id,
        }
    report = _sql_quality_report(
        store,
        session_id=session_id,
        start=start,
        end=end,
        replay_fingerprint=replay_fp,
    )
    store.save_quality_report(asdict(report), quality_fingerprint(report))

    if not report.passed:
        store.record_connection(
            source=RESEARCH_STATUS_SOURCE,
            status="BLOCKED",
            reason="QUALITY_GATE_FAIL",
            metadata={
                "capture_session_id": session_id,
                "replay_fingerprint": replay_fp,
                "quality_reasons": list(report.reasons),
                "manifest_id": manifest_id,
            },
        )
        result = {
            "status": "BLOCKED",
            "reason": "QUALITY_GATE_FAIL",
            "capture_session_id": session_id,
            "replay_fingerprint": replay_fp,
            "quality_reasons": list(report.reasons),
            "research_run_id": research_run_id,
        }
        _finish_research_run(
            store,
            session_id=session_id,
            replay_fingerprint=replay_fp,
            status="BLOCKED",
            result=result,
        )
        return result

    walk = WalkForwardConfig(
        purge_ms=PREREGISTERED_CRYPTO_PROTOCOL.purge_ms,
        embargo_ms=PREREGISTERED_CRYPTO_PROTOCOL.embargo_ms,
    )
    policy = EconomicPolicySpec(
        long_threshold=PREREGISTERED_CRYPTO_PROTOCOL.long_threshold,
        short_threshold=PREREGISTERED_CRYPTO_PROTOCOL.short_threshold,
        round_trip_cost_bps=PREREGISTERED_CRYPTO_PROTOCOL.base_cost_bps,
        round_trip_slippage_bps=PREREGISTERED_CRYPTO_PROTOCOL.base_slippage_bps,
    )
    stress = tuple(
        StressScenario(
            name=name,
            cost_multiplier=cost_multiplier,
            slippage_multiplier=slippage_multiplier,
        )
        for name, cost_multiplier, slippage_multiplier in PREREGISTERED_CRYPTO_PROTOCOL.stress_scenarios
    )
    lead_lag = LeadLagConfig(
        response_delays_ms=PREREGISTERED_CRYPTO_PROTOCOL.forecast_horizons_ms,
        refractory_seconds=PREREGISTERED_CRYPTO_PROTOCOL.refractory_seconds,
    )
    total_runs = 0
    persisted = 0
    blocked_pairs: list[str] = []
    for leader_symbol in PREREGISTERED_CRYPTO_PROTOCOL.symbols:
        for target_symbol in PREREGISTERED_CRYPTO_PROTOCOL.symbols:
            if leader_symbol == target_symbol:
                continue
            target_spec_by_horizon = [
                ForecastTargetSpec(
                    horizon_ms=horizon,
                    alignment_tolerance_ms=PREREGISTERED_CRYPTO_PROTOCOL.horizon_alignment_tolerance_ms,
                )
                for horizon in PREREGISTERED_CRYPTO_PROTOCOL.forecast_horizons_ms
            ]
            for target_spec in target_spec_by_horizon:
                total_runs += 1
                dataset = _dataset_rows(
                    store,
                    session_id=session_id,
                    start=start,
                    end=end,
                    leader_symbol=leader_symbol,
                    target_symbol=target_symbol,
                    target_spec=target_spec,
                )
                if len(dataset) < walk.min_train_rows + walk.test_rows:
                    blocked_pairs.append(
                        f"{leader_symbol}->{target_symbol}@{target_spec.horizon_ms}:INSUFFICIENT_DATASET"
                    )
                    continue
                feature_names = tuple(MICROSTRUCTURE_FEATURES)
                if any(set(row.snapshot.feature_values) != set(feature_names) for row in dataset):
                    blocked_pairs.append(f"{leader_symbol}->{target_symbol}@{target_spec.horizon_ms}:FEATURE_SCHEMA_MISMATCH")
                    continue
                report_oos = run_quality_gated_walk_forward(
                    dataset,
                    feature_names,
                    walk,
                    policy,
                    quality_report=asdict(report),
                    minimum_quality_rows=report.rows,
                    replay_fingerprint=replay_fp,
                    model_id=f"crypto-microstructure-ridge-logit-wf-{leader_symbol}-{target_symbol}",
                    model_version="3",
                    placebo_block_size=PREREGISTERED_CRYPTO_PROTOCOL.placebo_block_size,
                    placebo_iterations=PREREGISTERED_CRYPTO_PROTOCOL.placebo_iterations,
                    stress_scenarios=stress,
                    protocol=PREREGISTERED_CRYPTO_PROTOCOL,
                )
                persist_validation_report(
                    store,
                    report_oos,
                    dataset,
                    replay_fingerprint=replay_fp,
                    target_spec=target_spec,
                    config=walk,
                    policy=policy,
                    model_id=f"crypto-ridge-logit-wf-{leader_symbol}-{target_symbol}",
                    model_version="1",
                    protocol=PREREGISTERED_CRYPTO_PROTOCOL,
                )
                persisted += 1
                store.record_connection(
                    source=RESEARCH_STATUS_SOURCE,
                    status="RUN_COMPLETE",
                    metadata={
                        "capture_session_id": session_id,
                        "leader_symbol": leader_symbol,
                        "target_symbol": target_symbol,
                        "horizon_ms": target_spec.horizon_ms,
                        "validation_status": report_oos.status,
                        "promotion_eligible": report_oos.promotion_eligible,
                    },
                )

    status = "COMPLETE" if persisted == total_runs else "PARTIAL_BLOCKED"
    store.record_connection(
        source=RESEARCH_STATUS_SOURCE,
        status=status,
        metadata={
            "capture_session_id": session_id,
            "manifest_id": manifest_id,
            "replay_fingerprint": replay_fp,
            "total_runs": total_runs,
            "persisted_runs": persisted,
            "blocked_runs": blocked_pairs,
        },
    )
    result = {
        "status": status,
        "capture_session_id": session_id,
        "replay_fingerprint": replay_fp,
        "manifest_id": manifest_id,
        "research_run_id": research_run_id,
        "total_runs": total_runs,
        "persisted_runs": persisted,
        "blocked_runs": blocked_pairs,
    }
    _finish_research_run(
        store,
        session_id=session_id,
        replay_fingerprint=replay_fp,
        status="COMPLETE" if status == "COMPLETE" else "PARTIAL_BLOCKED",
        result=result,
    )
    return result
