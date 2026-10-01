"""Unified quantitative evidence read-model for Cryptonita.

This module is intentionally off the market hot path. It reads bounded, persisted
research artifacts plus the in-process market projection and exposes one coherent
state contract to the frontend.
"""

from __future__ import annotations

import json
import math
import threading
from datetime import datetime, timezone
from statistics import mean, median, pstdev
from typing import Any

from .market_cache import MARKET_CACHE
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .forecast import FEATURE_SET_VERSION
from .quant_store import QuantCryptoStore

_EVIDENCE_LOCK = threading.RLock()
_EVIDENCE_CACHE: dict[str, Any] = {"expires_at": 0.0, "payload": None}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _dt(value: Any) -> datetime | None:
    text = _iso(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _query(conn, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def _current_research_fingerprint(
    conn,
    session_id: str | None,
) -> str | None:
    if not session_id:
        return None
    rows = _query(
        conn,
        """
        SELECT replay_fingerprint
        FROM crypto_research_runs
        WHERE capture_session_id=%s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (session_id,),
    )
    if not rows:
        return None
    value = str(rows[0].get("replay_fingerprint") or "").strip()
    return value or None


def _cohort(conn, now: datetime) -> dict[str, Any]:
    rows = _query(
        conn,
        """
        SELECT session_id,study_id,provider,venue,region,instance_id,code_version,
               symbols_json,streams_json,protocol_hash,started_at,ended_at,status,metadata
        FROM crypto_capture_sessions
        WHERE study_id=%s
        ORDER BY started_at DESC
        LIMIT 8
        """,
        (PREREGISTERED_CRYPTO_PROTOCOL.study_id,),
    )
    active = next(
        (row for row in rows if str(row.get("status")) in {"STARTING", "RUNNING"}),
        None,
    )
    reference = active or (rows[0] if rows else None)
    if reference is None:
        return {
            "status": "NO_SESSION",
            "session_id": None,
            "started_at": None,
            "expected_end_at": None,
            "elapsed_seconds": 0.0,
            "remaining_seconds": float(PREREGISTERED_CRYPTO_PROTOCOL.prospect_days * 86400),
            "progress_pct": 0.0,
            "mature": False,
            "symbols": list(PREREGISTERED_CRYPTO_PROTOCOL.symbols),
        }

    started = _dt(reference["started_at"]) or now
    expected_end = started.timestamp() + PREREGISTERED_CRYPTO_PROTOCOL.prospect_days * 86400
    expected_end_dt = datetime.fromtimestamp(expected_end, timezone.utc)
    elapsed = max(0.0, (now - started).total_seconds())
    total = float(PREREGISTERED_CRYPTO_PROTOCOL.prospect_days * 86400)
    progress = min(100.0, elapsed / total * 100.0) if total else 0.0
    ended = _dt(reference.get("ended_at"))
    status = str(reference.get("status") or "UNKNOWN")
    mature = now >= expected_end_dt and ended is None or (
        ended is not None and ended >= expected_end_dt
    )

    return {
        "status": "MATURE" if mature else "ACCUMULATING",
        "session_id": str(reference["session_id"]),
        "runtime_status": status,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat() if ended else None,
        "expected_end_at": expected_end_dt.isoformat(),
        "elapsed_seconds": elapsed,
        "remaining_seconds": max(0.0, (expected_end_dt - now).total_seconds()),
        "progress_pct": progress,
        "mature": bool(mature),
        "symbols": list(PREREGISTERED_CRYPTO_PROTOCOL.symbols),
        "protocol_hash": str(reference.get("protocol_hash") or ""),
        "code_version": reference.get("code_version"),
    }


def _quality_gate(conn, cohort: dict[str, Any], now: datetime) -> dict[str, Any]:
    rows = _query(
        conn,
        """
        SELECT report_id,created_at,replay_fingerprint,status,report_hash,report_json
        FROM crypto_quality_reports
        ORDER BY created_at DESC
        LIMIT 1
        """,
    )
    latest = rows[0] if rows else None
    if latest is None:
        return {
            "state": "WAITING",
            "status": "NO_REPORT",
            "current_session": False,
            "created_at": None,
            "age_seconds": None,
            "replay_fingerprint": None,
            "reasons": ["NO_QUALITY_REPORT"],
            "report": {},
        }

    created = _dt(latest["created_at"])
    age = (now - created).total_seconds() if created else None
    report = _json(latest["report_json"])
    current_session = bool(
        cohort.get("started_at")
        and created
        and created >= (_dt(cohort["started_at"]) or created)
    )
    status = str(latest["status"] or report.get("status") or "UNKNOWN")
    if status == "PASS" and current_session:
        state = "PASS"
    elif status == "FAIL" and current_session:
        state = "FAIL"
    else:
        state = "WAITING"

    return {
        "state": state,
        "status": status,
        "current_session": current_session,
        "created_at": created.isoformat() if created else None,
        "age_seconds": age,
        "replay_fingerprint": latest["replay_fingerprint"],
        "rows": int(report.get("rows") or 0),
        "reasons": list(report.get("reasons") or []),
        "symbol_stats": report.get("symbol_stats") or {},
        "event_type_counts": report.get("event_type_counts") or {},
        "report": {
            "future_event_count": int(report.get("future_event_count") or 0),
            "negative_transport_latency_count": int(report.get("negative_transport_latency_count") or 0),
            "required_source_gap_count": int(report.get("required_source_gap_count") or 0),
            "receive_time_reversal_count": int(report.get("receive_time_reversal_count") or 0),
        },
    }


def _research_gate(conn) -> dict[str, Any]:
    rows = _query(
        conn,
        """
        SELECT created_at,status,reason,metadata
        FROM crypto_connection_events
        WHERE source='gorila.crypto.research_runner'
        ORDER BY created_at DESC
        LIMIT 1
        """,
    )
    latest = rows[0] if rows else None
    if latest is None:
        return {"status": "NOT_STARTED", "reason": "NO_RESEARCH_RUN", "created_at": None}
    return {
        "status": str(latest.get("status") or "UNKNOWN"),
        "reason": latest.get("reason"),
        "created_at": _iso(latest.get("created_at")),
        "metadata": _json(latest.get("metadata")),
    }


def _validation_gate(conn, replay_fingerprint: str | None = None) -> dict[str, Any]:
    if not replay_fingerprint:
        return {
            "state": "BLOCKED",
            "status": "NO_CURRENT_RESEARCH_FINGERPRINT",
            "promotion_eligible": False,
            "oos_rows": 0,
            "latest": None,
        }

    rows = _query(
        conn,
        """
        SELECT run_id,created_at,replay_fingerprint,model_id,model_version,
               target_kind,horizon_ms,status,placebo_p_value,placebo_iterations,
               promotion_eligible,config_json,aggregate_metrics_json,stability_json,stress_json
        FROM crypto_validation_runs
        WHERE replay_fingerprint=%s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (replay_fingerprint,),
    )
    latest = rows[0] if rows else None
    if latest is None:
        return {
            "state": "BLOCKED",
            "status": "NO_OOS_RUN",
            "promotion_eligible": False,
            "oos_rows": 0,
            "latest": None,
        }

    oos_row = _query(
        conn,
        "SELECT COUNT(*) AS n FROM crypto_validation_oos WHERE run_id=%s",
        (latest["run_id"],),
    )[0]
    aggregate = _json(latest["aggregate_metrics_json"])
    stability = _json(latest["stability_json"])
    stress = _json(latest["stress_json"])
    eligible = bool(latest["promotion_eligible"])
    status = str(latest["status"] or "UNKNOWN")
    return {
        "state": "PASS" if eligible else "BLOCKED",
        "status": status,
        "promotion_eligible": eligible,
        "oos_rows": int(oos_row["n"] or 0),
        "latest": {
            "run_id": latest["run_id"],
            "created_at": _iso(latest["created_at"]),
            "replay_fingerprint": latest["replay_fingerprint"],
            "model_id": latest["model_id"],
            "model_version": latest["model_version"],
            "target_kind": latest["target_kind"],
            "horizon_ms": int(latest["horizon_ms"]),
            "placebo_p_value": latest["placebo_p_value"],
            "placebo_iterations": int(latest["placebo_iterations"] or 0),
            "aggregate": aggregate,
            "stability": stability,
            "stress": stress,
        },
    }


def _forecast_shadow(
    conn,
    replay_fingerprint: str | None = None,
) -> dict[str, Any]:
    if not replay_fingerprint:
        return {
            "count": 0,
            "outcomes_count": 0,
            "state": "EMPTY",
            "latest": None,
            "recent_probability_mean": None,
            "recent_probability_min": None,
            "recent_probability_max": None,
        }

    count_row = _query(
        conn,
        "SELECT COUNT(*) AS n FROM crypto_forecast_shadow WHERE replay_fingerprint=%s",
        (replay_fingerprint,),
    )[0]
    outcome_row = _query(
        conn,
        """
        SELECT COUNT(*) AS n
        FROM crypto_forecast_outcomes o
        JOIN crypto_forecast_shadow s ON s.forecast_id=o.forecast_id
        WHERE s.replay_fingerprint=%s
        """,
        (replay_fingerprint,),
    )[0]
    rows = _query(
        conn,
        """
        SELECT forecast_id,created_at,status,model_id,model_version,symbol,target_symbol,
               horizon_ms,target_kind,semantics,probability_response_positive,
               decision_event_time,decision_received_time,feature_set_hash
        FROM crypto_forecast_shadow
        WHERE replay_fingerprint=%s
        ORDER BY created_at DESC
        LIMIT 8
        """,
        (replay_fingerprint,),
    )
    latest = rows[0] if rows else None
    probabilities = [
        float(row["probability_response_positive"])
        for row in rows
        if row.get("probability_response_positive") is not None
    ]
    return {
        "count": int(count_row["n"] or 0),
        "outcomes_count": int(outcome_row["n"] or 0),
        "state": "EMPTY" if not rows else "SHADOW_READY",
        "latest": (
            {
                **row,
                "created_at": _iso(row["created_at"]),
                "decision_event_time": _iso(row["decision_event_time"]),
                "decision_received_time": _iso(row["decision_received_time"]),
            }
            for row in rows[:1]
        ).__next__() if rows else None,
        "recent_probability_mean": mean(probabilities) if probabilities else None,
        "recent_probability_min": min(probabilities) if probabilities else None,
        "recent_probability_max": max(probabilities) if probabilities else None,
    }


def _lead_lag(
    conn,
    replay_fingerprint: str | None = None,
) -> dict[str, Any]:
    if not replay_fingerprint:
        return {"observation_count": 0, "pairs": []}

    counts = _query(
        conn,
        "SELECT COUNT(*) AS n FROM crypto_lead_lag_shadow WHERE replay_fingerprint=%s",
        (replay_fingerprint,),
    )
    rows = _query(
        conn,
        """
        SELECT leader_symbol,target_symbol,delay_ms,COUNT(*) AS n,
               AVG(signed_target_response_bps) AS mean_response_bps,
               AVG(information_lag_ms) AS mean_information_lag_ms,
               percentile_cont(0.50) WITHIN GROUP (ORDER BY information_lag_ms) AS median_information_lag_ms
        FROM crypto_lead_lag_shadow
        WHERE replay_fingerprint=%s
        GROUP BY leader_symbol,target_symbol,delay_ms
        ORDER BY n DESC
        LIMIT 24
        """,
        (replay_fingerprint,),
    )
    return {
        "observation_count": int(counts[0]["n"]) if counts else 0,
        "pairs": [
            {
                "leader_symbol": row["leader_symbol"],
                "target_symbol": row["target_symbol"],
                "delay_ms": int(row["delay_ms"]),
                "n": int(row["n"]),
                "mean_response_bps": float(row["mean_response_bps"]) if row["mean_response_bps"] is not None else None,
                "mean_information_lag_ms": float(row["mean_information_lag_ms"]) if row["mean_information_lag_ms"] is not None else None,
                "median_information_lag_ms": float(row["median_information_lag_ms"]) if row["median_information_lag_ms"] is not None else None,
            }
            for row in rows
        ],
    }


def _opportunity_shadow(
    conn,
    replay_fingerprint: str | None = None,
) -> dict[str, Any]:
    if not replay_fingerprint:
        return {"count": 0, "state_counts": {}, "latest": []}

    count_row = _query(
        conn,
        "SELECT COUNT(*) AS n FROM crypto_opportunity_shadow WHERE replay_fingerprint=%s",
        (replay_fingerprint,),
    )[0]
    states = _query(
        conn,
        """
        SELECT status,COUNT(*) AS n
        FROM crypto_opportunity_shadow
        WHERE replay_fingerprint=%s
        GROUP BY status
        ORDER BY n DESC
        """,
        (replay_fingerprint,),
    )
    rows = _query(
        conn,
        """
        SELECT opportunity_id,created_at,status,leader_symbol,target_symbol,direction,
               leader_return_bps,detection_event_time,detection_received_time,
               first_reaction_information_lag_ms,convergence_information_lag_ms,
               max_favorable_excursion_bps,max_adverse_excursion_bps
        FROM crypto_opportunity_shadow
        WHERE replay_fingerprint=%s
        ORDER BY created_at DESC
        LIMIT 8
        """,
        (replay_fingerprint,),
    )
    return {
        "count": int(count_row["n"] or 0),
        "state_counts": {str(row["status"]): int(row["n"]) for row in states},
        "latest": [
            {
                **row,
                "created_at": _iso(row["created_at"]),
                "detection_event_time": _iso(row["detection_event_time"]),
                "detection_received_time": _iso(row["detection_received_time"]),
            }
            for row in rows
        ],
    }


def _observed_regime() -> dict[str, Any]:
    snapshot = MARKET_CACHE.snapshot(
        symbols=PREREGISTERED_CRYPTO_PROTOCOL.symbols,
        cursor=0,
        limit=480,
    )
    events = snapshot["events"]
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        if event["event_type"] == "trade" and event["price"] is not None:
            by_symbol.setdefault(event["symbol"], []).append(event)

    result: dict[str, Any] = {}
    for symbol in PREREGISTERED_CRYPTO_PROTOCOL.symbols:
        rows = by_symbol.get(symbol, [])
        prices = [float(row["price"]) for row in rows if row.get("price") is not None]
        if len(prices) < 30:
            result[symbol] = {
                "state": "INSUFFICIENT_OBSERVED_SAMPLE",
                "validated": False,
                "n_trades": len(prices),
            }
            continue

        log_returns = [
            10_000.0 * math.log(prices[i] / prices[i - 1])
            for i in range(1, len(prices))
            if prices[i] > 0 and prices[i - 1] > 0
        ]
        flow_signed = sum(
            (1.0 if row.get("side") == "BUY" else -1.0) * float(row.get("quantity") or 0.0)
            for row in rows
        )
        flow_gross = sum(float(row.get("quantity") or 0.0) for row in rows)
        flow = flow_signed / flow_gross if flow_gross else 0.0
        trend_bps = 10_000.0 * math.log(prices[-1] / prices[0]) if prices[0] > 0 else 0.0
        vol = pstdev(log_returns) if len(log_returns) >= 2 else 0.0

        if vol >= 8.0:
            state = "HIGH_VOL_OBSERVED"
        elif abs(trend_bps) >= 20.0 and flow >= 0.15:
            state = "TREND_UP_OBSERVED"
        elif abs(trend_bps) >= 20.0 and flow <= -0.15:
            state = "TREND_DOWN_OBSERVED"
        elif abs(trend_bps) < 8.0 and abs(flow) < 0.10:
            state = "QUIET_OBSERVED"
        else:
            state = "MIXED_OBSERVED"

        result[symbol] = {
            "state": state,
            "validated": False,
            "n_trades": len(prices),
            "trend_bps": trend_bps,
            "vol_bps_per_trade": vol,
            "trade_flow_imbalance": flow,
        }

    return {
        "status": "OBSERVED_NOT_VALIDATED",
        "validated": False,
        "symbols": result,
        "method": {
            "source": "durable-commit market projection",
            "features": ["return_bps", "dispersion_of_trade_returns", "trade_flow_imbalance"],
            "note": "descriptive state only; never eligible for Opportunity Clock activation",
        },
    }


def _opportunity_clock(
    *,
    cohort: dict[str, Any],
    quality: dict[str, Any],
    validation: dict[str, Any],
    forecast: dict[str, Any],
    opportunity: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if not cohort.get("mature"):
        blockers.append("PROSPECTIVE_COHORT_NOT_MATURE")
    if quality.get("state") != "PASS":
        blockers.append("QUALITY_GATE_NOT_PASS")
    if validation.get("state") != "PASS":
        blockers.append("PIT_OOS_NOT_PROMOTION_ELIGIBLE")
    if forecast.get("count", 0) <= 0:
        blockers.append("NO_FORECAST_SHADOW")
    if opportunity.get("count", 0) <= 0:
        blockers.append("NO_OPPORTUNITY_SHADOW")

    active = not blockers
    return {
        "state": "ACTIVE" if active else "LOCKED",
        "validated": bool(active),
        "mode": "VALIDATED" if active else "SHADOW",
        "horizon_ms": validation.get("latest", {}).get("horizon_ms") if active and validation.get("latest") else None,
        "remaining_seconds": None,
        "blockers": blockers,
        "rule": "Opportunity Clock can activate only when cohort + current Quality PASS + PIT/OOS promotion eligibility + forecast shadow + opportunity shadow are all present.",
    }


def build_evidence_snapshot(*, ttl_seconds: float = 15.0) -> dict[str, Any]:
    """Build the frontend evidence contract with a bounded read-model cache to protect Postgres."""
    import time

    now_epoch = time.monotonic()
    with _EVIDENCE_LOCK:
        if _EVIDENCE_CACHE["payload"] is not None and now_epoch < float(_EVIDENCE_CACHE["expires_at"]):
            return _EVIDENCE_CACHE["payload"]

        store = QuantCryptoStore(require_durable=True)
        conn = store.connect()
        try:
            now = _now()
            cohort = _cohort(conn, now)
            quality = _quality_gate(conn, cohort, now)
            research = _research_gate(conn)
            replay_fingerprint = _current_research_fingerprint(
                conn,
                cohort.get("session_id"),
            )
            if replay_fingerprint is None and quality.get("current_session"):
                replay_fingerprint = quality.get("replay_fingerprint")
            validation = _validation_gate(conn, replay_fingerprint)
            forecast = _forecast_shadow(conn, replay_fingerprint)
            lead_lag = _lead_lag(conn, replay_fingerprint)
            opportunity = _opportunity_shadow(conn, replay_fingerprint)
        finally:
            conn.close()

        regime = _observed_regime()
        clock = _opportunity_clock(
            cohort=cohort,
            quality=quality,
            validation=validation,
            forecast=forecast,
            opportunity=opportunity,
        )

        payload = {
            "generated_at": now.isoformat(),
            "study": {
                "study_id": PREREGISTERED_CRYPTO_PROTOCOL.study_id,
                "version": PREREGISTERED_CRYPTO_PROTOCOL.version,
                "protocol_hash": PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash,
                "prospect_days": PREREGISTERED_CRYPTO_PROTOCOL.prospect_days,
                "min_trade_rows_per_symbol": PREREGISTERED_CRYPTO_PROTOCOL.min_trade_rows_per_symbol,
                "forecast_horizons_ms": list(PREREGISTERED_CRYPTO_PROTOCOL.forecast_horizons_ms),
                "alpha_feature_set": FEATURE_SET_VERSION,
                "alpha_model_version": "2",
                "promotion_latency_guard": {
                    "median_max_ratio": 0.50,
                    "p95_max_ratio": 0.75,
                    "scope": "forecast_horizon",
                },
            },
            "cohort": cohort,
            "quality_gate": quality,
            "research": research,
            "pit_oos": validation,
            "forecast_shadow": forecast,
            "lead_lag_shadow": lead_lag,
            "opportunity_shadow": opportunity,
            "regime": regime,
            "opportunity_clock": clock,
        }

        _EVIDENCE_CACHE["payload"] = payload
        _EVIDENCE_CACHE["expires_at"] = now_epoch + ttl_seconds
        return payload
