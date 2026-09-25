import json
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg


SCHEMA = """
CREATE TABLE IF NOT EXISTS forecasts (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    last DOUBLE PRECISION,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    spread_bps DOUBLE PRECISION,
    microprice DOUBLE PRECISION,
    model_id TEXT,
    status TEXT NOT NULL,
    direction TEXT,
    p_up DOUBLE PRECISION,
    p_down DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    horizon_seconds INTEGER,
    validated BOOLEAN NOT NULL DEFAULT FALSE,
    gatillazo TEXT NOT NULL,
    data_source TEXT,
    quote_timestamp TIMESTAMPTZ,
    evaluation JSONB,
    features JSONB,
    cohort_tag TEXT
);

CREATE INDEX IF NOT EXISTS idx_forecasts_symbol_created
ON forecasts(symbol, created_at DESC);

CREATE TABLE IF NOT EXISTS model_registry (
    id BIGSERIAL PRIMARY KEY,
    registered_at TIMESTAMPTZ NOT NULL,
    model_id TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL,
    dataset_version TEXT,
    features_version TEXT,
    validation_type TEXT,
    accuracy DOUBLE PRECISION,
    brier DOUBLE PRECISION,
    brier_skill DOUBLE PRECISION,
    calibration_status TEXT,
    regime JSONB,
    cpcv_status TEXT,
    pbo_status TEXT,
    dsr_status TEXT,
    selection_rule TEXT,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_model_registry_model_registered
ON model_registry(model_id, registered_at DESC);

CREATE TABLE IF NOT EXISTS forecast_outcomes (
    id BIGSERIAL PRIMARY KEY,
    forecast_id BIGINT NOT NULL REFERENCES forecasts(id) ON DELETE CASCADE,
    resolved_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    forecast_created_at TIMESTAMPTZ NOT NULL,
    target_horizon_seconds INTEGER NOT NULL,
    actual_elapsed_seconds DOUBLE PRECISION NOT NULL,
    forecast_direction TEXT,
    forecast_p_up DOUBLE PRECISION,
    forecast_confidence DOUBLE PRECISION,
    entry_price DOUBLE PRECISION,
    exit_price DOUBLE PRECISION,
    realized_return_bps DOUBLE PRECISION NOT NULL,
    realized_direction TEXT NOT NULL,
    prediction_correct BOOLEAN,
    binary_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    brier_loss DOUBLE PRECISION,
    probabilistic_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    probabilistic_label SMALLINT,
    probabilistic_brier_loss DOUBLE PRECISION,
    resolution_source TEXT NOT NULL,
    metadata JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_forecast_outcomes_forecast_id
ON forecast_outcomes(forecast_id);

CREATE INDEX IF NOT EXISTS idx_forecast_outcomes_symbol_resolved
ON forecast_outcomes(symbol, resolved_at DESC);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    model_id TEXT,
    lookback_days INTEGER,
    bars INTEGER,
    samples INTEGER,
    test_count INTEGER,
    accuracy DOUBLE PRECISION,
    brier DOUBLE PRECISION,
    baseline_brier DOUBLE PRECISION,
    validated BOOLEAN NOT NULL DEFAULT FALSE,
    metrics JSONB
);

CREATE TABLE IF NOT EXISTS research_runs (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    model_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result JSONB,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_research_runs_symbol_created
ON research_runs(symbol, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_research_runs_status_created
ON research_runs(status, created_at DESC);
"""


def database_url() -> str | None:
    return os.getenv("DATABASE_URL")


def database_connect_kwargs() -> dict[str, Any]:
    return {"sslmode": "require", "connect_timeout": 10}


@contextmanager
def connection():
    url = database_url()
    if not url:
        yield None
        return
    with psycopg.connect(url, **database_connect_kwargs()) as conn:
        yield conn


def init_db() -> bool:
    if not database_url():
        return False
    with connection() as conn:
        if conn is None:
            return False
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            cur.execute("ALTER TABLE forecasts ADD COLUMN IF NOT EXISTS cohort_tag TEXT")
            cur.execute("ALTER TABLE forecast_outcomes ADD COLUMN IF NOT EXISTS probabilistic_eligible BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE forecast_outcomes ADD COLUMN IF NOT EXISTS probabilistic_label SMALLINT")
            cur.execute("ALTER TABLE forecast_outcomes ADD COLUMN IF NOT EXISTS probabilistic_brier_loss DOUBLE PRECISION")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_forecasts_cohort_tag_created ON forecasts(cohort_tag, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_probabilistic ON forecast_outcomes(probabilistic_eligible, resolved_at)")
        conn.commit()
    return True


def persistence_summary() -> dict[str, Any]:
    if not database_url():
        return {
            "configured": False,
            "ready": False,
            "forecast_count": None,
            "backtest_count": None,
            "last_forecast_id": None,
            "last_backtest_id": None,
            "last_forecast_created_at": None,
            "last_backtest_created_at": None,
        }

    sql = """
    SELECT
        (SELECT COUNT(*) FROM forecasts) AS forecast_count,
        (SELECT COUNT(*) FROM backtest_runs) AS backtest_count,
        (SELECT MAX(id) FROM forecasts) AS last_forecast_id,
        (SELECT MAX(id) FROM backtest_runs) AS last_backtest_id,
        (SELECT MAX(created_at) FROM forecasts) AS last_forecast_created_at,
        (SELECT MAX(created_at) FROM backtest_runs) AS last_backtest_created_at
    """
    try:
        with connection() as conn:
            if conn is None:
                return {
                    "configured": True,
                    "ready": False,
                    "forecast_count": None,
                    "backtest_count": None,
                    "last_forecast_id": None,
                    "last_backtest_id": None,
                    "last_forecast_created_at": None,
                    "last_backtest_created_at": None,
                }
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
        return {
            "configured": True,
            "ready": True,
            "forecast_count": int(row[0]),
            "backtest_count": int(row[1]),
            "last_forecast_id": int(row[2]) if row[2] is not None else None,
            "last_backtest_id": int(row[3]) if row[3] is not None else None,
            "last_forecast_created_at": row[4].isoformat() if row[4] is not None else None,
            "last_backtest_created_at": row[5].isoformat() if row[5] is not None else None,
        }
    except Exception as exc:
        return {
            "configured": True,
            "ready": False,
            "forecast_count": None,
            "backtest_count": None,
            "last_forecast_id": None,
            "last_backtest_id": None,
            "last_forecast_created_at": None,
            "last_backtest_created_at": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def record_forecast(payload: dict[str, Any]) -> int | None:
    if not database_url():
        return None
    sql = """
    INSERT INTO forecasts (
        created_at, symbol, last, bid, ask, spread_bps, microprice,
        model_id, status, direction, p_up, p_down, confidence,
        horizon_seconds, validated, gatillazo, data_source,
        quote_timestamp, evaluation, features, cohort_tag
    ) VALUES (
        %(created_at)s, %(symbol)s, %(last)s, %(bid)s, %(ask)s,
        %(spread_bps)s, %(microprice)s, %(model_id)s, %(status)s,
        %(direction)s, %(p_up)s, %(p_down)s, %(confidence)s,
        %(horizon_seconds)s, %(validated)s, %(gatillazo)s,
        %(data_source)s, %(quote_timestamp)s, %(evaluation)s::jsonb,
        %(features)s::jsonb, %(cohort_tag)s
    ) RETURNING id
    """
    row = dict(
        created_at=payload.get("created_at") or datetime.now(timezone.utc),
        symbol=payload.get("symbol"),
        last=payload.get("last"),
        bid=payload.get("bid"),
        ask=payload.get("ask"),
        spread_bps=payload.get("spread_bps"),
        microprice=payload.get("microprice"),
        model_id=(payload.get("forecast") or {}).get("model_id"),
        status=payload.get("forecast_status") or "NO_FORECAST",
        direction=(payload.get("forecast") or {}).get("direction"),
        p_up=(payload.get("forecast") or {}).get("raw_probability_up"),
        p_down=(payload.get("forecast") or {}).get("raw_probability_down"),
        confidence=(payload.get("forecast") or {}).get("confidence_raw"),
        horizon_seconds=(payload.get("forecast") or {}).get("horizon_seconds"),
        validated=bool((payload.get("forecast") or {}).get("validated")),
        gatillazo=payload.get("gatillazo") or "BLOCKED_VALIDATION",
        data_source=payload.get("data_source"),
        quote_timestamp=payload.get("quote_timestamp"),
        evaluation=json.dumps(payload.get("evaluation") or {}),
        features=json.dumps((payload.get("forecast") or {}).get("components") or {}),
        cohort_tag=payload.get("cohort_tag"),
    )
    with connection() as conn:
        if conn is None:
            return None
        with conn.cursor() as cur:
            cur.execute(sql, row)
            result = cur.fetchone()
        conn.commit()
        return int(result[0])


def record_backtest(symbol: str, result: dict[str, Any]) -> int | None:
    if not database_url():
        return None
    ev = result.get("evaluation") or {}
    sql = """
    INSERT INTO backtest_runs (
        created_at, symbol, model_id, lookback_days, bars, samples,
        test_count, accuracy, brier, baseline_brier, validated, metrics
    ) VALUES (
        %(created_at)s, %(symbol)s, %(model_id)s, %(lookback_days)s,
        %(bars)s, %(samples)s, %(test_count)s, %(accuracy)s,
        %(brier)s, %(baseline_brier)s, %(validated)s, %(metrics)s::jsonb
    ) RETURNING id
    """
    row = {
        "created_at": datetime.now(timezone.utc),
        "symbol": symbol,
        "model_id": result.get("model_id"),
        "lookback_days": result.get("lookback_days"),
        "bars": result.get("bars"),
        "samples": ev.get("sample_count"),
        "test_count": ev.get("test_count"),
        "accuracy": ev.get("accuracy"),
        "brier": ev.get("brier"),
        "baseline_brier": ev.get("baseline_brier"),
        "validated": bool(ev.get("validated")),
        "metrics": json.dumps(ev),
    }
    with connection() as conn:
        if conn is None:
            return None
        with conn.cursor() as cur:
            cur.execute(sql, row)
            result_row = cur.fetchone()
        conn.commit()
        return int(result_row[0])


def active_research_run(
    symbol: str,
    model_id: str = "multihorizon-meta-research-v1",
    *,
    stale_after_minutes: int = 30,
) -> dict[str, Any] | None:
    if not database_url():
        return None

    sql = """
    SELECT id
    FROM research_runs
    WHERE symbol = %(symbol)s
      AND model_id = %(model_id)s
      AND status IN ('QUEUED', 'RUNNING')
      AND updated_at >= NOW() - (%(stale_after_minutes)s * INTERVAL '1 minute')
    ORDER BY created_at DESC
    LIMIT 1
    """
    try:
        with connection() as conn:
            if conn is None:
                return None
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    {
                        "symbol": symbol,
                        "model_id": model_id,
                        "stale_after_minutes": int(stale_after_minutes),
                    },
                )
                row = cur.fetchone()
        return get_research_run(str(row[0])) if row else None
    except Exception:
        return None


def recover_incomplete_research_runs() -> int:
    """Mark active research jobs from a previous web-process lifetime as failed."""
    if not database_url():
        return 0

    sql = """
    UPDATE research_runs
    SET
        updated_at = NOW(),
        status = 'ERROR',
        finished_at = NOW(),
        error = 'INCOMPLETE_RESEARCH_RUN_RECOVERED_AFTER_SERVICE_RESTART'
    WHERE status IN ('QUEUED', 'RUNNING')
    """
    try:
        with connection() as conn:
            if conn is None:
                return 0
            with conn.cursor() as cur:
                cur.execute(sql)
                updated = cur.rowcount
            conn.commit()
        return int(updated)
    except Exception:
        return 0


def create_research_run(symbol: str, model_id: str = "multihorizon-meta-research-v1") -> str | None:
    if not database_url():
        return None

    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    sql = """
    INSERT INTO research_runs (
        id, created_at, updated_at, symbol, model_id, status
    ) VALUES (
        %(id)s, %(created_at)s, %(updated_at)s, %(symbol)s, %(model_id)s, 'QUEUED'
    )
    """
    with connection() as conn:
        if conn is None:
            return None
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "id": run_id,
                    "created_at": now,
                    "updated_at": now,
                    "symbol": symbol,
                    "model_id": model_id,
                },
            )
        conn.commit()
    return run_id


def update_research_run(
    run_id: str,
    *,
    status: str,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> bool:
    if not database_url():
        return False

    sql = """
    UPDATE research_runs
    SET
        updated_at = %(updated_at)s,
        status = %(status)s,
        result = %(result)s::jsonb,
        error = %(error)s,
        started_at = COALESCE(%(started_at)s, started_at),
        finished_at = COALESCE(%(finished_at)s, finished_at)
    WHERE id = %(id)s
    """
    with connection() as conn:
        if conn is None:
            return False
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "id": run_id,
                    "updated_at": datetime.now(timezone.utc),
                    "status": status,
                    "result": json.dumps(result) if result is not None else None,
                    "error": error,
                    "started_at": started_at,
                    "finished_at": finished_at,
                },
            )
            updated = cur.rowcount
        conn.commit()
    return bool(updated)


def get_research_run(run_id: str) -> dict[str, Any] | None:
    if not database_url():
        return None

    sql = """
    SELECT
        id, created_at, updated_at, symbol, model_id, status,
        started_at, finished_at, result, error
    FROM research_runs
    WHERE id = %(id)s
    """
    try:
        with connection() as conn:
            if conn is None:
                return None
            with conn.cursor() as cur:
                cur.execute(sql, {"id": run_id})
                row = cur.fetchone()
        if row is None:
            return None
        return {
            "run_id": str(row[0]),
            "created_at": row[1].isoformat(),
            "updated_at": row[2].isoformat(),
            "symbol": row[3],
            "model_id": row[4],
            "status": row[5],
            "started_at": row[6].isoformat() if row[6] is not None else None,
            "finished_at": row[7].isoformat() if row[7] is not None else None,
            "result": row[8],
            "error": row[9],
        }
    except Exception:
        return None


def latest_research_run(symbol: str) -> dict[str, Any] | None:
    if not database_url():
        return None

    sql = """
    SELECT id
    FROM research_runs
    WHERE symbol = %(symbol)s
    ORDER BY created_at DESC
    LIMIT 1
    """
    try:
        with connection() as conn:
            if conn is None:
                return None
            with conn.cursor() as cur:
                cur.execute(sql, {"symbol": symbol})
                row = cur.fetchone()
        return get_research_run(str(row[0])) if row else None
    except Exception:
        return None


def record_model_registry(record: dict[str, Any]) -> int | None:
    if not database_url():
        return None
    sql = """
    INSERT INTO model_registry (
        registered_at, model_id, version, status, dataset_version,
        features_version, validation_type, accuracy, brier, brier_skill,
        calibration_status, regime, cpcv_status, pbo_status, dsr_status,
        selection_rule, metadata
    ) VALUES (
        %(registered_at)s, %(model_id)s, %(version)s, %(status)s,
        %(dataset_version)s, %(features_version)s, %(validation_type)s,
        %(accuracy)s, %(brier)s, %(brier_skill)s, %(calibration_status)s,
        %(regime)s::jsonb, %(cpcv_status)s, %(pbo_status)s, %(dsr_status)s,
        %(selection_rule)s, %(metadata)s::jsonb
    ) RETURNING id
    """
    row = dict(record)
    row["regime"] = json.dumps(record.get("regime") or {})
    row["metadata"] = json.dumps(record)
    with connection() as conn:
        if conn is None:
            return None
        with conn.cursor() as cur:
            cur.execute(sql, row)
            result = cur.fetchone()
        conn.commit()
        return int(result[0])


def pending_due_forecasts(limit: int = 20) -> list[dict[str, Any]]:
    if not database_url():
        return []
    limit = max(1, min(int(limit), 100))
    sql = f"""
    SELECT
        f.id,
        f.created_at,
        f.symbol,
        f.horizon_seconds,
        f.direction,
        f.p_up,
        f.confidence,
        COALESCE(
            (f.bid + f.ask) / 2.0,
            f.last
        ) AS entry_price
    FROM forecasts f
    LEFT JOIN forecast_outcomes o
      ON o.forecast_id = f.id
    WHERE o.forecast_id IS NULL
      AND f.horizon_seconds IS NOT NULL
      AND f.created_at + (f.horizon_seconds || ' seconds')::interval <= NOW()
      AND NOW() <= f.created_at
            + ((f.horizon_seconds + 60) || ' seconds')::interval
    ORDER BY f.created_at DESC
    LIMIT {limit}
    """
    try:
        with connection() as conn:
            if conn is None:
                return []
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [
            {
                "id": int(row[0]),
                "created_at": row[1],
                "symbol": row[2],
                "horizon_seconds": int(row[3]),
                "direction": row[4],
                "p_up": float(row[5]) if row[5] is not None else None,
                "confidence": float(row[6]) if row[6] is not None else None,
                "entry_price": float(row[7]) if row[7] is not None else None,
            }
            for row in rows
        ]
    except Exception:
        return []


def repair_probabilistic_outcomes() -> int:
    """Backfill the probabilistic eligibility definition for persisted outcomes."""
    if not database_url():
        return 0
    sql = """
    UPDATE forecast_outcomes
    SET
        binary_eligible = (
            forecast_p_up IS NOT NULL
            AND realized_direction IN ('UP', 'DOWN')
            AND actual_elapsed_seconds <= target_horizon_seconds + 60
        ),
        brier_loss = CASE
            WHEN forecast_p_up IS NOT NULL
             AND realized_direction IN ('UP', 'DOWN')
             AND actual_elapsed_seconds <= target_horizon_seconds + 60
            THEN (
                forecast_p_up
                - CASE WHEN realized_direction = 'UP' THEN 1.0 ELSE 0.0 END
            ) ^ 2
            ELSE NULL
        END,
        probabilistic_eligible = (
            forecast_p_up IS NOT NULL
            AND realized_return_bps <> 0
            AND actual_elapsed_seconds <= target_horizon_seconds + 60
        ),
        probabilistic_label = CASE
            WHEN realized_return_bps > 0 THEN 1
            WHEN realized_return_bps < 0 THEN 0
            ELSE NULL
        END,
        probabilistic_brier_loss = CASE
            WHEN forecast_p_up IS NOT NULL
             AND realized_return_bps <> 0
             AND actual_elapsed_seconds <= target_horizon_seconds + 60
            THEN (
                forecast_p_up
                - CASE WHEN realized_return_bps > 0 THEN 1.0 ELSE 0.0 END
            ) ^ 2
            ELSE NULL
        END,
        metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{eligibility_reason}',
            to_jsonb(
                CASE
                    WHEN forecast_p_up IS NULL THEN 'MISSING_P_UP'
                    WHEN realized_direction NOT IN ('UP', 'DOWN') THEN 'REALIZED_MOVE_BELOW_THRESHOLD'
                    WHEN actual_elapsed_seconds > target_horizon_seconds + 60 THEN 'TIMING_EXPIRED'
                    ELSE 'ELIGIBLE'
                END
            )
        )
    WHERE TRUE
    """
    with connection() as conn:
        if conn is None:
            return 0
        with conn.cursor() as cur:
            cur.execute(sql)
            updated = cur.rowcount
        conn.commit()
        return int(updated)


def record_outcome(outcome: dict[str, Any]) -> int | None:
    if not database_url():
        return None
    sql = """
    INSERT INTO forecast_outcomes (
        forecast_id, resolved_at, symbol, forecast_created_at,
        target_horizon_seconds, actual_elapsed_seconds,
        forecast_direction, forecast_p_up, forecast_confidence,
        entry_price, exit_price, realized_return_bps,
        realized_direction, prediction_correct, binary_eligible,
        brier_loss, probabilistic_eligible, probabilistic_label,
        probabilistic_brier_loss, resolution_source, metadata
    ) VALUES (
        %(forecast_id)s, %(resolved_at)s, %(symbol)s, %(forecast_created_at)s,
        %(target_horizon_seconds)s, %(actual_elapsed_seconds)s,
        %(forecast_direction)s, %(forecast_p_up)s, %(forecast_confidence)s,
        %(entry_price)s, %(exit_price)s, %(realized_return_bps)s,
        %(realized_direction)s, %(prediction_correct)s, %(binary_eligible)s,
        %(brier_loss)s, %(probabilistic_eligible)s, %(probabilistic_label)s,
        %(probabilistic_brier_loss)s, %(resolution_source)s, %(metadata)s::jsonb
    )
    ON CONFLICT (forecast_id) DO NOTHING
    RETURNING id
    """
    row = dict(outcome)
    row.setdefault("probabilistic_eligible", False)
    row.setdefault("probabilistic_label", None)
    row.setdefault("probabilistic_brier_loss", None)
    row["metadata"] = json.dumps(outcome.get("metadata") or {})
    with connection() as conn:
        if conn is None:
            return None
        with conn.cursor() as cur:
            cur.execute(sql, row)
            result = cur.fetchone()
            if result is None:
                cur.execute(
                    "SELECT id FROM forecast_outcomes WHERE forecast_id = %(forecast_id)s",
                    {"forecast_id": outcome["forecast_id"]},
                )
                result = cur.fetchone()
        conn.commit()
        return int(result[0]) if result else None


def get_outcome(forecast_id: int) -> dict[str, Any] | None:
    if not database_url():
        return None
    sql = """
    SELECT
        id, forecast_id, resolved_at, symbol, forecast_created_at,
        target_horizon_seconds, actual_elapsed_seconds,
        forecast_direction, forecast_p_up, forecast_confidence,
        entry_price, exit_price, realized_return_bps,
        realized_direction, prediction_correct, binary_eligible,
        brier_loss, probabilistic_eligible, probabilistic_label,
        probabilistic_brier_loss, resolution_source, metadata
    FROM forecast_outcomes
    WHERE forecast_id = %(forecast_id)s
    """
    try:
        with connection() as conn:
            if conn is None:
                return None
            with conn.cursor() as cur:
                cur.execute(sql, {"forecast_id": int(forecast_id)})
                row = cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "forecast_id": int(row[1]),
            "resolved_at": row[2].isoformat(),
            "symbol": row[3],
            "forecast_created_at": row[4].isoformat(),
            "target_horizon_seconds": int(row[5]),
            "actual_elapsed_seconds": float(row[6]),
            "forecast_direction": row[7],
            "forecast_probability_up": float(row[8]) if row[8] is not None else None,
            "forecast_confidence": float(row[9]) if row[9] is not None else None,
            "entry_price": float(row[10]) if row[10] is not None else None,
            "exit_price": float(row[11]) if row[11] is not None else None,
            "realized_return_bps": float(row[12]),
            "realized_direction": row[13],
            "prediction_correct": row[14],
            "binary_eligible": row[15],
            "brier_loss": float(row[16]) if row[16] is not None else None,
            "probabilistic_eligible": bool(row[17]),
            "probabilistic_label": int(row[18]) if row[18] is not None else None,
            "probabilistic_brier_loss": float(row[19]) if row[19] is not None else None,
            "resolution_source": row[20],
            "metadata": row[21],
        }
    except Exception:
        return None


def eligible_outcomes(limit: int = 100) -> list[dict[str, Any]]:
    if not database_url():
        return []
    limit = max(1, min(int(limit), 100))
    sql = f"""
    SELECT
        id, forecast_id, resolved_at, symbol, forecast_created_at,
        target_horizon_seconds, actual_elapsed_seconds,
        forecast_direction, forecast_p_up, forecast_confidence,
        entry_price, exit_price, realized_return_bps,
        realized_direction, prediction_correct, binary_eligible,
        brier_loss, probabilistic_eligible, probabilistic_label,
        probabilistic_brier_loss, resolution_source, metadata
    FROM forecast_outcomes
    WHERE binary_eligible = TRUE
    ORDER BY resolved_at ASC, id ASC
    LIMIT {limit}
    """
    with connection() as conn:
        if conn is None:
            return []
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [
        {
            "id": int(row[0]),
            "forecast_id": int(row[1]),
            "resolved_at": row[2].isoformat(),
            "symbol": row[3],
            "forecast_created_at": row[4].isoformat(),
            "target_horizon_seconds": int(row[5]),
            "actual_elapsed_seconds": float(row[6]),
            "forecast_direction": row[7],
            "forecast_p_up": float(row[8]) if row[8] is not None else None,
            "forecast_confidence": float(row[9]) if row[9] is not None else None,
            "entry_price": float(row[10]) if row[10] is not None else None,
            "exit_price": float(row[11]) if row[11] is not None else None,
            "realized_return_bps": float(row[12]),
            "realized_direction": row[13],
            "prediction_correct": row[14],
            "binary_eligible": bool(row[15]),
            "brier_loss": float(row[16]) if row[16] is not None else None,
            "probabilistic_eligible": bool(row[17]),
            "probabilistic_label": int(row[18]) if row[18] is not None else None,
            "probabilistic_brier_loss": float(row[19]) if row[19] is not None else None,
            "resolution_source": row[20],
            "metadata": row[21],
        }
        for row in rows
    ]


def outcome_summary() -> dict[str, Any]:
    if not database_url():
        return {
            "configured": False,
            "ready": False,
            "outcome_count": None,
            "binary_eligible_count": None,
            "correct_count": None,
            "mean_realized_return_bps": None,
            "last_resolved_at": None,
            "eligibility_breakdown": None,
        }
    sql = """
    SELECT
        COUNT(*) AS outcome_count,
        COUNT(*) FILTER (WHERE binary_eligible) AS binary_eligible_count,
        COUNT(*) FILTER (WHERE prediction_correct IS TRUE) AS correct_count,
        AVG(realized_return_bps) AS mean_realized_return_bps,
        MAX(resolved_at) AS last_resolved_at,
        COUNT(*) FILTER (
            WHERE forecast_p_up IS NULL
        ) AS missing_p_up_count,
        COUNT(*) FILTER (
            WHERE forecast_p_up IS NOT NULL
              AND (forecast_direction IS NULL OR forecast_direction NOT IN ('UP', 'DOWN'))
        ) AS non_binary_forecast_direction_count,
        COUNT(*) FILTER (
            WHERE forecast_p_up IS NOT NULL
              AND forecast_direction IN ('UP', 'DOWN')
              AND (realized_direction IS NULL OR realized_direction NOT IN ('UP', 'DOWN'))
        ) AS realized_move_below_threshold_count,
        COUNT(*) FILTER (
            WHERE forecast_p_up IS NOT NULL
              AND forecast_direction IN ('UP', 'DOWN')
              AND realized_direction IN ('UP', 'DOWN')
              AND actual_elapsed_seconds > target_horizon_seconds + 60
        ) AS timing_expired_count,
        COUNT(*) FILTER (
            WHERE binary_eligible
              AND forecast_p_up IS NOT NULL
              AND forecast_direction IN ('UP', 'DOWN')
              AND realized_direction IN ('UP', 'DOWN')
              AND actual_elapsed_seconds <= target_horizon_seconds + 60
        ) AS eligible_consistent_count,
        COUNT(*) FILTER (
            WHERE forecast_p_up IS NOT NULL
              AND probabilistic_eligible
        ) AS probabilistic_eligible_count,
        COUNT(*) FILTER (
            WHERE probabilistic_eligible AND probabilistic_label = 1
        ) AS probabilistic_up_count,
        COUNT(*) FILTER (
            WHERE probabilistic_eligible AND probabilistic_label = 0
        ) AS probabilistic_down_count,
        COUNT(*) FILTER (
            WHERE probabilistic_eligible AND forecast_direction = 'NEUTRAL'
        ) AS neutral_but_probabilistic_eligible_count,
        AVG(probabilistic_brier_loss) FILTER (
            WHERE probabilistic_eligible
        ) AS mean_probabilistic_brier
    FROM forecast_outcomes
    """
    try:
        with connection() as conn:
            if conn is None:
                raise RuntimeError("database unavailable")
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
        return {
            "configured": True,
            "ready": True,
            "outcome_count": int(row[0]),
            "binary_eligible_count": int(row[1]),
            "correct_count": int(row[2]),
            "mean_realized_return_bps": round(float(row[3]), 4) if row[3] is not None else None,
            "last_resolved_at": row[4].isoformat() if row[4] is not None else None,
            "eligibility_breakdown": {
                "MISSING_P_UP": int(row[5]),
                "NON_BINARY_FORECAST_DIRECTION": int(row[6]),
                "REALIZED_MOVE_BELOW_THRESHOLD": int(row[7]),
                "TIMING_EXPIRED": int(row[8]),
                "ELIGIBLE_CONSISTENT": int(row[9]),
                "PROBABILISTIC_ELIGIBLE": int(row[10]),
                "PROBABILISTIC_UP": int(row[11]),
                "PROBABILISTIC_DOWN": int(row[12]),
                "NEUTRAL_BUT_PROBABILISTIC_ELIGIBLE": int(row[13]),
                "MEAN_PROBABILISTIC_BRIER": round(float(row[14]), 6) if row[14] is not None else None,
            },
            "probabilistic_eligibility_rule": "p_up_present + realized_direction in {UP,DOWN} + timing_within_60s",
        }
    except Exception as exc:
        return {
            "configured": True,
            "ready": False,
            "outcome_count": None,
            "binary_eligible_count": None,
            "correct_count": None,
            "mean_realized_return_bps": None,
            "last_resolved_at": None,
            "eligibility_breakdown": None,
            "error": f"{type(exc).__name__}: {exc}",
        }

