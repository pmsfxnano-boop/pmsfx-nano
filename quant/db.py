import json
import os
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
    features JSONB
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
"""


def database_url() -> str | None:
    return os.getenv("DATABASE_URL")


def database_connect_kwargs() -> dict[str, str]:
    return {"sslmode": "require"}


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
        quote_timestamp, evaluation, features
    ) VALUES (
        %(created_at)s, %(symbol)s, %(last)s, %(bid)s, %(ask)s,
        %(spread_bps)s, %(microprice)s, %(model_id)s, %(status)s,
        %(direction)s, %(p_up)s, %(p_down)s, %(confidence)s,
        %(horizon_seconds)s, %(validated)s, %(gatillazo)s,
        %(data_source)s, %(quote_timestamp)s, %(evaluation)s::jsonb,
        %(features)s::jsonb
    ) RETURNING id
    """
    row = dict(
        created_at=datetime.now(timezone.utc),
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
