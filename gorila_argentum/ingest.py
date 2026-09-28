from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from datetime import datetime, timezone
from .config import settings
from .sources import argentina_datos_fx,argentina_datos_risk,bcra_fx,twelve_data_daily,byma_status,byma_historical_daily
from .bcra_macro import bcra_macro_cycle
from .storage import Store
from .drift import rolling_drift
from .canonical_data import reconcile_all, canonical_daily_series

_DAILY_HISTORY_REFRESH_SECONDS = max(
    900,
    int(os.getenv("GORILA_DAILY_HISTORY_REFRESH_SECONDS", "21600")),
)

def _daily_history_due(store: Store, *, force: bool = False) -> bool:
    """Use durable source-health timestamps, not process-local state, for refresh scheduling."""
    if force:
        return True
    expected_sources = {f"BYMADATA/{symbol}/historical" for symbol in settings.core_symbols}
    conn = store.connect()
    try:
        if store.pg:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT source,last_success_at
                       FROM source_health
                       WHERE source LIKE 'BYMADATA/%/historical'"""
                )
                rows = cur.fetchall()
        else:
            rows = conn.execute(
                """SELECT source,last_success_at
                   FROM source_health
                   WHERE source LIKE 'BYMADATA/%/historical'"""
            ).fetchall()
    finally:
        conn.close()
        store.conn = None

    by_source = {str(source): last_success for source, last_success in rows}
    if any(source not in by_source or not by_source[source] for source in expected_sources):
        return True

    now = datetime.now(timezone.utc)
    for source in expected_sources:
        try:
            stamp = by_source[source]
            dt = stamp if isinstance(stamp, datetime) else datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_seconds = max(0.0, (now - dt.astimezone(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return True
        if age_seconds >= _DAILY_HISTORY_REFRESH_SECONDS:
            return True
    return False

def run_batch(*, force_daily_history: bool = False, include_macro: bool = True):
    store=Store(); store.init()
    macro_funcs=[argentina_datos_fx,argentina_datos_risk,bcra_fx,byma_status,bcra_macro_cycle] if include_macro else []
    results=[]
    daily_history_refreshed = _daily_history_due(store, force=force_daily_history)
    if macro_funcs:
        with ThreadPoolExecutor(max_workers=min(settings.batch_workers,len(macro_funcs))) as ex:
            futures=[ex.submit(fn) for fn in macro_funcs]
            for fut in as_completed(futures):
                results.append(fut.result())

    if daily_history_refreshed:
        # Public BYMADATA history is deliberately throttled. Repeatedly asking
        # for the same ~1y series every 5 minutes is unnecessary and increases
        # latency/rate-limit risk without adding information.
        for idx, symbol in enumerate(settings.core_symbols):
            if idx:
                time.sleep(1.05)
            results.append(byma_historical_daily(symbol))

        if os.getenv("GORILA_RAVA_PUBLIC_ENABLED", "true").strip().lower() in {"1", "true", "yes"}:
            for symbol in settings.core_symbols:
                time.sleep(0.75)
                results.append(rava_public_historical_daily(symbol))

        if settings.twelve_data_api_key:
            for symbol in settings.symbols:
                results.append(twelve_data_daily(symbol))
    total=0
    for r in results:
        if r.rows:
            total += store.insert_observations(r.rows)
            store.upsert_health(r.source,"HEALTHY",rows=len(r.rows),latency_ms=r.latency_ms,success=True)
        else:
            store.upsert_health(r.source,"DEGRADED",last_error=r.error,rows=0,latency_ms=r.latency_ms,success=False)

    canonical_results = []
    drift_results = []
    if daily_history_refreshed:
        # Raw vendor rows are never consumed directly by the model. Rebuild the
        # deterministic daily canonical layer only when daily history changed.
        canonical_results = reconcile_all(
            store,
            settings.core_symbols,
            field="close",
            limit_sessions=2500,
        )

    if daily_history_refreshed:
        for symbol in settings.core_symbols:
            series = canonical_daily_series(store, symbol, "close", limit=180)
            close_values = [value for _, value in series]
            result = rolling_drift(close_values, current_size=30, reference_size=90)
            store.save_drift(
                symbol,
                "close",
                result,
                metadata={"trigger": "ingest", "rows_inserted": total},
            )

            returns = [
                (close_values[i] / close_values[i - 1]) - 1.0
                for i in range(1, len(close_values))
                if close_values[i - 1] > 0 and close_values[i] > 0
            ]
            return_result = rolling_drift(returns, current_size=30, reference_size=90)
            store.save_drift(
                symbol,
                "return_1d",
                return_result,
                metadata={
                    "trigger": "ingest",
                    "rows_inserted": total,
                    "source_field": "close",
                },
            )
            drift_results.append({
                "symbol": symbol,
                "close_status": result.get("status"),
                "return_status": return_result.get("status"),
                "close_psi": result.get("psi"),
                "return_psi": return_result.get("psi"),
                "close_ks": result.get("ks"),
                "return_ks": return_result.get("ks"),
            })

    return {"sources":len(results),"rows_inserted":total,
            "daily_history_refreshed": daily_history_refreshed,
            "daily_history_refresh_interval_seconds": _DAILY_HISTORY_REFRESH_SECONDS,
            "macro_ingestion_included": bool(include_macro),
            "rava_public_enabled": os.getenv("GORILA_RAVA_PUBLIC_ENABLED", "true").strip().lower() in {"1", "true", "yes"},
            "results":[{"source":r.source,"rows":len(r.rows),"error":r.error,"latency_ms":round(r.latency_ms or 0,2)} for r in results],
            "canonical_daily": canonical_results,
            "drift":drift_results}
