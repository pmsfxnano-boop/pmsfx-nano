from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
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
_LAST_DAILY_HISTORY_REFRESH_AT = 0.0

def _daily_history_due(*, force: bool = False) -> bool:
    global _LAST_DAILY_HISTORY_REFRESH_AT
    now = time.monotonic()
    if force or (now - _LAST_DAILY_HISTORY_REFRESH_AT) >= _DAILY_HISTORY_REFRESH_SECONDS:
        _LAST_DAILY_HISTORY_REFRESH_AT = now
        return True
    return False

def run_batch(*, force_daily_history: bool = False):
    store=Store(); store.init()
    macro_funcs=[argentina_datos_fx,argentina_datos_risk,bcra_fx,byma_status,bcra_macro_cycle]
    results=[]
    daily_history_refreshed = _daily_history_due(force=force_daily_history)
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
            "results":[{"source":r.source,"rows":len(r.rows),"error":r.error,"latency_ms":round(r.latency_ms or 0,2)} for r in results],
            "canonical_daily": canonical_results,
            "drift":drift_results}
