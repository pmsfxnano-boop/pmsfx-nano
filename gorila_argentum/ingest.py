from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import settings
from .sources import argentina_datos_fx,argentina_datos_risk,bcra_fx,twelve_data_daily,byma_status,yahoo_chart_daily\nfrom .bcra_macro import bcra_macro_cycle
from .storage import Store
from .drift import rolling_drift

def run_batch():
    store=Store(); store.init()
    # Macro sources are independent and can run concurrently. Yahoo historical
    # pulls are deliberately serialized because the provider rate-limits parallel
    # chart requests and a 429 here starves the cross-sectional daily panel.
    macro_funcs=[argentina_datos_fx,argentina_datos_risk,bcra_fx,byma_status,bcra_macro_cycle]
    results=[]
    with ThreadPoolExecutor(max_workers=min(settings.batch_workers,len(macro_funcs))) as ex:
        futures=[ex.submit(fn) for fn in macro_funcs]
        for fut in as_completed(futures):
            results.append(fut.result())

    for symbol in settings.core_symbols:
        results.append(yahoo_chart_daily(symbol))

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
    drift_results=[]
    for symbol in settings.core_symbols:
        series = store.recent_series(symbol, "close", limit=180)
        close_values = [value for _, value in series]
        result = rolling_drift(close_values, current_size=30, reference_size=90)
        store.save_drift(symbol, "close", result, metadata={"trigger": "ingest", "rows_inserted": total})

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
            metadata={"trigger": "ingest", "rows_inserted": total, "source_field": "close"},
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
            "results":[{"source":r.source,"rows":len(r.rows),"error":r.error,"latency_ms":round(r.latency_ms or 0,2)} for r in results],
            "drift":drift_results}
