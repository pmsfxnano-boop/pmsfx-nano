from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import settings
from .sources import argentina_datos_fx,argentina_datos_risk,bcra_fx,twelve_data_daily,byma_status,yahoo_chart_daily
from .storage import Store
from .drift import rolling_drift

def run_batch():
    store=Store(); store.init()
    funcs=[argentina_datos_fx,argentina_datos_risk,bcra_fx,byma_status]
    funcs += [lambda s=s: yahoo_chart_daily(s) for s in settings.core_symbols]
    if settings.twelve_data_api_key:
        funcs += [lambda s=s: twelve_data_daily(s) for s in settings.symbols]
    results=[]
    with ThreadPoolExecutor(max_workers=settings.batch_workers) as ex:
        futures=[ex.submit(fn) for fn in funcs]
        for fut in as_completed(futures): results.append(fut.result())
    total=0
    for r in results:
        if r.rows:
            total += store.insert_observations(r.rows)
            store.upsert_health(r.source,"HEALTHY",rows=len(r.rows),latency_ms=r.latency_ms,success=True)
        else:
            store.upsert_health(r.source,"DEGRADED",last_error=r.error,rows=0,latency_ms=r.latency_ms,success=False)
    drift_results=[]
    for symbol in settings.core_symbols:
        series=store.recent_series(symbol,"close",limit=120)
        result=rolling_drift([value for _,value in series],current_size=30,reference_size=90)
        store.save_drift(symbol,"close",result,metadata={"trigger":"ingest","rows_inserted":total})
        drift_results.append({"symbol":symbol,"status":result.get("status"),"psi":result.get("psi"),
                              "ks":result.get("ks"),"mean_shift_z":result.get("mean_shift_z")})
    return {"sources":len(results),"rows_inserted":total,
            "results":[{"source":r.source,"rows":len(r.rows),"error":r.error,"latency_ms":round(r.latency_ms or 0,2)} for r in results],
            "drift":drift_results}
