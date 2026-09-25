from __future__ import annotations
import time, httpx
from datetime import datetime, timezone
from .config import settings

def now(): return datetime.now(timezone.utc)
def iso(dt): return dt.astimezone(timezone.utc).isoformat()

class SourceResult:
    def __init__(self, source, rows=None, error=None, latency_ms=None):
        self.source=source; self.rows=rows or []; self.error=error; self.latency_ms=latency_ms

def _client():
    return httpx.Client(timeout=settings.http_timeout_s, headers={"User-Agent":"Gorila-Argentum/0.1"})

def argentina_datos_fx():
    source="ArgentinaDatos/FX"; t0=time.perf_counter(); received=now()
    try:
        with _client() as c:
            data=c.get(settings.argentina_datos_fx_url); data.raise_for_status(); payload=data.json()
        rows=[]
        for x in payload if isinstance(payload,list) else []:
            casa=str(x.get("casa") or "").lower()
            stamp=x.get("fecha") or received.isoformat()
            for field,key in [("buy","compra"),("sell","venta")]:
                val=x.get(key)
                if val is not None:
                    rows.append({"symbol":f"USD_{casa.upper()}","field":field,"value":float(val),"event_time":str(stamp),"received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000,"metadata":{"casa":casa}})
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)

def argentina_datos_risk():
    source="ArgentinaDatos/EMBI+"; t0=time.perf_counter(); received=now()
    try:
        with _client() as c:
            data=c.get(settings.argentina_datos_risk_url); data.raise_for_status(); payload=data.json()
        rows=[]
        for x in payload[-250:] if isinstance(payload,list) else []:
            v=x.get("valor"); d=x.get("fecha")
            if v is not None and d:
                rows.append({"symbol":"EMBI_ARG","field":"embi_bps","value":float(v),"event_time":f"{d}T23:59:59+00:00","received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000})
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)

def bcra_fx():
    source="BCRA/FX"; t0=time.perf_counter(); received=now()
    try:
        with _client() as c:
            data=c.get(settings.bcra_url); data.raise_for_status(); payload=data.json()
        rows=[]
        results=payload.get("results") if isinstance(payload,dict) else []
        for item in results or []:
            stamp=item.get("fecha") or item.get("Fecha") or received.date().isoformat()
            detalles=item.get("detalle") or item.get("Detalle") or []
            for detail in detalles:
                value=detail.get("tipoCotizacion") or detail.get("tipoCambio") or detail.get("valor")
                if value is not None:
                    try:
                        rows.append({"symbol":"USD_BCRA","field":"reference","value":float(value),"event_time":str(stamp),"received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000})
                    except (TypeError,ValueError):
                        pass
        if not rows: raise RuntimeError("BCRA payload parsed but no USD reference quote found")
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)

def twelve_data_daily(symbol):
    source=f"TwelveData/{symbol}"; t0=time.perf_counter(); received=now()
    if not settings.twelve_data_api_key: return SourceResult(source,error="TWELVE_DATA_API_KEY_MISSING")
    try:
        params={"symbol":symbol,"interval":settings.twelve_data_interval,"outputsize":5000,"apikey":settings.twelve_data_api_key}
        with _client() as c:
            data=c.get("https://api.twelvedata.com/time_series",params=params); data.raise_for_status(); payload=data.json()
        if payload.get("status")=="error": raise RuntimeError(payload.get("message","Twelve Data error"))
        rows=[]
        for x in payload.get("values") or []:
            if x.get("close") is None: continue
            rows.append({"symbol":symbol,"field":"close","value":float(x["close"]),"event_time":str(x["datetime"]),"received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000})
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)

def byma_status():
    if not settings.byma_url:
        return SourceResult("BYMA/MarketData",error="BYMA_MARKET_DATA_URL_NOT_CONFIGURED")
    return SourceResult("BYMA/MarketData",error="BYMA_ADAPTER_ENDPOINT_CONFIG_REQUIRED")
