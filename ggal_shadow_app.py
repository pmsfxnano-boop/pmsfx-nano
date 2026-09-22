from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from ggal_shadow import db, run_cycle, source_health

app=FastAPI(title="PMSF-X Nano — GGAL Shadow", version="0.2.0")
HTML="""<!doctype html><html lang="es"><head><meta charset="utf-8"><meta http-equiv="refresh" content="60"><title>PMSF-X GGAL Shadow</title><style>body{font-family:system-ui;max-width:900px;margin:40px auto;padding:0 18px}section{border:1px solid #ddd;border-radius:12px;padding:18px;margin:14px 0}code{word-break:break-all}</style></head><body><h1>GGAL + Riesgo País — SHADOW</h1><p><b>Estado:</b> EXPERIMENTAL · SHADOW · Gatillazo: DESHABILITADO</p><div id="x">Cargando…</div><script>fetch('/api/shadow').then(r=>r.json()).then(x=>{document.getElementById('x').innerHTML='<section><h2>Forecast</h2><pre>'+JSON.stringify(x.forecast,null,2)+'</pre></section><section><h2>Salud independiente</h2><pre>'+JSON.stringify(x.health,null,2)+'</pre></section><section><h2>Validación</h2><pre>'+JSON.stringify(x.validation,null,2)+'</pre></section>'})</script></body></html>"""
@app.get("/",response_class=HTMLResponse)
def home(): return HTML
@app.get("/health")
def health(): return {"ok":True,"service":"ggal-shadow","mode":"SHADOW","triggers_enabled":False,"sources":source_health()}
@app.get("/api/shadow")
def shadow():
    c=db()
    latest=c.execute("SELECT * FROM ggal_shadow_forecasts ORDER BY id DESC LIMIT 1").fetchone()
    m=c.execute("SELECT event_time,close,received_time,source FROM ggal_shadow_market_events ORDER BY id DESC LIMIT 1").fetchone()
    r=c.execute("SELECT event_time,embi_bps,received_time,source FROM ggal_shadow_risk_events ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    if not latest:
        result=run_cycle()
        return {"forecast":result["model"],"health":result["capture"]["health"],"validation":result["model"],"capture":result["capture"]}
    return {"forecast":dict(latest),"health":source_health(),"validation":{"accuracy_oos":latest["accuracy_oos"],"brier_oos":latest["brier_oos"],"sample_count":latest["sample_count"],"type":"chronological_oos_holdout"},"latest_inputs":{"ggal":dict(m) if m else None,"embi":dict(r) if r else None}}
