from fastapi.responses import HTMLResponse

HTML=HTMLResponse("""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GORILA ARGENTUM</title>
<style>
:root{--bg:#0b0b0b;--panel:#151515;--panel2:#1b1b1b;--text:#e8e8e8;--muted:#777;--green:#36d27b;--red:#ef5350;--amber:#d8aa3f;--line:#262626}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#171717 0,#0b0b0b 42%,#080808 100%);color:var(--text);font:14px Inter,system-ui,Arial,sans-serif}
header{display:flex;justify-content:space-between;align-items:center;padding:22px 26px;border-bottom:1px solid var(--line);position:sticky;top:0;background:rgba(11,11,11,.94);backdrop-filter:blur(12px);z-index:3}
.brand{letter-spacing:.14em;font-weight:800}.state{display:flex;gap:9px;align-items:center;color:var(--muted)}.dot{width:9px;height:9px;border-radius:999px;background:var(--green);box-shadow:0 0 14px rgba(54,210,123,.4)}
main{padding:22px;display:grid;grid-template-columns:1.4fr 1fr;gap:18px;max-width:1600px;margin:auto}
section{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:14px;padding:18px;min-height:170px;box-shadow:0 10px 40px rgba(0,0,0,.22)}
h2{margin:0 0 14px;font-size:12px;letter-spacing:.12em;color:#a7a7a7}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.metric{padding:13px;border:1px solid var(--line);border-radius:10px;background:#111}.label{color:var(--muted);font-size:11px}.value{font-size:24px;font-weight:700;margin-top:5px}.green{color:var(--green)}.red{color:var(--red)}
pre{white-space:pre-wrap;color:#bdbdbd;margin:0;font-size:12px}.wide{grid-column:1/-1}
@media(max-width:900px){main{grid-template-columns:1fr}.grid{grid-template-columns:repeat(2,1fr)}}
</style></head>
<body>
<header><div class="brand">GORILA ARGENTUM</div><div class="state"><span class="dot"></span><span id="status">ENGINE ONLINE</span></div></header>
<main>
<section><h2>ARGENTINA MARKET STATE</h2><div class="grid" id="fx"></div></section>
<section><h2>DATA FLOW</h2><pre id="sources">cargando…</pre></section>
<section><h2>TEMPORAL ENGINE</h2><div class="grid">
<div class="metric"><div class="label">SIGNAL AGE</div><div class="value" id="age">—</div></div>
<div class="metric"><div class="label">VALIDITY</div><div class="value" id="validity">—</div></div>
<div class="metric"><div class="label">REGIME</div><div class="value" id="regime">UNKNOWN</div></div>
<div class="metric"><div class="label">STATE</div><div class="value" id="risk">—</div></div>
</div></section>
<section><h2>COUPLING MATRIX</h2><pre id="coupling">WAITING FOR DEPTH…</pre></section>
<section class="wide"><h2>DRIFT MONITOR</h2><div class="grid" id="drift"></div><pre id="drift_detail">cargando…</pre></section>
<section class="wide"><h2>WORKFLOW</h2><pre id="workflow">INGEST → STATE → COUPLING → FEATURES → REGIME → PREDICTION → TIMING → OUTCOME</pre></section>
</main>
<script>
const $=id=>document.getElementById(id);
function metric(k,v,cls=""){return '<div class="metric"><div class="label">'+k+'</div><div class="value '+cls+'">'+(v==null?'—':v)+'</div></div>'}
async function refresh(){
 try{
  const [s,h,c,dv]=await Promise.all([fetch('/api/state/live').then(r=>r.json()),fetch('/health').then(r=>r.json()),fetch('/api/coupling/current').then(r=>r.json()),fetch('/api/drift?limit=50').then(r=>r.json())]);
  const fx=s.fx||{}, sp=fx.spreads||{};
  $('fx').innerHTML=[
    metric('USD OFICIAL',fx.official),metric('MEP',fx.mep),metric('CCL',fx.ccl),
    metric('BLUE',fx.blue),metric('MEP/OFFICIAL',sp.mep_official!=null?(sp.mep_official*100).toFixed(2)+'%':null),
    metric('CCL/OFFICIAL',sp.ccl_official!=null?(sp.ccl_official*100).toFixed(2)+'%':null),
    metric('CCL/MEP',sp.ccl_mep!=null?(sp.ccl_mep*100).toFixed(2)+'%':null),
    metric('EMBI',s.risk?.embi_bps)
  ].join('');
  $('sources').textContent=(h.sources||[]).map(x=>x.source+'  '+x.status+'  '+(x.latency_ms==null?'—':x.latency_ms.toFixed(1)+'ms')).join('\n')||'no source state';
  $('coupling').textContent=JSON.stringify({state:c.structural_state,mean_abs:c.mean_abs_coupling,mean_signed:c.mean_signed_coupling,edges:c.edges},null,2);
  const latest={};
  for(const row of (dv.items||[])){ const k=row.symbol+'|'+row.field; if(!latest[k]) latest[k]=row; }
  $('drift').innerHTML=Object.values(latest).map(x=>metric(x.symbol+' '+x.field,x.status||'UNKNOWN',x.status==='OK'?'green':(x.status==='ALERT'?'red':''))).join('')||metric('DRIFT','NO DATA');
  $('drift_detail').textContent=JSON.stringify(Object.values(latest).map(x=>({symbol:x.symbol,field:x.field,status:x.status,psi:x.psi,ks:x.ks,mean_shift_z:x.mean_shift_z,std_ratio:x.std_ratio,reference_n:x.reference_n,current_n:x.current_n,created_at:x.created_at})),null,2);
 }catch(e){$('status').textContent='DEGRADED';}
}
refresh();setInterval(refresh,1000);
</script></body></html>""")
