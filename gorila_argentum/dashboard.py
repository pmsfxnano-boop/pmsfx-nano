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

/* Research-control visual layer */
:root{
 --accent:#55d6f4;--accent2:#4a88ff;--surface:#0d1926;--surface2:#112334;--line2:#20394d;
}
body{
 background:
 radial-gradient(circle at 78% -12%,rgba(61,153,190,.16),transparent 34%),
 radial-gradient(circle at 10% 0%,rgba(48,81,145,.10),transparent 28%),
 #071019;
 color:#edf5fa;
 font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
}
header{
 border-bottom:1px solid var(--line2);
 background:rgba(7,16,25,.92);
 box-shadow:0 8px 30px rgba(0,0,0,.22);
}
main{padding:20px;max-width:1680px}
section{
 background:linear-gradient(145deg,rgba(16,31,46,.97),rgba(9,20,31,.97));
 border:1px solid var(--line2);
 border-radius:14px;
 box-shadow:0 16px 44px rgba(0,0,0,.22);
}
h2{color:#8fa5b7}
.metric{background:#0a1622;border-color:#20374a}
.value{letter-spacing:-.02em}
.heroBand{
 grid-column:1/-1;padding:18px;border-radius:14px;border:1px solid var(--line2);
 background:linear-gradient(135deg,rgba(15,35,51,.96),rgba(8,20,30,.94));
 margin-bottom:0;
}
.heroBand .eyebrow{font-size:10px;letter-spacing:.18em;color:var(--accent);font-weight:800;text-transform:uppercase}
.heroBand h1{margin:6px 0 4px;font-size:28px;letter-spacing:-.025em}
.heroBand p{margin:0;color:#8094a6;max-width:980px;line-height:1.55;font-size:12px}
.heroMeta{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}
.heroChip{padding:7px 10px;border:1px solid #264157;border-radius:999px;background:#0b1723;color:#bdd0dc;font-size:9px;font-weight:800;letter-spacing:.07em}
.heroChip.live{color:#a9f5d3;border-color:#265b44}
.heroChip.blocked{color:#ffb5bf;border-color:#65323b}
.archRibbon{display:grid;grid-template-columns:repeat(13,minmax(82px,1fr));gap:6px;overflow-x:auto;padding:12px 0 2px}
.archNode{position:relative;min-height:48px;padding:8px 9px;border:1px solid #203a4f;border-radius:9px;background:#0a1622}
.archNode b{display:block;font-size:9px;margin-top:4px}
.archNode small{color:#657c8f;font-size:7px;letter-spacing:.1em}
.archNode:after{content:"→";position:absolute;right:-7px;top:50%;transform:translateY(-50%);color:#4d6679}
.archNode:last-child:after{display:none}
.archNode.guard{border-color:#604a26;background:#1b160d}
.riskStrip{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin:12px 0}
.riskBox{padding:11px;border:1px solid #20374a;border-radius:10px;background:#0a1622}
.riskBox .label{font-size:9px;color:#70869a;text-transform:uppercase;letter-spacing:.09em}
.riskBox b{display:block;margin-top:5px;font-size:14px}
.greenText{color:#50dfa0!important}.redText{color:#ff6f80!important}.amberText{color:#e5bc60!important}.cyanText{color:#5ad8f7!important}
@media(max-width:900px){.riskStrip{grid-template-columns:1fr 1fr}.archRibbon{grid-template-columns:repeat(13,90px)}}
@media(max-width:600px){main{padding:12px}.heroBand h1{font-size:23px}.riskStrip{grid-template-columns:1fr}}
</style></head>
<body>
<header><div class="brand">GORILA ARGENTUM</div><div class="state"><span class="dot"></span><span id="status">ENGINE ONLINE</span></div></header>
<main>

<section class="heroBand">
  <div class="eyebrow">Research Control Surface · Gorila Argentum</div>
  <h1>Observabilidad de extremo a extremo</h1>
  <p>La interfaz representa la arquitectura operativa sin añadir autoridad al sistema: observa datos, estado de mercado, acoplamiento, features, régimen, predicción/timing, Shadow, Outcome, Drift, Control Room, Promotion Gate, Continuous Learning, recalibración y audit.</p>
  <div class="heroMeta">
    <span class="heroChip live">● LIVE RUNTIME</span>
    <span class="heroChip">MODE · RESEARCH</span>
    <span class="heroChip">TRADING · DISABLED</span>
    <span class="heroChip blocked">PROMOTION · GATED</span>
  </div>
  <div class="riskStrip">
    <div class="riskBox"><div class="label">Persistence</div><b class="greenText">Postgres required</b></div>
    <div class="riskBox"><div class="label">Shadow</div><b class="cyanText">Point-in-time</b></div>
    <div class="riskBox"><div class="label">Learning</div><b class="amberText">Candidate cycle</b></div>
    <div class="riskBox"><div class="label">Promotion</div><b class="redText">Evidence gate</b></div>
  </div>
  <div class="archRibbon" aria-label="Architecture pipeline">
    <div class="archNode"><small>01</small><b>Data Fabric</b></div>
    <div class="archNode"><small>02</small><b>Market State</b></div>
    <div class="archNode"><small>03</small><b>Coupling</b></div>
    <div class="archNode"><small>04</small><b>Features</b></div>
    <div class="archNode"><small>05</small><b>Regime</b></div>
    <div class="archNode"><small>06</small><b>Prediction</b></div>
    <div class="archNode"><small>07</small><b>Timing</b></div>
    <div class="archNode"><small>08</small><b>Shadow</b></div>
    <div class="archNode"><small>09</small><b>Outcome</b></div>
    <div class="archNode"><small>10</small><b>Drift</b></div>
    <div class="archNode guard"><small>11</small><b>Promotion Gate</b></div>
    <div class="archNode"><small>12</small><b>Learning</b></div>
    <div class="archNode"><small>13</small><b>Audit</b></div>
  </div>
</section>
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
<section class="wide"><h2>CONTROL ROOM — BATCH 13</h2><div class="grid" id="control"></div><pre id="control_detail">cargando…</pre></section>
<section class="wide"><h2>SHADOW LEDGER — BATCH 14</h2><div class="grid" id="shadow"></div><pre id="shadow_detail">cargando…</pre></section>
<section class="wide"><h2>WORKFLOW</h2><pre id="workflow">INGEST → STATE → COUPLING → FEATURES → REGIME → PREDICTION → TIMING → OUTCOME</pre></section>
</main>
<script>
const $=id=>document.getElementById(id);
function metric(k,v,cls=""){return '<div class="metric"><div class="label">'+k+'</div><div class="value '+cls+'">'+(v==null?'—':v)+'</div></div>'}
async function refresh(){
 try{
  const [s,h,c,dv,ctl,ss,si,promo,learn]=await Promise.all([fetch('/api/state/live').then(r=>r.json()),fetch('/health').then(r=>r.json()),fetch('/api/coupling/current').then(r=>r.json()),fetch('/api/drift?limit=50').then(r=>r.json()),fetch('/api/control').then(r=>r.json()),fetch('/api/shadow/summary').then(r=>r.json()),fetch('/api/shadow?limit=20').then(r=>r.json()),fetch('/api/promotion').then(r=>r.json()),fetch('/api/learning?limit=1').then(r=>r.json())]);
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
  const p=ctl.promotion_gate||{};
  const rt=ctl.runtime||{};
  const mon=ctl.monitoring||{};
  $('control').innerHTML=[
    metric('RUNTIME',rt.mode||'UNKNOWN'),
    metric('STORAGE',rt.storage||'UNKNOWN'),
    metric('PROMOTION',p.status||'UNKNOWN',(p.status==='BLOCKED'?'red':(p.status==='PROMOTED'?'green':''))),
    metric('DRIFT ALERTS',(ctl.drift||{}).warnings_or_alerts?.length ?? 0,((ctl.drift||{}).warnings_or_alerts?.length||0)>0?'red':'green'),
    metric('PREDICTION DRIFT',mon.prediction_drift||'UNKNOWN'),
    metric('RECALIBRATION',mon.automatic_recalibration||'UNKNOWN'),
    metric('KILL SWITCH',mon.automatic_kill_switch||'UNKNOWN') ,metric('SHADOW LEDGER',mon.shadow_ledger||'UNKNOWN'),metric('PROMOTION GATE',promo.current_evaluation?.status||'UNKNOWN',promo.current_evaluation?.status==='BLOCKED'?'red':'green')
  ].join('');
  $('control_detail').textContent=JSON.stringify({
    promotion_gate:p,
    monitoring:mon,
    drift:{snapshots_seen:ctl.drift?.snapshots_seen,latest_series:ctl.drift?.latest_series},
    runtime:rt,
    promotion:promo.current_evaluation,
    learning:learn.items?.[0] || null,
    model_diagnostics:ctl.model_diagnostics,
    recalibration:ctl.recalibration
  },null,2);
  $('shadow').innerHTML=[
    metric('PREDICTIONS',ss.predictions),
    metric('OPEN',ss.open),
    metric('SETTLED',ss.settled),
    metric('ACCURACY',ss.accuracy==null?null:(ss.accuracy*100).toFixed(1)+'%',ss.accuracy==null?'':(ss.accuracy>=0.5?'green':'red')),
    metric('MEAN BRIER',ss.mean_brier==null?null:ss.mean_brier.toFixed(4)),
    metric('MEAN RETURN',ss.mean_return_pct==null?null:ss.mean_return_pct.toFixed(3)+'%')
  ].join('');
  $('shadow_detail').textContent=JSON.stringify((si.items||[]).map(x=>({
    id:x.id,symbol:x.symbol,model_version:x.model_version,status:x.status,
    p_up:x.probability_up,direction:x.direction,entry_price:x.entry_price,
    realized_direction:x.realized_direction,return_pct:x.return_pct,correct:x.correct,
    brier:x.brier,logloss:x.logloss,created_at:x.created_at,observed_at:x.observed_at
  })),null,2);
 }catch(e){$('status').textContent='DEGRADED';}
}
refresh();setInterval(refresh,1000);
</script></body></html>""")
