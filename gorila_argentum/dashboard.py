from fastapi.responses import HTMLResponse

HTML = HTMLResponse(r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#050607">
<title>GORILA ARGENTUM · Quant Research Control</title>
<style>
:root{
  --bg:#050607;--panel:#0a0d10;--panel2:#0e1216;--line:#1c242b;--line2:#2a3640;
  --text:#f3f7fa;--muted:#7b8a95;--dim:#56646e;--cyan:#63ddff;--green:#50dfa2;
  --amber:#dfb458;--red:#ff667c;--blue:#80a8ff;
}
*{box-sizing:border-box}
html,body{margin:0;min-height:100%;background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}
body{background:
 radial-gradient(900px 420px at 72% -10%,rgba(35,92,120,.16),transparent 60%),
 linear-gradient(180deg,#050607,#07090b 65%,#050607)}
button,select{font:inherit}
.top{position:sticky;top:0;z-index:20;display:flex;align-items:center;justify-content:space-between;gap:14px;
 padding:13px 18px;border-bottom:1px solid var(--line);background:rgba(5,6,7,.94);backdrop-filter:blur(10px)}
.brand{display:flex;align-items:center;gap:10px;min-width:0}
.g{color:var(--cyan);font-size:35px;line-height:1;font-weight:950;text-shadow:0 0 24px rgba(99,221,255,.18)}
.name{font-size:15px;font-weight:900;letter-spacing:.15em}
.sub{font-size:8px;color:var(--dim);letter-spacing:.15em;text-transform:uppercase;margin-top:3px}
.right{display:flex;align-items:center;gap:7px;flex-wrap:wrap;justify-content:flex-end}
.chip{padding:6px 9px;border:1px solid var(--line2);border-radius:999px;background:#080a0c;color:#aeb9c1;font-size:8px;font-weight:900;letter-spacing:.08em}
.chip.good{border-color:#255943;color:#9beec8}.chip.bad{border-color:#642d36;color:#ffafba}.chip.cyan{border-color:#285263;color:#a1eeff}
.shell{max-width:1580px;margin:auto;padding:18px}
.hero{border:1px solid var(--line2);border-radius:15px;background:#080a0d;padding:21px 20px;box-shadow:0 16px 45px rgba(0,0,0,.22)}
.eyebrow{font-size:8px;letter-spacing:.18em;color:var(--cyan);font-weight:900;text-transform:uppercase}
h1{margin:7px 0 6px;font-size:31px;letter-spacing:-.035em}
.hero p{margin:0;color:var(--muted);max-width:980px;font-size:11px;line-height:1.6}
.rail{display:grid;grid-template-columns:repeat(15,minmax(76px,1fr));gap:5px;overflow:auto;margin-top:17px;padding-bottom:2px}
.node{border:1px solid var(--line);border-radius:8px;padding:8px 7px;background:#07090b}
.node small{display:block;color:var(--dim);font-size:7px}.node b{display:block;margin-top:4px;font-size:8px}
.node.guard{border-color:#6a5128;background:#0f0d09}
.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:13px}
select,.btn{background:#080b0e;border:1px solid var(--line2);color:#dce6eb;border-radius:8px;padding:8px 10px;font-size:9px;font-weight:850}
.btn{cursor:pointer}.btn:hover{border-color:#3b5667}
.auto{margin-left:auto;color:var(--muted);font-size:9px;display:flex;gap:7px;align-items:center}
input{accent-color:var(--cyan)}
.grid{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(340px,.75fr);gap:12px;margin-top:12px}
.card{background:#080a0d;border:1px solid var(--line2);border-radius:13px;overflow:hidden}
.head{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:13px 15px;border-bottom:1px solid var(--line)}
.title{font-size:9px;font-weight:900;letter-spacing:.1em;text-transform:uppercase}.tiny{font-size:8px;color:var(--dim)}
.body{padding:14px}
.heroQuote{display:flex;align-items:flex-end;justify-content:space-between;gap:15px}
.symbol{font-size:23px;font-weight:900}.price{font-size:40px;font-weight:900;margin-top:5px}.quoteMeta{font-size:8px;color:var(--muted);margin-top:4px}
.status{padding:5px 8px;border:1px solid var(--line2);border-radius:999px;font-size:8px;font-weight:900}
.status.good{border-color:#255943;color:#9beec8;background:#07120d}.status.bad{border-color:#642d36;color:#ffafba;background:#14090b}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:12px}
.metric{padding:11px;border:1px solid var(--line);border-radius:9px;background:#07090b;min-width:0}
.label{font-size:7px;color:var(--dim);letter-spacing:.11em;text-transform:uppercase}.val{margin-top:6px;font-size:16px;font-weight:900;overflow-wrap:anywhere}
.chart{height:150px;margin-top:10px;border:1px solid var(--line);border-radius:9px;background:#06080a;overflow:hidden;position:relative}
.chart svg{width:100%;height:100%}
.mainLayout{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
.box{border:1px solid var(--line);border-radius:10px;background:#07090b;padding:12px}
.box h3{margin:0;font-size:9px;letter-spacing:.08em;text-transform:uppercase}
.big{font-size:24px;font-weight:900;margin-top:7px}
.row{display:flex;justify-content:space-between;gap:10px;padding:8px 0;border-bottom:1px solid #151c21;font-size:9px}
.row:last-child{border:0}.muted{color:var(--muted)}
.progress{height:5px;background:#10161b;border-radius:99px;overflow:hidden;margin-top:8px}.progress i{display:block;height:100%;background:var(--cyan);border-radius:99px}
.sideGrid{display:grid;grid-template-columns:1fr;gap:12px}
.kpiGrid{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}
.kpi{border:1px solid var(--line);border-radius:9px;background:#07090b;padding:10px}.kpi b{display:block;margin-top:5px;font-size:15px}
.table{overflow:auto}table{width:100%;border-collapse:collapse;font-size:8px}th,td{padding:8px;border-bottom:1px solid #151c21;text-align:left;white-space:nowrap}th{color:var(--dim);font-size:7px;letter-spacing:.08em;text-transform:uppercase}
.pipeline{display:grid;grid-template-columns:repeat(8,1fr);gap:6px;padding:12px}
.stage{border:1px solid var(--line);border-radius:8px;background:#07090b;padding:9px}.stage b{display:block;font-size:8px}.stage small{display:block;color:var(--dim);font-size:7px;margin-top:5px;line-height:1.35}
.footer{padding:10px 2px 20px;color:#46535c;font-size:7px;letter-spacing:.07em;text-transform:uppercase;display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap}
pre{margin:0;background:#06080a;border:1px solid var(--line);border-radius:9px;padding:9px;color:#90a0aa;font:8px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;max-height:190px;overflow:auto}
@media(max-width:1100px){
  .grid{grid-template-columns:1fr}
  .rail{grid-template-columns:repeat(15,104px)}
}
@media(max-width:760px){
  :root{--mobile-pad:12px}
  .shell{padding:var(--mobile-pad)}
  .top{
    padding:10px 12px;
    align-items:flex-start;
    flex-direction:column;
    gap:9px;
  }
  .brand{width:100%}
  .g{font-size:31px}
  .name{font-size:14px;letter-spacing:.12em}
  .sub{font-size:7px;letter-spacing:.12em}
  .right{width:100%;justify-content:flex-start;overflow:auto;flex-wrap:nowrap;padding-bottom:2px}
  .chip,.top .btn{font-size:9px;padding:7px 10px;flex:0 0 auto}
  .hero{padding:16px 14px;border-radius:14px}
  .eyebrow{font-size:9px}
  h1{font-size:24px;line-height:1.12}
  .hero p{font-size:12px;line-height:1.55}
  .rail{
    margin-left:-2px;margin-right:-2px;
    grid-template-columns:repeat(15,128px);
    gap:7px;
    padding:4px 2px 7px;
    scroll-snap-type:x proximity;
  }
  .node{padding:10px 9px;scroll-snap-align:start}
  .node small{font-size:9px}
  .node b{font-size:10px}
  .toolbar{gap:8px}
  select,.btn{font-size:11px;padding:10px 12px}
  .auto{font-size:10px;margin-left:0}
  .grid,.mainLayout{grid-template-columns:1fr}
  .card{border-radius:13px}
  .head{padding:13px 14px}
  .title{font-size:10px}
  .tiny{font-size:9px}
  .body{padding:13px}
  .symbol{font-size:25px}
  .price{font-size:36px}
  .quoteMeta{font-size:9px;line-height:1.35}
  .label{font-size:8px}
  .status{font-size:9px;padding:6px 9px}
  .metrics,.kpiGrid{grid-template-columns:1fr 1fr}
  .metric{padding:12px}
  .val{font-size:17px}
  .chart{height:170px}
  .mainLayout{gap:9px}
  .box{padding:12px}
  .box h3{font-size:10px}
  .row{font-size:10px;padding:9px 0}
  .big{font-size:25px}
  .kpi{padding:11px}
  .kpi b{font-size:16px}
  .pipeline{grid-template-columns:repeat(2,1fr);padding:11px;gap:8px}
  .stage{padding:11px}
  .stage b{font-size:9px}
  .stage small{font-size:8px}
  table{font-size:9px}
  th{font-size:8px}
  th,td{padding:9px}
  pre{font-size:9px}
  .footer{font-size:8px;line-height:1.45}
}
@media(max-width:450px){
  .metrics,.kpiGrid{grid-template-columns:1fr 1fr}
  .price{font-size:34px}
  h1{font-size:22px}
}
</style>
</head>
<body>
<header class="top">
  <div class="brand"><div class="g">G</div><div><div class="name">GORILA ARGENTUM</div><div class="sub">quant research · live intelligence · research control</div></div></div>
  <div class="right">
    <span id="live" class="chip">CONNECTING</span>
    <span id="storage" class="chip">STORAGE · —</span>
    <span id="gate" class="chip bad">PROMOTION · BLOCKED</span>
    <button id="refresh" class="btn">REFRESH</button>
    <a class="btn" href="/docs">API</a>
  </div>
</header>

<main class="shell">
<section class="hero">
  <div class="eyebrow">Research control surface · production runtime</div>
  <h1>Predictive engine + Argentina macro context</h1>
  <p>El frontend consulta un snapshot de baja latencia: la cotización viva se mantiene separada del cálculo histórico/multi-horizonte, que se sirve desde una caché corta. La capa argentina se actualiza en segundo plano y se mantiene como contexto hasta demostrar valor predictivo OOS.</p>
  <div class="rail">
    <div class="node"><small>01</small><b>Data Fabric</b></div><div class="node"><small>02</small><b>Market State</b></div><div class="node"><small>03</small><b>Coupling</b></div><div class="node"><small>04</small><b>Features</b></div><div class="node"><small>05</small><b>Regime</b></div><div class="node"><small>06</small><b>Prediction</b></div><div class="node"><small>07</small><b>Timing</b></div><div class="node"><small>08</small><b>Shadow</b></div><div class="node"><small>09</small><b>Outcome</b></div><div class="node"><small>10</small><b>Drift</b></div><div class="node"><small>11</small><b>Control</b></div><div class="node guard"><small>12</small><b>Promotion Gate</b></div><div class="node"><small>13</small><b>Learning</b></div><div class="node"><small>14</small><b>Recalibration</b></div><div class="node"><small>15</small><b>Audit</b></div>
  </div>
  <div class="toolbar">
    <select id="ticker"><option>AAPL</option><option>MSFT</option><option>NVDA</option><option>TSLA</option></select>
    <button id="hardRefresh" class="btn">REFRESH ENGINE</button>
    <label class="auto"><input id="auto" type="checkbox" checked> live refresh · 2 s</label>
  </div>
</section>

<section class="grid">
  <div class="card">
    <div class="head"><div><div class="title">Global predictive engine</div><div class="tiny">microstructure + historical specialist + multi-horizon research</div></div><span id="modelState" class="status">—</span></div>
    <div class="body">
      <div class="heroQuote">
        <div><div id="sym" class="symbol">AAPL</div><div id="px" class="price">—</div><div id="qmeta" class="quoteMeta">—</div></div>
        <div style="text-align:right"><div class="label">Forecast</div><div id="direction" class="big">—</div><div id="prob" class="muted">P(UP) —</div></div>
      </div>
      <div class="metrics">
        <div class="metric"><div class="label">Bid</div><div id="bid" class="val">—</div></div>
        <div class="metric"><div class="label">Ask</div><div id="ask" class="val">—</div></div>
        <div class="metric"><div class="label">Spread</div><div id="spread" class="val">—</div></div>
        <div class="metric"><div class="label">Confidence</div><div id="confidence" class="val">—</div></div>
      </div>
      <div class="chart"><svg id="priceChart" viewBox="0 0 800 150" preserveAspectRatio="none"><path id="pricePath" d="" fill="none" stroke="currentColor" stroke-width="2.2"/></svg></div>
      <div class="mainLayout">
        <div class="box"><h3>OOS validation</h3><div class="row"><span class="muted">status</span><b id="valStatus">—</b></div><div class="row"><span class="muted">accuracy</span><b id="oosAcc">—</b></div><div class="row"><span class="muted">Brier</span><b id="brier">—</b></div><div class="row"><span class="muted">Brier skill</span><b id="brierSkill">—</b></div></div>
        <div class="box"><h3>Multi-horizon</h3><div id="horizons"><div class="row"><span class="muted">300 / 900 / 1800 s</span><b>—</b></div></div><div class="progress"><i id="horizonBar" style="width:0%"></i></div></div>
      </div>
      <div class="mainLayout">
        <div class="box"><h3>Model integrity</h3><div class="row"><span class="muted">model</span><b id="modelId">—</b></div><div class="row"><span class="muted">health</span><b id="healthState">—</b></div><div class="row"><span class="muted">gatillazo</span><b id="triggerState">—</b></div></div>
        <div class="box"><h3>Research evidence</h3><div class="row"><span class="muted">samples</span><b id="samples">—</b></div><div class="row"><span class="muted">bars</span><b id="bars">—</b></div><div class="row"><span class="muted">forecast id</span><b id="forecastId">—</b></div></div>
      </div>
    </div>
  </div>

  <div class="sideGrid">
    <div class="card">
      <div class="head"><div><div class="title">Argentina macro fabric</div><div class="tiny">BCRA + EMBI + market FX context</div></div><span id="macroState" class="status">—</span></div>
      <div class="body">
        <div class="kpiGrid">
          <div class="kpi"><div class="label">USD BCRA</div><b id="bcra">—</b></div>
          <div class="kpi"><div class="label">USD MEP</div><b id="mep">—</b></div>
          <div class="kpi"><div class="label">USD CCL</div><b id="ccl">—</b></div>
          <div class="kpi"><div class="label">USD Blue</div><b id="blue">—</b></div>
          <div class="kpi"><div class="label">EMBI</div><b id="embi">—</b></div>
          <div class="kpi"><div class="label">MEP / official</div><b id="spreadMep">—</b></div>
        </div>
        <div class="row"><span class="muted">macro loop</span><b id="macroUpdated">—</b></div>
        <div class="row"><span class="muted">sources</span><b id="sourceState">—</b></div>
      </div>
    </div>

    <div class="card">
      <div class="head"><div><div class="title">Research gate</div><div class="tiny">No automatic promotion</div></div><span class="status bad">BLOCKED</span></div>
      <div class="body">
        <div class="kpiGrid"><div class="kpi"><div class="label">Accuracy</div><b id="gateAcc">—</b></div><div class="kpi"><div class="label">Rank IC</div><b id="rankIc">—</b></div><div class="kpi"><div class="label">CPCV return</div><b id="cpcv">—</b></div></div>
        <div id="gateReasons" style="margin-top:8px"></div>
      </div>
    </div>

    <div class="card">
      <div class="head"><div><div class="title">Runtime & shadow</div><div class="tiny">durability, outcomes, continuous cycle</div></div><span id="runtimeStatus" class="status">—</span></div>
      <div class="body">
        <div class="row"><span class="muted">Postgres</span><b id="pg">—</b></div>
        <div class="row"><span class="muted">primary feed</span><b id="primaryFeed">—</b></div>
        <div class="row"><span class="muted">autonomous cycle</span><b id="autonomousState">—</b></div>
        <div class="row"><span class="muted">circuit breaker</span><b id="circuitState">—</b></div>
        <div class="row"><span class="muted">latest tick</span><b id="tick">—</b></div>
        <div class="row"><span class="muted">shadow predictions</span><b id="shadowPred">—</b></div>
        <div class="row"><span class="muted">shadow settled</span><b id="shadowSettled">—</b></div>
        <div class="row"><span class="muted">shadow accuracy</span><b id="shadowAcc">—</b></div>
      </div>
    </div>
  </div>
</section>

<section class="card" style="margin-top:12px">
  <div class="head"><div><div class="title">Operational pipeline</div><div class="tiny">ordered process with visible state, no hidden transitions</div></div><span id="pipelineState" class="status">—</span></div>
  <div class="pipeline">
    <div class="stage"><b>01 DATA</b><small>sources, latency, quality</small></div><div class="stage"><b>02 STATE</b><small>live quote + macro</small></div><div class="stage"><b>03 FEATURES</b><small>point-in-time vector</small></div><div class="stage"><b>04 REGIME</b><small>context classifier</small></div><div class="stage"><b>05 PREDICT</b><small>specialists + meta</small></div><div class="stage"><b>06 SHADOW</b><small>no real execution</small></div><div class="stage"><b>07 OUTCOME</b><small>future resolution</small></div><div class="stage"><b>08 GATE</b><small>OOS/robustness</small></div>
  </div>
</section>

<section class="grid" style="margin-top:12px">
  <div class="card"><div class="head"><div><div class="title">Source health</div><div class="tiny">recent Argentina feed observations</div></div></div><div class="body table"><table><thead><tr><th>source</th><th>status</th><th>rows</th><th>latency</th><th>last success</th></tr></thead><tbody id="sources"></tbody></table></div></div>
  <div class="card"><div class="head"><div><div class="title">Runtime telemetry</div><div class="tiny">latest audit/runtime payload</div></div></div><div class="body"><pre id="telemetry">{}</pre></div></div>
</section>

<footer class="footer">
  <span>RESEARCH · TRADING EXECUTION DISABLED · AUTOMATIC PROMOTION OFF</span>
  <span>DRIFT MONITOR · CONTROL ROOM — BATCH 13 · SHADOW LEDGER — BATCH 14</span>
</footer>
</main>

<script>
const $=id=>document.getElementById(id);
const fmt=(v,d=3)=>v===null||v===undefined||v===''?'—':Number(v).toLocaleString('en-US',{maximumFractionDigits:d});
const pct=(v,d=1)=>v===null||v===undefined?'—':(Number(v)*100).toFixed(d)+'%';
let timer=null;
async function json(url,opts={}){const r=await fetch(url,{cache:'no-store',...opts});const t=await r.text();let d;try{d=JSON.parse(t)}catch{throw new Error('Non-JSON '+url)}if(!r.ok)throw new Error(d.detail||('HTTP '+r.status));return d}
function setStatus(el,text,good=false,bad=false){el.textContent=text;el.className='status '+(good?'good':bad?'bad':'')}
function renderChart(rows){
  const pts=(rows||[]).map(x=>({t:x.time,v:Number(x.close)})).filter(x=>Number.isFinite(x.v));
  const path=$('pricePath');
  if(!pts.length){path.setAttribute('d','');return}
  const vals=pts.map(x=>x.v), min=Math.min(...vals), max=Math.max(...vals), span=(max-min)||1;
  const w=800,h=150,pad=7;
  const d=pts.map((x,i)=>{
    const xx=pad+(i/Math.max(1,pts.length-1))*(w-2*pad);
    const yy=pad+(1-(x.v-min)/span)*(h-2*pad);
    return (i?'L':'M')+xx.toFixed(2)+' '+yy.toFixed(2);
  }).join(' ');
  path.setAttribute('d',d);
}
function renderTerminal(d){
  const q=d.quote?.quote||{};
  const f=d.forecast||{};
  const fc=f.forecast||null;
  const ev=fc?f.evaluation||{}:f.evaluation||{};
  $('sym').textContent=d.symbol;
  $('px').textContent=fmt(q.last,4);
  $('qmeta').textContent=(d.quote?.source||'—')+' · '+(q.quoteTimestamp||q.timestamp||d.quote?.received_at||'—');
  $('bid').textContent=fmt(q.bidPrice,4);$('ask').textContent=fmt(q.askPrice,4);
  const bid=Number(q.bidPrice),ask=Number(q.askPrice);$('spread').textContent=bid>0&&ask>0?((ask-bid)/((ask+bid)/2)*10000).toFixed(2)+' bps':'—';
  $('direction').textContent=fc?.direction||'NEUTRAL';
  $('prob').textContent='P(UP) '+pct(fc?.raw_probability_up);
  $('confidence').textContent=pct(fc?.confidence_raw);
  renderChart(d.chart);
  setStatus($('modelState'),f.forecast_status||'NO_FORECAST',!!fc?.validated,!fc);
  $('valStatus').textContent=ev.validated?'VALIDATED':(ev.validation_reason||'EXPERIMENTAL');
  $('oosAcc').textContent=pct(ev.accuracy);$('brier').textContent=fmt(ev.brier,5);$('brierSkill').textContent=fmt(ev.brier_skill,5);
  $('modelId').textContent=fc?.model_id||'—';
  $('engineSource').textContent=f.engine_source||f.data_source||'—';
  const age=f.engine_freshness?.age_seconds;
  $('engineAge').textContent=age===undefined||age===null?'—':(Number(age)<60?Number(age).toFixed(0)+' s':(Number(age)/60).toFixed(1)+' min');
  $('healthState').textContent=f.model_health?.safe_mode?'SAFE_MODE':(f.data_health?.status||'—');
  $('triggerState').textContent=f.gatillazo||'BLOCKED';
  $('samples').textContent=f.model?.resolved_flow_samples??f.historical_bars??'—';$('bars').textContent=f.model?.historical_bars??f.historical_bars??'—';$('forecastId').textContent=f.forecast_id??'—';
  const mh=d.forecast?.multi_horizon||{};
  const hs=mh?.horizons||{};
  const parts=Object.entries(hs).map(([h,x])=>h+'s '+pct(x.latest_forecast?.p_up)).join(' · ');
  $('horizons').innerHTML=parts?'<div class="row"><span class="muted">P(UP)</span><b>'+parts+'</b></div>':'<div class="row"><span class="muted">state</span><b>—</b></div>';
  const hc=d.forecast?.horizon_consensus||{};
  $('horizonBar').style.width=Math.max(0,Math.min(100,Number(hc.confluence_index||0)*100))+'%';
}
function renderMacro(m){
  $('bcra').textContent=fmt(m.fx?.official,2);$('mep').textContent=fmt(m.fx?.mep,2);$('ccl').textContent=fmt(m.fx?.ccl,2);$('blue').textContent=fmt(m.fx?.blue,2);$('embi').textContent=fmt(m.risk?.embi_bps,0);$('spreadMep').textContent=pct(m.fx?.spreads?.mep_official);
}
async function refreshTerminal(){
  const symbol=$('ticker').value;
  try{
    const d=await json('/api/gorila/terminal/'+encodeURIComponent(symbol));
    renderTerminal(d);
    renderMacro(d.macro||{});
    setStatus($('live'),'ONLINE',true,false);
    setStatus($('macroState'),'MACRO READY',true,false);
  }catch(e){
    setStatus($('live'),'ERROR',false,true);
    $('telemetry').textContent=String(e);
  }
}
async function refreshControl(){
  try{
    const snap=await json('/api/gorila/control');
    const h=snap.health||{}, p=snap.promotion||{}, sh=snap.shadow||{}, latest=(snap.runtime?.items||[])[0];
    setStatus($('storage'),h.database?.ready?'STORAGE · POSTGRES':'STORAGE · DEGRADED',!!h.database?.ready,!h.database?.ready);
    $('tick').textContent=latest?.status||'—';$('pg').textContent=h.database?.ready?'READY':'—';
    $('shadowPred').textContent=sh.predictions??'—';$('shadowSettled').textContent=sh.settled??'—';$('shadowAcc').textContent=pct(sh.accuracy);
    const runtimeState=h.autonomous_runtime||{};
    const readiness=(latest?.result?.audit||{}).readiness||{};
    $('autonomousState').textContent=runtimeState.status||latest?.status||'—';
    $('autonomousState').style.color=(String(runtimeState.status||'').includes('ERROR'))?'#ffafba':'#9beec8';
    $('circuitState').textContent=readiness.circuit_breaker||'—';
    $('circuitState').style.color=readiness.circuit_breaker==='NORMAL'?'#9beec8':'#ffafba';
    $('macroUpdated').textContent=h.macro_ingest?.updated_at||'—';
    const required=new Set(['BCRA/FX','ArgentinaDatos/FX','ArgentinaDatos/EMBI+']);
    const requiredRows=(h.sources||[]).filter(x=>required.has(x.source));
    const requiredHealthy=requiredRows.filter(x=>x.status==='HEALTHY').length;
    $('sourceState').textContent=requiredHealthy+'/'+requiredRows.length+' core healthy';
    const feed=h.primary_market_data||{};
    $('primaryFeed').textContent=feed.status||'—';
    $('primaryFeed').style.color=(String(feed.status||'').startsWith('READY'))?'#9beec8':'#ffafba';
    setStatus($('runtimeStatus'),latest?.status||'NO TICK',latest?.status==='COMPLETED',latest&&latest.status!=='COMPLETED');
    const e=p.current_evaluation||{};
    $('gateAcc').textContent=pct(e.evidence?.oos_accuracy);
    $('rankIc').textContent=fmt(e.evidence?.rank_ic,4);
    $('cpcv').textContent=fmt(e.evidence?.cpcv_mean_return_pct,2)+'%';
    const reasons=(e.reasons||[]).slice(0,5);
    $('gateReasons').innerHTML=reasons.map(x=>'<div class="row"><span class="muted">gate</span><b>'+x+'</b></div>').join('')||'<div class="row"><span class="muted">gate</span><b>—</b></div>';
    setStatus($('gate'),'PROMOTION · '+(e.status||'BLOCKED'),false,e.status!=='ELIGIBLE');
    $('sources').innerHTML=(h.sources||[]).map(x=>'<tr><td>'+x.source+'</td><td>'+x.status+'</td><td>'+x.rows_last_batch+'</td><td>'+fmt(x.latency_ms,1)+' ms</td><td>'+(x.last_success_at||'—')+'</td></tr>').join('');
    $('telemetry').textContent=JSON.stringify(snap,null,2);
    setStatus($('pipelineState'),latest?.status||'WAITING',latest?.status==='COMPLETED',false);
  }catch(e){$('telemetry').textContent=String(e)}
}
let controlTimer=null, terminalTimer=null;
$('ticker').addEventListener('change',()=>{refreshTerminal();refreshControl()});
$('hardRefresh').addEventListener('click',()=>{refreshTerminal();refreshControl()});
$('refresh').addEventListener('click',()=>{refreshTerminal();refreshControl()});
$('auto').addEventListener('change',e=>{
  if(terminalTimer){clearInterval(terminalTimer);terminalTimer=null}
  if(controlTimer){clearInterval(controlTimer);controlTimer=null}
  if(e.target.checked){
    terminalTimer=setInterval(refreshTerminal,2000);
    controlTimer=setInterval(refreshControl,10000);
  }
});
refreshTerminal();refreshControl();
terminalTimer=setInterval(refreshTerminal,2000);
controlTimer=setInterval(refreshControl,10000);
</script>
<!-- Compatibility markers preserved for the research dashboard contract:
     /api/drift?limit=50
     /api/control
     /api/shadow/summary
     DRIFT MONITOR
     CONTROL ROOM — BATCH 13
     SHADOW LEDGER — BATCH 14
-->
</body></html>""")
