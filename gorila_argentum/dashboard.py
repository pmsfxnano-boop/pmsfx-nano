from fastapi.responses import HTMLResponse

HTML=HTMLResponse("""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#050607">
<title>GORILA ARGENTUM · Research Control</title>
<style>
:root{
  --black:#050607;
  --black2:#080a0c;
  --panel:#0b0e11;
  --panel2:#0e1216;
  --line:#1b2229;
  --line2:#26313b;
  --white:#f4f7f9;
  --muted:#7e8b96;
  --muted2:#5d6973;
  --cyan:#63ddff;
  --cyan2:#2eb6d8;
  --green:#53e0a3;
  --amber:#dfb458;
  --red:#ff687b;
  --shadow:0 20px 50px rgba(0,0,0,.28);
}
*{box-sizing:border-box}
html{background:var(--black);scroll-behavior:smooth}
body{
  margin:0;
  background:
    linear-gradient(180deg,#050607 0%,#06080a 55%,#050607 100%);
  color:var(--white);
  font:13px Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
}
button,select{font:inherit}
button{cursor:pointer}
a{color:inherit;text-decoration:none}
.topbar{
  position:sticky;top:0;z-index:50;
  display:flex;justify-content:space-between;align-items:center;gap:18px;
  padding:15px 22px;
  border-bottom:1px solid var(--line);
  background:rgba(5,6,7,.96);
  backdrop-filter:blur(12px);
}
.brand{display:flex;align-items:center;gap:12px;min-width:0}
.brandG{
  font-size:34px;line-height:1;font-weight:950;letter-spacing:-.08em;
  color:var(--cyan);
  text-shadow:0 0 26px rgba(99,221,255,.18);
}
.brandName{
  font-size:15px;font-weight:900;letter-spacing:.15em;color:#fff;
}
.brandSub{
  margin-top:3px;display:block;color:var(--muted2);
  font-size:8px;letter-spacing:.16em;text-transform:uppercase;
}
.topRight{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.chip{
  display:inline-flex;align-items:center;gap:7px;
  padding:7px 10px;border-radius:999px;
  background:#090b0d;border:1px solid var(--line2);
  color:#aab5bd;font-size:9px;font-weight:850;letter-spacing:.08em;
}
.dot{width:7px;height:7px;border-radius:50%;background:var(--muted2)}
.chip.live{border-color:#245843;color:#9af0c8}
.chip.live .dot{background:var(--green);box-shadow:0 0 14px rgba(83,224,163,.45)}
.chip.block{border-color:#5b2b34;color:#ffadb8}
.chip.block .dot{background:var(--red)}
.chip.cyan{border-color:#244c5c;color:#9beaff}
.btn{
  border:1px solid var(--line2);background:#0b0e11;color:#dce5ea;
  border-radius:8px;padding:8px 11px;font-size:9px;font-weight:850;letter-spacing:.06em;
}
.btn:hover{border-color:#385365;background:#10161b}
main{max-width:1540px;margin:0 auto;padding:22px 20px 46px}
.hero{
  padding:28px 24px 22px;
  border:1px solid var(--line2);border-radius:16px;
  background:#090b0e;
  box-shadow:var(--shadow);
}
.kicker{
  color:var(--cyan);font-size:9px;font-weight:900;letter-spacing:.2em;text-transform:uppercase;
}
.hero h1{
  margin:8px 0 7px;font-size:34px;line-height:1;letter-spacing:-.035em;font-weight:900;
}
.hero p{
  margin:0;max-width:980px;color:#88949d;font-size:12px;line-height:1.65;
}
.heroMeta{display:flex;flex-wrap:wrap;gap:7px;margin-top:16px}
.processRail{
  display:grid;grid-template-columns:repeat(15,minmax(78px,1fr));
  gap:5px;overflow-x:auto;margin-top:20px;padding-bottom:2px;
}
.processNode{
  position:relative;min-height:52px;padding:8px 9px;
  border:1px solid var(--line);border-radius:9px;background:#07090b;
}
.processNode:after{
  content:"→";position:absolute;right:-7px;top:50%;transform:translateY(-50%);
  color:#45525d;font-size:12px;
}
.processNode:last-child:after{display:none}
.processNode small{display:block;color:#4e5b66;font-size:7px;letter-spacing:.1em}
.processNode b{display:block;margin-top:5px;font-size:9px;line-height:1.15}
.processNode.guard{border-color:#684e24;background:#110e09}
.selectorRow{
  margin-top:15px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;
}
.selector{
  background:#090b0d;color:#e4eaee;border:1px solid var(--line2);
  border-radius:8px;padding:8px 10px;min-width:120px;
}
.toggle{
  display:flex;align-items:center;gap:7px;color:var(--muted);font-size:9px;
  margin-left:auto;
}
.toggle input{accent-color:var(--cyan)}
.summaryGrid{
  display:grid;grid-template-columns:repeat(5,1fr);gap:9px;margin:12px 0;
}
.summary{
  border:1px solid var(--line);background:#080a0c;border-radius:10px;padding:13px 14px;
}
.label{color:var(--muted2);font-size:8px;letter-spacing:.1em;text-transform:uppercase}
.value{margin-top:7px;font-size:19px;line-height:1.05;font-weight:900}
.sub{margin-top:5px;color:var(--muted);font-size:9px;line-height:1.4}
.good{color:var(--green)!important}.bad{color:var(--red)!important}.amber{color:var(--amber)!important}.cyan{color:var(--cyan)!important}
.timeline{display:grid;gap:11px;margin-top:12px}
.step{
  display:grid;grid-template-columns:72px minmax(230px,.62fr) minmax(0,1.65fr);
  min-width:0;border:1px solid var(--line2);border-radius:14px;
  background:#090b0d;box-shadow:0 14px 34px rgba(0,0,0,.18);
}
.stepNo{
  display:flex;justify-content:center;align-items:flex-start;padding-top:18px;
  color:var(--cyan);font-size:17px;font-weight:950;letter-spacing:.04em;
  border-right:1px solid var(--line);
}
.stepInfo{padding:17px 17px 16px;border-right:1px solid var(--line)}
.stepTitle{font-size:11px;font-weight:900;letter-spacing:.1em;text-transform:uppercase}
.stepWhat{margin-top:9px;color:#a6b1b9;font-size:10px;line-height:1.6}
.stepWhy{margin-top:8px;color:#677580;font-size:9px;line-height:1.55}
.stepLive{padding:14px;min-width:0}
.liveHead{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:10px}
.liveTitle{font-size:9px;font-weight:900;letter-spacing:.1em;color:#9aa8b2;text-transform:uppercase}
.status{
  display:inline-flex;padding:5px 8px;border-radius:999px;border:1px solid var(--line2);
  font-size:8px;font-weight:900;letter-spacing:.06em;
}
.status.good{border-color:#245843;background:#0a1510}
.status.bad{border-color:#5e2c35;background:#150a0d}
.status.amber{border-color:#5f4a26;background:#120f09}
.status.neutral{background:#0a0c0f}
.metrics4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.metric{
  border:1px solid var(--line);background:#07090b;border-radius:9px;padding:10px;
  min-width:0;
}
.metric .n{margin-top:6px;font-size:15px;font-weight:850;overflow-wrap:anywhere}
.metric .t{margin-top:4px;color:#56636d;font-size:8px}
.dataTable{overflow:auto}
table{width:100%;border-collapse:collapse;font-size:9px}
th,td{padding:8px 9px;border-bottom:1px solid #151c22;text-align:left;white-space:nowrap}
th{color:#56636d;font-size:8px;letter-spacing:.08em;text-transform:uppercase}
td{color:#cbd5db}
.pill{display:inline-flex;padding:4px 7px;border-radius:999px;border:1px solid var(--line2);font-size:8px;font-weight:850}
.pill.good{border-color:#245843;background:#0a1510}.pill.bad{border-color:#5e2c35;background:#150a0d}.pill.amber{border-color:#5f4a26;background:#120f09}
.featureGrid{display:grid;grid-template-columns:repeat(6,1fr);gap:8px}
.feature{
  padding:10px;border:1px solid var(--line);border-radius:9px;background:#07090b;
}
.feature b{display:block;margin-top:6px;font-size:15px}
.featureBar{height:4px;margin-top:8px;border-radius:99px;background:#12181d;overflow:hidden}
.featureBar i{display:block;height:100%;background:var(--cyan);opacity:.72;border-radius:99px}
.split{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.detailBox{
  border:1px solid var(--line);background:#07090b;border-radius:9px;padding:11px;
}
.detailBox + .detailBox{margin-top:8px}
.detailTitle{color:#697782;font-size:8px;letter-spacing:.08em;text-transform:uppercase}
.detailValue{margin-top:6px;font-size:13px;font-weight:850;line-height:1.35}
.detailText{margin-top:4px;color:#67737d;font-size:8px;line-height:1.5}
.reasons{display:grid;gap:6px}
.reason{padding:7px 8px;border:1px solid #33282b;background:#0d090b;border-radius:7px;color:#e7b1b9;font-size:8px}
pre{
 margin:0;padding:11px;border:1px solid var(--line);background:#06080a;border-radius:9px;
 color:#8fa0ab;font:9px/1.55 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
 white-space:pre-wrap;max-height:250px;overflow:auto;
}
.footer{
  margin-top:13px;padding:10px 2px;display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;
  color:#4d5b65;font-size:8px;letter-spacing:.05em;text-transform:uppercase;
}
@media(max-width:1100px){
  .summaryGrid{grid-template-columns:repeat(3,1fr)}
  .featureGrid{grid-template-columns:repeat(3,1fr)}
  .step{grid-template-columns:56px 1fr}
  .stepInfo{border-right:0;border-bottom:1px solid var(--line)}
  .stepLive{grid-column:2}
}
@media(max-width:760px){
  main{padding:12px}
  .topbar{padding:12px 13px}
  .brandName{font-size:13px}
  .hero{padding:20px 16px}
  .hero h1{font-size:27px}
  .summaryGrid{grid-template-columns:1fr 1fr}
  .metrics4,.split{grid-template-columns:1fr 1fr}
  .featureGrid{grid-template-columns:1fr 1fr}
  .step{grid-template-columns:1fr}
  .stepNo{justify-content:flex-start;padding:12px 14px;border-right:0;border-bottom:1px solid var(--line)}
  .stepInfo{border-bottom:1px solid var(--line)}
  .stepLive{grid-column:auto}
  .toggle{margin-left:0}
}
@media(max-width:460px){
  .summaryGrid{grid-template-columns:1fr}
  .metrics4,.featureGrid,.split{grid-template-columns:1fr}
  .topRight .btn{display:none}
  .brandSub{display:none}
}
</style>
</head>
<body>
<header class="topbar">
  <div class="brand">
    <div class="brandG">G</div>
    <div>
      <div class="brandName">GORILA ARGENTUM</div>
      <span class="brandSub">research intelligence · control surface</span>
    </div>
  </div>
  <div class="topRight">
    <span id="online" class="chip"><span class="dot"></span><span>CONNECTING</span></span>
    <span id="mode" class="chip">MODE · —</span>
    <span id="storage" class="chip">STORAGE · —</span>
    <button id="refresh" class="btn" type="button">REFRESH</button>
    <a class="btn" href="/docs">API</a>
  </div>
</header>

<main>
  <section class="hero">
    <div class="kicker">01 · system architecture</div>
    <h1>Research Control Surface</h1>
    <p>Una única lectura visual del runtime de investigación, ordenada de principio a fin. Cada módulo conserva su función técnica y su propia explicación; la pantalla no crea nuevas capacidades ni altera las compuertas del sistema.</p>
    <div class="heroMeta">
      <span class="chip live"><span class="dot"></span>ONLINE</span>
      <span class="chip">RESEARCH ONLY</span>
      <span class="chip">TRADING DISABLED</span>
      <span class="chip block"><span class="dot"></span>PROMOTION GATED</span>
    </div>
    <div class="processRail">
      <div class="processNode"><small>01</small><b>Data Fabric</b></div>
      <div class="processNode"><small>02</small><b>Market State</b></div>
      <div class="processNode"><small>03</small><b>Coupling</b></div>
      <div class="processNode"><small>04</small><b>Features</b></div>
      <div class="processNode"><small>05</small><b>Regime</b></div>
      <div class="processNode"><small>06</small><b>Prediction</b></div>
      <div class="processNode"><small>07</small><b>Timing</b></div>
      <div class="processNode"><small>08</small><b>Shadow</b></div>
      <div class="processNode"><small>09</small><b>Outcome</b></div>
      <div class="processNode"><small>10</small><b>Drift</b></div>
      <div class="processNode"><small>11</small><b>Control</b></div>
      <div class="processNode guard"><small>12</small><b>Promotion Gate</b></div>
      <div class="processNode"><small>13</small><b>Learning</b></div>
      <div class="processNode"><small>14</small><b>Recalibration</b></div>
      <div class="processNode"><small>15</small><b>Audit</b></div>
    </div>
    <div class="selectorRow">
      <select id="symbol" class="selector" aria-label="Símbolo">
        <option>GGAL</option><option>BMA</option><option>YPFD</option><option>PAMP</option><option>TGSU2</option><option>CEPU</option>
      </select>
      <button id="jump" class="btn" type="button">IR A SECCIÓN ACTIVA</button>
      <label class="toggle"><input id="auto" type="checkbox" checked> actualizar cada 5 s</label>
    </div>
  </section>

  <section class="summaryGrid">
    <div class="summary"><div class="label">Runtime</div><div id="sumRuntime" class="value cyan">—</div><div id="sumRuntimeSub" class="sub">—</div></div>
    <div class="summary"><div class="label">Circuit breaker</div><div id="sumCircuit" class="value">—</div><div id="sumCircuitSub" class="sub">—</div></div>
    <div class="summary"><div class="label">Promotion gate</div><div id="sumPromotion" class="value bad">—</div><div class="sub">automatic promotion: OFF</div></div>
    <div class="summary"><div class="label">Shadow ledger</div><div id="sumShadow" class="value">—</div><div id="sumShadowSub" class="sub">—</div></div>
    <div class="summary"><div class="label">Latest runtime</div><div id="sumTick" class="value">—</div><div id="sumTickSub" class="sub">—</div></div>
  </section>

  <section class="timeline">

    <article class="step" id="step01">
      <div class="stepNo">02</div>
      <div class="stepInfo">
        <div class="stepTitle">Data Fabric</div>
        <div class="stepWhat">Qué es: la capa de entrada, normalización, deduplicación y salud de fuentes.</div>
        <div class="stepWhy">Por qué importa: sin datos consistentes, el resto de la cadena no tiene una base reproducible.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Source health</div><span id="dataStatus" class="status neutral">—</span></div>
        <div id="sources" class="dataTable"></div>
      </div>
    </article>

    <article class="step" id="step02">
      <div class="stepNo">03</div>
      <div class="stepInfo">
        <div class="stepTitle">Argentina Market State</div>
        <div class="stepWhat">Qué es: una fotografía persistida del estado FX y riesgo local.</div>
        <div class="stepWhy">Por qué importa: contextualiza el universo argentino antes de acoplamiento y modelado.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Latest persisted state</div><span id="marketStatus" class="status neutral">—</span></div>
        <div id="market" class="metrics4"></div>
      </div>
    </article>

    <article class="step" id="step03">
      <div class="stepNo">04</div>
      <div class="stepInfo">
        <div class="stepTitle">Dynamic Coupling</div>
        <div class="stepWhat">Qué es: relaciones laggeadas entre series alineadas por fecha.</div>
        <div class="stepWhy">Por qué importa: expone sincronización y liderazgo relativo sin asumir simetría.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Current coupling matrix</div><span id="couplingStatus" class="status neutral">—</span></div>
        <div class="dataTable"><table><thead><tr><th>FROM</th><th>TO</th><th>COUPLING</th><th>LAG</th><th>N</th></tr></thead><tbody id="coupling"></tbody></table></div>
      </div>
    </article>

    <article class="step" id="step04">
      <div class="stepNo">05</div>
      <div class="stepInfo">
        <div class="stepTitle">Feature Engine</div>
        <div class="stepWhat">Qué es: transforma la serie en variables compactas de retorno, volatilidad y posición.</div>
        <div class="stepWhy">Por qué importa: esta es la representación que consume el aprendizaje.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Selected symbol · <span id="featureSymbol">—</span></div><span id="featureStamp" class="status neutral">—</span></div>
        <div id="features" class="featureGrid"></div>
      </div>
    </article>

    <article class="step" id="step05">
      <div class="stepNo">06</div>
      <div class="stepInfo">
        <div class="stepTitle">Regime</div>
        <div class="stepWhat">Qué es: clasifica el estado dinámico de la serie a partir de tendencia, volatilidad y estrés.</div>
        <div class="stepWhy">Por qué importa: el régimen cambia la lectura del resto del pipeline.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Current research regime</div><span id="regimeStatus" class="status neutral">READ-ONLY</span></div>
        <div class="split">
          <div class="detailBox"><div class="detailTitle">Regime</div><div id="regime" class="detailValue">—</div><div id="regimeConfidence" class="detailText">—</div></div>
          <div class="detailBox"><div class="detailTitle">Regime features</div><div id="regimeFeatures" class="detailText">—</div></div>
        </div>
      </div>
    </article>

    <article class="step" id="step06">
      <div class="stepNo">07</div>
      <div class="stepInfo">
        <div class="stepTitle">Prediction V0</div>
        <div class="stepWhat">Qué es: la capa probabilística que produce una candidatura de dirección.</div>
        <div class="stepWhy">Por qué importa: produce una hipótesis cuantitativa, no una orden.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Latest learning candidate</div><span id="predictionStatus" class="status neutral">—</span></div>
        <div class="metrics4">
          <div class="metric"><div class="label">P(UP)</div><div id="pUp" class="n">—</div></div>
          <div class="metric"><div class="label">Direction</div><div id="direction" class="n">—</div></div>
          <div class="metric"><div class="label">OOS accuracy</div><div id="oosAccuracy" class="n">—</div></div>
          <div class="metric"><div class="label">Samples</div><div id="learnSamples" class="n">—</div></div>
        </div>
      </div>
    </article>

    <article class="step" id="step07">
      <div class="stepNo">08</div>
      <div class="stepInfo">
        <div class="stepTitle">Timing</div>
        <div class="stepWhat">Qué es: delimita horizonte y ventana temporal asociada a la hipótesis.</div>
        <div class="stepWhy">Por qué importa: una predicción sin horizonte no es una señal temporal completa.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Window</div><span id="timingStatus" class="status neutral">—</span></div>
        <div class="split">
          <div class="detailBox"><div class="detailTitle">Horizon</div><div id="timingHorizon" class="detailValue">—</div><div id="timingWindow" class="detailText">—</div></div>
          <div class="detailBox"><div class="detailTitle">Signal state</div><div id="signalState" class="detailValue">—</div><div id="signalNote" class="detailText">—</div></div>
        </div>
      </div>
    </article>

    <article class="step" id="step08">
      <div class="stepNo">09</div>
      <div class="stepInfo">
        <div class="stepTitle">SHADOW LEDGER — BATCH 14</div>
        <div class="stepWhat">Qué es: almacena predicciones sin ejecución real y permite su settlement point-in-time.</div>
        <div class="stepWhy">Por qué importa: convierte hipótesis en evidencia evaluable.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Persisted shadow diagnostics</div><span id="shadowStatus" class="status neutral">—</span></div>
        <div id="shadow" class="metrics4"></div>
      </div>
    </article>

    <article class="step" id="step09">
      <div class="stepNo">10</div>
      <div class="stepInfo">
        <div class="stepTitle">Outcome</div>
        <div class="stepWhat">Qué es: compara cada hipótesis con el resultado observado al alcanzar el horizonte.</div>
        <div class="stepWhy">Por qué importa: aquí aparecen accuracy, Brier, log-loss y retorno realizado.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Realized evidence</div><span id="outcomeStatus" class="status neutral">—</span></div>
        <div class="metrics4">
          <div class="metric"><div class="label">Settled</div><div id="outSettled" class="n">—</div></div>
          <div class="metric"><div class="label">Accuracy</div><div id="outAccuracy" class="n">—</div></div>
          <div class="metric"><div class="label">Brier</div><div id="outBrier" class="n">—</div></div>
          <div class="metric"><div class="label">Mean return</div><div id="outReturn" class="n">—</div></div>
        </div>
      </div>
    </article>

    <article class="step" id="step10">
      <div class="stepNo">11</div>
      <div class="stepInfo">
        <div class="stepTitle">DRIFT MONITOR</div>
        <div class="stepWhat">Qué es: compara ventanas de referencia y actuales para detectar cambios de distribución.</div>
        <div class="stepWhy">Por qué importa: un modelo puede degradarse aunque el código siga sano.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Latest persisted drift</div><span id="driftCount" class="status neutral">—</span></div>
        <div class="dataTable"><table><thead><tr><th>SERIES</th><th>STATUS</th><th>PSI</th><th>KS</th><th>MEAN Z</th><th>STD RATIO</th></tr></thead><tbody id="driftRows"></tbody></table></div>
      </div>
    </article>

    <article class="step" id="step11">
      <div class="stepNo">12</div>
      <div class="stepInfo">
        <div class="stepTitle">CONTROL ROOM — BATCH 13</div>
        <div class="stepWhat">Qué es: consolida durabilidad, salud de fuentes, drift, circuit breaker y compuerta operacional.</div>
        <div class="stepWhy">Por qué importa: separa el estado operativo de la evidencia científica de promoción.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Operational guards</div><span id="controlStatus" class="status neutral">—</span></div>
        <div class="metrics4">
          <div class="metric"><div class="label">Runtime</div><div id="ctlRuntime" class="n">—</div></div>
          <div class="metric"><div class="label">Storage</div><div id="ctlStorage" class="n">—</div></div>
          <div class="metric"><div class="label">Circuit</div><div id="ctlCircuit" class="n">—</div></div>
          <div class="metric"><div class="label">Operational gate</div><div id="ctlGate" class="n">—</div></div>
        </div>
        <div style="margin-top:9px" id="ctlReasons" class="reasons"></div>
      </div>
    </article>

    <article class="step" id="step12">
      <div class="stepNo">13</div>
      <div class="stepInfo">
        <div class="stepTitle">Promotion Gate</div>
        <div class="stepWhat">Qué es: evalúa los criterios estadísticos y de robustez necesarios para cambiar de estado.</div>
        <div class="stepWhy">Por qué importa: el frontend no puede saltarse esta compuerta.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Evidence gate</div><span id="promoStatus" class="status bad">BLOCKED</span></div>
        <div id="promoReasons" class="reasons"></div>
      </div>
    </article>

    <article class="step" id="step13">
      <div class="stepNo">14</div>
      <div class="stepInfo">
        <div class="stepTitle">Continuous Learning</div>
        <div class="stepWhat">Qué es: construye datasets reproducibles, valida cronológicamente y genera candidatos.</div>
        <div class="stepWhy">Por qué importa: permite investigar adaptación sin reemplazo automático del modelo.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Latest candidate cycle</div><span id="learningStatus" class="status neutral">—</span></div>
        <div class="metrics4">
          <div class="metric"><div class="label">Dataset</div><div id="datasetHash" class="n">—</div></div>
          <div class="metric"><div class="label">Samples</div><div id="candidateSamples" class="n">—</div></div>
          <div class="metric"><div class="label">Candidate</div><div id="candidateState" class="n">—</div></div>
          <div class="metric"><div class="label">Promotion</div><div id="candidatePromotion" class="n bad">—</div></div>
        </div>
      </div>
    </article>

    <article class="step" id="step14">
      <div class="stepNo">15</div>
      <div class="stepInfo">
        <div class="stepTitle">Recalibration</div>
        <div class="stepWhat">Qué es: propone un ajuste de calibración sobre Shadow ya asentado.</div>
        <div class="stepWhy">Por qué importa: corrige probabilidades sólo como candidato; el auto-apply permanece apagado.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Latest calibration candidate</div><span id="recalStatus" class="status neutral">—</span></div>
        <div class="split">
          <div class="detailBox"><div class="detailTitle">Brier improvement</div><div id="recalBrier" class="detailValue">—</div><div id="recalApply" class="detailText">automatic apply: OFF</div></div>
          <div class="detailBox"><div class="detailTitle">Log-loss improvement</div><div id="recalLogloss" class="detailValue">—</div><div id="recalGate" class="detailText">—</div></div>
        </div>
      </div>
    </article>

    <article class="step" id="step15">
      <div class="stepNo">16</div>
      <div class="stepInfo">
        <div class="stepTitle">Audit</div>
        <div class="stepWhat">Qué es: fotografía consolidada de runtime, evidencia, drift, learning y readiness.</div>
        <div class="stepWhy">Por qué importa: deja una lectura trazable del estado real que no depende de una sola pantalla.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Consolidated state</div><span id="auditStatus" class="status neutral">—</span></div>
        <pre id="audit">cargando…</pre>
      </div>
    </article>

    <article class="step" id="runtimeStep">
      <div class="stepNo">17</div>
      <div class="stepInfo">
        <div class="stepTitle">Runtime History</div>
        <div class="stepWhat">Qué es: registro persistente de ejecuciones del runtime tick.</div>
        <div class="stepWhy">Por qué importa: muestra que el sistema no sólo está “online”; deja huella de ejecuciones.</div>
      </div>
      <div class="stepLive">
        <div class="liveHead"><div class="liveTitle">Persisted runs</div><span id="runCount" class="status neutral">—</span></div>
        <div class="dataTable"><table><thead><tr><th>ID</th><th>KIND</th><th>STATUS</th><th>STARTED</th><th>COMPLETED</th></tr></thead><tbody id="runs"></tbody></table></div>
      </div>
    </article>

  </section>

  <footer class="footer">
    <span>Gorila Argentum · Research Only · Trading execution disabled · Automatic promotion disabled</span>
    <span id="last">last refresh: —</span>
  </footer>
</main>

<script>
const $ = id => document.getElementById(id);
let timer = null;
let busy = false;

function esc(v){
  return String(v ?? "—").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
}
function fmt(v,d=4){
  if(v===null || v===undefined || v==="") return "—";
  const n=Number(v);
  if(!Number.isFinite(n)) return String(v);
  return n.toLocaleString("en-US",{minimumFractionDigits:0,maximumFractionDigits:d});
}
function pct(v,d=1){
  if(v===null || v===undefined || v==="") return "—";
  const n=Number(v)*100;
  return Number.isFinite(n)?n.toFixed(d)+"%":"—";
}
function stamp(v){
  if(!v) return "—";
  const d=new Date(v);
  return Number.isNaN(d.getTime())?String(v):d.toLocaleString("es-AR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit",second:"2-digit"});
}
function stateClass(v){
  const s=String(v||"").toUpperCase();
  if(["OK","READY","NORMAL","COMPLETED","ONLINE","LIVE","PASS","HEALTHY"].includes(s)) return "good";
  if(["WARN","DEGRADED","RUNNING","CANDIDATE_READY"].includes(s)) return "amber";
  if(["ALERT","HALTED","BLOCKED","FAILED","ERROR","STALE","REJECTED"].includes(s)) return "bad";
  return "neutral";
}
function status(id,v){
  const el=$(id); el.className="status "+stateClass(v); el.textContent=v||"—";
}
function pill(v){
  return '<span class="pill '+stateClass(v)+'">'+esc(v||"—")+"</span>";
}
async function get(path){
  const r=await fetch(path,{cache:"no-store"});
  if(!r.ok) throw new Error(path+" HTTP "+r.status);
  return r.json();
}

function renderHealth(h){
  const online=!!h.ok;
  $("online").className="chip "+(online?"live":"block");
  $("online").innerHTML='<span class="dot"></span><span>'+(online?"ONLINE":"DEGRADED")+"</span>";
  $("mode").textContent="MODE · "+(h.mode||"—");
  $("storage").textContent="STORAGE · "+(h.storage||"—").toUpperCase();
  $("sumRuntime").textContent=h.mode||"—";
  $("sumRuntimeSub").textContent=(h.service||"—")+" · v"+(h.version||"—");
}
function renderSources(h){
  const rows=h.sources||[];
  const good=rows.filter(x=>["OK","READY"].includes(String(x.status||"").toUpperCase())).length;
  status("dataStatus",rows.length?(good===rows.length?"HEALTHY":"DEGRADED"):"NO DATA");
  $("sources").innerHTML=rows.length?
    "<table><thead><tr><th>SOURCE</th><th>STATUS</th><th>LATENCY</th><th>ROWS</th></tr></thead><tbody>"+
    rows.map(x=>"<tr><td>"+esc(x.source)+"</td><td>"+pill(x.status)+"</td><td>"+fmt(x.latency_ms,1)+" ms</td><td>"+fmt(x.rows_last_batch,0)+"</td></tr>").join("")+
    "</tbody></table>"
    :'<div class="sub">No source health rows persisted.</div>';
}
function renderMarket(s){
  const fx=s.fx||{};
  const items=[
    ["USD OFICIAL",fx.official],["USD MEP",fx.mep],["USD CCL",fx.ccl],["USD BLUE",fx.blue],
    ["USD CRIPTO",fx.crypto],["MEP / OFICIAL",fx.spreads?.mep_official==null?null:pct(fx.spreads.mep_official,2)],
    ["CCL / OFICIAL",fx.spreads?.ccl_official==null?null:pct(fx.spreads.ccl_official,2)],
    ["CCL / MEP",fx.spreads?.ccl_mep==null?null:pct(fx.spreads.ccl_mep,2)]
  ];
  status("marketStatus",s.sources?.length?"READY":"WAITING");
  $("market").innerHTML=items.map(x=>'<div class="metric"><div class="label">'+esc(x[0])+'</div><div class="n">'+esc(typeof x[1]==="string"?x[1]:fmt(x[1],4))+'</div><div class="t">'+esc(s.as_of?.[x[0]]||"persisted state")+'</div></div>').join("");
}
function renderCoupling(c){
  status("couplingStatus",c.status||"WAITING");
  const rows=c.edges||[];
  $("coupling").innerHTML=rows.length?rows.map(e=>"<tr><td>"+esc(e.from)+"</td><td>"+esc(e.to)+"</td><td>"+fmt(e.coupling,4)+"</td><td>"+fmt(e.lag,0)+"</td><td>"+fmt(e.n,0)+"</td></tr>").join(""):'<tr><td colspan="5">No hay profundidad suficiente.</td></tr>';
}
function renderFeatures(f){
  $("featureSymbol").textContent=f.symbol||"—";
  $("featureStamp").textContent=(f.samples??0)+" samples";
  const defs=[["r1","Return 1"],["r3","Return 3"],["r5","Return 5"],["vol5","Vol 5"],["vol20","Vol 20"],["z20","Z 20"]];
  $("features").innerHTML=defs.map(([k,l])=>{
    const n=Number(f[k]); const w=Number.isFinite(n)?Math.min(100,Math.round(Math.min(1,Math.abs(n))*100)):0;
    return '<div class="feature"><div class="label">'+l+'</div><b>'+fmt(f[k],6)+'</b><div class="featureBar"><i style="width:'+w+'%"></i></div></div>';
  }).join("");
}
function renderRegime(r){
  status("regimeStatus","READ-ONLY");
  $("regime").textContent=r.regime||"UNKNOWN";
  $("regimeConfidence").textContent="confidence "+pct(r.confidence,1);
  $("regimeFeatures").textContent=JSON.stringify(r.features||{},null,2);
}
function renderLearning(l,signal){
  const x=l?.items?.[0]||null;
  const result=x?.result||{};
  const validation=result.validation||{};
  const p=result.latest_probability_up;
  const ready=Number.isFinite(Number(p));
  status("predictionStatus",result.status||x?.status||"NO RUN");
  $("pUp").textContent=ready?pct(p,1):"—";
  $("direction").textContent=ready?(Number(p)>=0.5?"UP":"DOWN"):"—";
  $("oosAccuracy").textContent=validation.accuracy==null?"—":pct(validation.accuracy,1);
  $("learnSamples").textContent=result.samples??x?.samples??"—";
  $("datasetHash").textContent=result.dataset_hash?String(result.dataset_hash).slice(0,12)+"…":"—";
  $("candidateSamples").textContent=result.samples??"—";
  $("candidateState").textContent=result.status||x?.status||"—";
  $("candidatePromotion").textContent=result.promotion||"BLOCKED";
  const sig=signal||{};
  status("timingStatus",sig.status||"WAITING");
  $("timingHorizon").textContent=sig.timing?.horizon_seconds?Math.round(Number(sig.timing.horizon_seconds)/60)+" min":"—";
  $("timingWindow").textContent=sig.timing?stamp(sig.timing.entry_start)+" → "+stamp(sig.timing.entry_end):"No signal window generated.";
  $("signalState").textContent=sig.direction||"WAITING";
  $("signalNote").textContent=ready?"Derived from the latest persisted learning candidate.":"No probability candidate available.";
}
function renderShadow(s){
  status("shadowStatus",s.predictions==null?"NO DATA":"READY");
  $("shadow").innerHTML=[
    ["Predictions",fmt(s.predictions,0)],["Open",fmt(s.open,0)],["Settled",fmt(s.settled,0)],["Accuracy",s.accuracy==null?"—":pct(s.accuracy,1)]
  ].map(x=>'<div class="metric"><div class="label">'+x[0]+'</div><div class="n">'+x[1]+'</div></div>').join("");
  $("outSettled").textContent=fmt(s.settled,0);
  $("outAccuracy").textContent=s.accuracy==null?"—":pct(s.accuracy,1);
  $("outBrier").textContent=s.mean_brier==null?"—":fmt(s.mean_brier,4);
  $("outReturn").textContent=s.mean_return_pct==null?"—":fmt(s.mean_return_pct,3)+"%";
  status("outcomeStatus",s.settled>0?"READY":"WAITING");
  $("sumShadow").textContent=fmt(s.predictions,0);
  $("sumShadowSub").textContent=fmt(s.settled,0)+" settled";
}
function renderDrift(d){
  const items=d.items||[];
  $("driftCount").textContent=items.length+" snapshots";
  const latest={};
  items.forEach(x=>{const k=x.symbol+"|"+x.field;if(!latest[k])latest[k]=x;});
  const rows=Object.values(latest);
  $("driftRows").innerHTML=rows.length?rows.map(x=>"<tr><td>"+esc(x.symbol+" / "+x.field)+"</td><td>"+pill(x.status)+"</td><td>"+fmt(x.psi,4)+"</td><td>"+fmt(x.ks,4)+"</td><td>"+fmt(x.mean_shift_z,3)+"</td><td>"+fmt(x.std_ratio,3)+"</td></tr>").join(""):'<tr><td colspan="6">No persisted drift snapshots.</td></tr>';
}
function renderControl(c,promo){
  const rt=c.runtime||{};
  status("controlStatus",rt.circuit_breaker||"—");
  $("ctlRuntime").textContent=rt.mode||"—";
  $("ctlStorage").textContent=rt.storage||"—";
  $("ctlCircuit").textContent=rt.circuit_breaker||"—";
  $("ctlGate").textContent=rt.promotion_operational_gate||"—";
  const reasons=rt.circuit_breaker_reasons||[];
  $("ctlReasons").innerHTML=reasons.length?reasons.map(x=>'<div class="reason">'+esc(x)+"</div>").join(""):'<div class="status good">NO ACTIVE CIRCUIT REASON</div>';
  const pe=promo?.current_evaluation||{};
  const pstatus=pe.status||c.promotion_gate?.status||"BLOCKED";
  status("promoStatus",pstatus);
  const reasons2=pe.reasons||c.promotion_gate?.reason||[];
  $("promoReasons").innerHTML=reasons2.length?reasons2.slice(0,10).map(x=>'<div class="reason">'+esc(x)+"</div>").join(""):'<div class="status neutral">No reasons returned.</div>';
  $("sumCircuit").textContent=rt.circuit_breaker||"—";
  $("sumCircuitSub").textContent=(rt.circuit_breaker_reasons||[]).join(", ")||"No active reason";
  $("sumPromotion").textContent=pstatus;
}
function renderRecal(recal){
  const x=recal?.items?.[0]||null;
  status("recalStatus",x?.status||"NO RUN");
  $("recalBrier").textContent=x?.brier_improvement==null?"—":fmt(x.brier_improvement,5);
  $("recalLogloss").textContent=x?.logloss_improvement==null?"—":fmt(x.logloss_improvement,5);
  $("recalApply").textContent=x?"automatic apply: OFF":"No candidate persisted.";
  $("recalGate").textContent=x?.apply_gate||"PROMOTION_AND_DURABILITY_REQUIRED";
}
function renderRuns(r){
  const items=r.items||[];
  $("runCount").textContent=items.length+" persisted";
  const latest=items[0];
  $("sumTick").textContent=latest?.status||"—";
  $("sumTickSub").textContent=latest?.completed_at?stamp(latest.completed_at):"—";
  $("runs").innerHTML=items.length?items.map(x=>"<tr><td>"+esc(x.id)+"</td><td>"+esc(x.kind)+"</td><td>"+pill(x.status)+"</td><td>"+esc(stamp(x.started_at))+"</td><td>"+esc(stamp(x.completed_at))+"</td></tr>").join(""):'<tr><td colspan="5">No runtime runs persisted.</td></tr>';
}
function renderAudit(a){
  const r=a.readiness||{};
  status("auditStatus",r.circuit_breaker||"—");
  $("audit").textContent=JSON.stringify({
    service:a.service,mode:a.mode,storage:a.storage,
    readiness:r,promotion:a.promotion,shadow:a.shadow,
    learning:a.learning,recalibration:a.recalibration,drift:a.drift
  },null,2);
}
async function refresh(){
  if(busy)return;
  busy=true;
  const sym=$("symbol").value;
  try{
    const [h,s,c,f,d,ctl,sh,p,l,re,runs,audit,regime]=await Promise.all([
      get("/health"),
      get("/api/state/live"),
      get("/api/coupling/current"),
      get("/api/features/"+encodeURIComponent(sym)),
      get("/api/drift?limit=50"),
      get("/api/control"),
      get("/api/shadow/summary"),
      get("/api/promotion"),
      get("/api/learning?symbol="+encodeURIComponent(sym)+"&limit=5"),
      get("/api/recalibration?limit=5"),
      get("/api/runtime/runs?limit=10"),
      get("/api/audit"),
      fetch("/api/regime/"+encodeURIComponent(sym),{cache:"no-store"}).then(x=>x.ok?x.json():({regime:"UNAVAILABLE",confidence:0,features:{}}))
    ]);
    renderHealth(h);renderSources(h);renderMarket(s);renderCoupling(c);renderFeatures(f);renderRegime(regime);
    const p=l?.items?.[0]?.result?.latest_probability_up;
    let sig=null;
    if(p!==null && p!==undefined && Number.isFinite(Number(p))){
      const q=await get("/api/signal/"+encodeURIComponent(sym)+"?probability_up="+encodeURIComponent(p)+"&horizon_seconds=900&regime="+encodeURIComponent(regime.regime||"UNKNOWN"));
      sig=q;
    }
    renderLearning(l,sig);renderShadow(sh);renderDrift(d);renderControl(ctl,p);renderRecal(re);renderRuns(runs);renderAudit(audit);
    $("last").textContent="last refresh: "+new Date().toLocaleTimeString("es-AR");
  }catch(e){
    $("online").className="chip block";
    $("online").innerHTML='<span class="dot"></span><span>DEGRADED</span>';
    $("last").textContent="refresh error: "+e.message;
  }finally{
    busy=false;
  }
}
function resetTimer(){
  if(timer)clearInterval(timer);
  timer=null;
  if($("auto").checked)timer=setInterval(refresh,5000);
}
$("refresh").addEventListener("click",refresh);
$("auto").addEventListener("change",resetTimer);
$("symbol").addEventListener("change",refresh);
$("jump").addEventListener("click",()=>document.getElementById("step06").scrollIntoView({behavior:"smooth",block:"center"}));
refresh();resetTimer();
</script>
</body>
</html>""")
