import { useEffect, useMemo, useRef, useState } from "react";
import {
  fetchBcra,
  fetchChart,
  fetchControl,
  fetchHealth,
  fetchMatrix,
  fetchTerminal,
  type ChartResponse,
  type ControlResponse,
  type HealthResponse,
  type JsonMap,
  type MatrixItem,
  type MatrixResponse,
  type TerminalResponse,
} from "./api";

const SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"] as const;
const TIMEFRAMES = ["1D", "5D", "1M", "3M", "6M", "1Y"] as const;
type SymbolId = typeof SYMBOLS[number];
type Timeframe = typeof TIMEFRAMES[number];

function n(value: any): number | null {
  const x = Number(value);
  return Number.isFinite(x) ? x : null;
}
function fmt(value: any, digits = 2): string {
  const x = n(value);
  return x == null ? "—" : x.toLocaleString("es-AR", { maximumFractionDigits: digits });
}
function pct(value: any, digits = 2): string {
  const x = n(value);
  return x == null ? "—" : `${x >= 0 ? "+" : ""}${(x * 100).toFixed(digits)}%`;
}
function age(value: any): string {
  const x = n(value);
  if (x == null) return "—";
  if (x < 2) return "<2s";
  if (x < 60) return `${x.toFixed(0)}s`;
  return `${(x / 60).toFixed(1)}m`;
}
function when(value: any): string {
  if (!value) return "—";
  const d = new Date(String(value));
  return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleTimeString("es-AR", { hour12: false });
}
function signalText(item: any): string {
  if (!item) return "NO DATA";
  if (item.status === "NO_DATA") return "FORECAST UNAVAILABLE";
  return String(item.signal || item.status || "—");
}
function signalClass(item: any): string {
  const s = String(item?.signal || item?.status || "").toUpperCase();
  return s === "UP" || s === "HEALTHY" || s === "LIVE" ? "good" : s === "DOWN" || s === "STALE" || s === "BLOCKED" ? "bad" : "warn";
}
function pick(obj: any, ...keys: string[]): any {
  for (const k of keys) if (obj?.[k] !== undefined && obj?.[k] !== null) return obj[k];
  return null;
}

function StatusPill({ label, tone = "neutral" }: { label: string; tone?: "good" | "warn" | "bad" | "neutral" }) {
  return <span className={`pill ${tone}`}><i />{label}</span>;
}

function Brand() {
  return <div className="brand"><div className="mark">G<span>+</span></div><div><b>GORILA</b><small>ARGENTUM · QUANT</small></div></div>;
}

function Matrix({ data, selected, onSelect }: { data: MatrixResponse | null; selected: SymbolId; onSelect: (s: SymbolId) => void }) {
  const items = data?.items || [];
  return <section className="panel">
    <div className="section-head"><div><small>MERCADO ARGENTINO</small><h2>Cross-sectional signal matrix</h2></div><span className="section-note">{items.length || SYMBOLS.length} activos · snapshot backend</span></div>
    <div className="matrix">
      {SYMBOLS.map(symbol => {
        const item = items.find((x: MatrixItem) => x.symbol === symbol);
        const score = n(item?.signal_score);
        const last = n(item?.market?.last);
        const pUp = n(item?.probability?.up);
        const edge = n(item?.relative_alpha?.relative_edge_pp ?? item?.relative_edge_pp);
        const tone = signalClass(item);
        return <button key={symbol} className={`matrix-card ${selected === symbol ? "selected" : ""}`} onClick={() => onSelect(symbol)}>
          <div className="matrix-top"><b>{symbol}</b><StatusPill label={signalText(item)} tone={tone as any} /></div>
          <div className="matrix-price">{fmt(last, 3)}</div>
          <div className="score"><span style={{ width: `${Math.max(0, Math.min(100, score ?? 0))}%` }} /></div>
          <div className="mini-grid">
            <span>P(UP)<b>{pUp == null ? "—" : pct(pUp, 1)}</b></span>
            <span>EDGE<b>{edge == null ? "—" : `${edge > 0 ? "+" : ""}${edge.toFixed(2)}pp`}</b></span>
          </div>
          <small className="matrix-meta">{item?.market?.data_grade || "UNKNOWN"} · {age(item?.market?.market_event_age_seconds ?? item?.market?.market_age_seconds)}</small>
        </button>;
      })}
    </div>
  </section>;
}

function PriceChart({ data, selected, timeframe, onTimeframe }: { data: ChartResponse | null; selected: SymbolId; timeframe: Timeframe; onTimeframe: (t: Timeframe) => void }) {
  const rows = data?.rows || [];
  const values = rows.map(r => n(r.close)).filter((x): x is number => x != null);
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 1;
  const span = Math.max(max - min, Math.abs(max) * 0.0005, 1e-6);
  const W = 1000, H = 360, padX = 24, padY = 22;
  const point = (v: number, i: number) => {
    const x = padX + (i / Math.max(1, values.length - 1)) * (W - padX * 2);
    const y = padY + (1 - (v - (min - span * .08)) / (span * 1.16)) * (H - padY * 2);
    return [x, y] as const;
  };
  const pts = values.map(point);
  const line = pts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const area = pts.length ? `${line} ${pts.at(-1)![0]},${H - padY} ${pts[0][0]},${H - padY}` : "";
  const last = values.at(-1) ?? null;
  const change = n(data?.change_pct);
  return <section className="panel chart-panel">
    <div className="section-head">
      <div><small>SERIE CANÓNICA · SOLO VISUALIZACIÓN</small><h2>{selected} · precio</h2></div>
      <div className="chart-tools">{TIMEFRAMES.map(tf => <button key={tf} className={tf === timeframe ? "active" : ""} onClick={() => onTimeframe(tf)}>{tf}</button>)}</div>
    </div>
    <div className="chart-meta">
      <strong>{fmt(last, 4)}</strong>
      <span className={change == null ? "" : change >= 0 ? "goodText" : "badText"}>{change == null ? "—" : pct(change)}</span>
      <span>{data?.resolution || "—"} · {rows.length} puntos</span>
      <span>{when(data?.coverage?.start)} → {when(data?.coverage?.end)}</span>
    </div>
    <div className="chart-wrap">
      {pts.length ? <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" role="img" aria-label={`Serie de precio de ${selected}`}>
        {[.2,.4,.6,.8].map(k => <line key={k} x1={padX} x2={W-padX} y1={H*k} y2={H*k} stroke="rgba(170,190,204,.12)" />)}
        <polygon points={area} fill="rgba(99,221,255,.11)" />
        <polyline points={line} fill="none" stroke="#69d5e7" strokeWidth="3" vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
        {last != null && pts.at(-1) && <circle cx={pts.at(-1)![0]} cy={pts.at(-1)![1]} r="5" fill="#69d5e7" />}
      </svg> : <div className="chart-empty">SIN SERIE DISPONIBLE · el backend no reportó observaciones para este horizonte.</div>}
    </div>
  </section>;
}

function Detail({ data, bcra, health }: { data: TerminalResponse | null; bcra: JsonMap | null; health: HealthResponse | null }) {
  const signal = data?.signal || {};
  const market = signal.market || {};
  const probability = signal.probability || {};
  const forecast = data?.forecast?.forecast || {};
  const macroItems = data?.macro?.items || {};
  const bcraItems = bcra?.items || macroItems;
  const db = health?.database || {};
  const live = health?.argentina_live || data?.stream || {};
  const gate = health?.primary_market_data || {};
  return <div className="detail-grid">
    <section className="panel detail-main">
      <div className="section-head"><div><small>INSTRUMENT STATE</small><h2>{data?.symbol || "—"}</h2></div><StatusPill label={data ? signalText(signal) : "SYNC"} tone={signalClass(signal) as any} /></div>
      <div className="headline-price">
        <strong>{fmt(data?.quote?.last ?? market.last, 4)}</strong>
        <span>{data?.quote?.source || market.source || "—"} · event {age(pick(data, "quote_age_seconds") ?? market.market_event_age_seconds ?? market.market_age_seconds)}</span>
      </div>
      <div className="signal-row">
        <div><small>FORECAST</small><b>{forecast.direction || signal.signal || "—"}</b><span>P(UP) {forecast.p_up == null ? "—" : pct(forecast.p_up, 1)}</span></div>
        <div><small>CONFIDENCE</small><b>{probability.confidence == null ? "—" : pct(probability.confidence, 1)}</b><span>edge {probability.edge == null ? "—" : pct(probability.edge, 1)}</span></div>
        <div><small>SCORE</small><b>{fmt(signal.signal_score, 1)}</b><span>{signal.status || "—"}</span></div>
        <div><small>VALIDATION</small><b>{signal.validation?.validated ? "VALIDATED" : "RESEARCH"}</b><span>promotion remains gated</span></div>
      </div>
      <div className="two-col">
        <div className="data-box"><small>MARKET QUALITY</small><div><span>grade</span><b>{market.data_grade || "—"}</b></div><div><span>bid / ask</span><b>{fmt(market.bid, 4)} / {fmt(market.ask, 4)}</b></div><div><span>spread</span><b>{fmt(market.spread_bps, 2)} bp</b></div><div><span>event time</span><b>{when(market.quote_timestamp || data?.quote?.quoteTimestamp)}</b></div></div>
        <div className="data-box"><small>RUNTIME</small><div><span>live state</span><b>{live.status || "—"}</b></div><div><span>symbols updated</span><b>{live.updated_symbols ?? "—"}</b></div><div><span>event age</span><b>{age(live.median_event_age_seconds)}</b></div><div><span>db</span><b>{db.status || (db.ready ? "READY" : "—")}</b></div></div>
      </div>
    </section>
    <section className="panel">
      <div className="section-head"><div><small>MACRO FABRIC</small><h2>BCRA / riesgo</h2></div><span className="section-note">cache controlado</span></div>
      <div className="macro-list">
        {[
          ["FX mayorista", pick(bcraItems?.BCRA_WHOLESALE_FX, "value") ?? pick(macroItems?.BCRA_WHOLESALE_FX, "value")],
          ["Reservas", pick(bcraItems?.BCRA_RESERVAS_USD, "value") ?? pick(macroItems?.BCRA_RESERVAS_USD, "value")],
          ["Base monetaria", pick(bcraItems?.BCRA_BASE_MONETARIA, "value") ?? pick(macroItems?.BCRA_BASE_MONETARIA, "value")],
          ["BADLAR / TAMAR", `${fmt(pick(bcraItems?.BCRA_BADLAR_NA, "value"),2)} / ${fmt(pick(bcraItems?.BCRA_TAMAR_NA, "value"),2)}`],
        ].map(([label, value]) => <div key={String(label)}><span>{label}</span><b>{value == null ? "—" : typeof value === "string" ? value : fmt(value, 2)}</b></div>)}
      </div>
      <div className="macro-footer">{String(gate.provider || "BYMADATA")} · {gate.status || "—"} · {age(live.median_event_age_seconds)}</div>
    </section>
  </div>;
}

function Research({ control }: { control: ControlResponse | null }) {
  const p = control?.promotion || {};
  const current = p.current_evaluation || {};
  const shadow = control?.shadow || {};
  const health = control?.health || {};
  const runtime = control?.runtime || {};
  const decision = pick(p, "latest_decision") || {};
  const gates: Array<[string, string, string]> = [
    ["MARKET DATA", String(health.primary_market_data?.status || "—"), health.primary_market_data?.status === "READY_LIVE" ? "good" : "warn"],
    ["DATABASE", String(health.database?.status || (health.database?.ready ? "READY" : "—")), health.database?.ready ? "good" : "bad"],
    ["RUNTIME", String(health.autonomous_runtime?.status || "—"), health.autonomous_runtime?.running ? "good" : "warn"],
    ["SHADOW", String(shadow?.status || "—"), shadow?.status === "HEALTHY" ? "good" : "warn"],
    ["PROMOTION", String(current.status || current.state || decision.status || "BLOCKED"), current.eligible || decision.allowed ? "good" : "bad"],
  ];
  return <section className="panel">
    <div className="section-head"><div><small>RESEARCH CONTROL</small><h2>Evidence gates</h2></div><span className="section-note">no execution authority</span></div>
    <div className="gates">{gates.map(([name,state,tone]) => <div key={name} className="gate"><span>{name}</span><b className={tone}>{state}</b><small>{name==="PROMOTION" ? "automatic promotion disabled" : "backend state"}</small></div>)}</div>
    <div className="research-strip"><span>Runtime runs</span><b>{Array.isArray(runtime.items) ? runtime.items.length : 0}</b><span>Decision</span><b>{decision.status || decision.decision || "—"}</b><span>Build</span><b>{health.build?.commit || health.build?.version || "—"}</b></div>
  </section>;
}

function App() {
  const [selected, setSelected] = useState<SymbolId>("GGAL");
  const [timeframe, setTimeframe] = useState<Timeframe>("1D");
  const [matrix, setMatrix] = useState<MatrixResponse | null>(null);
  const [terminal, setTerminal] = useState<TerminalResponse | null>(null);
  const [chart, setChart] = useState<ChartResponse | null>(null);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [bcra, setBcra] = useState<JsonMap | null>(null);
  const [control, setControl] = useState<ControlResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const firstLoad = useRef(true);

  const load = async (kind: "hot" | "cold") => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 12_000);
    try {
      const results = await Promise.allSettled(
        kind === "hot"
          ? [fetchMatrix(controller.signal), fetchTerminal(selected, controller.signal)]
          : [fetchMatrix(controller.signal), fetchTerminal(selected, controller.signal), fetchHealth(controller.signal), fetchControl(controller.signal), fetchBcra(controller.signal)]
      );
      let failed = 0;
      const values = results.map(r => r.status === "fulfilled" ? r.value : null);
      if (results[0].status === "fulfilled") setMatrix(values[0] as MatrixResponse); else failed++;
      if (results[1].status === "fulfilled") setTerminal(values[1] as TerminalResponse); else failed++;
      if (kind === "cold") {
        if (results[2].status === "fulfilled") setHealth(values[2] as HealthResponse); else failed++;
        if (results[3].status === "fulfilled") setControl(values[3] as ControlResponse); else failed++;
        if (results[4].status === "fulfilled") setBcra(values[4] as JsonMap); else failed++;
      }
      if (failed >= (kind === "cold" ? 4 : 2)) setError("BACKEND DEGRADED · mostrando el último estado confirmado");
      else setError(null);
      setTick(v => v + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : "BACKEND DEGRADED");
    } finally {
      window.clearTimeout(timeout);
    }
  };

  useEffect(() => {
    void load("cold");
    const hot = window.setInterval(() => void load("hot"), 5_000);
    const cold = window.setInterval(() => void load("cold"), 30_000);
    return () => { window.clearInterval(hot); window.clearInterval(cold); };
  }, [selected]);

  useEffect(() => {
    const controller = new AbortController();
    void fetchChart(selected, timeframe, controller.signal).then(setChart).catch(() => setChart(null));
    return () => controller.abort();
  }, [selected, timeframe]);

  const session = terminal?.session || matrix?.argentina_session || health?.market_session || {};
  const isOpen = Boolean(session.open);
  const lastUpdated = useMemo(() => new Date(), [tick]);

  return <main>
    <header className="topbar">
      <Brand />
      <div className="top-status">
        <StatusPill label={`BYMA · ${isOpen ? "OPEN" : "CLOSED"}`} tone={isOpen ? "good" : "warn"} />
        <StatusPill label={error ? "DEGRADED" : "LIVE BUS"} tone={error ? "warn" : "good"} />
        <span className="update">updated {lastUpdated.toLocaleTimeString("es-AR", { hour12: false })}</span>
      </div>
    </header>
    <div className="hero">
      <div><small>QUANTITATIVE MARKET SYSTEM · RESEARCH ONLY</small><h1>Argentina, <span>from the event bus outward.</span></h1><p>Hot market state, canonical series, macro context and research gates share one observable contract. The terminal never grants execution authority.</p></div>
      <div className="hero-stat"><b>{terminal?.quote?.last != null ? fmt(terminal.quote.last,4) : "—"}</b><span>{selected} · {terminal?.quote?.source || "awaiting source"}</span></div>
    </div>
    <Matrix data={matrix} selected={selected} onSelect={setSelected} />
    <PriceChart data={chart} selected={selected} timeframe={timeframe} onTimeframe={setTimeframe} />
    <Detail data={terminal} bcra={bcra} health={health} />
    <Research control={control} />
    <footer><span>Gorila Argentum · RESEARCH MODE · EXECUTION OFF</span><span>tick {tick} · {when(terminal?.quote?.quoteTimestamp)}</span></footer>
  </main>;
}

export default App;
