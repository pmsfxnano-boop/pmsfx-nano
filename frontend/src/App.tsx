import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent,
  type ReactNode,
} from "react";
import {
  API_BASE,
  fetchEvidence,
  fetchHealth,
  fetchHistory,
  fetchMarketStream,
  fetchProspectiveStatus,
  type Candle,
  type EvidenceSnapshot,
  type MarketStreamResponse,
  type ProspectiveStatus,
  type StreamEvent,
  type SymbolSnapshot,
} from "./api";

type View = "terminal" | "opportunity" | "research" | "quality";
type SymbolId = "BTCUSDT" | "ETHUSDT" | "SOLUSDT";
type Resolution = "1m" | "5m" | "15m" | "1h" | "4h" | "1d";

const SYMBOLS: SymbolId[] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

const RESOLUTIONS: Array<{ id: Resolution; label: string; ms: number }> = [
  { id: "1m", label: "1m", ms: 60_000 },
  { id: "5m", label: "5m", ms: 300_000 },
  { id: "15m", label: "15m", ms: 900_000 },
  { id: "1h", label: "1H", ms: 3_600_000 },
  { id: "4h", label: "4H", ms: 14_400_000 },
  { id: "1d", label: "1D", ms: 86_400_000 },
];

function price(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  if (value >= 10_000) return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (value >= 100) return value.toLocaleString("en-US", { maximumFractionDigits: 3 });
  return value.toLocaleString("en-US", { maximumFractionDigits: 5 });
}

function compact(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(value);
}

function pct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function bp(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(2)} bp`;
}

function age(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  if (value < 1000) return `${Math.round(value)}ms`;
  if (value < 10_000) return `${(value / 1000).toFixed(1)}s`;
  return `${Math.round(value / 1000)}s`;
}

function timeOf(value: string | null | undefined) {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "—";
  return dt.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function normalizeStatus(status: string) {
  return status === "LIVE" ? "live" : status === "DEGRADED" || status === "DELAYED" ? "warn" : "off";
}

function Status({ status, label = true }: { status: string; label?: boolean }) {
  return (
    <span className={`status-chip status-${normalizeStatus(status)}`}>
      <i />
      {label && <b>{status}</b>}
    </span>
  );
}

function Icon({ name }: { name: "pulse" | "clock" | "research" | "shield" | "chart" | "arrow" }) {
  const common = { width: 16, height: 16, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.7, strokeLinecap: "round" as const, strokeLinejoin: "round" as const };
  if (name === "clock") return <svg {...common}><circle cx="12" cy="12" r="8.5" /><path d="M12 7.5v5l3.3 2" /></svg>;
  if (name === "research") return <svg {...common}><path d="M5 18V8.5M11.5 18V5M18 18v-7.5" /><path d="M3 18.5h18" /></svg>;
  if (name === "shield") return <svg {...common}><path d="M12 3.5 19 6v5.1c0 4.4-2.5 7.5-7 9.4-4.5-1.9-7-5-7-9.4V6z" /><path d="m9.2 12 1.8 1.8 3.8-4" /></svg>;
  if (name === "chart") return <svg {...common}><path d="M4 18.5V5.5M4 18.5h17" /><path d="m7 15 3.3-4 3.1 2.2 4.4-6 2.2 2" /></svg>;
  if (name === "arrow") return <svg {...common}><path d="M5 12h13" /><path d="m13 7 5 5-5 5" /></svg>;
  return <svg {...common}><path d="M4 12a8 8 0 0 1 16 0" /><path d="M6 16a6 6 0 0 0 12 0" /><path d="M8 8a5.7 5.7 0 0 1 8 0" /></svg>;
}

function Brand() {
  return (
    <div className="brand">
      <div className="brand-g"><span>G</span><em /></div>
      <div className="brand-text">
        <strong>GORILA</strong>
        <small>ARGENTUM</small>
      </div>
    </div>
  );
}

function TopNav({ view, onView }: { view: View; onView: (next: View) => void }) {
  const items: Array<{ id: View; label: string; icon: "chart" | "clock" | "research" | "shield" }> = [
    { id: "terminal", label: "Terminal", icon: "chart" },
    { id: "opportunity", label: "Opportunity", icon: "clock" },
    { id: "research", label: "Research", icon: "research" },
    { id: "quality", label: "Quality", icon: "shield" },
  ];
  return (
    <nav className="top-nav" aria-label="Workspace">
      {items.map((item) => (
        <button
          key={item.id}
          className={view === item.id ? "nav-item active" : "nav-item"}
          onClick={() => onView(item.id)}
        >
          <Icon name={item.icon} />
          <span>{item.label}</span>
        </button>
      ))}
    </nav>
  );
}

function SymbolStrip({
  snapshots,
  selected,
  onSelect,
  eventsBySymbol,
}: {
  snapshots: SymbolSnapshot[];
  selected: SymbolId;
  onSelect: (symbol: SymbolId) => void;
  eventsBySymbol: Record<string, StreamEvent[]>;
}) {
  return (
    <div className="symbol-strip">
      {SYMBOLS.map((symbol) => {
        const item = snapshots.find((s) => s.symbol === symbol);
        const events = eventsBySymbol[symbol] || [];
        const trend = item?.window_change_pct ?? 0;
        return (
          <button
            key={symbol}
            className={selected === symbol ? "symbol-card selected" : "symbol-card"}
            onClick={() => onSelect(symbol)}
          >
            <div className="symbol-card-top">
              <span className="symbol-name"><Status status={item?.status ?? "OFF"} label={false} />{symbol.replace("USDT", "")}<small>/USDT</small></span>
              <span className={trend >= 0 ? "positive" : "negative"}>{pct(item?.window_change_pct)}</span>
            </div>
            <div className="symbol-card-mid">
              <strong>{price(item?.price)}</strong>
              <span>{age(item?.freshness_ms)}</span>
            </div>
            <MiniSpark events={events} trend={trend} />
            <div className="symbol-card-bottom">
              <span>bid {price(item?.bid)}</span>
              <span>ask {price(item?.ask)}</span>
              <span>{bp(item?.spread_bps)}</span>
            </div>
          </button>
        );
      })}
    </div>
  );
}

function MiniSpark({ events, trend }: { events: StreamEvent[]; trend: number }) {
  const values = events.filter((e) => e.event_type === "trade" && e.price != null).slice(-28).map((e) => e.price!);
  const data = values.length >= 2 ? values : [1, 1 + trend / 100];
  const min = Math.min(...data);
  const max = Math.max(...data);
  const points = data.map((v, i) => {
    const x = (i / Math.max(1, data.length - 1)) * 100;
    const y = 26 - ((v - min) / Math.max(1e-9, max - min)) * 20;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <svg className="mini-spark" viewBox="0 0 100 30" preserveAspectRatio="none" aria-hidden="true">
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.6" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

function ChartCanvas({
  candles,
  livePrice,
  selected,
  resolution,
}: {
  candles: Candle[];
  livePrice: number | null;
  selected: string;
  resolution: Resolution;
}) {
  const ref = useRef<HTMLCanvasElement | null>(null);
  const wrap = useRef<HTMLDivElement | null>(null);
  const [hover, setHover] = useState<{ x: number; candle: Candle } | null>(null);

  useEffect(() => {
    const canvas = ref.current;
    const parent = wrap.current;
    if (!canvas || !parent) return;

    const draw = () => {
      const rect = parent.getBoundingClientRect();
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.max(1, rect.width);
      const height = Math.max(1, rect.height);
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext("2d");
      if (!ctx) return;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const pad = { left: 18, right: 18, top: 20, bottom: 30 };
      const plotW = width - pad.left - pad.right;
      const plotH = height - pad.top - pad.bottom;
      ctx.fillStyle = "#071017";
      ctx.fillRect(0, 0, width, height);

      for (let i = 1; i <= 5; i++) {
        const y = pad.top + (plotH * i) / 6;
        ctx.strokeStyle = "rgba(154,178,193,.085)";
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
      }

      if (!candles.length) {
        ctx.fillStyle = "rgba(194,210,219,.48)";
        ctx.font = "12px Inter, sans-serif";
        ctx.fillText("Acumulando histórico…", pad.left, height / 2);
        return;
      }

      const rangeValues = candles.flatMap((c) => [c.high, c.low]);
      if (livePrice != null) rangeValues.push(livePrice);
      let min = Math.min(...rangeValues);
      let max = Math.max(...rangeValues);
      const margin = Math.max((max - min) * 0.08, Math.abs(max) * 0.00025);
      min -= margin;
      max += margin;

      const yFor = (v: number) => pad.top + (1 - (v - min) / Math.max(1e-9, max - min)) * plotH;
      const candleW = Math.max(2, Math.min(12, (plotW / candles.length) * 0.58));
      const step = plotW / Math.max(1, candles.length);

      candles.forEach((c, i) => {
        const x = pad.left + i * step + step / 2;
        const positive = c.close >= c.open;
        ctx.strokeStyle = positive ? "#65e0ae" : "#ff6a7b";
        ctx.fillStyle = positive ? "#65e0ae" : "#ff6a7b";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, yFor(c.high));
        ctx.lineTo(x, yFor(c.low));
        ctx.stroke();
        const top = yFor(Math.max(c.open, c.close));
        const bottom = yFor(Math.min(c.open, c.close));
        ctx.globalAlpha = 0.88;
        ctx.fillRect(x - candleW / 2, top, candleW, Math.max(1.3, bottom - top));
        ctx.globalAlpha = 1;
      });

      if (livePrice != null) {
        const y = yFor(livePrice);
        ctx.setLineDash([5, 5]);
        ctx.strokeStyle = "#69d5e7";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = "#69d5e7";
        ctx.font = "10px IBM Plex Mono, monospace";
        ctx.textAlign = "right";
        ctx.fillText(price(livePrice), width - pad.right, y - 5);
        ctx.textAlign = "left";
      }

      ctx.fillStyle = "rgba(167,187,198,.55)";
      ctx.font = "9px IBM Plex Mono, monospace";
      ctx.fillText(selected.replace("USDT", ""), pad.left, 11);
      ctx.textAlign = "right";
      ctx.fillText(resolution.toUpperCase(), width - pad.right, 11);
      ctx.textAlign = "left";
    };

    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(parent);
    return () => observer.disconnect();
  }, [candles, livePrice, selected, resolution]);

  const onMove = (event: MouseEvent<HTMLCanvasElement>) => {
    if (!candles.length || !ref.current) return;
    const bounds = ref.current.getBoundingClientRect();
    const frac = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width));
    const idx = Math.min(candles.length - 1, Math.max(0, Math.round(frac * (candles.length - 1))));
    setHover({ x: event.clientX - bounds.left, candle: candles[idx] });
  };

  return (
    <div className="chart-canvas-wrap" ref={wrap}>
      <canvas ref={ref} onMouseMove={onMove} onMouseLeave={() => setHover(null)} />
      {hover && (
        <div className="chart-hover" style={{ left: Math.max(8, hover.x - 74) } as CSSProperties}>
          <strong>{price(hover.candle.close)}</strong>
          <span>{timeOf(hover.candle.time)}</span>
          <div>O {price(hover.candle.open)} · H {price(hover.candle.high)}</div>
          <div>L {price(hover.candle.low)} · V {compact(hover.candle.volume)}</div>
        </div>
      )}
    </div>
  );
}

function OpportunityClock({ evidence }: { evidence: EvidenceSnapshot | null }) {
  const clock = evidence?.opportunity_clock;
  const active = clock?.state === "ACTIVE" && clock.validated;
  const remaining = clock?.remaining_seconds ?? null;
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  void now;
  const mins = remaining == null ? null : Math.floor(Math.max(0, remaining) / 60);
  const secs = remaining == null ? null : Math.floor(Math.max(0, remaining) % 60);
  const progress = active && remaining != null && clock?.horizon_ms
    ? Math.max(0, Math.min(1, remaining / (clock.horizon_ms / 1000)))
    : 0;

  return (
    <section className={active ? "clock-panel active" : "clock-panel locked"}>
      <div className="panel-kicker">
        <Icon name="clock" />OPPORTUNITY CLOCK
        <span>{clock?.mode ?? "SHADOW"}</span>
      </div>
      <div className="clock-body">
        <div className="clock-ring" style={{ "--progress": progress } as CSSProperties}>
          <div className="clock-core">
            <small>{active ? "WINDOW" : "GATE"}</small>
            <strong>{active && mins != null && secs != null ? `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}` : "LOCKED"}</strong>
            <span>{active ? `${Math.round((clock.horizon_ms ?? 0) / 1000)}s` : "PIT / OOS"}</span>
          </div>
        </div>
        <div className="clock-copy">
          <div className="clock-state"><Status status={active ? "LIVE" : "GATED"} /><strong>{active ? "VALIDATED WINDOW" : "WAITING FOR EVIDENCE"}</strong></div>
          <p>
            {active
              ? "Temporal state is backed by the validated quantitative evidence contract."
              : "The clock cannot activate until the prospective cohort, current Quality Gate, PIT/OOS and forecast/opportunity evidence all pass."}
          </p>
          <div className="clock-metrics">
            <div><span>COHORT</span><strong>{evidence?.cohort.status ?? "SYNC"}</strong></div>
            <div><span>QUALITY</span><strong>{evidence?.quality_gate.state ?? "SYNC"}</strong></div>
            <div><span>PIT / OOS</span><strong>{evidence?.pit_oos.state ?? "SYNC"}</strong></div>
          </div>
        </div>
      </div>
      <div className="clock-foot">
        <span>{active ? "MODEL STATE" : "BLOCKERS"}</span>
        <strong>{active ? "PROMOTION-ELIGIBLE" : String(clock?.blockers?.length ?? 0) + " BLOCKERS"}</strong>
      </div>
    </section>
  );
}

function EvidenceMatrix({ evidence }: { evidence: EvidenceSnapshot | null }) {
  const gates = [
    ["COHORT", evidence?.cohort.status ?? "SYNC", evidence?.cohort.mature],
    ["QUALITY", evidence?.quality_gate.state ?? "SYNC", evidence?.quality_gate.state === "PASS"],
    ["PIT / OOS", evidence?.pit_oos.state ?? "SYNC", evidence?.pit_oos.promotion_eligible],
    ["FORECAST", evidence?.forecast_shadow.state ?? "SYNC", (evidence?.forecast_shadow.count ?? 0) > 0],
    ["OPPORTUNITY", evidence?.opportunity_shadow.count ? "PRESENT" : "EMPTY", (evidence?.opportunity_shadow.count ?? 0) > 0],
  ] as const;
  return (
    <section className="section-block evidence-matrix">
      <SectionHead icon="shield" kicker="EVIDENCE BUS" title="Promotion gates" note={evidence?.generated_at ? timeOf(evidence.generated_at) : "SYNC"} />
      <div className="evidence-grid">
        {gates.map(([name, state, passed]) => (
          <div className="evidence-card" key={name}>
            <span>{name}</span>
            <strong className={passed ? "positive" : "muted"}>{state}</strong>
            <small>{passed ? "gate satisfied" : "not yet satisfied"}</small>
          </div>
        ))}
      </div>
    </section>
  );
}

function MetricCard({
  label,
  value,
  helper,
  accent = false,
}: {
  label: string;
  value: string;
  helper?: string;
  accent?: boolean;
}) {
  return (
    <div className={accent ? "metric-card accent" : "metric-card"}>
      <span>{label}</span>
      <strong>{value}</strong>
      {helper && <small>{helper}</small>}
    </div>
  );
}

function Microstructure({ snapshot, events }: { snapshot?: SymbolSnapshot; events: StreamEvent[] }) {
  const imbalance = snapshot?.imbalance;
  const imbalanceText = imbalance == null ? "—" : `${(imbalance * 100).toFixed(1)}%`;
  const pressure = imbalance == null ? "—" : imbalance >= 0 ? "BID PRESSURE" : "ASK PRESSURE";
  const trades = events.filter((event) => event.event_type === "trade" && event.price != null).slice(-120);
  const grossQty = trades.reduce((sum, event) => sum + (event.quantity ?? 0), 0);
  const signedQty = trades.reduce((sum, event) => sum + (event.side === "BUY" ? 1 : -1) * (event.quantity ?? 0), 0);
  const flow = grossQty > 0 ? signedQty / grossQty : null;
  const oldest = trades.length ? new Date(trades[0].received_time).getTime() : 0;
  const newest = trades.length ? new Date(trades[trades.length - 1].received_time).getTime() : 0;
  const spanMin = oldest && newest ? Math.max((newest - oldest) / 60_000, 1 / 60) : null;
  const printsPerMin = spanMin ? trades.length / spanMin : null;
  return (
    <section className="section-block">
      <SectionHead icon="pulse" kicker="MICROSTRUCTURE" title="Market state" note="top-of-book + trade flow" />
      <div className="micro-grid">
        <MetricCard label="MID" value={price(snapshot?.price)} helper="trade / quote mid" accent />
        <MetricCard label="BID" value={price(snapshot?.bid)} helper={compact(snapshot?.bid_qty)} />
        <MetricCard label="ASK" value={price(snapshot?.ask)} helper={compact(snapshot?.ask_qty)} />
        <MetricCard label="SPREAD" value={bp(snapshot?.spread_bps)} helper="quote width" />
        <MetricCard label="QUEUE" value={imbalanceText} helper={pressure} />
        <MetricCard label="TRADE FLOW" value={flow == null ? "—" : `${(flow * 100).toFixed(1)}%`} helper={flow == null ? "no prints" : flow >= 0 ? "buy aggression" : "sell aggression"} />
        <MetricCard label="PRINTS / MIN" value={printsPerMin == null ? "—" : printsPerMin.toFixed(1)} helper="recent 120 trades" />
        <MetricCard label="FRESHNESS" value={age(snapshot?.freshness_ms)} helper="receive age" />
      </div>
      <div className="imbalance-bar">
        <span style={{ width: `${Math.max(3, Math.min(97, ((imbalance ?? 0) + 1) * 50))}%` }} />
      </div>
      <div className="imbalance-labels"><span>BID QUEUE</span><span>NEUTRAL</span><span>ASK QUEUE</span></div>
    </section>
  );
}

function SectionHead({
  icon,
  kicker,
  title,
  note,
}: {
  icon: "pulse" | "clock" | "research" | "shield" | "chart" | "arrow";
  kicker: string;
  title: string;
  note?: ReactNode;
}) {
  return (
    <div className="section-head">
      <div className="section-head-left">
        <div className="section-icon"><Icon name={icon} /></div>
        <div>
          <span className="section-kicker">{kicker}</span>
          <h2>{title}</h2>
        </div>
      </div>
      <div className="section-note">{note}</div>
    </div>
  );
}

function Tape({ events }: { events: StreamEvent[] }) {
  const rows = events.filter((e) => e.event_type === "trade").slice(-34).reverse();
  return (
    <section className="section-block tape-panel">
      <SectionHead icon="pulse" kicker="FLOW" title="Live tape" note={<span>{rows.length} prints</span>} />
      <div className="tape-table">
        <div className="tape-head"><span>TIME</span><span>SIDE</span><span>PRICE</span><span>SIZE</span></div>
        <div className="tape-scroll">
          {rows.map((event) => (
            <div className="tape-row" key={event.ledger_seq}>
              <span>{timeOf(event.received_time)}</span>
              <span className={event.side === "BUY" ? "positive" : "negative"}>{event.side}</span>
              <strong>{price(event.price)}</strong>
              <span>{compact(event.quantity)}</span>
            </div>
          ))}
          {!rows.length && <div className="empty">Esperando prints…</div>}
        </div>
      </div>
    </section>
  );
}

function Timeline() {
  const steps = [
    { label: "MARKET", state: "LIVE", detail: "Binance websocket" },
    { label: "LEDGER", state: "DURABLE", detail: "PostgreSQL capture" },
    { label: "QUALITY", state: "GATED", detail: "Freshness + integrity" },
    { label: "RESEARCH", state: "SHADOW", detail: "PIT / OOS cohort" },
    { label: "PROMOTION", state: "BLOCKED", detail: "No validated model yet" },
  ];
  return (
    <section className="section-block timeline-panel">
      <SectionHead icon="research" kicker="PIPELINE" title="Quantitative timeline" note="single system" />
      <div className="timeline">
        {steps.map((step, i) => (
          <div className="timeline-step" key={step.label}>
            <div className={i === 0 ? "timeline-node active" : "timeline-node"} />
            {i < steps.length - 1 && <div className="timeline-line" />}
            <div className="timeline-copy">
              <strong>{step.label}</strong>
              <span>{step.detail}</span>
              <b>{step.state}</b>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function ResearchView({ status, evidence }: { status: ProspectiveStatus | null; evidence: EvidenceSnapshot | null }) {
  const live = status?.symbols_live;
  const stages = [
    ["Prospective capture", evidence?.cohort.status ?? (live ? "LIVE" : "STARTING"), evidence?.cohort.session_id ? `${evidence.cohort.progress_pct.toFixed(2)}% of protocol window` : "No active cohort."],
    ["Data quality", evidence?.quality_gate.state ?? "WAITING", evidence?.quality_gate.current_session ? "Current capture report" : "Waiting for current-session quality report."],
    ["PIT / OOS", evidence?.pit_oos.state ?? "BLOCKED", evidence?.pit_oos.oos_rows ? `${compact(evidence.pit_oos.oos_rows)} OOS rows` : "No valid OOS rows yet."],
    ["Forecast", evidence?.forecast_shadow.state ?? "EMPTY", evidence?.forecast_shadow.count ? `${compact(evidence.forecast_shadow.count)} shadow forecasts` : "Shadow forecast store empty."],
    ["Execution", "OFF", "Execution remains disabled by design."],
  ];
  const regimeRows = Object.entries(evidence?.regime.symbols ?? {});
  return (
    <div className="view-stack">
      <section className="hero-panel compact-hero">
        <div>
          <span className="eyebrow">RESEARCH CONTROL</span>
          <h1>Evidence before promotion.</h1>
          <p>Research state is now connected to the exact cohort, Quality Gate, PIT/OOS artifacts and forecast shadow persisted by the quantitative backend.</p>
        </div>
        <div className="research-lock"><Icon name="shield" /><span>MODEL PROMOTION</span><strong>{evidence?.pit_oos.promotion_eligible ? "ELIGIBLE" : "BLOCKED"}</strong></div>
      </section>
      <EvidenceMatrix evidence={evidence} />
      <section className="section-block">
        <SectionHead icon="research" kicker="STATE MACHINE" title="Research pipeline" note={evidence?.research.status ?? "SYNC"} />
        <div className="stage-grid">
          {stages.map(([name, state, detail]) => (
            <div className="stage-card" key={name}>
              <span>{name}</span>
              <strong>{state}</strong>
              <p>{detail}</p>
            </div>
          ))}
        </div>
      </section>
      <section className="section-block">
        <SectionHead icon="chart" kicker="OBSERVED REGIMES" title="Regime layer" note={evidence?.regime.status ?? "SYNC"} />
        <div className="regime-grid">
          {regimeRows.map(([symbol, row]) => (
            <div className="regime-card" key={symbol}>
              <span>{symbol.replace("USDT", "")}</span>
              <strong>{String(row.state)}</strong>
              <small>{row.n_trades ? `${row.n_trades} recent trades` : "sample not mature"}</small>
              <b>{row.validated ? "VALIDATED" : "OBSERVED ONLY"}</b>
            </div>
          ))}
        </div>
      </section>
      <Timeline />
    </div>
  );
}

function QualityView({
  health,
  status,
  evidence,
}: {
  health: Record<string, unknown> | null;
  status: ProspectiveStatus | null;
  evidence: EvidenceSnapshot | null;
}) {
  const symbolHealth = (health?.symbol_health as Array<Record<string, unknown>> | undefined) ?? [];
  const sources = status?.source_health ?? [];
  return (
    <div className="view-stack">
      <section className="hero-panel compact-hero">
        <div>
          <span className="eyebrow">DATA QUALITY</span>
          <h1>Every green state has evidence.</h1>
          <p>Current-session Quality Gate, transport health and PIT/OOS readiness are read from the same persisted evidence contract.</p>
        </div>
        <Status status={String(health?.status ?? "SYNC")} />
      </section>
      <EvidenceMatrix evidence={evidence} />
      <section className="quality-grid">
        <div className="section-block">
          <SectionHead icon="shield" kicker="SYMBOL HEALTH" title="Required symbols" />
          <div className="quality-list">
            {SYMBOLS.map((symbol) => {
              const item = symbolHealth.find((row) => row.symbol === symbol);
              return (
                <div className="quality-row" key={symbol}>
                  <strong>{symbol.replace("USDT", "")}</strong>
                  <Status status={String(item?.status ?? "UNKNOWN")} />
                  <span>{item?.last_received_time ? timeOf(String(item.last_received_time)) : "—"}</span>
                </div>
              );
            })}
          </div>
        </div>
        <div className="section-block">
          <SectionHead icon="shield" kicker="RUNTIME" title="System health" />
          <div className="quality-list">
            {[
              ["Worker", health?.worker_alive ? "LIVE" : "OFF"],
              ["Quality monitor", health?.quality_monitor_alive ? "LIVE" : "OFF"],
              ["Heartbeat", health?.heartbeat_alive ? "LIVE" : "OFF"],
              ["Storage", String(health?.storage_backend ?? "UNKNOWN").toUpperCase()],
            ].map(([label, value]) => (
              <div className="quality-row" key={label}>
                <strong>{label}</strong><span>{value}</span><span className="muted">runtime</span>
              </div>
            ))}
          </div>
        </div>
        <div className="section-block full">
          <SectionHead icon="research" kicker="QUALITY REPORT" title="Current evidence" note={evidence?.quality_gate.status ?? "SYNC"} />
          <div className="quality-list">
            <div className="quality-row"><strong>Rows</strong><span>{compact(evidence?.quality_gate.rows ?? null)}</span><span>{evidence?.quality_gate.current_session ? "current cohort" : "historical"}</span></div>
            <div className="quality-row"><strong>Replay fingerprint</strong><span>{evidence?.quality_gate.replay_fingerprint ? evidence.quality_gate.replay_fingerprint.slice(0, 12) : "—"}</span><span>{evidence?.quality_gate.reasons.length ? `${evidence.quality_gate.reasons.length} reasons` : "no blockers"}</span></div>
            <div className="quality-row"><strong>PIT/OOS rows</strong><span>{compact(evidence?.pit_oos.oos_rows ?? null)}</span><span>{evidence?.pit_oos.state ?? "—"}</span></div>
          </div>
        </div>
        <div className="section-block full">
          <SectionHead icon="research" kicker="SOURCE HEALTH" title="Ingestion channels" />
          <div className="source-grid">
            {sources.map((source, index) => (
              <div className="source-card" key={String(source.source ?? index)}>
                <span>{String(source.source ?? "source")}</span>
                <strong>{String(source.status ?? "UNKNOWN")}</strong>
                <small>last event {timeOf(String(source.last_event_time ?? ""))}</small>
              </div>
            ))}
            {!sources.length && <div className="empty">Aún sin estado persistido.</div>}
          </div>
        </div>
      </section>
    </div>
  );
}

function TerminalView({
  selected,
  snapshots,
  events,
  candles,
  resolution,
  onResolution,
  historyLoading,
  historyError,
  status,
  evidence,
}: {
  selected: SymbolId;
  snapshots: SymbolSnapshot[];
  events: Record<string, StreamEvent[]>;
  candles: Candle[];
  resolution: Resolution;
  onResolution: (resolution: Resolution) => void;
  historyLoading: boolean;
  historyError: string | null;
  status: ProspectiveStatus | null;
  evidence: EvidenceSnapshot | null;
}) {
  const snapshot = snapshots.find((item) => item.symbol === selected);
  const selectedEvents = events[selected] || [];
  return (
    <div className="view-stack">
      <section className="hero-grid">
        <div className="hero-panel">
          <div className="hero-copy">
            <span className="eyebrow"><Status status={snapshot?.status ?? "SYNC"} label={false} />LIVE MARKET TERMINAL</span>
            <div className="hero-symbol"><h1>{selected.replace("USDT", "")}</h1><span>/ USDT</span><b className={snapshot?.window_change_pct != null && snapshot.window_change_pct >= 0 ? "positive" : "negative"}>{pct(snapshot?.window_change_pct)}</b></div>
            <strong className="hero-price">{price(snapshot?.price)}</strong>
            <div className="hero-meta">
              <span>Binance Spot</span>
              <span>trade + bookTicker</span>
              <span>ledger seq {selectedEvents.at(-1)?.ledger_seq ?? "—"}</span>
            </div>
          </div>
          <div className="hero-rail">
            <div><span>Bid</span><strong>{price(snapshot?.bid)}</strong></div>
            <div><span>Ask</span><strong>{price(snapshot?.ask)}</strong></div>
            <div><span>Spread</span><strong>{bp(snapshot?.spread_bps)}</strong></div>
          </div>
        </div>
        <OpportunityClock evidence={evidence} />
      </section>

      <section className="section-block chart-panel">
        <div className="chart-header">
          <div>
            <span className="section-kicker">PRICE ACTION</span>
            <h2>{selected.replace("USDT", "")} historical market structure</h2>
          </div>
          <div className="resolution-tabs">
            {RESOLUTIONS.map((item) => (
              <button key={item.id} className={resolution === item.id ? "resolution active" : "resolution"} onClick={() => onResolution(item.id)}>
                {item.label}
              </button>
            ))}
          </div>
        </div>
        {historyError && <div className="inline-error">{historyError}</div>}
        <div className="chart-frame">
          {historyLoading && <div className="chart-loading"><span />Fetching durable history…</div>}
          <ChartCanvas candles={candles} livePrice={snapshot?.price ?? null} selected={selected} resolution={resolution} />
        </div>
        <div className="chart-foot">
          <span>Source: durable PostgreSQL ledger</span>
          <span>{candles.length} candles</span>
          <span>Live overlay: Binance projection</span>
        </div>
      </section>

      <div className="two-col">
        <Microstructure snapshot={snapshot} events={selectedEvents} />
        <Tape events={selectedEvents} />
      </div>

      <Timeline />
      <ResearchRibbon status={status} evidence={evidence} />
    </div>
  );
}

function ResearchRibbon({ status, evidence }: { status: ProspectiveStatus | null; evidence: EvidenceSnapshot | null }) {
  return (
    <section className="research-ribbon">
      <div><span>PROSPECTIVE</span><strong>{evidence?.cohort.status ?? (status?.symbols_live ? "ACTIVE" : "STARTING")}</strong></div>
      <div><span>QUALITY</span><strong>{evidence?.quality_gate.state ?? "WAITING"}</strong></div>
      <div><span>PIT / OOS</span><strong>{evidence?.pit_oos.state ?? "BLOCKED"}</strong></div>
      <div><span>PROMOTION</span><strong>{evidence?.pit_oos.promotion_eligible ? "ELIGIBLE" : "BLOCKED"}</strong></div>
      <div className="ribbon-note">Observed regimes remain descriptive until the exact PIT/OOS evidence gate validates them.</div>
    </section>
  );
}

export default function App() {
  const [view, setView] = useState<View>("terminal");
  const [selected, setSelected] = useState<SymbolId>("BTCUSDT");
  const [resolution, setResolution] = useState<Resolution>("5m");
  const [snapshots, setSnapshots] = useState<SymbolSnapshot[]>([]);
  const [events, setEvents] = useState<Record<string, StreamEvent[]>>({});
  const [candles, setCandles] = useState<Candle[]>([]);
  const [cursor, setCursor] = useState(0);
  const cursorRef = useRef(0);
  const [channel, setChannel] = useState("SYNC");
  const [error, setError] = useState<string | null>(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [researchStatus, setResearchStatus] = useState<ProspectiveStatus | null>(null);
  const [health, setHealth] = useState<Record<string, unknown> | null>(null);
  const [evidence, setEvidence] = useState<EvidenceSnapshot | null>(null);
  const polling = useRef(false);

  const loadMarket = useCallback(async (reset = false) => {
    if (polling.current) return;
    polling.current = true;
    try {
      const payload: MarketStreamResponse = await fetchMarketStream(
        reset ? 0 : cursorRef.current,
      );
      setSnapshots(payload.symbols);
      cursorRef.current = payload.next_cursor;
      setCursor(payload.next_cursor);
      setChannel(payload.status);
      setError(null);
      setEvents((previous) => {
        const next = { ...previous };
        for (const event of payload.events) {
          const current = next[event.symbol] ?? [];
          const merged = [...current, event];
          const seen = new Set<number>();
          next[event.symbol] = merged.filter((row) => {
            if (seen.has(row.ledger_seq)) return false;
            seen.add(row.ledger_seq);
            return true;
          }).slice(-720);
        }
        return next;
      });
    } catch (err) {
      setChannel("OFFLINE");
      setError(err instanceof Error ? err.message : "market_stream_error");
    } finally {
      polling.current = false;
    }
  }, []);

  const loadHistory = useCallback(async (symbol: SymbolId, nextResolution: Resolution) => {
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const payload = await fetchHistory(symbol, nextResolution);
      setCandles(payload.candles);
    } catch (err) {
      setCandles([]);
      setHistoryError(err instanceof Error ? err.message : "history_error");
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  const loadState = useCallback(async () => {
    try {
      const [status, healthPayload, evidencePayload] = await Promise.all([
        fetchProspectiveStatus(),
        fetchHealth(),
        fetchEvidence(),
      ]);
      setResearchStatus(status);
      setHealth(healthPayload);
      setEvidence(evidencePayload);
    } catch {
      // Market stream remains the primary live channel.
    }
  }, []);

  useEffect(() => {
    loadMarket(true);
    loadState();
    const timer = window.setInterval(() => {
      if (!document.hidden) loadMarket(false);
    }, 750);
    const stateTimer = window.setInterval(() => {
      if (!document.hidden) loadState();
    }, 5000);
    return () => {
      window.clearInterval(timer);
      window.clearInterval(stateTimer);
    };
  }, [loadMarket, loadState]);

  useEffect(() => {
    loadHistory(selected, resolution);
  }, [selected, resolution, loadHistory]);

  const live = channel === "LIVE";
  const headerStatus = live ? "LIVE" : channel;
  const serviceHost = useMemo(() => new URL(API_BASE).hostname, []);

  return (
    <div className="app">
      <header className="topbar">
        <Brand />
        <TopNav view={view} onView={setView} />
        <div className="topbar-right">
          <Status status={headerStatus} />
          <span className="latency-label">READ MODEL · HOT</span>
        </div>
      </header>

      <main className="shell">
        <div className="shell-meta">
          <div><span>GORILA / ARGENTUM</span><b>QUANT TERMINAL</b></div>
          <span className="service-host">{serviceHost}</span>
        </div>

        <SymbolStrip snapshots={snapshots} selected={selected} onSelect={(next) => { setSelected(next); setView("terminal"); }} eventsBySymbol={events} />

        {view === "terminal" && (
          <TerminalView
            selected={selected}
            snapshots={snapshots}
            events={events}
            candles={candles}
            resolution={resolution}
            onResolution={setResolution}
            historyLoading={historyLoading}
            historyError={historyError}
            status={researchStatus}
            evidence={evidence}
          />
        )}

        {view === "opportunity" && (
          <div className="view-stack">
            <section className="hero-panel opportunity-hero">
              <span className="eyebrow">OPPORTUNITY LAYER</span>
              <h1>Time is a variable.</h1>
              <p>The Opportunity Clock lives here as a temporal research layer. Once a validated forecasting model exists, the clock can bind to setup onset, decay and execution windows without redesigning the terminal.</p>
            </section>
            <div className="opportunity-grid">
              <OpportunityClock evidence={evidence} />
              <Timeline />
            </div>
            <section className="section-block">
              <SectionHead icon="clock" kicker="CURRENT ASSET" title={selected.replace("USDT", "")} note={<Status status={snapshots.find((item) => item.symbol === selected)?.status ?? "SYNC"} />} />
              <div className="opportunity-stats">
                <MetricCard label="WINDOW" value={resolution.toUpperCase()} helper="selected horizon" accent />
                <MetricCard label="LAST TRADE" value={timeOf(snapshots.find((item) => item.symbol === selected)?.last_trade_time)} helper="exchange event" />
                <MetricCard label="FRESHNESS" value={age(snapshots.find((item) => item.symbol === selected)?.freshness_ms)} helper="receive age" />
                <MetricCard label="MODEL STATE" value="SHADOW" helper="no promotion" />
              </div>
            </section>
          </div>
        )}

        {view === "research" && <ResearchView status={researchStatus} evidence={evidence} />}
        {view === "quality" && <QualityView health={health} status={researchStatus} evidence={evidence} />}
      </main>

      <footer className="footer-bar">
        <span>LIVE MARKET · durable ledger · PIT/OOS gated</span>
        <span className="mono">cursor {cursor || "sync"}</span>
      </footer>

      {error && (
        <div className="toast">
          <Status status="DEGRADED" label={false} />
          <span>{error}</span>
          <button onClick={() => loadMarket(true)}>RECONNECT</button>
        </div>
      )}

      <div className="mobile-nav">
        <TopNav view={view} onView={setView} />
      </div>
    </div>
  );
}
