import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
} from "react";
import {
  API_BASE,
  fetchMarketStream,
  type StreamEvent,
  type SymbolSnapshot,
} from "./api";

const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"] as const;
const TIMEFRAMES = [
  { id: "1m", label: "1m", ms: 60_000 },
  { id: "5m", label: "5m", ms: 300_000 },
  { id: "15m", label: "15m", ms: 900_000 },
  { id: "1h", label: "1H", ms: 3_600_000 },
  { id: "1d", label: "1D", ms: 86_400_000 },
] as const;

type SymbolId = (typeof SYMBOLS)[number];
type TimeframeId = (typeof TIMEFRAMES)[number]["id"];
type ChannelState = "LIVE" | "DEGRADED" | "OFFLINE" | "SYNC";

type SeriesPoint = {
  t: number;
  y: number;
};

function fmtPrice(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "—";
  if (value >= 10_000) return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (value >= 100) return value.toLocaleString("en-US", { maximumFractionDigits: 3 });
  return value.toLocaleString("en-US", { maximumFractionDigits: 5 });
}

function fmtCompact(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(value);
}

function fmtMs(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "—";
  if (value < 1000) return `${Math.round(value)}ms`;
  return `${(value / 1000).toFixed(1)}s`;
}

function fmtTime(iso: string | null | undefined) {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 2,
  });
}

function toneForStatus(status: string): "live" | "warn" | "off" {
  if (status === "LIVE") return "live";
  if (status === "DEGRADED" || status === "DELAYED" || status === "STALE") return "warn";
  return "off";
}

function Logo() {
  return (
    <div className="brand" aria-label="Gorila Argentum">
      <div className="brand-mark" aria-hidden="true">
        <span>G</span>
        <i />
      </div>
      <div className="brand-copy">
        <strong>GORILA</strong>
        <span>ARGENTUM</span>
      </div>
    </div>
  );
}

function StatusDot({ status }: { status: string }) {
  const tone = toneForStatus(status);
  return <span className={`status-dot status-${tone}`} aria-label={status} />;
}

function SectionTitle({
  kicker,
  title,
  right,
}: {
  kicker?: string;
  title: string;
  right?: ReactNode;
}) {
  return (
    <div className="section-title">
      <div>
        {kicker && <span className="section-kicker">{kicker}</span>}
        <h2>{title}</h2>
      </div>
      {right}
    </div>
  );
}

function PriceChart({
  symbol,
  events,
  timeframe,
}: {
  symbol: string;
  events: StreamEvent[];
  timeframe: TimeframeId;
}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const [hover, setHover] = useState<{ x: number; y: number; point: SeriesPoint } | null>(null);

  const series = useMemo<SeriesPoint[]>(() => {
    const tf = TIMEFRAMES.find((item) => item.id === timeframe)?.ms ?? 300_000;
    const now = Date.now();
    const filtered = events
      .filter((event) => event.event_type === "trade" && event.price != null)
      .map((event) => ({ t: new Date(event.received_time).getTime(), y: event.price! }))
      .filter((point) => Number.isFinite(point.t) && now - point.t <= tf)
      .slice(-420);

    if (filtered.length > 1) return filtered;
    return events
      .filter((event) => event.event_type === "trade" && event.price != null)
      .slice(-240)
      .map((event) => ({ t: new Date(event.received_time).getTime(), y: event.price! }));
  }, [events, timeframe]);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wrap = wrapRef.current;
    if (!canvas || !wrap) return;

    const render = () => {
      const rect = wrap.getBoundingClientRect();
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.max(1, Math.floor(rect.width));
      const height = Math.max(1, Math.floor(rect.height));
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;

      const ctx = canvas.getContext("2d");
      if (!ctx) return;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const pad = { top: 22, right: 18, bottom: 24, left: 18 };
      const plotW = Math.max(1, width - pad.left - pad.right);
      const plotH = Math.max(1, height - pad.top - pad.bottom);

      ctx.fillStyle = "rgba(5, 10, 15, .42)";
      ctx.fillRect(0, 0, width, height);

      ctx.strokeStyle = "rgba(155, 176, 194, .09)";
      ctx.lineWidth = 1;
      for (let i = 1; i < 5; i++) {
        const y = pad.top + (plotH * i) / 5;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
      }
      for (let i = 1; i < 6; i++) {
        const x = pad.left + (plotW * i) / 6;
        ctx.beginPath();
        ctx.moveTo(x, pad.top);
        ctx.lineTo(x, height - pad.bottom);
        ctx.stroke();
      }

      if (series.length < 2) {
        ctx.fillStyle = "rgba(205, 220, 232, .50)";
        ctx.font = "12px Inter, system-ui, sans-serif";
        ctx.fillText("Esperando flujo de mercado…", pad.left, height / 2);
        return;
      }

      const ys = series.map((point) => point.y);
      const min = Math.min(...ys);
      const max = Math.max(...ys);
      const range = Math.max(max - min, Math.abs(max) * 0.0001, 0.0000001);
      const x0 = series[0].t;
      const x1 = series[series.length - 1].t || x0 + 1;
      const mapX = (value: number) => pad.left + ((value - x0) / Math.max(1, x1 - x0)) * plotW;
      const mapY = (value: number) => pad.top + (1 - (value - min) / range) * plotH;

      const area = ctx.createLinearGradient(0, pad.top, 0, height - pad.bottom);
      area.addColorStop(0, "rgba(81, 207, 230, .22)");
      area.addColorStop(1, "rgba(81, 207, 230, 0)");

      ctx.beginPath();
      series.forEach((point, index) => {
        const x = mapX(point.t);
        const y = mapY(point.y);
        if (index === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.lineTo(mapX(series[series.length - 1].t), height - pad.bottom);
      ctx.lineTo(mapX(series[0].t), height - pad.bottom);
      ctx.closePath();
      ctx.fillStyle = area;
      ctx.fill();

      ctx.beginPath();
      series.forEach((point, index) => {
        const x = mapX(point.t);
        const y = mapY(point.y);
        if (index === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = "#69d5e7";
      ctx.shadowColor = "rgba(105, 213, 231, .34)";
      ctx.shadowBlur = 10;
      ctx.lineWidth = 2;
      ctx.lineJoin = "round";
      ctx.lineCap = "round";
      ctx.stroke();
      ctx.shadowBlur = 0;

      const last = series[series.length - 1];
      const lastX = mapX(last.t);
      const lastY = mapY(last.y);
      ctx.fillStyle = "#0b131a";
      ctx.beginPath();
      ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
      ctx.fill();
      ctx.strokeStyle = "#8ce8f4";
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.arc(lastX, lastY, 3, 0, Math.PI * 2);
      ctx.stroke();

      ctx.fillStyle = "rgba(188, 207, 222, .52)";
      ctx.font = "10px ui-monospace, SFMono-Regular, Menlo, monospace";
      ctx.fillText(fmtPrice(max), pad.left, 13);
      ctx.fillText(fmtPrice(min), pad.left, height - 5);
      ctx.textAlign = "right";
      ctx.fillText(symbol, width - pad.right, 13);
      ctx.textAlign = "left";
    };

    const observer = new ResizeObserver(render);
    observer.observe(wrap);
    render();
    return () => observer.disconnect();
  }, [series, symbol]);

  const hitTest = (event: ReactMouseEvent<HTMLCanvasElement>) => {
    if (!series.length || !wrapRef.current) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const px = event.clientX - rect.left;
    const fraction = Math.max(0, Math.min(1, px / rect.width));
    const index = Math.round(fraction * (series.length - 1));
    const point = series[index];
    if (!point) return;
    setHover({ x: px, y: 22, point });
  };

  return (
    <div
      ref={wrapRef}
      className="chart-wrap"
      onMouseLeave={() => setHover(null)}
    >
      <canvas ref={canvasRef} onMouseMove={hitTest} aria-label={`${symbol} live price chart`} />
      {hover && (
        <div
          className="chart-tooltip"
          style={{ left: Math.max(8, hover.x - 55), top: hover.y } as CSSProperties}
        >
          <strong>{fmtPrice(hover.point.y)}</strong>
          <span>{fmtTime(new Date(hover.point.t).toISOString())}</span>
        </div>
      )}
    </div>
  );
}

function Spark({ events }: { events: StreamEvent[] }) {
  const points = useMemo(() => {
    const values = events
      .filter((event) => event.event_type === "trade" && event.price != null)
      .slice(-24)
      .map((event) => event.price!);
    if (values.length < 2) return "";
    const min = Math.min(...values);
    const max = Math.max(...values);
    return values.map((y, index) => {
      const x = (index / (values.length - 1)) * 100;
      const yy = 29 - ((y - min) / Math.max(0.0000001, max - min)) * 24;
      return `${x.toFixed(1)},${yy.toFixed(1)}`;
    }).join(" ");
  }, [events]);

  return (
    <svg className="spark" viewBox="0 0 100 32" preserveAspectRatio="none" aria-hidden="true">
      {points && (
        <polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.7" vectorEffect="non-scaling-stroke" />
      )}
    </svg>
  );
}

function SymbolRail({
  selected,
  symbols,
  onSelect,
  eventsBySymbol,
}: {
  selected: SymbolId;
  symbols: SymbolSnapshot[];
  onSelect: (symbol: SymbolId) => void;
  eventsBySymbol: Record<string, StreamEvent[]>;
}) {
  return (
    <aside className="symbol-rail">
      <div className="rail-label">MARKET</div>
      {SYMBOLS.map((symbol) => {
        const data = symbols.find((item) => item.symbol === symbol);
        return (
          <button
            key={symbol}
            type="button"
            className={`symbol-button ${selected === symbol ? "selected" : ""}`}
            onClick={() => onSelect(symbol)}
          >
            <div className="symbol-button-top">
              <span><StatusDot status={data?.status ?? "UNKNOWN"} />{symbol.replace("USDT", "")}</span>
              <span className={data?.window_change_pct != null && data.window_change_pct >= 0 ? "positive" : "negative"}>
                {data?.window_change_pct == null ? "—" : `${data.window_change_pct >= 0 ? "+" : ""}${data.window_change_pct.toFixed(2)}%`}
              </span>
            </div>
            <div className="symbol-button-bottom">
              <strong>{fmtPrice(data?.price ?? null)}</strong>
              <span>{fmtMs(data?.freshness_ms ?? null)}</span>
            </div>
            <Spark events={eventsBySymbol[symbol] ?? []} />
          </button>
        );
      })}
    </aside>
  );
}

function Tape({ events }: { events: StreamEvent[] }) {
  const rows = useMemo(
    () =>
      events
        .filter((event) => event.event_type === "trade" && event.price != null)
        .slice(-28)
        .reverse(),
    [events]
  );

  return (
    <div className="tape">
      <div className="tape-head">
        <span>TIME</span>
        <span>SIDE</span>
        <span>PRICE</span>
        <span>SIZE</span>
      </div>
      <div className="tape-body">
        {rows.map((event) => (
          <div className="tape-row" key={event.ledger_seq}>
            <span className="mono muted">{fmtTime(event.received_time)}</span>
            <span className={event.side === "BUY" ? "positive" : "negative"}>{event.side ?? "—"}</span>
            <span className="mono">{fmtPrice(event.price)}</span>
            <span className="mono muted">{fmtCompact(event.quantity)}</span>
          </div>
        ))}
        {!rows.length && <div className="empty-state">Sin operaciones recibidas todavía.</div>}
      </div>
    </div>
  );
}

function Microstructure({ snapshot }: { snapshot?: SymbolSnapshot }) {
  const metrics = [
    ["BID", fmtPrice(snapshot?.bid ?? null)],
    ["ASK", fmtPrice(snapshot?.ask ?? null)],
    ["SPREAD", snapshot?.spread_bps != null ? `${snapshot.spread_bps.toFixed(2)} bp` : "—"],
    ["FRESHNESS", fmtMs(snapshot?.freshness_ms ?? null)],
    ["BID SIZE", fmtCompact(snapshot?.bid_qty ?? null)],
    ["ASK SIZE", fmtCompact(snapshot?.ask_qty ?? null)],
  ];

  return (
    <div className="micro-grid">
      {metrics.map(([label, value]) => (
        <div className="micro-cell" key={label}>
          <span>{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}

function SystemStrip({ state, apiBase }: { state: ChannelState; apiBase: string }) {
  const items = [
    ["BINANCE WS", state === "LIVE" ? "LIVE" : state === "SYNC" ? "SYNC" : "DEGRADED", state === "LIVE" ? "live" : "warn"],
    ["POSTGRES", "DURABLE", "live"],
    ["CAPTURE", "PROSPECTIVE", "live"],
    ["FORECAST", "SHADOW", "neutral"],
    ["PROMOTION", "BLOCKED", "neutral"],
    ["EXECUTION", "OFF", "neutral"],
  ] as const;

  return (
    <div className="system-strip">
      {items.map(([label, value, tone]) => (
        <div className="system-item" key={label}>
          <span className={`status-dot status-${tone === "live" ? "live" : tone === "warn" ? "warn" : "off"}`} />
          <span className="system-label">{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
      <span className="api-label" title={apiBase}>READ API · {new URL(apiBase).hostname}</span>
    </div>
  );
}

export default function App() {
  const [selected, setSelected] = useState<SymbolId>("BTCUSDT");
  const [timeframe, setTimeframe] = useState<TimeframeId>("1d");
  const [snapshots, setSnapshots] = useState<SymbolSnapshot[]>([]);
  const [events, setEvents] = useState<Record<string, StreamEvent[]>>({});
  const [cursor, setCursor] = useState(0);
  const [channel, setChannel] = useState<ChannelState>("SYNC");
  const [error, setError] = useState<string | null>(null);
  const [lastTick, setLastTick] = useState<number | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const pollingRef = useRef(false);
  const cursorRef = useRef(0);

  const selectedSnapshot = snapshots.find((item) => item.symbol === selected);
  const selectedEvents = events[selected] ?? [];

  const poll = useCallback(async (reset = false) => {
    if (pollingRef.current) return;
    pollingRef.current = true;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    try {
      setChannel(reset || cursor === 0 ? "SYNC" : "LIVE");
      const payload = await fetchMarketStream(reset ? 0 : cursorRef.current, controller.signal);
      setSnapshots(payload.symbols);
      cursorRef.current = payload.next_cursor;
      setCursor(payload.next_cursor);
      setEvents((previous) => {
        const next = { ...previous };
        for (const event of payload.events) {
          const current = next[event.symbol] ?? [];
          const combined = [...current, event];
          const seen = new Set<number>();
          next[event.symbol] = combined
            .filter((row) => {
              if (seen.has(row.ledger_seq)) return false;
              seen.add(row.ledger_seq);
              return true;
            })
            .slice(-520);
        }
        return next;
      });
      setChannel(payload.status === "LIVE" ? "LIVE" : "DEGRADED");
      setError(null);
      setLastTick(Date.now());
    } catch (err) {
      if ((err as DOMException)?.name !== "AbortError") {
        setChannel("OFFLINE");
        setError(err instanceof Error ? err.message : "market_stream_error");
      }
    } finally {
      pollingRef.current = false;
    }
  }, []);

  useEffect(() => {
    poll(true);
    return () => abortRef.current?.abort();
  }, [poll]);

  useEffect(() => {
    const onVisibility = () => {
      if (!document.hidden) poll(true);
    };
    document.addEventListener("visibilitychange", onVisibility);
    const timer = window.setInterval(() => {
      if (!document.hidden) poll(false);
    }, 750);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibility);
      abortRef.current?.abort();
    };
  }, [poll]);

  const selectedLabel = selected.replace("USDT", "");
  const selectedWindowEvents = useMemo(() => {
    const tf = TIMEFRAMES.find((item) => item.id === timeframe)?.ms ?? 300_000;
    const cutoff = Date.now() - tf;
    const filtered = selectedEvents.filter((event) => new Date(event.received_time).getTime() >= cutoff);
    return filtered.length >= 2 ? filtered : selectedEvents;
  }, [selectedEvents, timeframe]);

  const online = channel === "LIVE";
  const change = selectedSnapshot?.window_change_pct ?? null;

  return (
    <div className="app-shell">
      <header className="topbar">
        <Logo />
        <div className="topbar-center">
          <nav className="desktop-nav" aria-label="Primary">
            <button className="nav-active">Terminal</button>
            <button>Research</button>
            <button>Quality</button>
            <button>System</button>
          </nav>
        </div>
        <div className="topbar-right">
          <div className="connection-pill">
            <StatusDot status={channel === "LIVE" ? "LIVE" : channel} />
            <span>{online ? "LIVE MARKET" : channel}</span>
          </div>
          <div className="update-clock mono">{lastTick ? fmtTime(new Date(lastTick).toISOString()) : "—"}</div>
        </div>
      </header>

      <div className="mobile-symbols">
        {SYMBOLS.map((symbol) => (
          <button
            key={symbol}
            className={selected === symbol ? "mobile-symbol active" : "mobile-symbol"}
            onClick={() => setSelected(symbol)}
          >
            {symbol.replace("USDT", "")}
          </button>
        ))}
      </div>

      <div className="workspace">
        <SymbolRail
          selected={selected}
          symbols={snapshots}
          eventsBySymbol={events}
          onSelect={setSelected}
        />

        <main className="terminal-main">
          <section className="hero">
            <div>
              <div className="hero-label"><span className="status-dot status-live" />REAL-TIME MARKET</div>
              <div className="hero-line">
                <h1>{selectedLabel}</h1>
                <span className="hero-pair">/ USDT</span>
                <span className={`hero-change ${change != null && change >= 0 ? "positive" : "negative"}`}>
                  {change == null ? "—" : `${change >= 0 ? "+" : ""}${change.toFixed(2)}%`}
                </span>
              </div>
              <div className="hero-price">{fmtPrice(selectedSnapshot?.price ?? null)}</div>
            </div>
            <div className="hero-meta">
              <div className="hero-status">
                <StatusDot status={selectedSnapshot?.status ?? channel} />
                <strong>{selectedSnapshot?.status ?? channel}</strong>
              </div>
              <span className="muted">Binance Spot · trade + bookTicker</span>
            </div>
          </section>

          <section className="chart-card surface">
            <div className="chart-toolbar">
              <div className="toolbar-left">
                <span className="toolbar-title">PRICE</span>
                <span className="toolbar-subtitle">{selectedLabel} · live ledger</span>
              </div>
              <div className="timeframe-group" role="group" aria-label="Chart timeframe">
                {TIMEFRAMES.map((item) => (
                  <button
                    key={item.id}
                    className={timeframe === item.id ? "tf-button active" : "tf-button"}
                    onClick={() => setTimeframe(item.id)}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </div>
            <PriceChart symbol={selectedLabel} events={selectedWindowEvents} timeframe={timeframe} />
            <div className="chart-footer">
              <span>source: Binance websocket</span>
              <span>cursor: <b className="mono">{cursor || "sync"}</b></span>
              <span>{selectedEvents.length} events in memory</span>
            </div>
          </section>

          <section className="grid-two">
            <div className="surface panel">
              <SectionTitle kicker="MICROSTRUCTURE" title="Market state" right={<span className="live-tag">LIVE</span>} />
              <Microstructure snapshot={selectedSnapshot} />
            </div>
            <div className="surface panel">
              <SectionTitle kicker="FLOW" title="Recent tape" right={<span className="muted">{selectedEvents.length} events</span>} />
              <Tape events={selectedEvents} />
            </div>
          </section>

          <section className="surface research-panel">
            <SectionTitle
              kicker="RESEARCH CONTROL"
              title="Quant pipeline state"
              right={<span className="gate-tag">NOT PROMOTED</span>}
            />
            <div className="research-grid">
              <div>
                <span>Prospective capture</span>
                <strong>ACTIVE</strong>
                <small>Eventos persistidos con identidad de proveedor.</small>
              </div>
              <div>
                <span>Quality gate</span>
                <strong>SEPARATED</strong>
                <small>La UI no transforma estado de calidad en señales.</small>
              </div>
              <div>
                <span>Forecast</span>
                <strong>SHADOW</strong>
                <small>Sin promoción automática mientras madure OOS.</small>
              </div>
              <div>
                <span>Execution</span>
                <strong>DISABLED</strong>
                <small>La terminal visualiza mercado; no ejecuta órdenes.</small>
              </div>
            </div>
          </section>

          <footer className="app-footer">
            <span>Gorila Argentum · quantitative market terminal</span>
            <span>API read-only · <span className="mono">/api/crypto/market/stream</span></span>
          </footer>
        </main>
      </div>

      {error && (
        <div className="toast" role="status">
          <StatusDot status="DEGRADED" />
          <span>Market stream: {error}</span>
          <button onClick={() => poll(true)}>RECONNECT</button>
        </div>
      )}

      <SystemStrip state={channel} apiBase={API_BASE} />
    </div>
  );
}
