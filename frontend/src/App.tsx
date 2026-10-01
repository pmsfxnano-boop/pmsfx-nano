import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { AreaSeries, createChart, type UTCTimestamp } from "lightweight-charts";
import {
  fetchConfig,
  fetchEvidence,
  fetchHealth,
  fetchHistory,
  fetchMarketStream,
  type ConfigResponse,
  type EvidenceResponse,
  type HealthResponse,
  type HistoryResponse,
  type MarketEvent,
  type SymbolMarket,
} from "./api";
import { createDirectBinanceFeed } from "./directBinance";

type Tab = "terminal" | "opportunity" | "research" | "quality";
const DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const RESOLUTIONS = ["1m", "5m", "15m", "1h", "4h", "1d"];

function num(value: unknown): number | null {
  const x = Number(value);
  return Number.isFinite(x) ? x : null;
}

function fmt(value: unknown, digits = 2): string {
  const x = num(value);
  return x == null ? "—" : x.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtCompact(value: unknown): string {
  const x = num(value);
  if (x == null) return "—";
  if (Math.abs(x) >= 1e9) return `${(x / 1e9).toFixed(2)}B`;
  if (Math.abs(x) >= 1e6) return `${(x / 1e6).toFixed(2)}M`;
  if (Math.abs(x) >= 1e3) return `${(x / 1e3).toFixed(1)}K`;
  return x.toFixed(0);
}

function pct(value: unknown, digits = 1): string {
  const x = num(value);
  return x == null ? "—" : `${x >= 0 ? "+" : ""}${x.toFixed(digits)}%`;
}

function ageSeconds(value: unknown): string {
  const x = num(value);
  if (x == null) return "—";
  if (x < 1) return "<1s";
  if (x < 60) return `${Math.round(x)}s`;
  return `${(x / 60).toFixed(1)}m`;
}

function timeOnly(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "—"
    : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

function symbolBase(symbol: string): string {
  return symbol.replace(/USDT$|\/USD$|USD$/i, "");
}

function toneForStatus(status: string | undefined): "good" | "warn" | "bad" | "neutral" {
  const s = String(status || "").toUpperCase();
  if (["LIVE", "PASS", "READY", "HEALTHY", "ACTIVE", "DURABLE"].some(v => s.includes(v))) return "good";
  if (["WAIT", "START", "SHADOW", "ACCUMULAT", "DEGRADED", "DELAYED", "LOCKED"].some(v => s.includes(v))) return "warn";
  if (["BLOCKED", "STALE", "FAIL", "NO_DATA", "ERROR"].some(v => s.includes(v))) return "bad";
  return "neutral";
}

function Icon({ name }: { name: "terminal" | "clock" | "research" | "quality" | "flow" | "pipeline" | "book" | "chart" | "grid" }) {
  const common = { width: 20, height: 20, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.8, strokeLinecap: "round" as const, strokeLinejoin: "round" as const };
  const paths: Record<string, ReactNode> = {
    terminal: <><path d="M5 4v16M5 14l5-5 4 4 5-6" /><path d="M16 20h4" /></>,
    clock: <><circle cx="12" cy="12" r="8.5" /><path d="M12 7v5l3 2" /></>,
    research: <><path d="M5 19V9M12 19V5M19 19v-7" /><path d="M3 19h18" /></>,
    quality: <><path d="M12 3l7 3v5c0 4.3-2.8 8-7 10-4.2-2-7-5.7-7-10V6l7-3z" /><path d="m9 12 2 2 4-4" /></>,
    flow: <><path d="M4 12c0-4.4 3.2-8 8-8s8 3.6 8 8" /><path d="M4 12c0 4.4 3.2 8 8 8s8-3.6 8-8" /><path d="M4 8v4h4M20 16v-4h-4" /></>,
    pipeline: <><path d="M4 18V9M10 18V5M16 18v-7M22 18V3" /><path d="M3 21h19" /></>,
    book: <><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v16H6.5A2.5 2.5 0 0 0 4 21.5z" /><path d="M4 5.5v16M8 7h8M8 11h8" /></>,
    chart: <><path d="M4 19V5M4 19h16" /><path d="m7 15 3-4 3 2 5-7" /></>,
    grid: <><rect x="4" y="4" width="6" height="6" rx="1" /><rect x="14" y="4" width="6" height="6" rx="1" /><rect x="4" y="14" width="6" height="6" rx="1" /><rect x="14" y="14" width="6" height="6" rx="1" /></>,
  };
  return <svg {...common}>{paths[name]}</svg>;
}

function StatusPill({ status }: { status: string }) {
  const tone = toneForStatus(status);
  return <span className={`status-pill ${tone}`}><i />{status}</span>;
}

function Brand({ live }: { live: boolean }) {
  return (
    <div className="brand">
      <div className="brand-mark">C<span>+</span></div>
      <div className="brand-copy">
        <strong>CRYPTONITA</strong>
        <small>QUANT TERMINAL</small>
      </div>
      <div className={`global-state ${live ? "live" : "warn"}`}><i />{live ? "LIVE" : "DEGRADED"}</div>
    </div>
  );
}

function SymbolStrip({
  symbols,
  selected,
  onSelect,
}: {
  symbols: SymbolMarket[];
  selected: string;
  onSelect: (symbol: string) => void;
}) {
  return (
    <div className="symbol-strip">
      {symbols.map(item => (
        <button
          key={item.symbol}
          className={`symbol-card ${selected === item.symbol ? "selected" : ""}`}
          onClick={() => onSelect(item.symbol)}
        >
          <div className="symbol-top">
            <span><b>{symbolBase(item.symbol)}</b><em>/{item.symbol.endsWith("USDT") ? "USDT" : "USD"}</em></span>
            <StatusPill status={item.status} />
          </div>
          <div className="symbol-price">{fmt(item.price, item.price && item.price < 10 ? 4 : 2)}</div>
          <div className="symbol-age">{ageSeconds((item.freshness_ms || 0) / 1000)}</div>
          <svg className="spark" viewBox="0 0 180 42" preserveAspectRatio="none" aria-hidden="true">
            <polyline
              className="spark-line"
              points="0,31 24,31 36,18 58,18 72,18 93,14 108,16 122,14 142,15 158,8 180,8"
            />
          </svg>
          <div className="symbol-meta">
            <span>bid <b>{fmt(item.bid, item.bid && item.bid < 10 ? 4 : 2)}</b></span>
            <span>ask <b>{fmt(item.ask, item.ask && item.ask < 10 ? 4 : 2)}</b></span>
            <span>spread <b>{item.spread_bps == null ? "—" : `${item.spread_bps.toFixed(2)} bp`}</b></span>
          </div>
        </button>
      ))}
    </div>
  );
}

function TerminalHero({ item }: { item: SymbolMarket | undefined }) {
  const spread = item?.spread_bps;
  const quoteReady = item?.bid != null && item?.ask != null;
  return (
    <section className="panel hero-terminal">
      <div className="eyebrow"><span className="dot live" />LIVE MARKET TERMINAL</div>
      <div className="hero-line">
        <div className="hero-symbol">{symbolBase(item?.symbol || "BTCUSDT")} <span>/{item?.symbol?.endsWith("USDT") ? "USDT" : "USD"}</span></div>
        <StatusPill status={item?.status || "WAITING"} />
      </div>
      <div className="hero-price">{fmt(item?.price, item?.price != null && item.price < 10 ? 5 : 2)}</div>
      <div className="hero-meta">
        <span>public market stream</span>
        <span>trade + bookTicker</span>
        <span>event age {ageSeconds((item?.freshness_ms || 0) / 1000)}</span>
      </div>
      <div className="quote-box">
        <div><small>Bid</small><b>{quoteReady ? fmt(item?.bid, item?.bid != null && item.bid < 10 ? 5 : 2) : "—"}</b></div>
        <div><small>Ask</small><b>{quoteReady ? fmt(item?.ask, item?.ask != null && item.ask < 10 ? 5 : 2) : "—"}</b></div>
        <div><small>Spread</small><b>{spread == null ? "—" : `${spread.toFixed(2)} bp`}</b></div>
      </div>
    </section>
  );
}

function PrimaryChart({
  history,
  liveEvents,
  symbol,
  resolution,
  onResolution,
}: {
  history: HistoryResponse | null;
  liveEvents: MarketEvent[];
  symbol: string;
  resolution: string;
  onResolution: (v: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const seriesRef = useRef<any>(null);
  const lastLiveKeyRef = useRef<string | null>(null);
  const lastChartTimeRef = useRef<number>(0);

  const displaySymbol = symbolBase(symbol || history?.symbol || "BTCUSDT");
  const displayQuote = (symbol || history?.symbol || "BTCUSDT").endsWith("USDT") ? "USDT" : "USD";
  const latestTrade = liveEvents.length ? liveEvents[liveEvents.length - 1] : null;
  const current = num(latestTrade?.price) ?? num(history?.candles.at(-1)?.close);
  const previous = history?.candles.length && history.candles.length > 1
    ? history.candles[history.candles.length - 2].close
    : null;
  const change = current != null && previous != null && previous !== 0
    ? ((current / previous) - 1) * 100
    : null;
  const hasLive = latestTrade?.price != null;

  const toTime = (value: string): UTCTimestamp => {
    const ms = Date.parse(value);
    return Math.floor(ms / 1000) as UTCTimestamp;
  };

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      autoSize: true,
      layout: {
        background: { type: "solid", color: "transparent" },
        textColor: "#718792",
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: "rgba(20,37,46,.72)" },
        horzLines: { color: "rgba(20,37,46,.72)" },
      },
      rightPriceScale: {
        borderColor: "#1a2b34",
        scaleMargins: { top: 0.10, bottom: 0.10 },
        minimumWidth: 74,
      },
      timeScale: {
        borderColor: "#1a2b34",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 3,
        barSpacing: 8,
        minBarSpacing: 4,
      },
      crosshair: {
        vertLine: {
          color: "rgba(105,215,233,.34)",
          width: 1,
          style: 3,
          labelBackgroundColor: "#0b2027",
        },
        horzLine: {
          color: "rgba(105,215,233,.22)",
          width: 1,
          style: 3,
          labelBackgroundColor: "#0b2027",
        },
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: true,
      },
      handleScale: {
        axisPressedMouseMove: true,
        mouseWheel: true,
        pinch: true,
      },
    });

    const series = chart.addSeries(AreaSeries, {
      lineColor: "#69d7e9",
      topColor: "rgba(105,215,233,.28)",
      bottomColor: "rgba(105,215,233,0)",
      lineWidth: 2,
      priceLineColor: "#69d7e9",
      priceLineWidth: 1,
      priceLineStyle: 3,
      lastValueVisible: true,
      crosshairMarkerVisible: true,
      crosshairMarkerRadius: 4,
      crosshairMarkerBorderColor: "#061017",
      crosshairMarkerBackgroundColor: "#69d7e9",
      title: displaySymbol,
    });

    chartRef.current = chart;
    seriesRef.current = series;

    const resizeObserver = new ResizeObserver(() => {
      chart.applyOptions({});
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      lastLiveKeyRef.current = null;
      lastChartTimeRef.current = 0;
    };
  }, [displaySymbol, resolution]);

  useEffect(() => {
    const series = seriesRef.current;
    if (!series || !history?.candles?.length) return;

    const points = history.candles
      .map(c => ({ time: toTime(c.time), value: c.close }))
      .filter(p => Number.isFinite(p.time) && Number.isFinite(p.value))
      .sort((a, b) => Number(a.time) - Number(b.time));

    const deduped: Array<{ time: UTCTimestamp; value: number }> = [];
    for (const point of points) {
      const previousPoint = deduped[deduped.length - 1];
      if (previousPoint && previousPoint.time === point.time) {
        deduped[deduped.length - 1] = point;
      } else {
        deduped.push(point);
      }
    }

    series.setData(deduped);
    lastChartTimeRef.current = Number(deduped.at(-1)?.time || 0);
    lastLiveKeyRef.current = null;
    chartRef.current?.timeScale().fitContent();
  }, [history?.symbol, history?.resolution, history?.candles.length]);

  useEffect(() => {
    const series = seriesRef.current;
    if (!series || !latestTrade?.price || !latestTrade.event_key) return;
    if (latestTrade.event_key === lastLiveKeyRef.current) return;

    const time = toTime(latestTrade.received_time);
    if (!Number.isFinite(time)) return;

    const previousTime = lastChartTimeRef.current;
    if (time < previousTime) {
      lastLiveKeyRef.current = latestTrade.event_key;
      return;
    }

    series.update({ time, value: Number(latestTrade.price) });
    lastChartTimeRef.current = Number(time);
    lastLiveKeyRef.current = latestTrade.event_key;
  }, [latestTrade?.event_key]);

  return (
    <section className="panel primary-chart-panel">
      <div className="primary-chart-head">
        <div>
          <div className="eyebrow">
            <Icon name="chart" />
            PRIMARY MARKET CHART
            <span className="chart-live-dot" />
          </div>
          <div className="primary-chart-title">
            <strong>{displaySymbol}</strong>
            <span>/{displayQuote}</span>
            {current != null && <b>{fmt(current, current < 10 ? 5 : 2)}</b>}
            {change != null && <em className={change >= 0 ? "up" : "down"}>{pct(change, 2)}</em>}
          </div>
        </div>
        <div className="resolution-bar">
          {RESOLUTIONS.map(r => (
            <button
              key={r}
              className={r === resolution ? "active" : ""}
              onClick={() => onResolution(r)}
              type="button"
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      <div className="primary-chart-frame primary-chart-canvas">
        <div ref={containerRef} className="lwc-market-chart" aria-label={`Live ${displaySymbol}/${displayQuote} market chart`} />
        <div className="chart-live-badge">
          <span />
          {hasLive ? "LIVE STREAM" : "DURABLE HISTORY"}
        </div>
      </div>

      <div className="primary-chart-foot">
        <span>{hasLive ? "Binance hot stream · durable context" : `${history?.resolution || resolution} · durable market ledger`}</span>
        <span>{history?.candles?.length || 0} durable bars</span>
        <span>canvas financial renderer</span>
      </div>
    </section>
  );
}

function Microstructure({ item, trades }: { item: SymbolMarket | undefined; trades: MarketEvent[] }) {
  const buys = trades.filter(t => t.side === "BUY").length;
  const pressure = trades.length ? (buys / trades.length) * 100 : null;
  const flowQty = trades.reduce((acc, t) => acc + (t.quantity || 0) * (t.side === "BUY" ? 1 : -1), 0);
  const grossQty = trades.reduce((acc, t) => acc + (t.quantity || 0), 0);
  const flow = grossQty ? (flowQty / grossQty) * 100 : null;
  const sorted = trades.slice().sort((a, b) => Date.parse(a.received_time) - Date.parse(b.received_time));
  const span = sorted.length > 1
    ? Math.max(1, (Date.parse(sorted.at(-1)!.received_time) - Date.parse(sorted[0].received_time)) / 60000)
    : 1;
  const ppm = trades.length / span;
  return (
    <section className="panel terminal-late-panel">
      <div className="section-head">
        <div><div className="eyebrow"><Icon name="flow" /> MICROSTRUCTURE</div><h2>Market state</h2></div>
        <span className="muted">top-of-book + trade flow</span>
      </div>
      <div className="metric-grid">
        <Metric label="MID" value={item?.bid != null && item.ask != null ? fmt((item.bid + item.ask) / 2, item.bid < 10 ? 5 : 2) : "—"} sub="trade / quote mid" />
        <Metric label="BID" value={fmt(item?.bid, item?.bid != null && item.bid < 10 ? 5 : 2)} sub={fmt(item?.bid_qty, 4)} />
        <Metric label="ASK" value={fmt(item?.ask, item?.ask != null && item.ask < 10 ? 5 : 2)} sub={fmt(item?.ask_qty, 4)} />
        <Metric label="SPREAD" value={item?.spread_bps == null ? "—" : `${item.spread_bps.toFixed(2)} bp`} sub="quote width" />
        <Metric label="QUEUE" value={pressure == null ? "—" : `${pressure.toFixed(1)}%`} sub="buy-side print share" />
        <Metric label="TRADE FLOW" value={flow == null ? "—" : `${flow >= 0 ? "+" : ""}${flow.toFixed(1)}%`} sub="signed quantity imbalance" />
        <Metric label="PRINTS / MIN" value={ppm.toFixed(1)} sub="recent observed tape" />
        <Metric label="FRESHNESS" value={ageSeconds((item?.freshness_ms || 0) / 1000)} sub="receive age" />
      </div>
      <div className="queue-bar">
        <span style={{ width: `${Math.max(4, Math.min(96, (pressure ?? 50)))}%` }} />
      </div>
      <div className="queue-labels"><span>BID FLOW</span><span>NEUTRAL</span><span>ASK FLOW</span></div>
    </section>
  );
}

function Metric({ label, value, sub }: { label: string; value: string; sub: string }) {
  return <div className="metric"><small>{label}</small><strong>{value}</strong><span>{sub}</span></div>;
}

function LiveTape({ trades }: { trades: MarketEvent[] }) {
  return (
    <section className="panel terminal-late-panel">
      <div className="section-head">
        <div><div className="eyebrow"><Icon name="flow" /> FLOW</div><h2>Live tape</h2></div>
        <span className="muted">{trades.length} prints</span>
      </div>
      <div className="tape">
        <div className="tape-head"><span>TIME</span><span>SIDE</span><span>PRICE</span><span>SIZE</span><span>LEDGER</span></div>
        {trades.slice().reverse().slice(0, 18).map(event => (
          <div className="tape-row" key={`${event.event_key}-${event.stream_seq}`}>
            <span>{timeOnly(event.received_time)}</span>
            <span className={event.side === "BUY" ? "buy" : "sell"}>{event.side || "—"}</span>
            <span>{fmt(event.price, event.price != null && event.price < 10 ? 5 : 2)}</span>
            <span>{fmt(event.quantity, 4)}</span>
            <span>{event.durable && event.ledger_seq != null ? event.ledger_seq : "—"}</span>
          </div>
        ))}
        {!trades.length && <div className="empty">Waiting for market observations.</div>}
      </div>
    </section>
  );
}

function QuantTimeline({
  health,
  evidence,
  market,
}: {
  health: HealthResponse | null;
  evidence: EvidenceResponse | null;
  market: "LIVE" | "DEGRADED";
}) {
  const cohort = String(evidence?.cohort?.status || "WAITING");
  const quality = String(evidence?.quality_gate?.state || "WAITING");
  const pit = String(evidence?.pit_oos?.state || "BLOCKED");
  const promotion = evidence?.opportunity_clock?.validated ? "ACTIVE" : "BLOCKED";
  const durability = health?.storage_backend || "UNKNOWN";
  return (
    <section className="panel terminal-late-panel">
      <div className="section-head">
        <div><div className="eyebrow"><Icon name="pipeline" /> PIPELINE</div><h2>Quantitative timeline</h2></div>
        <span className="muted">single system</span>
      </div>
      <div className="timeline">
        <TimelineNode label="MARKET" value={market} detail={health?.provider || "public market stream"} tone={toneForStatus(market)} />
        <TimelineNode label="LEDGER" value={durability} detail="durable persistence" tone={toneForStatus(durability)} />
        <TimelineNode label="QUALITY" value={quality} detail="data quality gate" tone={toneForStatus(quality)} />
        <TimelineNode label="PIT / OOS" value={pit} detail="validation boundary" tone={toneForStatus(pit)} />
        <TimelineNode label="CLOCK" value={promotion} detail="fail-closed activation" tone={toneForStatus(promotion)} />
      </div>
      <div className="state-strip">
        <div><small>PROSPECTIVE</small><b>{cohort}</b></div>
        <div><small>QUALITY</small><b>{quality}</b></div>
        <div><small>PIT / OOS</small><b>{pit}</b></div>
        <div><small>PROMOTION</small><b>{promotion}</b></div>
      </div>
      <p className="note">Observed market regimes remain descriptive until the exact cohort, Quality Gate and PIT/OOS evidence contract passes.</p>
    </section>
  );
}

function TimelineNode({ label, value, detail, tone }: { label: string; value: string; detail: string; tone: string }) {
  return (
    <div className={`timeline-node ${tone}`}>
      <div className="timeline-dot" />
      <small>{label}</small>
      <b>{value}</b>
      <span>{detail}</span>
    </div>
  );
}

function Opportunity({ evidence }: { evidence: EvidenceResponse | null }) {
  const clock = evidence?.opportunity_clock;
  const active = clock?.state === "ACTIVE";
  const progress = num(evidence?.cohort?.progress_pct) ?? 0;
  return (
    <section className="panel opportunity-panel">
      <div className="section-head">
        <div><div className="eyebrow"><Icon name="clock" /> OPPORTUNITY CLOCK</div><h2>Activation gate</h2></div>
        <span className="tag">{clock?.mode || "SHADOW"}</span>
      </div>
      <div className={`clock ${active ? "active" : "locked"}`}>
        <div className="clock-inner">
          <span>GATE</span>
          <strong>{clock?.state || "LOCKED"}</strong>
          <small>{active ? "VALIDATED" : "PIT / OOS"}</small>
        </div>
      </div>
      <div className="gate-copy">
        <StatusPill status={active ? "ACTIVE" : "GATED"} />
        <h3>{active ? "Evidence contract passed" : "Waiting for evidence"}</h3>
        <p>{clock?.rule || "The clock stays fail-closed until the prospective cohort, current Quality Gate, PIT/OOS and shadow evidence all pass."}</p>
      </div>
      <div className="gate-grid">
        <GateCard label="COHORT" value={String(evidence?.cohort?.status || "SYNC")} meta={`${progress.toFixed(1)}% of prospective window`} />
        <GateCard label="QUALITY" value={String(evidence?.quality_gate?.state || "SYNC")} meta={String(evidence?.quality_gate?.rows || 0) + " rows in latest report"} />
        <GateCard label="PIT / OOS" value={String(evidence?.pit_oos?.state || "SYNC")} meta={String(evidence?.pit_oos?.oos_rows || 0) + " OOS rows"} />
        <GateCard label="FORECAST" value={String(evidence?.forecast_shadow?.state || "SYNC")} meta={String(evidence?.forecast_shadow?.count || 0) + " shadow forecasts"} />
      </div>
      <div className="blockers">
        <span>BLOCKERS</span><b>{clock?.blockers?.length || 0}</b>
      </div>
    </section>
  );
}

function GateCard({ label, value, meta }: { label: string; value: string; meta: string }) {
  return <div className="gate-card"><small>{label}</small><b>{value}</b><span>{meta}</span></div>;
}

function PriceChart({
  history,
  resolution,
  onResolution,
}: {
  history: HistoryResponse | null;
  resolution: string;
  onResolution: (v: string) => void;
}) {
  const candles = history?.candles || [];
  const W = 1000;
  const H = 360;
  const pad = { l: 20, r: 20, t: 22, b: 22 };
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const high = highs.length ? Math.max(...highs) : 1;
  const low = lows.length ? Math.min(...lows) : 0;
  const span = Math.max(high - low, high * 0.001, 1e-9);
  const x = (i: number) => pad.l + ((i + 0.5) / Math.max(1, candles.length)) * (W - pad.l - pad.r);
  const y = (price: number) => pad.t + ((high - price) / span) * (H - pad.t - pad.b);
  const step = (W - pad.l - pad.r) / Math.max(1, candles.length);
  return (
    <section className="panel chart-panel">
      <div className="section-head">
        <div><div className="eyebrow"><Icon name="chart" /> PRICE ACTION</div><h2>{history?.symbol ? `${symbolBase(history.symbol)} historical market structure` : "Historical market structure"}</h2></div>
        <div className="resolution-bar">
          {RESOLUTIONS.map(r => <button key={r} className={r === resolution ? "active" : ""} onClick={() => onResolution(r)}>{r}</button>)}
        </div>
      </div>
      <div className="chart-sub"><span>{history?.symbol || "—"}</span><span>{history?.resolution || resolution}</span></div>
      <div className="chart">
        {candles.length ? (
          <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
            {[0.2, 0.4, 0.6, 0.8].map(v => <line key={v} className="gridline" x1={pad.l} x2={W - pad.r} y1={H * v} y2={H * v} />)}
            {candles.map((c, i) => {
              const up = c.close >= c.open;
              const cx = x(i);
              const bodyWidth = Math.max(2, step * 0.46);
              const top = y(Math.max(c.open, c.close));
              const bodyHeight = Math.max(2, Math.abs(y(c.open) - y(c.close)));
              return (
                <g key={c.time + i} className={up ? "candle up" : "candle down"}>
                  <line x1={cx} x2={cx} y1={y(c.high)} y2={y(c.low)} />
                  <rect x={cx - bodyWidth / 2} y={top} width={bodyWidth} height={bodyHeight} rx="1" />
                </g>
              );
            })}
          </svg>
        ) : <div className="empty">No durable candles available for this resolution yet.</div>}
      </div>
      <div className="chart-foot"><span>Source: durable market ledger</span><span>{candles.length} candles</span><span>history is outside the hot path</span></div>
    </section>
  );
}

function ResearchTab({ evidence }: { evidence: EvidenceResponse | null }) {
  const quality = evidence?.quality_gate || {};
  const validation = evidence?.pit_oos || {};
  const forecast = evidence?.forecast_shadow || {};
  const opp = evidence?.opportunity_shadow || {};
  const leadLag = evidence?.lead_lag_shadow || {};
  return (
    <div className="stack">
      <section className="panel">
        <div className="section-head"><div><div className="eyebrow"><Icon name="research" /> RESEARCH CONTROL</div><h2>Evidence contract</h2></div><span className="muted">read-only evidence model</span></div>
        <div className="research-grid">
          <ResearchCard label="PROSPECTIVE COHORT" value={String(evidence?.cohort?.status || "—")} meta={`${Number(evidence?.cohort?.progress_pct || 0).toFixed(1)}% progress`} />
          <ResearchCard label="QUALITY GATE" value={String(quality.state || "—")} meta={`${quality.rows || 0} rows · ${quality.status || "—"}`} />
          <ResearchCard label="PIT / OOS" value={String(validation.state || "—")} meta={`${validation.oos_rows || 0} OOS rows`} />
          <ResearchCard label="FORECAST SHADOW" value={String(forecast.state || "—")} meta={`${forecast.count || 0} forecasts · ${forecast.outcomes_count || 0} outcomes`} />
          <ResearchCard label="OPPORTUNITY SHADOW" value={String(opp.count || 0)} meta={`${Object.entries(opp.state_counts || {}).map(([k,v]) => `${k}:${v}`).join(" · ") || "no states"}`} />
          <ResearchCard label="LEAD / LAG" value={String(leadLag.observation_count || 0)} meta="shadow observations" />
        </div>
      </section>
      <section className="panel">
        <div className="section-head"><div><div className="eyebrow"><Icon name="book" /> PROTOCOL</div><h2>Reproducibility identity</h2></div></div>
        <div className="protocol">
          <div><small>STUDY</small><b>{evidence?.study?.study_id || "—"}</b></div>
          <div><small>VERSION</small><b>{evidence?.study?.version || "—"}</b></div>
          <div><small>PROSPECT</small><b>{evidence?.study?.prospect_days ? `${evidence.study.prospect_days} days` : "—"}</b></div>
          <div><small>FORECAST HORIZONS</small><b>{evidence?.study?.forecast_horizons_ms?.map(v => `${v}ms`).join(" · ") || "—"}</b></div>
          <div className="hash"><small>PROTOCOL HASH</small><b>{evidence?.study?.protocol_hash || "—"}</b></div>
        </div>
      </section>
    </div>
  );
}

function QualityTab({ health, evidence, market }: { health: HealthResponse | null; evidence: EvidenceResponse | null; market: MarketStreamLike }) {
  const q = evidence?.quality_gate || {};
  const symbols = market.symbols || [];
  return (
    <div className="stack">
      <section className="panel">
        <div className="section-head"><div><div className="eyebrow"><Icon name="quality" /> QUALITY</div><h2>Feed integrity</h2></div><StatusPill status={q.state || "WAITING"} /></div>
        <div className="quality-grid">
          {symbols.map(item => (
            <div className="quality-card" key={item.symbol}>
              <div><b>{symbolBase(item.symbol)}</b><StatusPill status={item.status} /></div>
              <span>freshness</span><strong>{ageSeconds((item.freshness_ms || 0) / 1000)}</strong>
              <small>spread {item.spread_bps == null ? "—" : `${item.spread_bps.toFixed(2)} bp`}</small>
            </div>
          ))}
        </div>
      </section>
      <section className="panel">
        <div className="section-head"><div><div className="eyebrow"><Icon name="pipeline" /> DURABILITY</div><h2>Persistence state</h2></div><StatusPill status={market.durability_status || "UNKNOWN"} /></div>
        <div className="durability-grid">
          <ResearchCard label="WORKER" value={health?.worker_alive ? "RUNNING" : "STOPPED"} meta="market ingestion lifecycle" />
          <ResearchCard label="QUALITY MONITOR" value={health?.quality_monitor_alive ? "RUNNING" : "STOPPED"} meta="bounded off-path monitor" />
          <ResearchCard label="HEARTBEAT" value={health?.heartbeat_alive ? "RUNNING" : "STOPPED"} meta="runtime liveness" />
          <ResearchCard label="LEDGER CURSOR" value={String(market.last_durable_stream_seq || 0)} meta="last durable stream sequence" />
        </div>
        <p className="note">Market presentation remains independent from PostgreSQL durability; the UI explicitly exposes degraded persistence rather than silently masking it.</p>
      </section>
    </div>
  );
}

type MarketStreamLike = {
  status?: string;
  market_status?: string;
  durability_status?: string;
  symbols: SymbolMarket[];
  last_durable_stream_seq?: number;
};

function ResearchCard({ label, value, meta }: { label: string; value: string; meta: string }) {
  return <div className="research-card"><small>{label}</small><strong>{value}</strong><span>{meta}</span></div>;
}

function App() {
  const [tab, setTab] = useState<Tab>("terminal");
  const [symbols, setSymbols] = useState<SymbolMarket[]>([]);
  const [events, setEvents] = useState<MarketEvent[]>([]);
  const [selected, setSelected] = useState("BTCUSDT");
  const cursorRef = useRef(0);
  const [marketStatus, setMarketStatus] = useState("STARTING");
  const [durabilityStatus, setDurabilityStatus] = useState("UNKNOWN");
  const [lastDurable, setLastDurable] = useState(0);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [evidence, setEvidence] = useState<EvidenceResponse | null>(null);
  const [config, setConfig] = useState<ConfigResponse | null>(null);
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [resolution, setResolution] = useState("5m");
  const [error, setError] = useState<string | null>(null);
  const [directSymbols, setDirectSymbols] = useState<SymbolMarket[]>([]);
  const [directEvents, setDirectEvents] = useState<MarketEvent[]>([]);
  const directFeedRef = useRef<(() => void) | null>(null);
  const inFlight = useRef(false);

  // Independent visual hot plane: when the API/read-model is unavailable or stale,
  // the terminal keeps receiving public Binance market data directly in the browser.
  useEffect(() => {
    directFeedRef.current = createDirectBinanceFeed((snapshot) => {
      setDirectSymbols(snapshot.symbols);
      setDirectEvents(snapshot.events);
    });
    return () => {
      directFeedRef.current?.();
      directFeedRef.current = null;
    };
  }, []);

  useEffect(() => {
    let disposed = false;
    let timer = 0;
    const poll = async () => {
      if (disposed || inFlight.current) return;
      inFlight.current = true;
      const controller = new AbortController();
      const timeout = window.setTimeout(() => controller.abort(), 5000);
      try {
        const response = await fetchMarketStream(cursorRef.current, 360, controller.signal);
        if (disposed) return;
        setMarketStatus(response.market_status || response.status || "DEGRADED");
        setDurabilityStatus(response.durability_status || "UNKNOWN");
        setLastDurable(response.last_durable_stream_seq || 0);
        setSymbols(response.symbols || []);

        setEvents(prev => {
          const incoming = response.events || [];
          const map = new Map<string, MarketEvent>();
          [...prev, ...incoming].forEach(event => map.set(event.event_key, event));
          return [...map.values()]
            .sort((a, b) => a.stream_seq - b.stream_seq)
            .slice(-360);
        });
        cursorRef.current = response.next_cursor || cursorRef.current;
        setError(null);
      } catch (err) {
        if (!disposed) setError(err instanceof Error ? err.message : "market stream unavailable");
      } finally {
        window.clearTimeout(timeout);
        inFlight.current = false;
        if (!disposed) timer = window.setTimeout(poll, 5000);
      }
    };
    void poll();
    return () => {
      disposed = true;
      window.clearTimeout(timer);
    };
  }, []);

  useEffect(() => {
    let disposed = false;
    const controller = new AbortController();
    const run = async () => {
      try {
        const [nextHealth, nextConfig] = await Promise.all([
          fetchHealth(controller.signal),
          fetchConfig(controller.signal),
        ]);
        if (!disposed) {
          setHealth(nextHealth);
          setConfig(nextConfig);
        }
      } catch {
        // Market stream remains authoritative for the hot UI.
      }
    };
    void run();
    const id = window.setInterval(() => void run(), 10000);
    return () => {
      disposed = true;
      controller.abort();
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    let disposed = false;
    const controller = new AbortController();
    const loadEvidence = async () => {
      try {
        const payload = await fetchEvidence(controller.signal);
        if (!disposed) setEvidence(payload);
      } catch {
        if (!disposed) setEvidence(null);
      }
    };
    void loadEvidence();
    const id = window.setInterval(() => void loadEvidence(), 15000);
    return () => {
      disposed = true;
      controller.abort();
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    let disposed = false;
    const load = async () => {
      try {
        const payload = await fetchHistory(selected, resolution, 240, controller.signal);
        if (!disposed) setHistory(payload);
      } catch {
        if (!disposed) setHistory(null);
      }
    };
    void load();
    const id = window.setInterval(() => void load(), 30000);
    return () => {
      disposed = true;
      controller.abort();
      window.clearInterval(id);
    };
  }, [selected, resolution]);

  const directLive = directSymbols.some(item => item.status === "LIVE");
  const displaySymbols = directLive ? directSymbols : symbols;
  const displayEvents = directLive ? directEvents : events;
  const displayMarketStatus = directLive ? "LIVE" : marketStatus;
  const selectedMarket = useMemo(
    () => displaySymbols.find(s => s.symbol === selected) || displaySymbols[0],
    [displaySymbols, selected],
  );
  const selectedTrades = useMemo(
    () => displayEvents.filter(event => event.symbol === (selectedMarket?.symbol || selected) && event.event_type === "trade"),
    [displayEvents, selectedMarket?.symbol, selected],
  );
  const live = displayMarketStatus === "LIVE" && selectedMarket?.status === "LIVE";

  const updateSelected = (symbol: string) => {
    setSelected(symbol);
    setTab("terminal");
  };

  const nav: Array<[Tab, string, "terminal" | "clock" | "research" | "quality"]> = [
    ["terminal", "Terminal", "terminal"],
    ["opportunity", "Opportunity", "clock"],
    ["research", "Research", "research"],
    ["quality", "Quality", "quality"],
  ];

  return (
    <main className="app-shell">
      <header className="topbar">
        <Brand live={live} />
        <div className="top-meta">
          <span>{config?.provider ? `${config.provider} spot` : "public market stream"}</span>
          <span>{symbols.length || DEFAULT_SYMBOLS.length} symbols</span>
          <span>{error ? "read-model degraded" : "stream synchronized"}</span>
        </div>
      </header>

      <div className="content">
        <div className="brandline"><span>CRYPTONITA</span><b>QUANT TERMINAL</b></div>

        {tab === "terminal" && (
          <>
            <SymbolStrip symbols={displaySymbols} selected={selectedMarket?.symbol || selected} onSelect={updateSelected} />
            <TerminalHero item={selectedMarket} />
            <PrimaryChart
              history={history}
              liveEvents={selectedTrades}
              symbol={selectedMarket?.symbol || selected}
              resolution={resolution}
              onResolution={setResolution}
            />
            <Microstructure item={selectedMarket} trades={selectedTrades} />
            <LiveTape trades={selectedTrades} />
            <QuantTimeline health={health} evidence={evidence} market={displayMarketStatus === "LIVE" ? "LIVE" : "DEGRADED"} />
          </>
        )}

        {tab === "opportunity" && (
          <>
            <Opportunity evidence={evidence} />
            <QuantTimeline health={health} evidence={evidence} market={marketStatus === "LIVE" ? "LIVE" : "DEGRADED"} />
            <PriceChart history={history} resolution={resolution} onResolution={setResolution} />
          </>
        )}

        {tab === "research" && <ResearchTab evidence={evidence} />}

        {tab === "quality" && <QualityTab health={health} evidence={evidence} market={{ symbols, status: marketStatus, market_status: marketStatus, durability_status: durabilityStatus, last_durable_stream_seq: lastDurable }} />}
      </div>

      <nav className="bottom-nav" aria-label="Primary">
        {nav.map(([key, label, icon]) => (
          <button key={key} className={tab === key ? "active" : ""} onClick={() => setTab(key)}>
            <Icon name={icon} />
            <span>{label}</span>
          </button>
        ))}
      </nav>
    </main>
  );
}

export default App;
