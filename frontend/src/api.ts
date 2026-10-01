export type StreamEvent = {
  ledger_seq: number;
  symbol: string;
  event_type: "trade" | "bookTicker";
  event_time: string;
  received_time: string;
  price: number | null;
  quantity: number | null;
  side: "BUY" | "SELL" | null;
};

export type SymbolSnapshot = {
  symbol: string;
  status: string;
  price: number | null;
  window_change_pct: number | null;
  bid: number | null;
  ask: number | null;
  bid_qty: number | null;
  ask_qty: number | null;
  spread_bps: number | null;
  imbalance: number | null;
  freshness_ms: number | null;
  last_trade_time: string | null;
  last_book_time: string | null;
};

export type MarketStreamResponse = {
  status: string;
  server_time: string;
  next_cursor: number;
  symbols: SymbolSnapshot[];
  events: StreamEvent[];
  cache_events_available?: number;
  forecast: {
    automatic_promotion: boolean;
    execution: boolean;
  };
};

export type Candle = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  trades: number;
};

export type HistoryResponse = {
  symbol: string;
  resolution: string;
  candles: Candle[];
};

export type ProspectiveStatus = {
  status: string;
  worker_alive: boolean;
  ledger: Record<string, unknown> | null;
  source_health: Array<Record<string, unknown>>;
  symbol_health: Array<Record<string, unknown>>;
  symbols_live: boolean;
  automatic_promotion: boolean;
  execution: boolean;
  capture_block_reason?: string | null;
};

const API_BASE = (
  import.meta.env.VITE_API_BASE_URL ||
  "https://gorila-crypto-cleanroom-binance-capture.onrender.com"
).replace(/\/$/, "");

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    signal,
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      if (payload?.detail) detail = String(payload.detail);
    } catch {
      // Preserve HTTP status when response is not JSON.
    }
    throw new Error(detail);
  }
  return (await response.json()) as T;
}

export function fetchMarketStream(cursor: number, signal?: AbortSignal) {
  return getJson<MarketStreamResponse>(
    `/api/crypto/market/stream?cursor=${encodeURIComponent(String(cursor))}&limit=360`,
    signal,
  );
}

export function fetchHistory(symbol: string, resolution: string, signal?: AbortSignal) {
  return getJson<HistoryResponse>(
    `/api/crypto/market/history?symbol=${encodeURIComponent(symbol)}&resolution=${encodeURIComponent(resolution)}&limit=360`,
    signal,
  );
}

export function fetchProspectiveStatus(signal?: AbortSignal) {
  return getJson<ProspectiveStatus>("/api/crypto/prospective/status", signal);
}

export function fetchHealth(signal?: AbortSignal) {
  return getJson<Record<string, unknown>>("/api/crypto/health", signal);
}

export { API_BASE };
