export type AnyMap = Record<string, any>;

export interface SymbolMarket {
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
}

export interface MarketEvent {
  stream_seq: number;
  ledger_seq: number | null;
  durable: boolean;
  event_key: string;
  symbol: string;
  event_type: "trade" | "bookTicker";
  event_time: string;
  received_time: string;
  price: number | null;
  quantity: number | null;
  side: "BUY" | "SELL" | null;
  bid?: number | null;
  ask?: number | null;
  bid_qty?: number | null;
  ask_qty?: number | null;
}

export interface MarketStream {
  status: string;
  market_status: string;
  durability_status: string;
  server_time: string;
  next_cursor: number;
  cursor_kind: string;
  last_durable_stream_seq: number;
  symbols: SymbolMarket[];
  events: MarketEvent[];
  cache_events_available: number;
  persistence: {
    degraded: boolean;
    last_error: string | null;
    dropped_events: number;
  };
  forecast: { automatic_promotion: boolean; execution: boolean };
}

export interface HistoryResponse {
  symbol: string;
  resolution: string;
  candles: Array<{
    time: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    trades: number;
  }>;
}

export interface HealthResponse extends AnyMap {
  status: string;
  provider: string;
  symbols: string[];
  streams: string[];
  worker_alive: boolean;
  quality_monitor_alive: boolean;
  heartbeat_alive: boolean;
  symbols_live: boolean;
  storage_backend: string;
  freshness_contract: {
    live_max_age_seconds: number;
    delayed_max_age_seconds: number;
  };
  market_plane?: AnyMap;
}

export interface EvidenceResponse {
  generated_at: string;
  study: {
    study_id: string;
    version: string;
    protocol_hash: string;
    prospect_days: number;
    min_trade_rows_per_symbol: number;
    forecast_horizons_ms: number[];
    alpha_feature_set?: string;
    alpha_model_version?: string;
    promotion_latency_guard?: {
      median_max_ratio?: number;
      p95_max_ratio?: number;
      scope?: string;
    };
  };
  cohort: AnyMap;
  quality_gate: AnyMap;
  research: AnyMap;
  pit_oos: AnyMap;
  forecast_shadow: AnyMap;
  online_shadow: {
    state?: string;
    count?: number;
    outcomes_count?: number;
    resolved_rate?: number;
    scored_count?: number;
    warmup_count?: number;
    recent_mean_probability?: number | null;
    by_horizon?: Record<string, {
      count?: number;
      scored_count?: number;
      mean_probability?: number | null;
    }>;
  };
  lead_lag_shadow: AnyMap;
  opportunity_shadow: AnyMap;
  regime: AnyMap;
  opportunity_clock: {
    state: string;
    validated: boolean;
    mode: string;
    horizon_ms: number | null;
    remaining_seconds: number | null;
    blockers: string[];
    rule: string;
    phase?: "LOCKED" | "ENTRY_WINDOW" | "DECAYING" | "EXIT_WINDOW" | "CLOSED" | string;
    window_started_at?: string | null;
    window_ends_at?: string | null;
    entry_window_end_at?: string | null;
    exit_window_start_at?: string | null;
    exit_window_end_at?: string | null;
    edge?: number | null;
    confidence?: number | null;
    probability?: number | null;
    decay_state?: string | null;
    leader_symbol?: string | null;
    target_symbol?: string | null;
    symbol?: string | null;
  };
}

export interface ConfigResponse extends AnyMap {
  provider: string;
  symbols: string[];
  streams: string[];
  study_id: string | null;
  protocol_version: string | null;
  protocol_hash: string | null;
  quality_required_event_types: string[];
}

// The production terminal uses a same-origin gateway. This removes browser-to-API
// DNS/CORS fragility while the gateway forwards /api/crypto/* to the durable service.
export const API_BASE = (
  import.meta.env.VITE_CRYPTO_API_BASE_URL ||
  (typeof window !== "undefined" ? window.location.origin : "https://gorila-crypto-cleanroom-binance-capture.onrender.com")
).replace(/\/$/, "");

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(API_BASE + path, {
    signal,
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  const body = await response.text();
  let payload: unknown = null;
  try {
    payload = body ? JSON.parse(body) : null;
  } catch {
    throw new Error(`Invalid JSON from API (HTTP ${response.status})`);
  }
  if (!response.ok) {
    const detail = (payload as AnyMap | null)?.detail;
    throw new Error(String(detail || `HTTP ${response.status}`));
  }
  return payload as T;
}

export const fetchMarketStream = (cursor = 0, limit = 360, signal?: AbortSignal) =>
  getJson<MarketStream>(`/api/crypto/market/stream?cursor=${encodeURIComponent(cursor)}&limit=${encodeURIComponent(limit)}`, signal);

export const fetchHistory = (symbol: string, resolution: string, limit = 240, signal?: AbortSignal) =>
  getJson<HistoryResponse>(
    `/api/crypto/market/history?symbol=${encodeURIComponent(symbol)}&resolution=${encodeURIComponent(resolution)}&limit=${encodeURIComponent(limit)}`,
    signal,
  );

export const fetchHealth = (signal?: AbortSignal) =>
  getJson<HealthResponse>("/api/crypto/health", signal);

export const fetchEvidence = (signal?: AbortSignal) =>
  getJson<EvidenceResponse>("/api/crypto/evidence", signal);

export const fetchConfig = (signal?: AbortSignal) =>
  getJson<ConfigResponse>("/api/crypto/config", signal);
