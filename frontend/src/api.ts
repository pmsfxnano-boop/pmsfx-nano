export type StreamEvent = {
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
  cursor_kind: string;
  market_status: string;
  durability_status: string;
  last_durable_stream_seq: number;
  symbols: SymbolSnapshot[];
  events: StreamEvent[];
  cache_events_available?: number;
  persistence?: {
    degraded: boolean;
    last_error: string | null;
    dropped_events: number;
  };
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


export type EvidenceSnapshot = {
  generated_at: string;
  study: {
    study_id: string;
    version: string;
    protocol_hash: string;
    prospect_days: number;
    min_trade_rows_per_symbol: number;
    forecast_horizons_ms: number[];
  };
  cohort: {
    status: string;
    session_id: string | null;
    runtime_status?: string;
    started_at: string | null;
    ended_at: string | null;
    expected_end_at: string | null;
    elapsed_seconds: number;
    remaining_seconds: number;
    progress_pct: number;
    mature: boolean;
    symbols: string[];
    protocol_hash?: string;
    code_version?: string | null;
  };
  quality_gate: {
    state: string;
    status: string;
    current_session: boolean;
    created_at: string | null;
    age_seconds: number | null;
    replay_fingerprint: string | null;
    rows?: number;
    reasons: string[];
    symbol_stats: Record<string, Record<string, number>>;
    event_type_counts: Record<string, number>;
    report: Record<string, number>;
  };
  research: {
    status: string;
    reason: string | null;
    created_at: string | null;
    metadata?: Record<string, unknown>;
  };
  pit_oos: {
    state: string;
    status: string;
    promotion_eligible: boolean;
    oos_rows: number;
    latest: null | {
      run_id: string;
      created_at: string;
      replay_fingerprint: string;
      model_id: string;
      model_version: string;
      target_kind: string;
      horizon_ms: number;
      placebo_p_value: number | null;
      placebo_iterations: number;
      aggregate: Record<string, unknown>;
      stability: Record<string, unknown>;
      stress: Record<string, unknown>;
    };
  };
  forecast_shadow: {
    count: number;
    outcomes_count: number;
    state: string;
    latest: Record<string, unknown> | null;
    recent_probability_mean: number | null;
    recent_probability_min: number | null;
    recent_probability_max: number | null;
  };
  lead_lag_shadow: {
    observation_count: number;
    pairs: Array<Record<string, unknown>>;
  };
  opportunity_shadow: {
    count: number;
    state_counts: Record<string, number>;
    latest: Array<Record<string, unknown>>;
  };
  regime: {
    status: string;
    validated: boolean;
    symbols: Record<string, Record<string, number | string | boolean>>;
    method: {
      source: string;
      features: string[];
      note: string;
    };
  };
  opportunity_clock: {
    state: "ACTIVE" | "LOCKED";
    validated: boolean;
    mode: "VALIDATED" | "SHADOW";
    horizon_ms: number | null;
    remaining_seconds: number | null;
    blockers: string[];
    rule: string;
  };
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

export function fetchEvidence(signal?: AbortSignal) {
  return getJson<EvidenceSnapshot>("/api/crypto/evidence", signal);
}

export { API_BASE };
