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
  forecast: {
    automatic_promotion: boolean;
    execution: boolean;
  };
};

const API_BASE = (
  import.meta.env.VITE_API_BASE_URL ||
  "https://gorila-crypto-cleanroom-binance-capture.onrender.com"
).replace(/\/$/, "");

export async function fetchMarketStream(cursor: number, signal?: AbortSignal) {
  const query = new URLSearchParams({
    cursor: String(cursor),
    limit: "360",
  });

  const response = await fetch(
    `${API_BASE}/api/crypto/market/stream?${query.toString()}`,
    {
      signal,
      headers: { Accept: "application/json" },
      cache: "no-store",
    }
  );

  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      if (payload?.detail) detail = String(payload.detail);
    } catch {
      // Keep the HTTP error when the body is not JSON.
    }
    throw new Error(detail);
  }

  return (await response.json()) as MarketStreamResponse;
}

export { API_BASE };
