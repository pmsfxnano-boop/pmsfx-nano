export type JsonMap = Record<string, any>;

export type MatrixItem = JsonMap & { symbol: string };
export type MatrixResponse = JsonMap & { items?: MatrixItem[]; counts?: JsonMap; argentina_session?: JsonMap; snapshot_runtime?: JsonMap };
export type TerminalResponse = JsonMap & {
  symbol: string;
  quote?: JsonMap | null;
  forecast?: JsonMap | null;
  signal?: JsonMap | null;
  macro?: JsonMap | null;
  chart?: Array<{ time: string; close: number }>;
  stream?: JsonMap;
  session?: JsonMap;
};
export type ChartResponse = JsonMap & {
  symbol: string;
  timeframe: string;
  resolution?: string;
  rows?: Array<{ time: string; close: number }>;
  coverage?: { start?: string | null; end?: string | null };
  last?: number | null;
  change_pct?: number | null;
  freshness?: JsonMap;
};
export type HealthResponse = JsonMap & {
  primary_market_data?: JsonMap;
  database?: JsonMap;
  market_session?: JsonMap;
  autonomous_runtime?: JsonMap;
  argentina_live?: JsonMap;
  argentina_signals?: JsonMap;
  macro_ingest?: JsonMap;
};
export type ControlResponse = JsonMap & {
  health?: HealthResponse;
  promotion?: JsonMap;
  shadow?: JsonMap;
  runtime?: JsonMap;
};

export const API_BASE = (
  import.meta.env.VITE_API_BASE_URL ||
  "https://gorila-argentum-research.onrender.com"
).replace(/\/$/, "");

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    signal,
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  const body = await response.text();
  let payload: any = null;
  try { payload = body ? JSON.parse(body) : null; } catch { /* handled below */ }
  if (!response.ok) {
    throw new Error(String(payload?.detail || `HTTP ${response.status}`));
  }
  return payload as T;
}

export const fetchHealth = (signal?: AbortSignal) =>
  getJson<HealthResponse>("/api/gorila/health", signal);

export const fetchMatrix = (signal?: AbortSignal) =>
  getJson<MatrixResponse>("/api/gorila/signal-matrix", signal);

export const fetchTerminal = (symbol: string, signal?: AbortSignal) =>
  getJson<TerminalResponse>(`/api/gorila/terminal/${encodeURIComponent(symbol)}`, signal);

export const fetchChart = (symbol: string, timeframe: string, signal?: AbortSignal) =>
  getJson<ChartResponse>(
    `/api/gorila/chart/${encodeURIComponent(symbol)}?timeframe=${encodeURIComponent(timeframe)}&limit=420`,
    signal,
  );

export const fetchBcra = (signal?: AbortSignal) =>
  getJson<JsonMap>("/api/gorila/bcra", signal);

export const fetchControl = (signal?: AbortSignal) =>
  getJson<ControlResponse>("/api/gorila/control", signal);
