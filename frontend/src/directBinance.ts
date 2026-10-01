export interface DirectSymbolMarket {
  symbol: string;
  status: string;
  price: number | null;
  bid: number | null;
  ask: number | null;
  bid_qty: number | null;
  ask_qty: number | null;
  spread_bps: number | null;
  freshness_ms: number | null;
}

export interface DirectMarketEvent {
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

export interface DirectFeedSnapshot {
  symbols: DirectSymbolMarket[];
  events: DirectMarketEvent[];
}

const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const STREAMS = SYMBOLS.flatMap((symbol) => [
  `${symbol.toLowerCase()}@trade`,
  `${symbol.toLowerCase()}@bookTicker`,
]);
const URL =
  "wss://data-stream.binance.vision/stream?streams=" +
  encodeURIComponent(STREAMS.join("/"));

export function createDirectBinanceFeed(
  onSnapshot: (snapshot: DirectFeedSnapshot) => void,
): () => void {
  let socket: WebSocket | null = null;
  let stopped = false;
  let reconnectTimer = 0;
  let reconnectMs = 500;
  let seq = 0;
  const events: DirectMarketEvent[] = [];
  const latest: Record<string, DirectSymbolMarket> = Object.fromEntries(
    SYMBOLS.map((symbol) => [
      symbol,
      {
        symbol,
        status: "STARTING",
        price: null,
        bid: null,
        ask: null,
        bid_qty: null,
        ask_qty: null,
        spread_bps: null,
        freshness_ms: null,
      },
    ]),
  );

  const emit = () => {
    const now = Date.now();
    const symbols = SYMBOLS.map((symbol) => {
      const item = latest[symbol];
      const freshness_ms = item.freshness_ms == null ? null : Math.max(0, now - item.freshness_ms);
      return {
        ...item,
        freshness_ms,
        status:
          freshness_ms != null && freshness_ms <= 5000
            ? "LIVE"
            : freshness_ms != null && freshness_ms <= 30000
              ? "DELAYED"
              : "NO_DATA",
      };
    });
    onSnapshot({ symbols, events: events.slice(-360) });
  };

  const scheduleReconnect = () => {
    if (stopped || reconnectTimer) return;
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = 0;
      connect();
    }, reconnectMs);
    reconnectMs = Math.min(10000, reconnectMs * 2);
  };

  const connect = () => {
    if (stopped) return;
    try {
      socket = new WebSocket(URL);
      socket.onopen = () => {
        reconnectMs = 500;
        emit();
      };
      socket.onmessage = (message) => {
        const envelope = JSON.parse(String(message.data)) as { data?: Record<string, unknown> };
        const data = envelope.data || envelope;
        const type = String(data.e || "");
        const symbol = String(data.s || "").toUpperCase();
        if (!SYMBOLS.includes(symbol)) return;

        const now = new Date().toISOString();
        const eventKey =
          type === "trade"
            ? `binance-direct-trade-${symbol}-${String(data.t)}`
            : `binance-direct-book-${symbol}-${String(data.u)}`;

        seq += 1;
        if (type === "trade") {
          const price = Number(data.p);
          const quantity = Number(data.q);
          const event: DirectMarketEvent = {
            stream_seq: seq,
            ledger_seq: null,
            durable: false,
            event_key: eventKey,
            symbol,
            event_type: "trade",
            event_time: new Date(Number(data.E || Date.now())).toISOString(),
            received_time: now,
            price: Number.isFinite(price) ? price : null,
            quantity: Number.isFinite(quantity) ? quantity : null,
            side: Boolean(data.m) ? "SELL" : "BUY",
          };
          events.push(event);
          latest[symbol] = {
            ...latest[symbol],
            price: event.price,
            freshness_ms: Date.now(),
          };
        } else if (type === "bookTicker") {
          const bid = Number(data.b);
          const ask = Number(data.a);
          const bidQty = Number(data.B);
          const askQty = Number(data.A);
          const mid = Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : null;
          const spread = Number.isFinite(bid) && bid > 0 && Number.isFinite(ask)
            ? (ask / bid - 1) * 10000
            : null;
          const event: DirectMarketEvent = {
            stream_seq: seq,
            ledger_seq: null,
            durable: false,
            event_key: eventKey,
            symbol,
            event_type: "bookTicker",
            event_time: now,
            received_time: now,
            price: mid,
            quantity: null,
            side: null,
            bid: Number.isFinite(bid) ? bid : null,
            ask: Number.isFinite(ask) ? ask : null,
            bid_qty: Number.isFinite(bidQty) ? bidQty : null,
            ask_qty: Number.isFinite(askQty) ? askQty : null,
          };
          events.push(event);
          latest[symbol] = {
            ...latest[symbol],
            price: latest[symbol].price ?? mid,
            bid: event.bid ?? null,
            ask: event.ask ?? null,
            bid_qty: event.bid_qty ?? null,
            ask_qty: event.ask_qty ?? null,
            spread_bps: spread,
            freshness_ms: Date.now(),
          };
        } else {
          return;
        }
        emit();
      };
      socket.onerror = () => {
        try { socket?.close(); } catch {}
      };
      socket.onclose = () => {
        socket = null;
        scheduleReconnect();
      };
    } catch {
      scheduleReconnect();
    }
  };

  connect();

  const freshnessTimer = window.setInterval(emit, 1000);

  return () => {
    stopped = true;
    window.clearInterval(freshnessTimer);
    if (reconnectTimer) window.clearTimeout(reconnectTimer);
    reconnectTimer = 0;
    try { socket?.close(); } catch {}
    socket = null;
  };
}
