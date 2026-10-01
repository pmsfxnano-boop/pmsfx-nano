import type { MarketEvent, SymbolMarket } from "./api";

export interface DirectFeedSnapshot {
  symbols: SymbolMarket[];
  events: MarketEvent[];
}

const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const STREAMS = SYMBOLS.flatMap((symbol) => [
  `${symbol.toLowerCase()}@trade`,
  `${symbol.toLowerCase()}@bookTicker`,
]);
const URL =
  "wss://data-stream.binance.vision/stream?streams=" +
  STREAMS.join("/");

export function createDirectBinanceFeed(
  onSnapshot: (snapshot: DirectFeedSnapshot) => void,
): () => void {
  let socket: WebSocket | null = null;
  let stopped = false;
  let reconnectTimer = 0;
  let reconnectMs = 500;
  let snapshotTimer = 0;
  let snapshotPending = false;
  let seq = 0;
  const events: MarketEvent[] = [];
  const latest: Record<string, SymbolMarket> = Object.fromEntries(
    SYMBOLS.map((symbol) => [
      symbol,
      {
        symbol,
        status: "STARTING",
        price: null,
        window_change_pct: null,
        bid: null,
        ask: null,
        bid_qty: null,
        ask_qty: null,
        spread_bps: null,
        imbalance: null,
        freshness_ms: null,
        last_trade_time: null,
        last_book_time: null,
      },
    ]),
  );

  const emit = () => {
    snapshotPending = false;
    snapshotTimer = 0;
    const now = Date.now();
    if (events.length > 240) {
      events.splice(0, events.length - 240);
    }
    const symbols = SYMBOLS.map((symbol) => {
      const item = latest[symbol];
      const freshnessMs =
        item.freshness_ms == null
          ? null
          : Math.max(0, now - item.freshness_ms);
      return {
        ...item,
        freshness_ms: freshnessMs,
        status:
          freshnessMs != null && freshnessMs <= 5000
            ? "LIVE"
            : freshnessMs != null && freshnessMs <= 30000
              ? "DELAYED"
              : "NO_DATA",
      };
    });
    onSnapshot({ symbols, events: events.slice(-240) });
  };

  // The provider can deliver hundreds of messages per second. React does not
  // need one component-tree render per tick, so coalesce provider bursts into
  // ~12.5 visual updates/sec while preserving the latest state and tape.
  const scheduleEmit = () => {
    if (stopped || snapshotPending) return;
    snapshotPending = true;
    snapshotTimer = window.setTimeout(emit, 80);
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
        scheduleEmit();
      };
      socket.onmessage = (message) => {
        try {
          const envelope = JSON.parse(String(message.data)) as {
            data?: Record<string, unknown>;
            [key: string]: unknown;
          };
          const data = (envelope.data ?? envelope) as Record<string, unknown>;
          const type = String(data.e || "");
          const symbol = String(data.s || "").toUpperCase();
          if (!SYMBOLS.includes(symbol)) return;

          const now = new Date().toISOString();
          seq += 1;

          if (type === "trade") {
            const price = Number(data.p);
            const quantity = Number(data.q);
            const event: MarketEvent = {
              stream_seq: seq,
              ledger_seq: null,
              durable: false,
              event_key: `binance-direct-trade-${symbol}-${String(data.t)}`,
              symbol,
              event_type: "trade",
              event_time: new Date(
                Number(data.E || Date.now()),
              ).toISOString(),
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
              last_trade_time: event.received_time,
            };
          } else if (type === "bookTicker") {
            const bid = Number(data.b);
            const ask = Number(data.a);
            const bidQty = Number(data.B);
            const askQty = Number(data.A);
            const mid =
              Number.isFinite(bid) && Number.isFinite(ask)
                ? (bid + ask) / 2
                : null;
            const spread =
              Number.isFinite(bid) &&
              bid > 0 &&
              Number.isFinite(ask)
                ? (ask / bid - 1) * 10000
                : null;

            const event: MarketEvent = {
              stream_seq: seq,
              ledger_seq: null,
              durable: false,
              event_key: `binance-direct-book-${symbol}-${String(data.u)}`,
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
              last_book_time: event.received_time,
            };
          } else {
            return;
          }

          if (events.length > 720) {
            events.splice(0, events.length - 720);
          }
          scheduleEmit();
        } catch {
          // Ignore malformed browser-side provider frames and remain connected.
        }
      };
      socket.onerror = () => {
        try {
          socket?.close();
        } catch {}
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
    if (snapshotTimer) window.clearTimeout(snapshotTimer);
    if (reconnectTimer) window.clearTimeout(reconnectTimer);
    reconnectTimer = 0;
    try {
      socket?.close();
    } catch {}
    socket = null;
  };
}
