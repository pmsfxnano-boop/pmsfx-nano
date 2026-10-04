import type { MarketEvent, SymbolMarket } from "./api";

export interface DirectFeedSnapshot {
  symbols: SymbolMarket[];
  events: MarketEvent[];
  connection: "CONNECTING" | "LIVE" | "STALE" | "OFFLINE";
  last_message_at: number | null;
}

const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const STREAMS = SYMBOLS.flatMap((symbol) => [
  `${symbol.toLowerCase()}@trade`,
  `${symbol.toLowerCase()}@bookTicker`,
]);
const URL =
  "wss://data-stream.binance.vision/stream?streams=" +
  STREAMS.join("/");

const LIVE_MAX_AGE_MS = 5000;
const STALE_MAX_AGE_MS = 15000;
const WATCHDOG_MS = 3000;

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
  let lastMessageAt: number | null = null;
  let connection: DirectFeedSnapshot["connection"] = "CONNECTING";
  const events: MarketEvent[] = [];
  const latest: Record<string, SymbolMarket> = Object.fromEntries(
    SYMBOLS.map((symbol) => [
      symbol,
      {
        symbol,
        status: "CONNECTING",
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
    if (events.length > 720) events.splice(0, events.length - 720);

    let liveCount = 0;
    let staleCount = 0;
    const symbols = SYMBOLS.map((symbol) => {
      const item = latest[symbol];
      const freshnessMs =
        item.freshness_ms == null
          ? null
          : Math.max(0, now - item.freshness_ms);

      const status =
        freshnessMs != null && freshnessMs <= LIVE_MAX_AGE_MS
          ? "LIVE"
          : freshnessMs != null && freshnessMs <= STALE_MAX_AGE_MS
            ? "DELAYED"
            : item.freshness_ms != null
              ? "STALE"
              : "NO_DATA";

      if (status === "LIVE") liveCount += 1;
      if (status === "DELAYED" || status === "STALE") staleCount += 1;

      return { ...item, freshness_ms: freshnessMs, status };
    });

    if (liveCount === SYMBOLS.length) connection = "LIVE";
    else if (liveCount > 0 || staleCount > 0) connection = "STALE";
    else if (socket) connection = "CONNECTING";
    else connection = "OFFLINE";

    onSnapshot({
      symbols,
      events: events.slice(-360),
      connection,
      last_message_at: lastMessageAt,
    });
  };

  const scheduleEmit = () => {
    if (stopped || snapshotPending) return;
    snapshotPending = true;
    snapshotTimer = window.setTimeout(emit, 80);
  };

  const closeSocketForRecovery = () => {
    if (!socket) return;
    try {
      socket.close();
    } catch {}
    socket = null;
  };

  const scheduleReconnect = () => {
    if (stopped || reconnectTimer) return;
    connection = "CONNECTING";
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = 0;
      connect();
    }, reconnectMs);
    reconnectMs = Math.min(10000, reconnectMs * 2);
    emit();
  };

  const connect = () => {
    if (stopped) return;
    connection = "CONNECTING";
    try {
      socket = new WebSocket(URL);

      socket.onopen = () => {
        reconnectMs = 500;
        connection = "LIVE";
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

          const receivedMs = Date.now();
          lastMessageAt = receivedMs;
          const receivedIso = new Date(receivedMs).toISOString();
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
              event_time: new Date(Number(data.E || receivedMs)).toISOString(),
              received_time: receivedIso,
              price: Number.isFinite(price) ? price : null,
              quantity: Number.isFinite(quantity) ? quantity : null,
              side: Boolean(data.m) ? "SELL" : "BUY",
            };
            events.push(event);
            latest[symbol] = {
              ...latest[symbol],
              price: event.price,
              freshness_ms: receivedMs,
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
              event_time: receivedIso,
              received_time: receivedIso,
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
              imbalance:
                Number.isFinite(bidQty) &&
                Number.isFinite(askQty) &&
                bidQty + askQty > 0
                  ? (bidQty - askQty) / (bidQty + askQty)
                  : null,
              freshness_ms: receivedMs,
              last_book_time: event.received_time,
            };
          } else {
            return;
          }

          scheduleEmit();
        } catch {
          // Ignore malformed provider frames and let the watchdog reconnect if needed.
        }
      };

      socket.onerror = () => {
        connection = "STALE";
        scheduleReconnect();
      };

      socket.onclose = () => {
        socket = null;
        scheduleReconnect();
      };
    } catch {
      scheduleReconnect();
    }
  };

  const watchdog = window.setInterval(() => {
    if (stopped) return;
    const age = lastMessageAt == null ? Infinity : Date.now() - lastMessageAt;
    if (age > STALE_MAX_AGE_MS) {
      connection = socket ? "STALE" : "OFFLINE";
      closeSocketForRecovery();
      scheduleReconnect();
    }
    emit();
  }, WATCHDOG_MS);

  const onVisibility = () => {
    if (document.visibilityState !== "visible" || stopped) return;
    const age = lastMessageAt == null ? Infinity : Date.now() - lastMessageAt;
    if (age > LIVE_MAX_AGE_MS) {
      closeSocketForRecovery();
      scheduleReconnect();
    }
    emit();
  };

  document.addEventListener("visibilitychange", onVisibility);
  connect();
  emit();

  return () => {
    stopped = true;
    document.removeEventListener("visibilitychange", onVisibility);
    window.clearInterval(watchdog);
    if (snapshotTimer) window.clearTimeout(snapshotTimer);
    if (reconnectTimer) window.clearTimeout(reconnectTimer);
    reconnectTimer = 0;
    try {
      socket?.close();
    } catch {}
    socket = null;
  };
}
