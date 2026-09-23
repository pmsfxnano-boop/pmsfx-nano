"""PMSF-X Nano — Tiingo IEX websocket market cache.

A single persistent websocket replaces high-frequency REST quote polling.
The cache is used by dashboard/state and outcome resolution first.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import websocket

STREAM_URL = "wss://api.tiingo.com/tiingo/equity/intraday"
DEFAULT_SYMBOLS = ("AAPL", "MSFT", "NVDA", "TSLA")
# Tiingo recommends the consolidated Equity Realtime websocket for derived
# real-time data. thresholdLevel=4 supplies liquidity bid/ask + reference price.
DEFAULT_THRESHOLD_LEVEL = 4

_LOCK = threading.RLock()
_LATEST: dict[str, dict[str, Any]] = {}
_STATUS: dict[str, Any] = {
    "status": "STOPPED",
    "connected_at": None,
    "last_message_at": None,
    "last_error": None,
    "messages": 0,
    "threshold_level": DEFAULT_THRESHOLD_LEVEL,
}
_THREAD: threading.Thread | None = None
_STOP = threading.Event()


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_message(payload: dict[str, Any]) -> None:
    data = payload.get("data")
    if not isinstance(data, list) or len(data) < 3:
        return

    service = payload.get("service")
    if service != "cons":
        return

    level = int(_STATUS.get("threshold_level") or DEFAULT_THRESHOLD_LEVEL)
    message_type = payload.get("messageType")
    if message_type != "A":
        return

    # Consolidated liquidity message at thresholdLevel=4:
    # [timestamp, ticker, lqSpread, lqBidSize, lqBidPrice,
    #  referencePrice, lqAskPrice, lqAskSize]
    if level == 4 and len(data) >= 8:
        ts, ticker = data[0], data[1]
        symbol = str(ticker).upper()
        try:
            reference_price = float(data[5])
        except (TypeError, ValueError):
            reference_price = None

        with _LOCK:
            current = copy.deepcopy(_LATEST.get(symbol, {}))
            current.update({
                "symbol": symbol,
                "quote_timestamp": str(ts),
                "received_at": _iso_now(),
                "source": "tiingo_equity_websocket",
            })
            if data[3] is not None:
                current["bid_size"] = data[3]
            if data[4] is not None:
                current["bid"] = data[4]
            if reference_price is not None:
                current["last"] = reference_price
                current["mid"] = reference_price
            if data[6] is not None:
                current["ask"] = data[6]
            if data[7] is not None:
                current["ask_size"] = data[7]
            _LATEST[symbol] = current
        return

    # Backward-compatible handling for IEX reference stream if configured.
    if level == 6 and len(data) >= 3:
        ts, ticker, ref_price = data[0], data[1], data[2]
        symbol = str(ticker).upper()
        try:
            price = float(ref_price)
        except (TypeError, ValueError):
            return
        with _LOCK:
            current = copy.deepcopy(_LATEST.get(symbol, {}))
            current.update({
                "symbol": symbol,
                "last": price,
                "quote_timestamp": str(ts),
                "received_at": _iso_now(),
                "source": "tiingo_iex_websocket_reference",
            })
            _LATEST[symbol] = current
        return

def _on_open(ws) -> None:
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        return
    symbols = [
        s.strip().upper()
        for s in os.getenv("PMSFX_MARKET_STREAM_SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")
        if s.strip()
    ]
    try:
        threshold = int(os.getenv("PMSFX_MARKET_STREAM_THRESHOLD", str(DEFAULT_THRESHOLD_LEVEL)))
    except ValueError:
        threshold = DEFAULT_THRESHOLD_LEVEL
    with _LOCK:
        _STATUS["threshold_level"] = threshold

    # Tiingo's documented websocket subscription uses the top-level
    # authorization field. Restricting tickers keeps the firehose bounded.
    subscribe = {
        "eventName": "subscribe",
        "authorization": token,
        "eventData": {
            "thresholdLevel": threshold,
            "tickers": symbols,
        },
    }
    ws.send(json.dumps(subscribe))
    with _LOCK:
        _STATUS["status"] = "CONNECTED"
        _STATUS["connected_at"] = _iso_now()
        _STATUS["last_error"] = None
    print(
        "PMSF-X TIINGO WS: CONNECTED",
        {"url": STREAM_URL, "service": "cons", "symbols": tuple(symbols), "threshold_level": threshold},
    )


def _on_message(_ws, raw_message: str) -> None:
    try:
        payload = json.loads(raw_message)
    except (TypeError, ValueError):
        return
    with _LOCK:
        _STATUS["last_message_at"] = _iso_now()
        _STATUS["messages"] = int(_STATUS.get("messages") or 0) + 1

    if payload.get("messageType") == "E":
        print("PMSF-X TIINGO WS ERROR MESSAGE:", payload)
        return

    if payload.get("messageType") == "I":
        response = payload.get("response") or {}
        if response.get("code") and response.get("code") != 200:
            print("PMSF-X TIINGO WS SUBSCRIBE ERROR:", payload)
        return

    _record_message(payload)


def _on_error(_ws, error) -> None:
    with _LOCK:
        _STATUS["status"] = "ERROR"
        _STATUS["last_error"] = f"{type(error).__name__}: {error}"
    print("PMSF-X TIINGO WS ERROR:", _STATUS["last_error"])


def _on_close(_ws, code, message) -> None:
    with _LOCK:
        if not _STOP.is_set():
            _STATUS["status"] = "DISCONNECTED"
        else:
            _STATUS["status"] = "STOPPED"
    print("PMSF-X TIINGO WS: CLOSED", {"code": code, "message": message})


def _run() -> None:
    while not _STOP.is_set():
        token = os.getenv("TIINGO_API_KEY")
        if not token:
            time.sleep(5)
            continue

        try:
            ws = websocket.WebSocketApp(
                STREAM_URL,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                ping_payload="PMSF-X",
            )
        except Exception as exc:
            _on_error(None, exc)

        if not _STOP.is_set():
            time.sleep(5)


def start_stream() -> None:
    global _THREAD
    if _THREAD is not None and _THREAD.is_alive():
        return
    _STOP.clear()
    _THREAD = threading.Thread(target=_run, name="pmsfx-tiingo-ws", daemon=True)
    _THREAD.start()
    print("PMSF-X TIINGO WS: STARTING")


def stop_stream() -> None:
    _STOP.set()
    with _LOCK:
        _STATUS["status"] = "STOPPED"


def get_snapshot(symbol: str) -> dict[str, Any] | None:
    key = str(symbol).upper().strip()
    with _LOCK:
        value = _LATEST.get(key)
        return copy.deepcopy(value) if value else None


def get_quote(symbol: str) -> dict[str, Any] | None:
    snap = get_snapshot(symbol)
    if not snap:
        return None

    quote = {
        "ticker": snap["symbol"],
        "last": snap.get("last"),
        "bidPrice": snap.get("bid"),
        "askPrice": snap.get("ask"),
        "bidSize": snap.get("bid_size") or 0,
        "askSize": snap.get("ask_size") or 0,
        "quoteTimestamp": snap.get("quote_timestamp"),
        "timestamp": snap.get("quote_timestamp"),
        "lastSaleTimestamp": snap.get("quote_timestamp"),
    }
    if quote["last"] is None and quote["bidPrice"] is not None and quote["askPrice"] is not None:
        quote["last"] = (float(quote["bidPrice"]) + float(quote["askPrice"])) / 2.0
    return quote


def stream_status() -> dict[str, Any]:
    with _LOCK:
        result = copy.deepcopy(_STATUS)
        result["cached_symbols"] = sorted(_LATEST.keys())
        return result
