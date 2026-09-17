import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

APP_DIR = Path(__file__).resolve().parent
TIINGO_IEX_URL = "https://api.tiingo.com/iex"
TIINGO_EQUITY_URL = "https://api.tiingo.com/tiingo/equity/intraday"

TICKER_ALIASES = {
    "APPLE": "AAPL",
    "MICROSOFT": "MSFT",
    "MICROSOF": "MSFT",
    "NVIDIA": "NVDA",
    "NDIVIA": "NVDA",
    "TESLA": "TSLA",
}

app = FastAPI(title="PMSF-X Nano", version="0.1.2")


def normalize_ticker(value: str) -> str:
    symbol = value.strip().upper()
    return TICKER_ALIASES.get(symbol, symbol)


def tiingo_headers() -> dict:
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")
    return {"Authorization": f"Token {token}", "Content-Type": "application/json"}


def first_record(data):
    if isinstance(data, list):
        return data[0] if data else None
    if isinstance(data, dict):
        return data
    return None


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(
        APP_DIR / "static" / "index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/health")
def health():
    configured = bool(os.getenv("TIINGO_API_KEY"))
    return {
        "status": "ok",
        "service": "pmsfx-nano",
        "version": "0.1.2",
        "tiingo_configured": configured,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/quote/{ticker}")
async def quote(ticker: str):
    symbol = normalize_ticker(ticker)
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")

    headers = tiingo_headers()
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{TIINGO_IEX_URL}/{symbol}", headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail="Tiingo IEX request failed")

        iex = first_record(response.json()) or {}

        # Only treat the IEX response as TOPS when real bid AND ask prices exist.
        # Zero/null sizes must not prevent the consolidated-feed fallback.
        has_iex_tops = iex.get("bidPrice") is not None and iex.get("askPrice") is not None

        if has_iex_tops:
            return {
                "source": "tiingo_iex_tops",
                "received_at": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "quote": iex,
            }

        # Fallback for accounts without the IEX FULL TOPS entitlement.
        response = await client.get(f"{TIINGO_EQUITY_URL}/{symbol}", headers=headers)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail="Tiingo equity intraday request failed")

        equity = first_record(response.json())
        if not equity:
            raise HTTPException(status_code=404, detail="No Tiingo quote returned")

        return {
            "source": "tiingo_equity_intraday",
            "received_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "quote": equity,
        }


@app.get("/api/state/{ticker}")
async def state(ticker: str):
    symbol = normalize_ticker(ticker)
    result = await quote(symbol)
    q = result["quote"]

    if result["source"] == "tiingo_iex_tops":
        last = q.get("last")
        bid = q.get("bidPrice")
        ask = q.get("askPrice")
        bid_size = q.get("bidSize") or 0
        ask_size = q.get("askSize") or 0
        feed_label = "Tiingo IEX TOPS"
    else:
        # Derived/consolidated Tiingo feed. These are liquidity-reference
        # metrics, not raw IEX exchange quotes.
        last = q.get("tngoLast")
        if last is None:
            last = q.get("last")
        bid = q.get("lqBidPrice")
        ask = q.get("lqAskPrice")
        bid_size = q.get("lqBidSize") or 0
        ask_size = q.get("lqAskSize") or 0
        feed_label = "Tiingo consolidated · liquidity reference"

    spread_bps = None
    microprice = None
    if bid is not None and ask is not None and bid > 0:
        mid = (bid + ask) / 2
        spread_bps = round((ask - bid) / mid * 10000, 3)
        if bid_size + ask_size > 0:
            microprice = round((ask * bid_size + bid * ask_size) / (bid_size + ask_size), 6)

    healthy = last is not None and bid is not None and ask is not None

    return {
        "symbol": symbol,
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread_bps": spread_bps,
        "microprice": microprice,
        "data_health": "HEALTHY" if healthy else "DEGRADED",
        "data_source": result["source"],
        "feed_label": feed_label,
        "quote_timestamp": q.get("quoteTimestamp") or q.get("timestamp"),
        "received_at": result["received_at"],
        "forecast": None,
        "gatillazo": "NO_FORECAST",
        "rule": "NO DATA -> NO STATE -> NO FORECAST -> NO GATILLAZO",
    }
