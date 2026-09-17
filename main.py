import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

APP_DIR = Path(__file__).resolve().parent
TIINGO_URL = "https://api.tiingo.com/iex"

app = FastAPI(title="PMSF-X Nano", version="0.1.0")


def tiingo_headers() -> dict:
    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise HTTPException(status_code=503, detail="TIINGO_API_KEY is not configured")
    return {"Authorization": f"Token {token}", "Content-Type": "application/json"}


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(APP_DIR / "static" / "index.html")


@app.get("/health")
def health():
    configured = bool(os.getenv("TIINGO_API_KEY"))
    return {
        "status": "ok",
        "service": "pmsfx-nano",
        "version": "0.1.0",
        "tiingo_configured": configured,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/quote/{ticker}")
async def quote(ticker: str):
    symbol = ticker.strip().upper()
    if not symbol or not symbol.isalnum():
        raise HTTPException(status_code=400, detail="Invalid ticker")

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(f"{TIINGO_URL}/{symbol}", headers=tiingo_headers())

    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail="Tiingo request failed")

    data = response.json()
    if isinstance(data, list):
        if not data:
            raise HTTPException(status_code=404, detail="No quote returned")
        data = data[0]

    return {
        "source": "tiingo_iex",
        "received_at": datetime.now(timezone.utc).isoformat(),
        "quote": data,
    }


@app.get("/api/state/{ticker}")
async def state(ticker: str):
    result = await quote(ticker)
    q = result["quote"]
    last = q.get("last")
    bid = q.get("bidPrice")
    ask = q.get("askPrice")
    bid_size = q.get("bidSize") or 0
    ask_size = q.get("askSize") or 0

    spread_bps = None
    microprice = None
    if bid is not None and ask is not None and bid > 0:
        mid = (bid + ask) / 2
        spread_bps = round((ask - bid) / mid * 10000, 3)
        if bid_size + ask_size > 0:
            microprice = round((ask * bid_size + bid * ask_size) / (bid_size + ask_size), 6)

    return {
        "symbol": ticker.upper(),
        "last": last,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread_bps": spread_bps,
        "microprice": microprice,
        "data_health": "HEALTHY" if last is not None else "DEGRADED",
        "forecast": None,
        "gatillazo": "NO_FORECAST",
        "rule": "NO DATA -> NO STATE -> NO FORECAST -> NO GATILLAZO",
    }
