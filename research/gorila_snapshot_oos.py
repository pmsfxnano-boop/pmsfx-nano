from __future__ import annotations

import argparse
import time

import httpx

from research.gorila_data_snapshot import write_snapshot

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]


def yahoo(symbol: str) -> dict[str, float]:
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "10y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-DataSnapshot/1.0"},
    )
    response.raise_for_status()
    result = (response.json().get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: empty Yahoo response")
    timestamps = result.get("timestamp") or []
    adjusted = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    if not adjusted:
        raise RuntimeError(f"{symbol}: adjusted close unavailable")
    return {
        time.strftime("%Y-%m-%d", time.gmtime(ts)): float(close)
        for ts, close in zip(timestamps, adjusted)
        if close is not None and close > 0
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    series = {symbol: yahoo(symbol) for symbol in SYMBOLS}
    digest = write_snapshot(
        args.output,
        series,
        {
            "provider": "Yahoo Finance chart API / adjusted close",
            "range": "10y",
            "interval": "1d",
            "symbols": SYMBOLS,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )
    print(digest)


if __name__ == "__main__":
    main()
