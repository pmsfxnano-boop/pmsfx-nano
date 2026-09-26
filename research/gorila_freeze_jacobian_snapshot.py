from __future__ import annotations

import json
import time

import httpx

from research.gorila_data_snapshot import write_snapshot

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]


def fetch(symbol: str) -> dict[str, float]:
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "5y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-Jacobian-Snapshot/2.0"},
    )
    response.raise_for_status()
    payload = response.json()
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: empty")
    timestamps = result.get("timestamp") or []
    closes = ((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    series = {
        time.strftime("%Y-%m-%d", time.gmtime(ts)): float(close)
        for ts, close in zip(timestamps, closes)
        if close is not None and close > 0
    }
    if len(series) < 600:
        raise RuntimeError(f"{symbol}: insufficient_history={len(series)}")
    return series


def main() -> int:
    series = {symbol: fetch(symbol) for symbol in SYMBOLS}
    digest = write_snapshot(
        "jacobian-frozen-snapshot.json",
        series,
        {"purpose": "gorila-jacobian-stress-v2", "symbols": SYMBOLS},
    )
    print(json.dumps({
        "status": "COMPLETE",
        "snapshot": "jacobian-frozen-snapshot.json",
        "data_snapshot_sha256": digest,
        "symbols": SYMBOLS,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
