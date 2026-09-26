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

    # Yahoo's adjusted-close stream has historically been inconsistent around
    # some BCBA corporate actions (notably CEPU's 8:1 split on 2017-02-03).
    # Reconcile split events explicitly while retaining dividend adjustment.
    split_events = ((result.get("events") or {}).get("splits") or {})
    splits = []
    for raw in split_events.values():
        try:
            stamp = int(raw.get("date"))
            numerator = float(raw.get("numerator"))
            denominator = float(raw.get("denominator"))
            if numerator > 0 and denominator > 0:
                splits.append((stamp, numerator / denominator))
        except (TypeError, ValueError):
            continue
    splits.sort()

    normalized = {}
    for ts, close in zip(timestamps, adjusted):
        if close is None or close <= 0:
            continue
        factor = 1.0
        for split_ts, ratio in splits:
            if ts < split_ts:
                factor *= ratio
        normalized[time.strftime("%Y-%m-%d", time.gmtime(ts))] = float(close) * factor
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    series = {symbol: yahoo(symbol) for symbol in SYMBOLS}
    digest = write_snapshot(
        args.output,
        series,
        {
            "provider": "Yahoo Finance chart API / adjusted close + split reconciliation",
            "range": "5y",
            "interval": "1d",
            "symbols": SYMBOLS,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )
    print(digest)


if __name__ == "__main__":
    main()
