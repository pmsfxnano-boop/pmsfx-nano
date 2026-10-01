#!/usr/bin/env python3
"""Production probe for the Cryptonita crypto market path.

Run this from a networked host after the durable Postgres service is available:

  python scripts/crypto_operational_probe.py \
    --base-url https://gorila-crypto-cleanroom-binance-capture.onrender.com \
    --duration 120 --concurrency 8

The probe is read-only. It does not mutate the ledger or runtime state.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def get_json(base_url: str, path: str, timeout: float = 5.0) -> tuple[int, float, dict[str, Any] | None, str | None]:
    url = base_url.rstrip("/") + path
    started = time.perf_counter()
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            elapsed = (time.perf_counter() - started) * 1000.0
            return int(response.status), elapsed, json.loads(body.decode("utf-8")), None
    except Exception as exc:
        elapsed = (time.perf_counter() - started) * 1000.0
        return 599, elapsed, None, f"{type(exc).__name__}: {exc}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--duration", type=float, default=120.0)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration <= 0 or args.concurrency <= 0:
        raise SystemExit("duration and concurrency must be positive")

    deadline = time.monotonic() + args.duration
    cursor = 0
    cursor_violations = 0
    duplicate_keys = 0
    gap_count = 0
    requests = 0
    errors: list[str] = []
    stream_latencies: list[float] = []
    diagnostic_latencies: list[float] = []
    max_cursor = 0

    # Sequential cursor consumer: this models the actual terminal read path.
    while time.monotonic() < deadline:
        query = urllib.parse.urlencode({"cursor": cursor, "limit": 360})
        status, elapsed, payload, error = get_json(
            args.base_url,
            f"/api/crypto/market/stream?{query}",
            args.timeout,
        )
        requests += 1
        stream_latencies.append(elapsed)
        if error or payload is None or status >= 400:
            errors.append(error or f"HTTP {status}")
            time.sleep(0.25)
            continue

        events = payload.get("events") or []
        returned_cursor = int(payload.get("next_cursor") or cursor)
        seqs = [int(row["stream_seq"]) for row in events if row.get("stream_seq") is not None]
        keys = [str(row["event_key"]) for row in events if row.get("event_key")]
        duplicate_keys += len(keys) - len(set(keys))
        if seqs and min(seqs) <= cursor:
            cursor_violations += 1
        if returned_cursor < cursor:
            cursor_violations += 1
        if seqs and returned_cursor < max(seqs):
            cursor_violations += 1
        for previous, current in zip(seqs, seqs[1:]):
            if current <= previous:
                cursor_violations += 1
            if current > previous + 1:
                gap_count += 1
        cursor = max(cursor, returned_cursor)
        max_cursor = max(max_cursor, cursor)

        # Small concurrent health/e2e fan-out to observe read latency under load.
        def probe(_: int) -> tuple[int, float, dict[str, Any] | None, str | None]:
            return get_json(args.base_url, "/api/crypto/operational/e2e", args.timeout)

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            for status2, elapsed2, diagnostic, error2 in pool.map(probe, range(args.concurrency)):
                diagnostic_latencies.append(elapsed2)
                if error2 or diagnostic is None or status2 >= 500:
                    errors.append(error2 or f"E2E HTTP {status2}")
                elif diagnostic.get("fail_closed") and "LEDGER_UNAVAILABLE" in (diagnostic.get("failures") or []):
                    errors.append("ledger_unavailable")

        time.sleep(0.05)

    def percentile(values: list[float], p: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, round((p / 100.0) * (len(ordered) - 1))))
        return ordered[index]

    summary = {
        "requests": requests,
        "cursor": {
            "final": cursor,
            "max": max_cursor,
            "violations": cursor_violations,
            "sequence_gaps_observed": gap_count,
            "duplicate_event_keys": duplicate_keys,
        },
        "latency_ms": {
            "stream_p50": percentile(stream_latencies, 50),
            "stream_p95": percentile(stream_latencies, 95),
            "stream_p99": percentile(stream_latencies, 99),
            "e2e_p50": percentile(diagnostic_latencies, 50),
            "e2e_p95": percentile(diagnostic_latencies, 95),
            "e2e_p99": percentile(diagnostic_latencies, 99),
        },
        "errors": errors[-20:],
        "error_count": len(errors),
        "duration_seconds": args.duration,
        "concurrency": args.concurrency,
        "base_url": args.base_url,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

    # Fail hard on cursor corruption or duplicate delivery in the sequential path.
    return 0 if cursor_violations == 0 and duplicate_keys == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
