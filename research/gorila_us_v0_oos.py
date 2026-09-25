from __future__ import annotations

import json
import math
import statistics
import time

import httpx

SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA"]


def fetch(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "5y", "interval": "1d", "events": "history"}
    r = httpx.get(url, params=params, timeout=30, headers={"User-Agent": "Gorila-US-V0-OOS/0.1"})
    r.raise_for_status()
    result = (r.json().get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: empty chart")
    ts = result.get("timestamp") or []
    close = ((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    rows = [
        (time.strftime("%Y-%m-%d", time.gmtime(t)), float(c))
        for t, c in zip(ts, close)
        if c is not None and c > 0
    ]
    if len(rows) < 120:
        raise RuntimeError(f"{symbol}: insufficient rows {len(rows)}")
    return rows


def feature_rows(rows):
    vals = [p for _, p in rows]
    out = []
    for i in range(25, len(vals) - 1):
        def lr(n):
            return math.log(vals[i] / vals[i - n])

        window = vals[i - 20:i]
        rets = [
            math.log(b / a)
            for a, b in zip(window, vals[i - 19:i])
            if a > 0 and b > 0
        ]
        vol = statistics.pstdev(rets) if len(rets) > 1 else 0.0
        sd = statistics.pstdev(window) if len(window) > 1 else 0.0
        z = (vals[i] - statistics.mean(window)) / sd if sd > 0 else 0.0
        x = [lr(1), lr(3), lr(5), vol, z]
        y = 1 if vals[i + 1] > vals[i] else 0
        out.append((x, y))
    return out


def sigmoid(z):
    z = max(-30.0, min(30.0, z))
    return 1.0 / (1.0 + math.exp(-z))


def fit(X, y):
    means = [sum(r[j] for r in X) / len(X) for j in range(len(X[0]))]
    scales = [
        max(1e-12, math.sqrt(sum((r[j] - means[j]) ** 2 for r in X) / len(X)))
        for j in range(len(X[0]))
    ]
    Z = [[(v - m) / s for v, m, s in zip(r, means, scales)] for r in X]
    w = [0.0] * len(Z[0])
    b = 0.0
    for _ in range(250):
        for row, target in zip(Z, y):
            p = sigmoid(b + sum(a * v for a, v in zip(w, row)))
            e = p - target
            for j in range(len(w)):
                w[j] -= 0.04 * e * row[j]
            b -= 0.04 * e
    return means, scales, w, b


def predict(model, row):
    means, scales, w, b = model
    return sigmoid(
        b + sum(a * (v - m) / s for a, v, m, s in zip(w, row, means, scales))
    )


def evaluate(samples):
    n = len(samples)
    split = int(n * 0.8)
    train = samples[:split]
    test = samples[split:]
    X = [x for x, _ in train]
    y = [y for _, y in train]
    if len(set(y)) < 2 or not test:
        raise RuntimeError("invalid training split")
    model = fit(X, y)
    probs = [predict(model, x) for x, _ in test]
    labels = [y for _, y in test]
    accuracy = sum((p >= 0.5) == bool(y) for p, y in zip(probs, labels)) / len(labels)
    brier = sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)
    base = sum(labels) / len(labels)
    baseline_brier = sum((base - y) ** 2 for y in labels) / len(labels)
    return {
        "samples": n,
        "train": len(train),
        "test": len(test),
        "accuracy": accuracy,
        "brier": brier,
        "baseline_brier": baseline_brier,
        "brier_skill": 1.0 - brier / baseline_brier if baseline_brier > 0 else None,
        "test_up_rate": base,
        "mean_predicted_up": statistics.mean(probs),
        "calibration_gap": abs(statistics.mean(probs) - base),
    }


results = {}
for symbol in SYMBOLS:
    try:
        rows = fetch(symbol)
        results[symbol] = evaluate(feature_rows(rows))
    except Exception as exc:
        results[symbol] = {"error": str(exc)}

print(
    json.dumps(
        {
            "status": "COMPLETE",
            "method": "gorila-v0-own-price-logistic-chronological-80-20",
            "universe": SYMBOLS,
            "history": "Yahoo Chart 5y daily",
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "results": results,
        },
        indent=2,
    )
)
