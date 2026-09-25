from __future__ import annotations

import json
import math
import statistics
import time
from itertools import combinations

import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [5, 10]
COSTS_BPS = [25, 50, 100]
TRAIN_MIN_DATES = 504
TEST_SIZE_DATES = 126


def get_json(url, params=None):
    r = httpx.get(
        url,
        params=params,
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-PairwiseRank/0.1"},
    )
    r.raise_for_status()
    return r.json()


def yahoo(symbol):
    j = get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        {"range": "5y", "interval": "1d", "events": "history"},
    )
    res = (j.get("chart", {}).get("result") or [None])[0]
    if not res:
        raise RuntimeError(symbol + ": empty")
    ts = res.get("timestamp") or []
    close = ((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {
        time.strftime("%Y-%m-%d", time.gmtime(t)): float(c)
        for t, c in zip(ts, close)
        if c is not None and c > 0
    }


def logret(series, dates, i, n=1):
    if i < n:
        return None
    a = series.get(dates[i - n])
    b = series.get(dates[i])
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return math.log(b / a)


def percentile_rank(value, values):
    n = len(values)
    less = sum(v < value for v in values)
    equal = sum(v == value for v in values)
    return (less + 0.5 * equal) / n if n > 1 else 0.5


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(X, y, epochs=60, lr=0.035, l2=0.002):
    means = [sum(row[j] for row in X) / len(X) for j in range(len(X[0]))]
    scales = [
        max(1e-12, math.sqrt(sum((row[j] - means[j]) ** 2 for row in X) / len(X)))
        for j in range(len(X[0]))
    ]
    Z = [[(v - m) / s for v, m, s in zip(row, means, scales)] for row in X]
    w = [0.0] * len(Z[0])
    b = 0.0
    for _ in range(epochs):
        for row, target in zip(Z, y):
            p = sigmoid(b + sum(a * v for a, v in zip(w, row)))
            e = p - target
            for j in range(len(w)):
                w[j] -= lr * (e * row[j] + l2 * w[j])
            b -= lr * e
    return means, scales, w, b


def score(model, row):
    means, scales, w, b = model
    return b + sum(
        a * (v - mean) / scale
        for a, v, mean, scale in zip(w, row, means, scales)
    )


def build_dates(series):
    return sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))


def feature_snapshot(series, dates, idx):
    r1 = {s: logret(series[s], dates, idx, 1) for s in SYMBOLS}
    r3 = {s: logret(series[s], dates, idx, 3) for s in SYMBOLS}
    r5 = {s: logret(series[s], dates, idx, 5) for s in SYMBOLS}
    if any(r1[s] is None or r3[s] is None or r5[s] is None for s in SYMBOLS):
        return None
    vals1, vals3, vals5 = list(r1.values()), list(r3.values()), list(r5.values())
    out = {}
    for s in SYMBOLS:
        hist = [logret(series[s], dates, k, 1) for k in range(max(25, idx - 60), idx)]
        hist = [v for v in hist if v is not None]
        if len(hist) < 30:
            return None
        vol = statistics.pstdev(hist[-20:]) if len(hist[-20:]) > 1 else 0.0
        prices = [series[s][dates[k]] for k in range(max(0, idx - 20), idx)]
        sd = statistics.pstdev(prices) if len(prices) > 1 else 0.0
        z = (series[s][dates[idx]] - statistics.mean(prices)) / sd if sd > 0 else 0.0
        out[s] = [
            r1[s], r3[s], r5[s], vol, z,
            percentile_rank(r1[s], vals1),
            percentile_rank(r3[s], vals3),
            percentile_rank(r5[s], vals5),
            r1[s] - statistics.median(vals1),
            r3[s] - statistics.median(vals3),
            r5[s] - statistics.median(vals5),
        ]
    return out


def make_dataset(series, horizon):
    dates = build_dates(series)
    data = {}
    for i in range(60, len(dates) - horizon):
        snap = feature_snapshot(series, dates, i)
        if snap is None:
            continue
        fwd = {s: logret(series[s], dates, i + horizon, horizon) for s in SYMBOLS}
        if any(v is None for v in fwd.values()):
            continue
        data[dates[i]] = {"x": snap, "fwd": fwd}
    return dates, data


def pairwise_training_rows(data, dates):
    X, y = [], []
    for d in dates:
        snap = data[d]["x"]
        fwd = data[d]["fwd"]
        for a, b in combinations(SYMBOLS, 2):
            X.append([u - v for u, v in zip(snap[a], snap[b])])
            y.append(1 if fwd[a] > fwd[b] else 0)
    return X, y


def execute(oos_by_date, horizon, cost_bps, mode):
    returns = []
    sorted_dates = sorted(oos_by_date)
    date_pos = {d: i for i, d in enumerate(sorted_dates)}
    last = -10**9
    for d in sorted_dates:
        if date_pos[d] - last < horizon:
            continue
        rows = oos_by_date[d]
        ranked = sorted(
            SYMBOLS,
            key=lambda s: rows[s]["score"] if mode == "model" else rows[s]["momentum"],
        )
        long_s, short_s = ranked[-1], ranked[0]
        returns.append(
            0.5 * (rows[long_s]["fwd"] - rows[short_s]["fwd"])
            - cost_bps / 10000.0
        )
        last = date_pos[d]
    if not returns:
        return {"trades": 0, "net_return": 0.0, "cagr": 0.0}
    eq = 1.0
    for r in returns:
        eq *= 1.0 + r
    return {
        "trades": len(returns),
        "net_return": eq - 1.0,
        "cagr": eq ** (252.0 / (len(returns) * horizon)) - 1.0 if eq > 0 else -1.0,
    }


def run_horizon(series, horizon):
    _, data = make_dataset(series, horizon)
    usable = sorted(data)
    oos = {}
    pairwise_hits = []
    folds = 0
    start = TRAIN_MIN_DATES
    while start + TEST_SIZE_DATES <= len(usable):
        train_dates = usable[: start - horizon]
        test_dates = usable[start:start + TEST_SIZE_DATES]
        X, y = pairwise_training_rows(data, train_dates)
        if len(X) < 1000 or len(set(y)) < 2:
            start += TEST_SIZE_DATES
            continue
        model = fit(X, y)
        for d in test_dates:
            scores = {s: score(model, data[d]["x"][s]) for s in SYMBOLS}
            oos[d] = {
                s: {
                    "score": scores[s],
                    "momentum": data[d]["x"][s][2],
                    "fwd": data[d]["fwd"][s],
                }
                for s in SYMBOLS
            }
            for a, b in combinations(SYMBOLS, 2):
                pred = scores[a] > scores[b]
                actual = data[d]["fwd"][a] > data[d]["fwd"][b]
                pairwise_hits.append(pred == actual)
        folds += 1
        start += TEST_SIZE_DATES

    if not oos:
        raise RuntimeError(f"{horizon}d: no OOS folds")
    execution = {}
    for cost in COSTS_BPS:
        model_m = execute(oos, horizon, cost, "model")
        mom_m = execute(oos, horizon, cost, "momentum")
        execution[str(cost)] = {
            "model_top_bottom": model_m,
            "momentum_top_bottom": mom_m,
            "delta_return": model_m["net_return"] - mom_m["net_return"],
        }
    return {
        "oos_dates": len(oos),
        "outer_folds": folds,
        "pairwise_accuracy": sum(pairwise_hits) / len(pairwise_hits),
        "pairwise_pairs": len(pairwise_hits),
        "execution": execution,
    }


series = {s: yahoo(s) for s in SYMBOLS}
results = {str(h): run_horizon(series, h) for h in HORIZONS}
print(json.dumps({
    "status": "COMPLETE",
    "method": "pairwise-relative-logistic-ranking-wfo-v1",
    "symbols": SYMBOLS,
    "horizons": HORIZONS,
    "costs_bps_roundtrip": COSTS_BPS,
    "train_min_dates": TRAIN_MIN_DATES,
    "test_size_dates": TEST_SIZE_DATES,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "results": results,
}, indent=2))
