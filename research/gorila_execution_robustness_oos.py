from __future__ import annotations

import json
import math
import statistics
import time

import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [5, 10]
THRESHOLDS = [(0.55, 0.45), (0.60, 0.40), (0.65, 0.35)]
COSTS_BPS = [25, 50, 100]
TRAIN_MIN = 504
TEST_SIZE = 126


def get_json(url, params=None):
    r = httpx.get(
        url,
        params=params,
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-Execution-Robustness/0.1"},
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


def ret(series, dates, i, n=1):
    if i < n:
        return None
    a = series.get(dates[i - n])
    b = series.get(dates[i])
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return math.log(b / a)


def features(series, dates, i):
    own = [ret(series, dates, i, k) for k in (1, 3, 5)]
    if any(v is None for v in own):
        return None
    hist = [ret(series, dates, k, 1) for k in range(max(25, i - 60), i)]
    hist = [v for v in hist if v is not None]
    if len(hist) < 30:
        return None
    vol = statistics.pstdev(hist[-20:]) if len(hist[-20:]) > 1 else 0.0
    vals = [series[dates[k]] for k in range(max(0, i - 20), i) if dates[k] in series]
    if len(vals) < 2:
        return None
    sd = statistics.pstdev(vals)
    z = (series[dates[i]] - statistics.mean(vals)) / sd if sd > 0 else 0.0
    return [own[0], own[1], own[2], vol, z]


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(X, y, epochs=220, lr=0.04, l2=0.001):
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


def predict(model, row):
    means, scales, w, b = model
    z = b + sum(
        a * (v - mean) / scale
        for a, v, mean, scale in zip(w, row, means, scales)
    )
    return sigmoid(z)


def make_dataset(series, symbol, horizon):
    dates = sorted(series[symbol])
    rows = []
    for i in range(85, len(dates) - horizon):
        x = features(series[symbol], dates, i)
        if x is None:
            continue
        p0 = series[symbol][dates[i]]
        p1 = series[symbol][dates[i + horizon]]
        rows.append(
            {
                "i": i,
                "date": dates[i],
                "x": x,
                "y": 1 if p1 > p0 else 0,
                "forward_return": math.log(p1 / p0),
            }
        )
    return rows


def wfo(rows, horizon):
    preds = []
    start = TRAIN_MIN + horizon
    fold = 0
    while start + TEST_SIZE <= len(rows):
        train_end = start - horizon
        train = rows[:train_end]
        test = rows[start:start + TEST_SIZE]
        if len(train) < TRAIN_MIN:
            start += TEST_SIZE
            fold += 1
            continue
        y = [r["y"] for r in train]
        if len(set(y)) < 2:
            start += TEST_SIZE
            fold += 1
            continue
        model = fit([r["x"] for r in train], y)
        for r in test:
            preds.append(
                {
                    "fold": fold,
                    "i": r["i"],
                    "date": r["date"],
                    "prob": predict(model, r["x"]),
                    "y": r["y"],
                    "forward_return": r["forward_return"],
                }
            )
        start += TEST_SIZE
        fold += 1
    return preds


def non_overlapping(rows, horizon):
    chosen = []
    last_i = None
    for row in sorted(rows, key=lambda r: r["i"]):
        if last_i is None or row["i"] - last_i >= horizon:
            chosen.append(row)
            last_i = row["i"]
    return chosen


def baseline_long_metrics(rows, cost_bps, horizon):
    trades = [r["forward_return"] - cost_bps / 10000.0 for r in rows]
    if not trades:
        return {"trades": 0, "net_return": 0.0, "cagr": None, "max_drawdown": 0.0}
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in trades:
        equity *= 1.0 + r
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak)
    cagr = equity ** (252.0 / (len(trades) * horizon)) - 1.0 if equity > 0 else -1.0
    return {
        "trades": len(trades),
        "net_return": equity - 1.0,
        "cagr": cagr,
        "max_drawdown": max_dd,
    }


def trade_metrics(rows, threshold_pair, cost_bps, horizon, mode):
    lo, hi = threshold_pair
    trades = []
    for row in rows:
        if row["prob"] >= hi:
            side = 1
        elif mode == "long_short" and row["prob"] <= lo:
            side = -1
        else:
            side = 0
        if side == 0:
            continue
        trades.append(side * row["forward_return"] - cost_bps / 10000.0)

    if not trades:
        return {
            "trades": 0,
            "exposure": 0.0,
            "net_return": 0.0,
            "cagr": None,
            "max_drawdown": 0.0,
            "sharpe_proxy": None,
            "hit_rate": None,
        }

    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    wins = 0
    for r in trades:
        wins += r > 0
        equity *= 1.0 + r
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak)

    mean_r = sum(trades) / len(trades)
    sd = statistics.pstdev(trades) if len(trades) > 1 else 0.0
    cagr = equity ** (252.0 / (len(trades) * horizon)) - 1.0 if equity > 0 else -1.0
    sharpe = (
        mean_r / sd * math.sqrt(252.0 / horizon)
        if sd > 0
        else None
    )
    return {
        "trades": len(trades),
        "exposure": len(trades) / len(rows),
        "net_return": equity - 1.0,
        "cagr": cagr,
        "max_drawdown": max_dd,
        "sharpe_proxy": sharpe,
        "hit_rate": wins / len(trades),
    }


series = {s: yahoo(s) for s in SYMBOLS}
results = {}
for symbol in SYMBOLS:
    results[symbol] = {}
    for horizon in HORIZONS:
        rows = make_dataset(series, symbol, horizon)
        preds = wfo(rows, horizon)
        exec_rows = non_overlapping(preds, horizon)
        horizon_result = {
            "test_observations": len(preds),
            "execution_observations": len(exec_rows),
            "baselines": {
                "always_long": {
                    str(cost): baseline_long_metrics(exec_rows, cost, horizon)
                    for cost in COSTS_BPS
                },
                "always_flat": {
                    str(cost): {"trades": 0, "net_return": 0.0, "cagr": 0.0, "max_drawdown": 0.0}
                    for cost in COSTS_BPS
                },
            },
            "modes": {},
        }
        for mode in ["long_only", "long_short"]:
            mode_result = {}
            for threshold in THRESHOLDS:
                key = f"{threshold[0]:.2f}/{threshold[1]:.2f}"
                mode_result[key] = {
                    str(cost): trade_metrics(
                        exec_rows, threshold, cost, horizon, mode
                    )
                    for cost in COSTS_BPS
                }
            horizon_result["modes"][mode] = mode_result
        results[symbol][str(horizon)] = horizon_result

print(
    json.dumps(
        {
            "status": "COMPLETE",
            "method": "purged-wfo-execution-robustness-longonly-vs-longshort-v1",
            "train_min": TRAIN_MIN,
            "test_size": TEST_SIZE,
            "horizons": HORIZONS,
            "thresholds": THRESHOLDS,
            "costs_bps_roundtrip": COSTS_BPS,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "results": results,
        },
        indent=2,
    )
)
