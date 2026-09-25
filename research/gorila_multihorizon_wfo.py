from __future__ import annotations

import json
import math
import statistics
import time
from dataclasses import dataclass

import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [1, 3, 5, 10]
COSTS_BPS = [0, 10, 25, 50]
THRESHOLDS = [(0.50, 0.50), (0.55, 0.45), (0.60, 0.40)]
TRAIN_MIN = 504
TEST_SIZE = 126


@dataclass
class Obs:
    i: int
    date: str
    x: list[float]
    y: int
    forward_return: float


def get_json(url, params=None):
    r = httpx.get(
        url,
        params=params,
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-MultiHorizon-WFO/0.1"},
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
    out = {}
    for t, c in zip(ts, close):
        if c is not None and c > 0:
            out[time.strftime("%Y-%m-%d", time.gmtime(t))] = float(c)
    return out


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
    vals = [
        series[dates[k]]
        for k in range(max(0, i - 20), i)
        if dates[k] in series
    ]
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
    out = []
    for i in range(85, len(dates) - horizon):
        x = features(series[symbol], dates, i)
        if x is None:
            continue
        p0 = series[symbol][dates[i]]
        p1 = series[symbol][dates[i + horizon]]
        fr = math.log(p1 / p0)
        y = 1 if p1 > p0 else 0
        out.append(Obs(i=i, date=dates[i], x=x, y=y, forward_return=fr))
    return out


def wfo(obs, horizon):
    preds = []
    start = TRAIN_MIN + horizon
    fold = 0
    while start + TEST_SIZE <= len(obs):
        train_end = start - horizon
        if train_end < TRAIN_MIN:
            start += TEST_SIZE
            fold += 1
            continue
        train = obs[:train_end]
        test = obs[start:start + TEST_SIZE]
        X = [o.x for o in train]
        y = [o.y for o in train]
        if len(set(y)) < 2:
            start += TEST_SIZE
            fold += 1
            continue
        model = fit(X, y)
        train_rate = sum(y) / len(y)
        for o in test:
            preds.append(
                {
                    "fold": fold,
                    "i": o.i,
                    "date": o.date,
                    "prob": predict(model, o.x),
                    "y": o.y,
                    "forward_return": o.forward_return,
                    "baseline_rate": train_rate,
                }
            )
        start += TEST_SIZE
        fold += 1
    return preds


def brier(rows):
    return sum((r["prob"] - r["y"]) ** 2 for r in rows) / len(rows)


def logloss(rows):
    eps = 1e-12
    return -sum(
        r["y"] * math.log(max(eps, r["prob"]))
        + (1 - r["y"]) * math.log(max(eps, 1 - r["prob"]))
        for r in rows
    ) / len(rows)


def ece(rows, bins=10):
    total = len(rows)
    value = 0.0
    for k in range(bins):
        lo = k / bins
        hi = (k + 1) / bins
        group = [
            r for r in rows
            if (r["prob"] >= lo and r["prob"] < hi)
            or (k == bins - 1 and r["prob"] == hi)
        ]
        if not group:
            continue
        conf = sum(r["prob"] for r in group) / len(group)
        freq = sum(r["y"] for r in group) / len(group)
        value += len(group) / total * abs(conf - freq)
    return value


def calibration(rows):
    rate = sum(r["y"] for r in rows) / len(rows)
    bb = sum((rate - r["y"]) ** 2 for r in rows) / len(rows)
    bs = brier(rows)
    return {
        "samples": len(rows),
        "accuracy": sum((r["prob"] >= 0.5) == bool(r["y"]) for r in rows) / len(rows),
        "brier": bs,
        "brier_skill": 1 - bs / bb if bb > 0 else None,
        "logloss": logloss(rows),
        "ece10": ece(rows, 10),
        "mean_prob": sum(r["prob"] for r in rows) / len(rows),
        "actual_up_rate": rate,
        "calibration_gap": sum(r["prob"] for r in rows) / len(rows) - rate,
    }


def non_overlapping(rows, horizon):
    chosen = []
    last_i = None
    for r in sorted(rows, key=lambda x: x["i"]):
        if last_i is None or r["i"] - last_i >= horizon:
            chosen.append(r)
            last_i = r["i"]
    return chosen


def trade_metrics(rows, threshold_pair, cost_bps, horizon):
    lo, hi = threshold_pair
    trades = []
    for r in rows:
        if r["prob"] >= hi:
            side = 1
        elif r["prob"] <= lo:
            side = -1
        else:
            side = 0
        if side == 0:
            continue
        net = side * r["forward_return"] - cost_bps / 10000.0
        trades.append(net)

    if not trades:
        return {
            "trades": 0,
            "exposure": 0.0,
            "net_return": 0.0,
            "mean_trade_return": None,
            "hit_rate": None,
            "max_drawdown": 0.0,
            "cagr": None,
            "sharpe_proxy": None,
        }

    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    wins = 0
    for r in trades:
        if r > 0:
            wins += 1
        equity *= 1.0 + r
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak)

    mean_r = sum(trades) / len(trades)
    sd = statistics.pstdev(trades) if len(trades) > 1 else 0.0
    ann = math.sqrt(252.0 / horizon) if horizon > 0 else 1.0
    cagr = equity ** (252.0 / (len(trades) * horizon)) - 1.0 if equity > 0 else -1.0
    return {
        "trades": len(trades),
        "exposure": len(trades) / len(rows),
        "net_return": equity - 1.0,
        "mean_trade_return": mean_r,
        "hit_rate": wins / len(trades),
        "max_drawdown": max_dd,
        "cagr": cagr,
        "sharpe_proxy": (mean_r / sd) * ann if sd > 0 else None,
    }


def evaluate_symbol(series, symbol):
    result = {}
    for horizon in HORIZONS:
        obs = make_dataset(series, symbol, horizon)
        preds = wfo(obs, horizon)
        if len(preds) < 150:
            result[str(horizon)] = {"status": "INSUFFICIENT_DATA", "samples": len(preds)}
            continue
        cal = calibration(preds)
        exec_rows = non_overlapping(preds, horizon)
        policies = {}
        for threshold_pair in THRESHOLDS:
            name = f"{threshold_pair[0]:.2f}/{threshold_pair[1]:.2f}"
            policies[name] = {
                str(cost): trade_metrics(exec_rows, threshold_pair, cost, horizon)
                for cost in COSTS_BPS
            }
        result[str(horizon)] = {
            "status": "READY",
            "fold_test_observations": len(preds),
            "non_overlapping_execution_observations": len(exec_rows),
            "calibration": cal,
            "execution": policies,
        }
    return result


series = {s: yahoo(s) for s in SYMBOLS}
results = {s: evaluate_symbol(series, s) for s in SYMBOLS}

print(
    json.dumps(
        {
            "status": "COMPLETE",
            "method": "expanding-purged-walk-forward-multihorizon-v1",
            "train_min": TRAIN_MIN,
            "test_size": TEST_SIZE,
            "purge_days": HORIZONS,
            "horizons": HORIZONS,
            "thresholds": THRESHOLDS,
            "costs_bps_roundtrip": COSTS_BPS,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "results": results,
        },
        indent=2,
    )
)
