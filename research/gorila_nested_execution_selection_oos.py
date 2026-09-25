from __future__ import annotations

import json
import math
import statistics
import time

import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [5, 10]
COSTS_BPS = [25, 50, 100]
TRAIN_MIN = 504
TEST_SIZE = 126
SELECTION_COST = 50
MIN_INNER_TRADES = 5

CANDIDATES = [
    ("flat", None, None),
    ("long_only", 0.55, None),
    ("long_only", 0.60, None),
    ("long_only", 0.65, None),
    ("long_short", 0.55, 0.45),
    ("long_short", 0.60, 0.40),
    ("long_short", 0.65, 0.35),
]


def get_json(url, params=None):
    r = httpx.get(
        url,
        params=params,
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-Nested-Selection/0.1"},
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


def non_overlapping(rows, horizon):
    chosen = []
    last_i = None
    for row in sorted(rows, key=lambda r: r["i"]):
        if last_i is None or row["i"] - last_i >= horizon:
            chosen.append(row)
            last_i = row["i"]
    return chosen


def apply_candidate(rows, prob_model):
    scored = []
    for row in rows:
        p = predict(prob_model, row["x"])
        scored.append({**row, "prob": p})
    return scored


def side_for(candidate, prob):
    name, hi, lo = candidate
    if name == "flat":
        return 0
    if name == "long_only":
        return 1 if prob >= hi else 0
    if prob >= hi:
        return 1
    if prob <= lo:
        return -1
    return 0


def strategy_metrics(rows, candidate, cost_bps, horizon):
    trades = []
    for row in rows:
        side = side_for(candidate, row["prob"])
        if side == 0:
            continue
        trades.append(side * row["forward_return"] - cost_bps / 10000.0)
    if not trades:
        return {"trades": 0, "net_return": 0.0, "cagr": 0.0, "max_drawdown": 0.0}
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


def select_candidate(outer_train, horizon):
    split = int(len(outer_train) * 0.8)
    inner_fit = outer_train[:split - horizon]
    inner_val = outer_train[split:]
    if len(inner_fit) < 200 or len(inner_val) < horizon * MIN_INNER_TRADES:
        return "flat", None, None, {"selection_status": "INSUFFICIENT_INNER_DATA"}

    y = [r["y"] for r in inner_fit]
    if len(set(y)) < 2:
        return "flat", None, None, {"selection_status": "SINGLE_CLASS"}

    model = fit([r["x"] for r in inner_fit], y)
    scored = apply_candidate(inner_val, model)
    scored = non_overlapping(scored, horizon)

    evaluations = []
    for candidate in CANDIDATES:
        m = strategy_metrics(scored, candidate, SELECTION_COST, horizon)
        if candidate[0] != "flat" and m["trades"] < MIN_INNER_TRADES:
            continue
        evaluations.append((m["net_return"], -m["max_drawdown"], candidate, m["trades"]))

    if not evaluations:
        return "flat", None, None, {"selection_status": "NO_CANDIDATE"}

    evaluations.sort(reverse=True, key=lambda x: (x[0], x[1]))
    chosen = evaluations[0][2]
    return chosen[0], chosen[1], chosen[2], {
        "selection_status": "OK",
        "selected": chosen,
        "inner_selection_cost_bps": SELECTION_COST,
        "inner_candidates": [
            {
                "candidate": e[2],
                "net_return": e[0],
                "max_drawdown": -e[1],
                "trades": e[3],
            }
            for e in evaluations
        ],
    }


def run_symbol(series, symbol, horizon):
    rows = make_dataset(series, symbol, horizon)
    start = TRAIN_MIN + horizon
    outer_results = []
    selection_counts = {}

    while start + TEST_SIZE <= len(rows):
        train_end = start - horizon
        outer_train = rows[:train_end]
        outer_test = rows[start:start + TEST_SIZE]
        if len(outer_train) < TRAIN_MIN:
            start += TEST_SIZE
            continue
        if len({r["y"] for r in outer_train}) < 2:
            start += TEST_SIZE
            continue

        selection = select_candidate(outer_train, horizon)
        candidate = (selection[0], selection[1], selection[2])
        selection_counts[str(candidate)] = selection_counts.get(str(candidate), 0) + 1

        model = fit([r["x"] for r in outer_train], [r["y"] for r in outer_train])
        scored = apply_candidate(outer_test, model)
        scored = non_overlapping(scored, horizon)
        tagged = [{**row, "candidate": candidate} for row in scored]
        outer_results.append({"candidate": candidate, "rows": tagged, "selection": selection[3]})
        start += TEST_SIZE

    all_rows = []
    for o in outer_results:
        all_rows.extend(o["rows"])

    results = {}
    for cost in COSTS_BPS:
        selected = []
        always_long = []
        for row in all_rows:
            side = side_for(row.get("candidate", ("flat", None, None)), row["prob"])
            if side != 0:
                selected.append(side * row["forward_return"] - cost / 10000.0)
            always_long.append(row["forward_return"] - cost / 10000.0)

        def summarize(trades):
            if not trades:
                return {"trades": 0, "net_return": 0.0, "cagr": 0.0}
            eq = 1.0
            for r in trades:
                eq *= 1.0 + r
            return {
                "trades": len(trades),
                "net_return": eq - 1.0,
                "cagr": eq ** (252.0 / (len(trades) * horizon)) - 1.0 if eq > 0 else -1.0,
            }

        results[str(cost)] = {
            "nested_selected": summarize(selected),
            "always_long": summarize(always_long),
        }

    return {
        "test_observations": len(all_rows),
        "outer_folds": len(outer_results),
        "selection_counts": selection_counts,
        "costs": results,
    }


series = {s: yahoo(s) for s in SYMBOLS}
results = {
    symbol: {str(h): run_symbol(series, symbol, h) for h in HORIZONS}
    for symbol in SYMBOLS
}

print(
    json.dumps(
        {
            "status": "COMPLETE",
            "method": "nested-inner-validation-strategy-selection-wfo-v1",
            "selection_cost_bps": SELECTION_COST,
            "candidates": CANDIDATES,
            "horizons": HORIZONS,
            "costs_bps_roundtrip": COSTS_BPS,
            "train_min": TRAIN_MIN,
            "test_size": TEST_SIZE,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "results": results,
        },
        indent=2,
    )
)
