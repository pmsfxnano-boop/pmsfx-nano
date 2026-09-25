from __future__ import annotations

import json
import math
import statistics
import time

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
        headers={"User-Agent": "Gorila-Argentum-CrossSectional/0.1"},
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
    if n <= 1:
        return 0.5
    less = sum(v < value for v in values)
    equal = sum(v == value for v in values)
    return (less + 0.5 * equal) / n


def auc(y, p):
    pairs = sorted(zip(p, y), key=lambda t: t[0])
    pos = sum(y)
    neg = len(y) - pos
    if pos == 0 or neg == 0:
        return 0.5
    rank_sum = 0.0
    i = 0
    while i < len(pairs):
        j = i + 1
        while j < len(pairs) and pairs[j][0] == pairs[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        rank_sum += avg_rank * sum(t[1] for t in pairs[i:j])
        i = j
    return (rank_sum - pos * (pos + 1) / 2.0) / (pos * neg)


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(X, y, epochs=240, lr=0.035, l2=0.002):
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
    return sigmoid(
        b + sum(
            a * (v - mean) / scale
            for a, v, mean, scale in zip(w, row, means, scales)
        )
    )


def build_dates(series):
    common = set.intersection(*(set(series[s]) for s in SYMBOLS))
    return sorted(common)


def feature_snapshot(series, dates, idx):
    rows = {}
    r1 = {s: logret(series[s], dates, idx, 1) for s in SYMBOLS}
    r3 = {s: logret(series[s], dates, idx, 3) for s in SYMBOLS}
    r5 = {s: logret(series[s], dates, idx, 5) for s in SYMBOLS}
    if any(r1[s] is None or r3[s] is None or r5[s] is None for s in SYMBOLS):
        return None
    vals1, vals3, vals5 = list(r1.values()), list(r3.values()), list(r5.values())
    disp = statistics.pstdev(vals1)
    for s in SYMBOLS:
        hist = [logret(series[s], dates, k, 1) for k in range(max(25, idx - 60), idx)]
        hist = [v for v in hist if v is not None]
        if len(hist) < 30:
            return None
        vol = statistics.pstdev(hist[-20:]) if len(hist[-20:]) > 1 else 0.0
        prices = [series[s][dates[k]] for k in range(max(0, idx - 20), idx)]
        sd = statistics.pstdev(prices) if len(prices) > 1 else 0.0
        z = (series[s][dates[idx]] - statistics.mean(prices)) / sd if sd > 0 else 0.0
        rows[s] = {
            "x": [
                r1[s], r3[s], r5[s], vol, z,
                percentile_rank(r1[s], vals1),
                percentile_rank(r3[s], vals3),
                percentile_rank(r5[s], vals5),
                r1[s] - statistics.median(vals1),
                r3[s] - statistics.median(vals3),
                r5[s] - statistics.median(vals5),
                disp,
            ],
            "momentum_score": r5[s],
        }
    return rows


def make_dataset(series, horizon):
    dates = build_dates(series)
    rows_by_date = {}
    for i in range(60, len(dates) - horizon):
        snap = feature_snapshot(series, dates, i)
        if snap is None:
            continue
        fwd = {s: logret(series[s], dates, i + horizon, horizon) for s in SYMBOLS}
        if any(v is None for v in fwd.values()):
            continue
        med = statistics.median(fwd.values())
        rows_by_date[dates[i]] = {
            s: {
                "date": dates[i],
                "symbol": s,
                "x": snap[s]["x"],
                "momentum_score": snap[s]["momentum_score"],
                "forward_return": fwd[s],
                "y": 1 if fwd[s] > med else 0,
            }
            for s in SYMBOLS
        }
    return dates, rows_by_date


def portfolio_return(rows, ranked_symbols, cost_bps):
    long_s, short_s = ranked_symbols[-1], ranked_symbols[0]
    return 0.5 * (
        rows[long_s]["forward_return"] - rows[short_s]["forward_return"]
    ) - cost_bps / 10000.0


def summarize_returns(returns, horizon):
    if not returns:
        return {"trades": 0, "net_return": 0.0, "cagr": 0.0}
    equity = 1.0
    for r in returns:
        equity *= 1.0 + r
    return {
        "trades": len(returns),
        "net_return": equity - 1.0,
        "cagr": equity ** (252.0 / (len(returns) * horizon)) - 1.0
        if equity > 0
        else -1.0,
    }


def execute_oos(oos_by_date, horizon, cost_bps, mode):
    returns = []
    sorted_dates = sorted(oos_by_date)
    last_idx = -10**9
    date_pos = {d: i for i, d in enumerate(sorted_dates)}
    for d in sorted_dates:
        if date_pos[d] - last_idx < horizon:
            continue
        rows = oos_by_date[d]
        if mode == "model":
            ranked = sorted(SYMBOLS, key=lambda s: rows[s]["prob"])
        else:
            ranked = sorted(SYMBOLS, key=lambda s: rows[s]["momentum_score"])
        returns.append(portfolio_return(rows, ranked, cost_bps))
        last_idx = date_pos[d]
    return summarize_returns(returns, horizon)


def run_horizon(series, horizon):
    _, dataset = make_dataset(series, horizon)
    usable_dates = sorted(dataset)
    if len(usable_dates) < TRAIN_MIN_DATES + TEST_SIZE_DATES:
        raise RuntimeError(f"{horizon}d: insufficient dates {len(usable_dates)}")

    oos_by_date = {}
    fold_count = 0
    start = TRAIN_MIN_DATES
    while start + TEST_SIZE_DATES <= len(usable_dates):
        train_dates = usable_dates[: start - horizon]
        test_dates = usable_dates[start:start + TEST_SIZE_DATES]
        train_rows = [dataset[d][s] for d in train_dates for s in SYMBOLS]
        if len(train_rows) < 1000 or len({r["y"] for r in train_rows}) < 2:
            start += TEST_SIZE_DATES
            continue
        model = fit([r["x"] for r in train_rows], [r["y"] for r in train_rows])
        for d in test_dates:
            oos_by_date[d] = {}
            for s in SYMBOLS:
                r = dataset[d][s]
                oos_by_date[d][s] = {**r, "prob": predict(model, r["x"])}
        fold_count += 1
        start += TEST_SIZE_DATES

    oos_rows = [oos_by_date[d][s] for d in sorted(oos_by_date) for s in SYMBOLS]
    y = [r["y"] for r in oos_rows]
    p = [r["prob"] for r in oos_rows]
    acc = sum((pp >= 0.5) == bool(yy) for pp, yy in zip(p, y)) / len(y)
    brier = sum((pp - yy) ** 2 for pp, yy in zip(p, y)) / len(y)
    result = {
        "oos_rows": len(oos_rows),
        "oos_dates": len(oos_by_date),
        "outer_folds": fold_count,
        "accuracy": acc,
        "brier": brier,
        "brier_skill": 1.0 - brier / 0.25,
        "auc": auc(y, p),
        "mean_predicted": statistics.mean(p),
        "actual_up_rate": statistics.mean(y),
        "calibration_gap": abs(statistics.mean(p) - statistics.mean(y)),
        "execution": {},
    }
    for cost in COSTS_BPS:
        result["execution"][str(cost)] = {
            "model_top_bottom": execute_oos(oos_by_date, horizon, cost, "model"),
            "momentum_top_bottom": execute_oos(oos_by_date, horizon, cost, "momentum"),
        }
    return result


series = {s: yahoo(s) for s in SYMBOLS}
results = {str(h): run_horizon(series, h) for h in HORIZONS}
print(json.dumps({
    "status": "COMPLETE",
    "method": "cross-sectional-relative-logistic-wfo-v2",
    "symbols": SYMBOLS,
    "horizons": HORIZONS,
    "costs_bps_roundtrip": COSTS_BPS,
    "train_min_dates": TRAIN_MIN_DATES,
    "test_size_dates": TEST_SIZE_DATES,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "results": results,
}, indent=2))
