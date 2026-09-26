from __future__ import annotations

import json
import math
import os
import statistics
import time

import httpx

from research.gorila_data_snapshot import load_or_fetch_series

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
WINDOWS = [int(x) for x in os.getenv("GORILA_JAC_WINDOWS", "60,90,120,180").split(",") if x.strip()]
RIDGES = [float(x) for x in os.getenv("GORILA_JAC_RIDGES", "0.0001,0.001,0.01").split(",") if x.strip()]
LAGS = [int(x) for x in os.getenv("GORILA_JAC_LAGS", "0,1,2").split(",") if x.strip()]
TRAIN_MIN = 504
TEST_SIZE = 126
FIT_EPOCHS = 220
FIT_LR = 0.04
FIT_L2 = 0.002
GROUPS = {
    "expansion_only": [0],
    "trace_only": [1],
    "non_normality_only": [2],
    "irreversibility_only": [3],
    "innovation_fraction_only": [4],
    "dissipative_only": [5],
    "structural": [0, 1, 2],
    "nonequilibrium": [3, 4, 5],
    "all": [0, 1, 2, 3, 4, 5],
}
STRESS_GROUPS = ["structural", "nonequilibrium", "all"]


def get_json(url, params=None):
    r = httpx.get(url, params=params, timeout=30,
                  headers={"User-Agent": "Gorila-Argentum-Jacobian-Stress/0.1"})
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
        for t, c in zip(ts, close) if c is not None and c > 0
    }


def transpose(a):
    return [list(col) for col in zip(*a)]


def matmul(a, b):
    return [
        [sum(a[i][k] * b[k][j] for k in range(len(b[0])))
         for j in range(len(b[0]))]
        for i in range(len(a))
    ]


def matvec(a, x):
    return [sum(v * z for v, z in zip(row, x)) for row in a]


def ridge_solve(x, y, ridge):
    xt = transpose(x)
    xtx = matmul(xt, x)
    n = len(xtx)
    for i in range(n):
        xtx[i][i] += ridge
    xty = matmul(xt, y)
    a = [row[:] + [xty[i][j] for j in range(len(xty[0]))]
         for i, row in enumerate(xtx)]
    m = len(a)
    for col in range(m):
        piv = max(range(col, m), key=lambda r: abs(a[r][col]))
        if abs(a[piv][col]) < 1e-12:
            raise RuntimeError("singular ridge system")
        a[col], a[piv] = a[piv], a[col]
        q = a[col][col]
        a[col] = [v / q for v in a[col]]
        for r in range(m):
            if r == col:
                continue
            q = a[r][col]
            if abs(a[r][col]) < 1e-15:
                continue
            a[r] = [u - q * v for u, v in zip(a[r], a[col])]
    return [row[m:] for row in a]


def frob(a):
    return math.sqrt(sum(v * v for row in a for v in row))


def spectral_norm(a, iters=35):
    at = transpose(a)
    b = matmul(at, a)
    n = len(b)
    x = [1.0 / math.sqrt(n)] * n
    for _ in range(iters):
        y = matvec(b, x)
        norm = math.sqrt(sum(v * v for v in y))
        if norm < 1e-15:
            return 0.0
        x = [v / norm for v in y]
    return math.sqrt(max(0.0, sum(v * v for v in matvec(a, x))))


def non_normality(a):
    at = transpose(a)
    ata = matmul(at, a)
    aat = matmul(a, at)
    return frob([[aat[i][j] - ata[i][j] for j in range(len(a))]
                 for i in range(len(a))]) / max(1e-12, frob(a) ** 2)


def jac_features(ret_rows, window, ridge):
    x = ret_rows[:-1]
    y = ret_rows[1:]
    b = ridge_solve(x, [list(v) for v in y], ridge)
    a = transpose(b)
    expansion = spectral_norm(a)
    trace = sum(a[i][i] for i in range(len(a)))
    nn = non_normality(a)

    d = len(SYMBOLS)
    c1 = [[0.0] * d for _ in range(d)]
    den = max(1, len(ret_rows) - 1)
    for t in range(1, len(ret_rows)):
        u, v = ret_rows[t - 1], ret_rows[t]
        for i in range(d):
            for j in range(d):
                c1[i][j] += u[i] * v[j] / den
    anti = math.sqrt(sum((c1[i][j] - c1[j][i]) ** 2
                         for i in range(d) for j in range(d)))
    c1n = math.sqrt(sum(c1[i][j] ** 2
                        for i in range(d) for j in range(d)))
    irr = anti / max(1e-12, c1n)

    pred = [matvec(a, row) for row in x]
    innov = [[y[i][j] - pred[i][j] for j in range(d)]
             for i in range(len(y))]
    ie = statistics.mean(sum(v * v for v in row) for row in innov)
    te = statistics.mean(sum(v * v for v in row) for row in y)
    innovation = ie / max(1e-12, te)
    dissipative = math.log1p(max(0.0, innovation))
    return [expansion, trace, nn, irr, innovation, dissipative]


def own_features(series, dates, i, symbol):
    vals = [series[symbol][d] for d in dates]
    if i < 25:
        return None
    lr = lambda n: math.log(vals[i] / vals[i - n])
    win = vals[i - 20:i]
    rr = [math.log(b / a) for a, b in zip(win, vals[i - 19:i]) if a > 0 and b > 0]
    vol = statistics.pstdev(rr) if len(rr) > 1 else 0.0
    sd = statistics.pstdev(win) if len(win) > 1 else 0.0
    z = (vals[i] - statistics.mean(win)) / sd if sd > 0 else 0.0
    return [lr(1), lr(3), lr(5), vol, z]


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(x, y):
    means = [sum(row[j] for row in x) / len(x) for j in range(len(x[0]))]
    scales = [
        max(1e-12, math.sqrt(sum((row[j] - means[j]) ** 2 for row in x) / len(x)))
        for j in range(len(x[0]))
    ]
    w = [0.0] * len(x[0])
    b = 0.0
    for _ in range(FIT_EPOCHS):
        for row, target in zip(x, y):
            z = b + sum(a * (v - m) / s
                        for a, v, m, s in zip(w, row, means, scales))
            p = sigmoid(z)
            e = p - target
            for j in range(len(w)):
                w[j] -= FIT_LR * (e * (row[j] - means[j]) / scales[j] + FIT_L2 * w[j])
            b -= FIT_LR * e
    return means, scales, w, b


def predict(model, row):
    means, scales, w, b = model
    return sigmoid(b + sum(a * (v - m) / s
                           for a, v, m, s in zip(w, row, means, scales)))


def metrics(model_rows, horizon, lag, idxs):
    dates = sorted(model_rows)
    n = len(dates)
    folds = []
    start = TRAIN_MIN
    while start < n:
        stop = min(n, start + TEST_SIZE)
        train_end = max(0, start - horizon - lag)
        train_dates = dates[:train_end]
        test_dates = dates[start:stop]
        if len(train_dates) < TRAIN_MIN or len(test_dates) < 20:
            start += TEST_SIZE
            continue
        for symbol in SYMBOLS:
            train = [model_rows[d][symbol] for d in train_dates if symbol in model_rows[d]]
            test = [model_rows[d][symbol] for d in test_dates if symbol in model_rows[d]]
            if len(train) < 100 or len(test) < 20 or len({r["y"] for r in train}) < 2:
                continue
            base = fit([r["own"] for r in train], [r["y"] for r in train])
            aug = fit([[*r["own"], *[r["dyn"][i] for i in idxs]] for r in train],
                      [r["y"] for r in train])
            bp = [predict(base, r["own"]) for r in test]
            ap = [predict(aug, [*r["own"], *[r["dyn"][i] for i in idxs]]) for r in test]
            y = [r["y"] for r in test]
            bb = sum((p - yy) ** 2 for p, yy in zip(bp, y)) / len(y)
            ab = sum((p - yy) ** 2 for p, yy in zip(ap, y)) / len(y)
            ba = sum((p >= 0.5) == bool(yy) for p, yy in zip(bp, y)) / len(y)
            aa = sum((p >= 0.5) == bool(yy) for p, yy in zip(ap, y)) / len(y)
            folds.append({
                "fold_start": test_dates[0],
                "fold_end": test_dates[-1],
                "symbol": symbol,
                "delta_brier": ab - bb,
                "delta_accuracy": aa - ba,
                "base_brier": bb,
                "aug_brier": ab,
            })
        start += TEST_SIZE

    if not folds:
        return None
    db = [r["delta_brier"] for r in folds]
    da = [r["delta_accuracy"] for r in folds]
    return {
        "n_evals": len(folds),
        "mean_delta_brier": statistics.mean(db),
        "median_delta_brier": statistics.median(db),
        "positive_brier_improvement_fraction": sum(v < 0 for v in db) / len(db),
        "mean_delta_accuracy": statistics.mean(da),
        "positive_accuracy_fraction": sum(v > 0 for v in da) / len(da),
        "folds": folds,
    }


DYN_CACHE = {}

def build_rows(series, window, ridge, horizon, lag):
    dates = sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))
    rets = []
    for i in range(1, len(dates)):
        row = []
        ok = True
        for s in SYMBOLS:
            a, b = series[s][dates[i-1]], series[s][dates[i]]
            if a <= 0 or b <= 0:
                ok = False
                break
            row.append(math.log(b / a))
        if ok:
            rets.append((dates[i], row))
    rdates = [d for d, _ in rets]
    rval = [r for _, r in rets]
    cache_key = (window, ridge)
    if cache_key not in DYN_CACHE:
        DYN_CACHE[cache_key] = {
            rdates[k]: jac_features(rval[k-window:k+1], window, ridge)
            for k in range(window, len(rval))
        }
    out = {}
    for k in range(window, len(rval) - horizon - lag):
        date = rdates[k]
        dyn = DYN_CACHE[cache_key][date]
        di = dates.index(date)
        out[date] = {}
        target_date = rdates[k + horizon + lag]
        for s in SYMBOLS:
            own = own_features(series, dates, di, s)
            if own is not None:
                out[date][s] = {
                    "own": own,
                    "dyn": dyn,
                    "y": 1 if series[s][target_date] > series[s][date] else 0,
                }
    return out


series, SNAPSHOT_SHA256 = load_or_fetch_series(SYMBOLS, yahoo, os.getenv("GORILA_DATA_SNAPSHOT"))
results = []
# Central ablation across all groups.
for horizon in HORIZONS:
    model_rows = build_rows(series, 120, 0.001, horizon, 0)
    for group, idxs in GROUPS.items():
        m = metrics(model_rows, horizon, 0, idxs)
        results.append({
            "kind": "central_ablation_wfo",
            "horizon": horizon,
            "window": 120,
            "ridge": 0.001,
            "lag": 0,
            "group": group,
            "metrics": m,
        })

# Parameter stress at zero lag for the three block-level candidates.
for horizon in HORIZONS:
    for window in WINDOWS:
        for ridge in RIDGES:
            model_rows = build_rows(series, window, ridge, horizon, 0)
            for group in STRESS_GROUPS:
                m = metrics(model_rows, horizon, 0, GROUPS[group])
                results.append({
                    "kind": "parameter_stress",
                    "horizon": horizon,
                    "window": window,
                    "ridge": ridge,
                    "lag": 0,
                    "group": group,
                    "metrics": m,
                })

# Execution-target lag stress at the canonical window/ridge.
for horizon in HORIZONS:
    for lag in LAGS:
        model_rows = build_rows(series, 120, 0.001, horizon, lag)
        for group in STRESS_GROUPS:
            m = metrics(model_rows, horizon, lag, GROUPS[group])
            results.append({
                "kind": "lag_stress",
                "horizon": horizon,
                "window": 120,
                "ridge": 0.001,
                "lag": lag,
                "group": group,
                "metrics": m,
            })

print(json.dumps({
    "status": "COMPLETE",
    "method": "jacobian-nonequilibrium-wfo-stress-v1",
    "symbols": SYMBOLS,
    "data_snapshot_sha256": SNAPSHOT_SHA256,
    "horizons": HORIZONS,
    "windows": WINDOWS,
    "ridges": RIDGES,
    "lags": LAGS,
    "train_min": TRAIN_MIN,
    "test_size": TEST_SIZE,
    "purge": "horizon_plus_lag",
    "fit": {"epochs": FIT_EPOCHS, "lr": FIT_LR, "l2": FIT_L2},
    "results": results,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}, indent=2))
