from __future__ import annotations

import json
import math
import statistics
import time

import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [5, 10]
WINDOW = 120
RIDGE = 1e-3

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


def get_json(url, params=None):
    r = httpx.get(url, params=params, timeout=30, headers={"User-Agent": "Gorila-Argentum-Jacobian-Ablation/0.1"})
    r.raise_for_status()
    return r.json()


def yahoo(symbol):
    j = get_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
                 {"range": "5y", "interval": "1d", "events": "history"})
    res = (j.get("chart", {}).get("result") or [None])[0]
    if not res:
        raise RuntimeError(symbol + ": empty")
    ts = res.get("timestamp") or []
    close = ((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {time.strftime("%Y-%m-%d", time.gmtime(t)): float(c)
            for t, c in zip(ts, close) if c is not None and c > 0}


def common_dates(series):
    return sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))


def returns_matrix(series, dates):
    out = []
    for i in range(1, len(dates)):
        row = []
        ok = True
        for s in SYMBOLS:
            a, b = series[s][dates[i - 1]], series[s][dates[i]]
            if a <= 0 or b <= 0:
                ok = False
                break
            row.append(math.log(b / a))
        if ok:
            out.append((dates[i], row))
    return out


def mat_transpose(A):
    return [list(col) for col in zip(*A)]


def matmul(A, B):
    return [[sum(A[i][k] * B[k][j] for k in range(len(B)))
             for j in range(len(B[0]))] for i in range(len(A))]


def matvec(A, x):
    return [sum(a * b for a, b in zip(row, x)) for row in A]


def ridge_solve(X, Y, ridge):
    XT = mat_transpose(X)
    XtX = matmul(XT, X)
    n = len(XtX)
    for i in range(n):
        XtX[i][i] += ridge
    XtY = matmul(XT, Y)
    A = [row[:] + [XtY[i][j] for j in range(len(XtY[0]))] for i, row in enumerate(XtX)]
    m = len(A)
    for col in range(m):
        piv = max(range(col, m), key=lambda r: abs(A[r][col]))
        if abs(A[piv][col]) < 1e-12:
            raise RuntimeError("singular ridge system")
        A[col], A[piv] = A[piv], A[col]
        q = A[col][col]
        A[col] = [v / q for v in A[col]]
        for r in range(m):
            if r == col:
                continue
            q = A[r][col]
            if abs(q) < 1e-15:
                continue
            A[r] = [u - q * v for u, v in zip(A[r], A[col])]
    return [row[m:] for row in A]


def frob(A):
    return math.sqrt(sum(v * v for row in A for v in row))


def spectral_norm_power(A, iters=80):
    AT = mat_transpose(A)
    B = matmul(AT, A)
    n = len(B)
    x = [1.0 / math.sqrt(n)] * n
    for _ in range(iters):
        y = matvec(B, x)
        norm = math.sqrt(sum(v * v for v in y))
        if norm < 1e-15:
            return 0.0
        x = [v / norm for v in y]
    return math.sqrt(max(0.0, sum(v * v for v in matvec(A, x))))


def commutator_non_normality(A):
    AT = mat_transpose(A)
    ATA = matmul(AT, A)
    AAT = matmul(A, AT)
    return frob([[AAT[i][j] - ATA[i][j] for j in range(len(A))]
                 for i in range(len(A))]) / max(1e-12, frob(A) ** 2)


def jacobian_noneq_features(ret_rows):
    X = ret_rows[:-1]
    Y = ret_rows[1:]
    B = ridge_solve(X, [list(y) for y in Y], RIDGE)
    A = mat_transpose(B)
    expansion = spectral_norm_power(A)
    trace = sum(A[i][i] for i in range(len(A)))
    non_normality = commutator_non_normality(A)

    d = len(SYMBOLS)
    C1 = [[0.0] * d for _ in range(d)]
    for t in range(1, len(ret_rows)):
        a, b = ret_rows[t - 1], ret_rows[t]
        for i in range(d):
            for j in range(d):
                C1[i][j] += a[i] * b[j] / max(1, len(ret_rows) - 1)
    antisym = math.sqrt(sum((C1[i][j] - C1[j][i]) ** 2 for i in range(d) for j in range(d)))
    norm_c1 = math.sqrt(sum(C1[i][j] ** 2 for i in range(d) for j in range(d)))
    irreversibility = antisym / max(1e-12, norm_c1)

    pred = [matvec(A, x) for x in X]
    innovations = [[Y[i][j] - pred[i][j] for j in range(d)] for i in range(len(Y))]
    innovation_energy = statistics.mean(sum(v * v for v in e) for e in innovations)
    total_energy = statistics.mean(sum(v * v for v in y) for y in Y)
    innovation_fraction = innovation_energy / max(1e-12, total_energy)
    dissipative_proxy = math.log1p(max(0.0, innovation_fraction))
    return [expansion, trace, non_normality, irreversibility, innovation_fraction, dissipative_proxy]


def own_features(series, dates, i, symbol):
    vals = [series[symbol][d] for d in dates]
    if i < 25:
        return None
    def lr(n):
        return math.log(vals[i] / vals[i - n])
    win = vals[i - 20:i]
    rr = [math.log(b / a) for a, b in zip(win, vals[i - 19:i]) if a > 0 and b > 0]
    vol = statistics.pstdev(rr) if len(rr) > 1 else 0.0
    sd = statistics.pstdev(win) if len(win) > 1 else 0.0
    z = (vals[i] - statistics.mean(win)) / sd if sd > 0 else 0.0
    return [lr(1), lr(3), lr(5), vol, z]


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(X, y, epochs=220, lr=0.04, l2=0.002):
    means = [sum(row[j] for row in X) / len(X) for j in range(len(X[0]))]
    scales = [max(1e-12, math.sqrt(sum((row[j] - means[j]) ** 2 for row in X) / len(X)))
              for j in range(len(X[0]))]
    w = [0.0] * len(X[0])
    b = 0.0
    for _ in range(epochs):
        for row, target in zip(X, y):
            z = b + sum(a * (v - m) / s for a, v, m, s in zip(w, row, means, scales))
            p = sigmoid(z)
            e = p - target
            for j in range(len(w)):
                w[j] -= lr * (e * (row[j] - means[j]) / scales[j] + l2 * w[j])
            b -= lr * e
    return means, scales, w, b


def predict(model, row):
    means, scales, w, b = model
    return sigmoid(b + sum(a * (v - m) / s for a, v, m, s in zip(w, row, means, scales)))


def build_dataset(series, horizon):
    dates = common_dates(series)
    rets = returns_matrix(series, dates)
    ret_dates = [d for d, _ in rets]
    ret_vals = [r for _, r in rets]
    out = {}
    for k in range(WINDOW, len(ret_vals) - horizon):
        date = ret_dates[k]
        dyn = jacobian_noneq_features(ret_vals[k - WINDOW:k + 1])
        date_idx = dates.index(date)
        out[date] = {}
        for s in SYMBOLS:
            own = own_features(series, dates, date_idx, s)
            if own is not None:
                idx = k + horizon
                if idx < len(ret_vals):
                    target_date = ret_dates[idx]
                    out[date][s] = {
                        "own": own,
                        "dyn": dyn,
                        "y": 1 if series[s][target_date] > series[s][date] else 0,
                    }
    return out


def metrics(rows, horizon, idxs):
    rows = sorted(rows, key=lambda r: r["date"])
    split = int(len(rows) * 0.8)
    train = rows[:max(0, split - horizon)]
    test = rows[split:]
    if len(train) < 100 or len(test) < 20:
        return None

    y = [r["y"] for r in train]
    base = fit([r["own"] for r in train], y)
    aug = fit([r["own"] + [r["dyn"][i] for i in idxs] for r in train], y)

    def eval_model(model, rows_, key):
        probs = [predict(model, r[key]) for r in rows_]
        labels = [r["y"] for r in rows_]
        return {
            "accuracy": sum((p >= 0.5) == bool(y_) for p, y_ in zip(probs, labels)) / len(labels),
            "brier": sum((p - y_) ** 2 for p, y_ in zip(probs, labels)) / len(labels),
        }

    base_test = eval_model(base, [{"own": r["own"], "y": r["y"]} for r in test], "own")
    aug_rows = [{"aug": r["own"] + [r["dyn"][i] for i in idxs], "y": r["y"]} for r in test]
    aug_test = eval_model(aug, aug_rows, "aug")
    return {
        "samples": len(rows),
        "train": len(train),
        "test": len(test),
        "baseline": base_test,
        "augmented": aug_test,
        "delta_accuracy": aug_test["accuracy"] - base_test["accuracy"],
        "delta_brier": aug_test["brier"] - base_test["brier"],
    }


series = {s: yahoo(s) for s in SYMBOLS}
all_data = {str(h): build_dataset(series, h) for h in HORIZONS}
results = {}
for h in HORIZONS:
    results[str(h)] = {}
    for group, idxs in GROUPS.items():
        results[str(h)][group] = {}
        data = all_data[str(h)]
        for s in SYMBOLS:
            rows = [{"date": d, **data[d][s]} for d in data if s in data[d]]
            results[str(h)][group][s] = metrics(rows, h, idxs)

print(json.dumps({
    "status": "COMPLETE",
    "method": "local-VAR1-Jacobian-feature-ablation-v1",
    "symbols": SYMBOLS,
    "horizons": HORIZONS,
    "window": WINDOW,
    "groups": GROUPS,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "results": results,
}, indent=2))
