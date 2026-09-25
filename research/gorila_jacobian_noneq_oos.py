from __future__ import annotations

import json
import math
import statistics
import time

import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [5, 10]
WINDOW = 120
TRAIN_MIN = 504
TEST_SIZE = 126
RIDGE = 1e-3


def get_json(url, params=None):
    r = httpx.get(
        url,
        params=params,
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-Jacobian-Noneq/0.1"},
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
    return [[sum(A[i][k] * B[k][j] for k in range(len(B))) for j in range(len(B[0]))] for i in range(len(A))]


def matvec(A, x):
    return [sum(a * b for a, b in zip(row, x)) for row in A]


def identity(n):
    return [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]


def ridge_solve(X, Y, ridge):
    XT = mat_transpose(X)
    XtX = matmul(XT, X)
    n = len(XtX)
    for i in range(n):
        XtX[i][i] += ridge
    XtY = matmul(XT, Y)
    A = [row[:] + [XtY[i][j] for j in range(len(XtY[0]))] for i, row in enumerate(XtX)]
    m, p = len(A), len(A[0])
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
    inv = [row[m:] for row in A]
    return inv


def covariance(rows):
    n = len(rows)
    d = len(rows[0])
    means = [sum(r[j] for r in rows) / n for j in range(d)]
    C = [[0.0] * d for _ in range(d)]
    den = max(1, n - 1)
    for r in rows:
        z = [r[j] - means[j] for j in range(d)]
        for i in range(d):
            for j in range(d):
                C[i][j] += z[i] * z[j] / den
    return C


def eigvals_qr(A, iters=80):
    # Small dense real matrix. QR iteration with Gram-Schmidt; sufficient for
    # regime diagnostics, where only spectral scale is required.
    n = len(A)
    M = [row[:] for row in A]
    for _ in range(iters):
        Q = [[0.0] * n for _ in range(n)]
        R = [[0.0] * n for _ in range(n)]
        cols = [[M[i][j] for i in range(n)] for j in range(n)]
        for j in range(n):
            v = cols[j][:]
            for k in range(j):
                r = sum(Q[i][k] * v[i] for i in range(n))
                R[k][j] = r
                for i in range(n):
                    v[i] -= r * Q[i][k]
            norm = math.sqrt(sum(vv * vv for vv in v))
            if norm < 1e-12:
                continue
            R[j][j] = norm
            for i in range(n):
                Q[i][j] = v[i] / norm
        M = matmul(R, Q)
    return [M[i][i] for i in range(n)]


def jacobian_noneq_features(ret_rows):
    X = [r[:-1] for r in ret_rows]
    Y = [r[1:] for r in ret_rows]
    Ymat = [list(y) for y in Y]
    B = ridge_solve(X, Ymat, RIDGE)
    A = mat_transpose(B)
    eig = eigvals_qr(A)
    spectral_radius = max(abs(v) for v in eig)
    max_real = max(eig)
    trace = sum(A[i][i] for i in range(len(A)))
    # lagged covariance measures temporal asymmetry / irreversibility proxy
    C0 = covariance(Y)
    C1 = [[0.0 for _ in SYMBOLS] for _ in SYMBOLS]
    d = len(SYMBOLS)
    for t in range(1, len(ret_rows)):
        a = ret_rows[t - 1]
        b = ret_rows[t]
        for i in range(d):
            for j in range(d):
                C1[i][j] += a[i] * b[j] / max(1, len(ret_rows) - 1)
    antisym = math.sqrt(sum((C1[i][j] - C1[j][i]) ** 2 for i in range(d) for j in range(d)))
    norm_c1 = math.sqrt(sum(C1[i][j] ** 2 for i in range(d) for j in range(d)))
    irreversibility = antisym / max(1e-12, norm_c1)
    pred = [matvec(A, x) for x in X]
    innovations = [[Y[i][j] - pred[i][j] for j in range(d)] for i in range(len(Y))]
    innovation_energy = statistics.mean(sum(v * v for v in e) for e in innovations)
    cross_entropy_proxy = math.log1p(max(0.0, innovation_energy))
    return [
        spectral_radius,
        max_real,
        trace,
        irreversibility,
        innovation_energy,
        cross_entropy_proxy,
    ]


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
    scales = [
        max(1e-12, math.sqrt(sum((row[j] - means[j]) ** 2 for row in X) / len(X)))
        for j in range(len(X[0]))
    ]
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
    features = {}
    for k in range(WINDOW, len(ret_vals) - horizon):
        dyn = jacobian_noneq_features(ret_vals[k - WINDOW:k + 1])
        date = ret_dates[k]
        features[date] = {}
        date_idx = dates.index(date)
        for s in SYMBOLS:
            own = own_features(series, dates, date_idx, s)
            if own is not None:
                features[date][s] = {"own": own, "dyn": dyn}
    out = {}
    for date, per in features.items():
        if date not in ret_dates:
            continue
        idx = ret_dates.index(date)
        if idx + horizon >= len(ret_vals):
            continue
        target_date = ret_dates[idx + horizon]
        for s in list(per):
            p0 = series[s][date]
            p1 = series[s][target_date]
            per[s]["y"] = 1 if p1 > p0 else 0
    return features


def chronological_metrics(rows):
    rows = sorted(rows, key=lambda r: r["date"])
    split = int(len(rows) * 0.8)
    train = rows[:split]
    test = rows[split:]
    if len(train) < 100 or len(test) < 20:
        return None
    y = [r["y"] for r in train]
    if len(set(y)) < 2:
        return None
    baseline = fit([r["own"] for r in train], y)
    augmented = fit([r["own"] + r["dyn"] for r in train], y)
    out = {}
    for name, model, key in [
        ("baseline", baseline, "own"),
        ("jacobian_noneq", augmented, "aug"),
    ]:
        probs = [predict(model, r["own"] if key == "own" else r["own"] + r["dyn"]) for r in test]
        labels = [r["y"] for r in test]
        acc = sum((p >= 0.5) == bool(y) for p, y in zip(probs, labels)) / len(labels)
        brier = sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)
        out[name] = {
            "samples": len(rows),
            "train": len(train),
            "test": len(test),
            "accuracy": acc,
            "brier": brier,
            "brier_skill_vs_constant": 1.0 - brier / 0.25,
        }
    out["delta_accuracy"] = out["jacobian_noneq"]["accuracy"] - out["baseline"]["accuracy"]
    out["delta_brier"] = out["jacobian_noneq"]["brier"] - out["baseline"]["brier"]
    return out


series = {s: yahoo(s) for s in SYMBOLS}
results = {}
for horizon in HORIZONS:
    results[str(horizon)] = {}
    data = build_dataset(series, horizon)
    for s in SYMBOLS:
        rows = []
        for d, per in data.items():
            if s in per:
                rows.append({"date": d, **per[s]})
        results[str(horizon)][s] = chronological_metrics(rows)

print(json.dumps({
    "status": "COMPLETE",
    "method": "local-VAR1-Jacobian-plus-nonequilibrium-proxy-WFO-v1",
    "symbols": SYMBOLS,
    "horizons": HORIZONS,
    "window": WINDOW,
    "ridge": RIDGE,
    "dynamic_features": [
        "spectral_radius",
        "max_real_eigenvalue",
        "trace_J",
        "lag-covariance_irreversibility_proxy",
        "innovation_energy",
        "log1p_innovation_energy",
    ],
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "results": results,
}, indent=2))
