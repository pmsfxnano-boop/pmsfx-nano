from __future__ import annotations
import json
import math
import statistics
import time
import httpx

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]


def get_json(url, params=None):
    r = httpx.get(
        url,
        params=params,
        timeout=30,
        headers={"User-Agent": "Gorila-Argentum-Coupling-Gate-OOS/0.1"},
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


def corr(xs, ys):
    n = min(len(xs), len(ys))
    if n < 12:
        return None
    xs = xs[-n:]
    ys = ys[-n:]
    mx = sum(xs) / n
    my = sum(ys) / n
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    den = math.sqrt(sum(x * x for x in dx) * sum(y * y for y in dy))
    return 0.0 if den == 0 else sum(x * y for x, y in zip(dx, dy)) / den


def coupling_state(target, series, dates, i):
    curr = []
    prev = []
    start = max(80, i - 80)
    for peer in SYMBOLS:
        if peer == target:
            continue

        a = [ret(series[peer], dates, k, 1) for k in range(max(start, i - 20), i)]
        b = [ret(series[target], dates, k, 1) for k in range(max(start, i - 20), i)]
        ab = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
        if len(ab) >= 15:
            c = corr([x for x, y in ab], [y for x, y in ab])
            curr.append(c)

        a2 = [ret(series[peer], dates, k, 1) for k in range(max(start, i - 80), max(start, i - 20))]
        b2 = [ret(series[target], dates, k, 1) for k in range(max(start, i - 80), max(start, i - 20))]
        ab2 = [(x, y) for x, y in zip(a2, b2) if x is not None and y is not None]
        if len(ab2) >= 20:
            c2 = corr([x for x, y in ab2], [y for x, y in ab2])
            prev.append(c2)

    if len(curr) < 2 or len(prev) < 2:
        return None

    mean_abs = sum(abs(x) for x in curr) / len(curr)
    prev_abs = sum(abs(x) for x in prev) / len(prev)
    signed = sum(curr) / len(curr)
    delta = mean_abs - prev_abs
    dispersion = statistics.pstdev(curr) if len(curr) > 1 else 0.0
    score = 0.5 * mean_abs + 0.5 * max(0.0, delta)
    return {
        "mean_abs": mean_abs,
        "delta_abs": delta,
        "signed": signed,
        "dispersion": dispersion,
        "score": score,
    }


def base_features(target, series, dates, i):
    own = [ret(series[target], dates, i, k) for k in (1, 3, 5)]
    if any(v is None for v in own):
        return None
    hist = [ret(series[target], dates, k, 1) for k in range(max(25, i - 60), i)]
    hist = [x for x in hist if x is not None]
    if len(hist) < 30:
        return None
    vol = statistics.pstdev(hist[-20:]) if len(hist[-20:]) > 1 else 0.0
    vals = [
        series[target].get(dates[k])
        for k in range(max(0, i - 20), i)
        if series[target].get(dates[k]) is not None
    ]
    sd = statistics.pstdev(vals) if len(vals) > 1 else 0.0
    z = (series[target][dates[i]] - statistics.mean(vals)) / sd if sd > 0 else 0.0
    return [own[0], own[1], own[2], vol, z]


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
        for r, t in zip(Z, y):
            z = b + sum(a * v for a, v in zip(w, r))
            p = 1 / (1 + math.exp(-max(-30, min(30, z))))
            e = p - t
            for j in range(len(w)):
                w[j] -= 0.03 * (e * r[j] + 0.002 * w[j])
            b -= 0.03 * e
    return means, scales, w, b


def predict(model, row):
    means, scales, w, b = model
    z = b + sum(a * (v - m0) / s for a, v, m0, s in zip(w, row, means, scales))
    return 1 / (1 + math.exp(-max(-30, min(30, z))))


def quantile(values, q):
    values = sorted(values)
    if not values:
        return None
    pos = (len(values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return values[lo]
    w = pos - lo
    return values[lo] * (1 - w) + values[hi] * w


def metrics(rows):
    if not rows:
        return {"samples": 0}
    labels = [y for _, y in rows]
    probs = [p for p, _ in rows]
    acc = sum((p >= 0.5) == bool(y) for p, y in rows) / len(rows)
    brier = sum((p - y) ** 2 for p, y in rows) / len(rows)
    actual_rate = sum(labels) / len(labels)
    mean_prob = sum(probs) / len(probs)
    base_brier = sum((actual_rate - y) ** 2 for y in labels) / len(labels)
    return {
        "samples": len(rows),
        "accuracy": acc,
        "brier": brier,
        "brier_skill": 1 - brier / base_brier if base_brier > 0 else None,
        "actual_up_rate": actual_rate,
        "mean_predicted_up": mean_prob,
        "calibration_gap": mean_prob - actual_rate,
    }


def evaluate(samples):
    split = int(len(samples) * 0.8)
    train, test = samples[:split], samples[split:]
    X = [x[0] for x in train]
    y = [x[2] for x in train]
    model = fit(X, y)
    scored = []
    train_scores = [x[1]["score"] for x in train]
    q1 = quantile(train_scores, 1 / 3)
    q2 = quantile(train_scores, 2 / 3)

    for base, state, label in test:
        scored.append((predict(model, base), state["score"], label))

    all_rows = [(p, y) for p, _, y in scored]
    low = [(p, y) for p, s, y in scored if s <= q1]
    mid = [(p, y) for p, s, y in scored if q1 < s < q2]
    high = [(p, y) for p, s, y in scored if s >= q2]

    return {
        "samples": len(samples),
        "train": len(train),
        "test": len(test),
        "score_q1_train": q1,
        "score_q2_train": q2,
        "overall": metrics(all_rows),
        "low_sync_tercile": metrics(low),
        "mid_sync_tercile": metrics(mid),
        "high_sync_tercile": metrics(high),
        "high_minus_low_brier": (
            metrics(high)["brier"] - metrics(low)["brier"]
            if low and high
            else None
        ),
    }


series = {s: yahoo(s) for s in SYMBOLS}
results = {}
for target in SYMBOLS:
    dates = sorted(series[target])
    samples = []
    for i in range(85, len(dates) - 1):
        b = base_features(target, series, dates, i)
        c = coupling_state(target, series, dates, i)
        if b is None or c is None:
            continue
        y = 1 if series[target][dates[i + 1]] > series[target][dates[i]] else 0
        samples.append((b, c, y))
    results[target] = evaluate(samples) if len(samples) >= 300 else {
        "status": "INSUFFICIENT_DATA",
        "samples": len(samples),
    }

print(
    json.dumps(
        {
            "status": "COMPLETE",
            "method": "baseline-predictor-conditioned-on-point-in-time-coupling-score-terciles-v1",
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "results": results,
        },
        indent=2,
    )
)
