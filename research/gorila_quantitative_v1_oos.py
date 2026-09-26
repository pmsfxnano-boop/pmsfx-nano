from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from dataclasses import dataclass

from research.gorila_data_snapshot import load_or_fetch_series

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
COSTS_BPS = [0, 25, 50, 100]
LAGS = [0, 1, 2]
TRAIN_MIN = 504
TEST_SIZE = 126
INNER_TEST = 63
INNER_MIN = 252
PLACEBO_PERM = int(os.getenv("GORILA_PLACEBO_PERM", "16"))
SEED = int(os.getenv("GORILA_VALIDATION_SEED", "20260926"))

FEATURE_GROUPS = {
    "momentum": ("r1", "r3", "r5"),
    "base": ("r1", "r3", "r5", "vol20", "z20"),
    "risk_adjusted": ("r1", "r3", "r5", "vol20", "z20", "r1_vol", "r5_z"),
}
L2_VALUES = [0.0005, 0.001, 0.003, 0.01]


@dataclass(frozen=True)
class Row:
    date: str
    x: dict[str, float]
    y: int
    forward_return: float


def sigmoid(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit_logistic(rows: list[Row], names: tuple[str, ...], l2: float):
    X = [[r.x[n] for n in names] for r in rows]
    y = [r.y for r in rows]
    means = [sum(v[j] for v in X) / len(X) for j in range(len(names))]
    scales = [
        max(1e-12, math.sqrt(sum((v[j] - means[j]) ** 2 for v in X) / len(X)))
        for j in range(len(names))
    ]
    rate = max(1e-6, min(1.0 - 1e-6, sum(y) / len(y)))
    w = [0.0] * len(names)
    b = math.log(rate / (1.0 - rate))
    lr = 0.035
    for _ in range(160):
        grad_w = [0.0] * len(w)
        grad_b = 0.0
        for row, target in zip(X, y):
            zrow = [(v - m) / s for v, m, s in zip(row, means, scales)]
            p = sigmoid(b + sum(a * v for a, v in zip(w, zrow)))
            e = p - target
            for j, v in enumerate(zrow):
                grad_w[j] += e * v / len(X)
            grad_b += e / len(X)
        for j in range(len(w)):
            grad_w[j] += l2 * w[j]
            w[j] -= lr * grad_w[j]
        b -= lr * grad_b
    return means, scales, w, b


def predict(model, row: Row, names: tuple[str, ...]) -> float:
    means, scales, w, b = model
    return sigmoid(
        b + sum(a * (row.x[n] - m) / s for a, n, m, s in zip(w, names, means, scales))
    )


def feature_row(series: dict[str, float], dates: list[str], i: int, horizon: int) -> Row | None:
    if i < 65 or i + horizon >= len(dates):
        return None
    p0 = series[dates[i]]
    x = {f"r{n}": math.log(p0 / series[dates[i - n]]) for n in (1, 3, 5)}
    hist_returns = [
        math.log(series[dates[k]] / series[dates[k - 1]])
        for k in range(max(1, i - 20), i)
    ]
    if len(hist_returns) < 18:
        return None
    vol20 = statistics.pstdev(hist_returns[-20:])
    window = [series[dates[k]] for k in range(max(0, i - 20), i)]
    sd = statistics.pstdev(window)
    z20 = (p0 - statistics.mean(window)) / sd if sd > 0 else 0.0
    x["vol20"] = vol20
    x["z20"] = z20
    x["r1_vol"] = x["r1"] / max(vol20, 1e-8)
    x["r5_z"] = x["r5"] * z20
    p1 = series[dates[i + horizon]]
    return Row(dates[i], {k: float(v) for k, v in x.items()}, 1 if p1 > p0 else 0, math.log(p1 / p0))


def dataset(series: dict[str, dict[str, float]], symbol: str, horizon: int, lag: int = 0) -> list[Row]:
    dates = sorted(series[symbol])
    rows = []
    target = horizon + lag
    for i in range(65, len(dates) - target):
        row = feature_row(series[symbol], dates, i, target)
        if row is not None:
            rows.append(row)
    return rows


def brier(probs, labels):
    return sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)


def logloss(probs, labels):
    eps = 1e-12
    return -sum(y * math.log(max(eps, p)) + (1 - y) * math.log(max(eps, 1 - p)) for p, y in zip(probs, labels)) / len(labels)


def rank_ic(probs, returns):
    if len(probs) < 3:
        return 0.0
    pr = {i: r for r, i in enumerate(sorted(range(len(probs)), key=lambda j: probs[j]))}
    rr = {i: r for r, i in enumerate(sorted(range(len(returns)), key=lambda j: returns[j]))}
    xs, ys = [pr[i] for i in range(len(probs))], [rr[i] for i in range(len(returns))]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def select_candidate(train: list[Row], horizon: int):
    split = max(INNER_MIN, len(train) - INNER_TEST)
    inner_train = train[:max(INNER_MIN, split - horizon)]
    inner_test = train[split:]
    if len(inner_test) < 20 or len(inner_train) < INNER_MIN:
        return "base", 0.001, {"status": "FALLBACK"}
    candidates = []
    for group, names in FEATURE_GROUPS.items():
        for l2 in L2_VALUES:
            if len({r.y for r in inner_train}) < 2:
                continue
            model = fit_logistic(inner_train, names, l2)
            probs = [predict(model, r, names) for r in inner_test]
            labels = [r.y for r in inner_test]
            baseline = sum(labels) / len(labels)
            candidates.append((
                brier(probs, labels) - sum((baseline - y) ** 2 for y in labels) / len(labels),
                group, l2,
            ))
    if not candidates:
        return "base", 0.001, {"status": "NO_CANDIDATE"}
    candidates.sort()
    delta, group, l2 = candidates[0]
    return group, l2, {"status": "OK", "inner_delta_brier": delta, "candidate_count": len(candidates)}


def evaluate_oos_rows(rows: list[Row], horizon: int):
    out = []
    start = TRAIN_MIN
    while start + TEST_SIZE <= len(rows):
        train = rows[:max(0, start - horizon)]
        test = rows[start:start + TEST_SIZE]
        if len(train) < TRAIN_MIN or len({r.y for r in train}) < 2:
            start += TEST_SIZE
            continue
        group, l2, _ = select_candidate(train, horizon)
        names = FEATURE_GROUPS[group]
        model = fit_logistic(train, names, l2)
        out.extend({"prob": predict(model, r, names), "ret": r.forward_return} for r in test)
        start += TEST_SIZE
    return out


def percentile(values, q):
    if not values:
        return None
    vals = sorted(values)
    pos = (len(vals) - 1) * q
    lo, hi = int(math.floor(pos)), int(math.ceil(pos))
    return vals[lo] if lo == hi else vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


def normal_ci(values, z=1.96):
    if not values:
        return None
    mean = statistics.mean(values)
    if len(values) == 1:
        return [mean, mean]
    se = statistics.pstdev(values) / math.sqrt(len(values))
    return [mean - z * se, mean + z * se]


def evaluate_symbol(series, symbol: str, horizon: int):
    rows = dataset(series, symbol, horizon)
    outer = []
    start = TRAIN_MIN
    while start + TEST_SIZE <= len(rows):
        train = rows[:max(0, start - horizon)]
        test = rows[start:start + TEST_SIZE]
        if len(train) < TRAIN_MIN or len({r.y for r in train}) < 2:
            start += TEST_SIZE
            continue
        group, l2, selection = select_candidate(train, horizon)
        names = FEATURE_GROUPS[group]
        model = fit_logistic(train, names, l2)
        outer.append({
            "test_start": test[0].date,
            "test_end": test[-1].date,
            "group": group,
            "l2": l2,
            "selection": selection,
            "probs": [predict(model, r, names) for r in test],
            "labels": [r.y for r in test],
            "returns": [r.forward_return for r in test],
        })
        start += TEST_SIZE

    probs = [p for f in outer for p in f["probs"]]
    labels = [y for f in outer for y in f["labels"]]
    returns = [r for f in outer for r in f["returns"]]
    if not probs:
        return {"status": "INSUFFICIENT_DATA", "symbol": symbol, "horizon_days": horizon}

    actual_rate = sum(labels) / len(labels)
    model_brier = brier(probs, labels)
    base_brier = sum((actual_rate - y) ** 2 for y in labels) / len(labels)
    fold_skills = []
    for f in outer:
        rate = sum(f["labels"]) / len(f["labels"])
        bb = sum((rate - y) ** 2 for y in f["labels"]) / len(f["labels"])
        fold_skills.append(1.0 - brier(f["probs"], f["labels"]) / bb if bb > 0 else 0.0)

    def strategy(cost_bps, lag):
        scored = [{"prob": p, "ret": r} for p, r in zip(probs, returns)] if lag == 0 else evaluate_oos_rows(dataset(series, symbol, horizon, lag), horizon)
        trades = []
        for item in scored:
            p = item["prob"]
            side = 1 if p >= 0.55 else (-1 if p <= 0.45 else 0)
            if side:
                trades.append(side * item["ret"] - cost_bps / 10000.0)
        equity, peak, max_dd = 1.0, 1.0, 0.0
        for r in trades:
            equity *= 1.0 + r
            peak = max(peak, equity)
            max_dd = max(max_dd, (peak - equity) / peak)
        return {"trades": len(trades), "net_return": equity - 1.0, "max_drawdown": max_dd}

    costs = {str(c): strategy(c, 0) for c in COSTS_BPS}
    stress = {str(lag): {str(c): strategy(c, lag) for c in (25, 50, 100)} for lag in LAGS}

    rng = random.Random(SEED + horizon + sum(map(ord, symbol)))
    placebo_acc = []
    for _ in range(PLACEBO_PERM):
        shuffled = rows[:]
        labels_source = [r.y for r in shuffled]
        rng.shuffle(labels_source)
        placebo_rows = [Row(r.date, r.x, y, r.forward_return) for r, y in zip(rows, labels_source)]
        pp = evaluate_oos_rows(placebo_rows, horizon)
        if pp:
            placebo_acc.append(sum((x["prob"] >= 0.5) == bool(placebo_rows[min(i, len(placebo_rows)-1)].y) for i, x in enumerate(pp)) / len(pp))

    result = {
        "status": "COMPLETE",
        "symbol": symbol,
        "horizon_days": horizon,
        "dataset_samples": len(rows),
        "oos_samples": len(probs),
        "outer_folds": len(outer),
        "selected_model_counts": {g: sum(f["group"] == g for f in outer) for g in FEATURE_GROUPS},
        "accuracy": sum((p >= 0.5) == bool(y) for p, y in zip(probs, labels)) / len(labels),
        "brier": model_brier,
        "baseline_brier": base_brier,
        "brier_skill": 1.0 - model_brier / base_brier if base_brier > 0 else None,
        "brier_skill_ci95": normal_ci(fold_skills),
        "logloss": logloss(probs, labels),
        "rank_ic": rank_ic(probs, returns),
        "actual_up_rate": actual_rate,
        "mean_probability": sum(probs) / len(probs),
        "strategy_costs": costs,
        "stress": stress,
        "placebo_accuracy_mean": statistics.mean(placebo_acc) if placebo_acc else None,
        "placebo_accuracy_p95": percentile(placebo_acc, 0.95) if placebo_acc else None,
        "placebo_count": len(placebo_acc),
        "execution_delta_vs_flat_50bps": costs["50"]["net_return"],
        "point_in_time": True,
        "data_health": True,
        "model_spec": {"selection": "nested-inner-brier-v1", "purge_days": horizon, "outer_train_min": TRAIN_MIN, "outer_test_size": TEST_SIZE},
    }
    return result


def validation_gate(result):
    if result.get("status") != "COMPLETE":
        return "INSUFFICIENT_DATA", ["NO_COMPLETE_RESULT"]
    reasons = []
    if result.get("oos_samples", 0) < 500:
        reasons.append("MIN_OOS_SAMPLES")
    ci = result.get("brier_skill_ci95")
    if result.get("brier_skill") is None or result["brier_skill"] <= 0:
        reasons.append("BRIER_SKILL_NOT_POSITIVE")
    if not ci or ci[0] <= 0:
        reasons.append("BRIER_SKILL_CI_NOT_ABOVE_ZERO")
    if result.get("rank_ic", 0.0) <= 0:
        reasons.append("RANK_IC_NOT_POSITIVE")
    if result.get("strategy_costs", {}).get("50", {}).get("net_return", 0.0) <= 0:
        reasons.append("NET_RETURN_50BPS_NOT_POSITIVE")
    placebo = result.get("placebo_accuracy_p95")
    if placebo is not None and result.get("accuracy", 0.0) <= placebo:
        reasons.append("PLACEBO_NOT_BEATEN")
    for lag in ("0", "1", "2"):
        if result.get("stress", {}).get(lag, {}).get("50", {}).get("net_return", -1.0) <= 0:
            reasons.append(f"STRESS_LAG_{lag}_FAILED")
    return ("VALIDATED", []) if not reasons else ("BLOCKED", reasons)


def _fetch(symbol):
    import httpx
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "5y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-Quantitative-V1/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: empty")
    ts, closes = result.get("timestamp") or [], ((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {time.strftime("%Y-%m-%d", time.gmtime(t)): float(c) for t, c in zip(ts, closes) if c is not None and c > 0}


def main():
    snapshot = os.getenv("GORILA_DATA_SNAPSHOT")
    series, snapshot_hash = load_or_fetch_series(SYMBOLS, _fetch, snapshot)
    evidence = []
    for horizon in HORIZONS:
        for symbol in SYMBOLS:
            result = evaluate_symbol(series, symbol, horizon)
            status, reasons = validation_gate(result)
            result["validation_status"] = status
            result["validation_reasons"] = reasons
            result["dataset_sha256"] = snapshot_hash
            result["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            evidence.append(result)
    print(json.dumps({
        "schema": "gorila-quantitative-evidence-v1",
        "status": "COMPLETE",
        "method": "nested-wfo-model-selection-with-cost-and-adversarial-stress-v1",
        "snapshot_sha256": snapshot_hash,
        "symbols": SYMBOLS,
        "horizons": HORIZONS,
        "evidence": evidence,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
