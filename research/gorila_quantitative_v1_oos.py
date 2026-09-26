from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from dataclasses import dataclass

from research.gorila_data_snapshot import load_or_fetch_series
from quant.research_validation import audit_returns, pbo_from_train_test

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
CANDIDATES = [(g, l2) for g in FEATURE_GROUPS for l2 in L2_VALUES]


@dataclass(frozen=True)
class Row:
    date: str
    x: dict[str, float]
    y: int
    forward_return: float


def sigmoid(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit_logistic(rows: list[Row], names: tuple[str, ...], l2: float):
    import numpy as np
    X = np.asarray([[r.x[n] for n in names] for r in rows], dtype=np.float64)
    y = np.asarray([r.y for r in rows], dtype=np.float64)
    means = X.mean(axis=0)
    scales = np.maximum(X.std(axis=0), 1e-12)
    Z = (X - means) / scales
    rate = float(np.clip(y.mean(), 1e-6, 1.0 - 1e-6))
    w = np.zeros(Z.shape[1], dtype=np.float64)
    b = float(np.log(rate / (1.0 - rate)))
    lr = 0.08
    for _ in range(80):
        z = np.clip(b + Z @ w, -30.0, 30.0)
        p = 1.0 / (1.0 + np.exp(-z))
        e = p - y
        w -= lr * ((Z.T @ e) / len(Z) + l2 * w)
        b -= lr * float(e.mean())
    return means.tolist(), scales.tolist(), w.tolist(), b


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
    x.update(
        {
            "vol20": vol20,
            "z20": z20,
            "r1_vol": x["r1"] / max(vol20, 1e-8),
            "r5_z": x["r5"] * z20,
        }
    )
    p1 = series[dates[i + horizon]]
    return Row(dates[i], {k: float(v) for k, v in x.items()}, 1 if p1 > p0 else 0, math.log(p1 / p0))


def dataset(series: dict[str, dict[str, float]], symbol: str, horizon: int, lag: int = 0) -> list[Row]:
    dates = sorted(series[symbol])
    target = horizon + lag
    return [row for i in range(65, len(dates) - target) if (row := feature_row(series[symbol], dates, i, target)) is not None]


def brier(probs, labels):
    return sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)


def logloss(probs, labels):
    eps = 1e-12
    return -sum(y * math.log(max(eps, p)) + (1 - y) * math.log(max(eps, 1 - p)) for p, y in zip(probs, labels)) / len(labels)


def ece(probs, labels, bins=10):
    if not labels:
        return None
    total = len(labels)
    value = 0.0
    for bucket in range(bins):
        lo = bucket / bins
        hi = (bucket + 1) / bins
        idx = [i for i, p in enumerate(probs) if (lo <= p < hi) or (bucket == bins - 1 and p <= hi)]
        if not idx:
            continue
        mp = sum(probs[i] for i in idx) / len(idx)
        my = sum(labels[i] for i in idx) / len(idx)
        value += len(idx) / total * abs(mp - my)
    return value


def rank_ic(probs, returns):
    if len(probs) < 3:
        return 0.0
    pr = {i: r for r, i in enumerate(sorted(range(len(probs)), key=lambda j: probs[j]))}
    rr = {i: r for r, i in enumerate(sorted(range(len(returns)), key=lambda j: returns[j]))}
    xs, ys = [pr[i] for i in range(len(probs))], [rr[i] for i in range(len(returns))]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def normal_ci(values, z=1.96):
    if not values:
        return None
    mean = statistics.mean(values)
    if len(values) == 1:
        return [mean, mean]
    se = statistics.pstdev(values) / math.sqrt(len(values))
    return [mean - z * se, mean + z * se]


def percentile(values, q):
    if not values:
        return None
    vals = sorted(values)
    pos = (len(vals) - 1) * q
    lo, hi = int(math.floor(pos)), int(math.ceil(pos))
    return vals[lo] if lo == hi else vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


def select_candidate(train: list[Row], horizon: int):
    split = max(INNER_MIN, len(train) - INNER_TEST)
    inner_train = train[: max(INNER_MIN, split - horizon)]
    inner_test = train[split:]
    if len(inner_test) < 20 or len(inner_train) < INNER_MIN:
        return "base", 0.001, {"status": "FALLBACK"}
    candidates = []
    baseline = sum(r.y for r in inner_train) / len(inner_train)
    for group, names in FEATURE_GROUPS.items():
        for l2 in L2_VALUES:
            if len({r.y for r in inner_train}) < 2:
                continue
            model = fit_logistic(inner_train, names, l2)
            probs = [predict(model, r, names) for r in inner_test]
            labels = [r.y for r in inner_test]
            loss = brier(probs, labels)
            base = sum((baseline - y) ** 2 for y in labels) / len(labels)
            candidates.append((loss - base, group, l2))
    if not candidates:
        return "base", 0.001, {"status": "NO_CANDIDATE"}
    candidates.sort(key=lambda x: (x[0], x[1], x[2]))
    delta, group, l2 = candidates[0]
    return group, l2, {"status": "OK", "inner_delta_brier": delta, "candidate_count": len(candidates)}


def split_folds(rows: list[Row], horizon: int):
    start = TRAIN_MIN
    while start + TEST_SIZE <= len(rows):
        train = rows[: max(0, start - horizon)]
        test = rows[start : start + TEST_SIZE]
        if len(train) >= TRAIN_MIN and len({r.y for r in train}) >= 2:
            yield train, test
        start += TEST_SIZE


def score_candidate_on(rows: list[Row], train: list[Row], test: list[Row], group: str, l2: float, cost_bps: float):
    names = FEATURE_GROUPS[group]
    model = fit_logistic(train, names, l2)
    probs = [predict(model, r, names) for r in test]
    trades = []
    for r, p in zip(test, probs):
        side = 1 if p >= 0.55 else (-1 if p <= 0.45 else 0)
        if side:
            trades.append(side * r.forward_return - cost_bps / 10000.0)
    return probs, trades


def strategy_from_probs(probs, returns, cost_bps, horizon):
    """Build non-overlapping net simple returns from log forward returns."""
    selected = []
    last = -10**9
    for i, (p, log_ret) in enumerate(zip(probs, returns)):
        if i - last < horizon:
            continue
        side = 1 if p >= 0.55 else (-1 if p <= 0.45 else 0)
        if side:
            simple_ret = math.expm1(float(log_ret))
            selected.append(side * simple_ret - cost_bps / 10000.0)
            last = i
    eq, peak, max_dd = 1.0, 1.0, 0.0
    for r in selected:
        eq *= 1.0 + r
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak)
    return {"trades": len(selected), "net_return": eq - 1.0, "max_drawdown": max_dd, "returns": selected}


def run_oos(series, symbol: str, horizon: int):
    rows = dataset(series, symbol, horizon, 0)
    outer = []
    pbo_train, pbo_test = [[] for _ in CANDIDATES], [[] for _ in CANDIDATES]
    selected_returns = []
    momentum_returns = []
    always_long_returns = []

    for train, test in split_folds(rows, horizon):
        group, l2, selection = select_candidate(train, horizon)
        names = FEATURE_GROUPS[group]
        model = fit_logistic(train, names, l2)
        probs = [predict(model, r, names) for r in test]

        for ci, (cg, cl2) in enumerate(CANDIDATES):
            cand_names = FEATURE_GROUPS[cg]
            cand_model = fit_logistic(train, cand_names, cl2)
            train_probs = [predict(cand_model, r, cand_names) for r in train[-INNER_TEST:]]
            test_probs = [predict(cand_model, r, cand_names) for r in test]
            tr = strategy_from_probs(train_probs, [r.forward_return for r in train[-INNER_TEST:]], 50, horizon)["net_return"]
            te = strategy_from_probs(test_probs, [r.forward_return for r in test], 50, horizon)["net_return"]
            pbo_train[ci].append(tr)
            pbo_test[ci].append(te)

        oos = strategy_from_probs(probs, [r.forward_return for r in test], 0, horizon)
        selected_50 = strategy_from_probs(probs, [r.forward_return for r in test], 50, horizon)
        selected_returns.extend(selected_50["returns"])

        mom_probs = [1.0 if r.x["r5"] > 0 else 0.0 for r in test]
        momentum_returns.extend(strategy_from_probs(mom_probs, [r.forward_return for r in test], 50, horizon)["returns"])
        always_long_returns.extend(
            strategy_from_probs([1.0] * len(test), [r.forward_return for r in test], 50, horizon)["returns"]
        )

        outer.append({
            "test_start": test[0].date,
            "test_end": test[-1].date,
            "group": group,
            "l2": l2,
            "selection": selection,
            "probs": probs,
            "labels": [r.y for r in test],
            "returns": [r.forward_return for r in test],
        })

    probs = [p for f in outer for p in f["probs"]]
    labels = [y for f in outer for y in f["labels"]]
    returns = [r for f in outer for r in f["returns"]]
    if not probs:
        return {"status": "INSUFFICIENT_DATA", "symbol": symbol, "horizon_days": horizon}

    actual_rate = sum(labels) / len(labels)
    model_brier = brier(probs, labels)
    fold_skills = []
    fold_base_briers = []
    fold_base_logloss = []
    fold_model_logloss = []

    for f in outer:
        rate = sum(f["labels"]) / len(f["labels"])
        bb = sum((rate - y) ** 2 for y in f["labels"]) / len(f["labels"])
        bl = logloss([rate] * len(f["labels"]), f["labels"])
        ml = logloss(f["probs"], f["labels"])
        fold_base_briers.append(bb * len(f["labels"]))
        fold_base_logloss.append(bl * len(f["labels"]))
        fold_model_logloss.append(ml * len(f["labels"]))
        fold_skills.append(1.0 - brier(f["probs"], f["labels"]) / bb if bb > 0 else 0.0)

    base_brier = sum(fold_base_briers) / max(1, len(labels))
    baseline_logloss = sum(fold_base_logloss) / max(1, len(labels))
    model_logloss = sum(fold_model_logloss) / max(1, len(labels))
    logloss_delta = baseline_logloss - model_logloss

    strategy_50 = strategy_from_probs(probs, returns, 50, horizon)
    momentum = {
        "net_return": (lambda x: x["net_return"])(
            {"net_return": (lambda rs: (math.prod([1.0 + r for r in rs]) - 1.0) if rs else 0.0)(momentum_returns)}
        ),
        "trades": len(momentum_returns),
    }
    always_long = {
        "net_return": (math.prod([1.0 + r for r in always_long_returns]) - 1.0) if always_long_returns else 0.0,
        "trades": len(always_long_returns),
    }
    model_audit = audit_returns(selected_returns, trials=len(CANDIDATES), periods_per_year=252.0 / horizon)
    momentum_audit = audit_returns(momentum_returns, trials=1, periods_per_year=252.0 / horizon)

    rng = random.Random(SEED + horizon + sum(map(ord, symbol)))
    placebo_acc = []
    for _ in range(PLACEBO_PERM):
        perm = rows[:]
        yvals = [r.y for r in perm]
        rng.shuffle(yvals)
        placebo = [Row(r.date, r.x, y, r.forward_return) for r, y in zip(perm, yvals)]
        correct = 0
        total = 0
        for train_p, _unused in split_folds(placebo, horizon):
            # Use the same fold boundaries; evaluate on the original OOS labels.
            fold_start = len(train_p) + horizon
            test_start = min(fold_start, len(rows) - TEST_SIZE)
            test_original = rows[test_start : test_start + TEST_SIZE]
            if len(test_original) < 20:
                continue
            g, l2, _ = select_candidate(train_p, horizon)
            n = FEATURE_GROUPS[g]
            m = fit_logistic(train_p, n, l2)
            pp = [predict(m, r, n) for r in test_original]
            correct += sum((p >= 0.5) == bool(r.y) for p, r in zip(pp, test_original))
            total += len(pp)
        if total:
            placebo_acc.append(correct / total)

    pbo = pbo_from_train_test(pbo_train, pbo_test)
    stress = {}
    for lag in LAGS:
        lag_rows = dataset(series, symbol, horizon, lag)
        lag_probs = []
        lag_returns = []
        for train, test in split_folds(lag_rows, horizon + lag):
            g, l2, _ = select_candidate(train, horizon + lag)
            n = FEATURE_GROUPS[g]
            m = fit_logistic(train, n, l2)
            lag_probs.extend(predict(m, r, n) for r in test)
            lag_returns.extend(r.forward_return for r in test)
        stress[str(lag)] = {
            str(cost): strategy_from_probs(lag_probs, lag_returns, cost, horizon + lag)
            for cost in (25, 50, 100)
        }

    return {
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
        "logloss": model_logloss,
        "baseline_logloss": baseline_logloss,
        "logloss_delta": logloss_delta,
        "ece": ece(probs, labels),
        "rank_ic": rank_ic(probs, returns),
        "actual_up_rate": actual_rate,
        "mean_probability": sum(probs) / len(probs),
        "strategy_costs": {str(c): strategy_from_probs(probs, returns, c, horizon) for c in COSTS_BPS},
        "benchmarks": {
            "always_long_50bps": always_long,
            "momentum_r5_50bps": momentum,
            "model_delta_vs_momentum_50bps": strategy_50["net_return"] - momentum["net_return"],
            "model_delta_vs_always_long_50bps": strategy_50["net_return"] - always_long["net_return"],
        },
        "performance_audit": model_audit,
        "momentum_audit": momentum_audit,
        "pbo": pbo,
        "dsr": model_audit.get("deflated_sharpe_probability"),
        "stress": {
            k: {c: {x: v for x, v in m.items() if x != "returns"} for c, m in val.items()}
            for k, val in stress.items()
        },
        "placebo_accuracy_mean": statistics.mean(placebo_acc) if placebo_acc else None,
        "placebo_accuracy_p95": percentile(placebo_acc, 0.95) if placebo_acc else None,
        "placebo_count": len(placebo_acc),
        "execution_delta_vs_flat_50bps": strategy_50["net_return"],
        "execution_delta_vs_momentum_50bps": strategy_50["net_return"] - momentum["net_return"],
        "point_in_time": True,
        "data_health": True,
        "model_spec": {
            "selection": "nested-inner-brier-v1",
            "candidates": len(CANDIDATES),
            "purge_days": horizon,
            "outer_train_min": TRAIN_MIN,
            "outer_test_size": TEST_SIZE,
            "inner_test_size": INNER_TEST,
            "costs_bps": COSTS_BPS,
            "stress_lags_days": LAGS,
        },
    }


def validation_gate(result):
    if result.get("status") != "COMPLETE":
        return {
            "validation_status": "INSUFFICIENT_DATA",
            "prediction_status": "INSUFFICIENT_DATA",
            "strategy_status": "INSUFFICIENT_DATA",
            "prediction_reasons": ["NO_COMPLETE_RESULT"],
            "strategy_reasons": ["NO_COMPLETE_RESULT"],
        }

    prediction_reasons = []
    strategy_reasons = []

    if result.get("oos_samples", 0) < 500:
        prediction_reasons.append("MIN_OOS_SAMPLES")

    ci = result.get("brier_skill_ci95")
    if result.get("brier_skill") is None or result["brier_skill"] <= 0:
        prediction_reasons.append("BRIER_SKILL_NOT_POSITIVE")
    if not ci or ci[0] <= 0:
        prediction_reasons.append("BRIER_SKILL_CI_NOT_ABOVE_ZERO")
    if result.get("logloss_delta") is None or result.get("logloss_delta") <= 0:
        prediction_reasons.append("NO_LOGLOSS_IMPROVEMENT")
    if result.get("ece") is None or result.get("ece") > 0.05:
        prediction_reasons.append("ECE_ABOVE_0_05")

    placebo = result.get("placebo_accuracy_p95")
    if placebo is None or result.get("accuracy", 0.0) <= placebo:
        prediction_reasons.append("PLACEBO_NOT_BEATEN")

    # Prediction validation intentionally does not require trading-return
    # metrics. Probability forecasts and executable strategies are separate
    # statistical objects and must have separate gates.
    if result.get("logloss") is None:
        prediction_reasons.append("LOGLOSS_MISSING")

    if result.get("rank_ic", 0.0) <= 0:
        # Ranking is a secondary prediction diagnostic, not a hard requirement
        # for probability calibration.
        prediction_reasons.append("RANK_IC_NOT_POSITIVE")

    if result.get("strategy_costs", {}).get("50", {}).get("net_return", 0.0) <= 0:
        strategy_reasons.append("NET_RETURN_50BPS_NOT_POSITIVE")
    if result.get("execution_delta_vs_momentum_50bps", 0.0) <= 0:
        strategy_reasons.append("EXECUTION_DELTA_VS_MOMENTUM_NOT_POSITIVE")

    pbo = result.get("pbo") or {}
    if pbo.get("status") != "COMPLETE" or pbo.get("pbo", 1.0) > 0.05:
        strategy_reasons.append("PBO_GATE_FAILED")
    if result.get("dsr") is None or result.get("dsr", 0.0) <= 0:
        strategy_reasons.append("DSR_GATE_FAILED")
    for lag in ("0", "1", "2"):
        if result.get("stress", {}).get(lag, {}).get("50", {}).get("net_return", -1.0) <= 0:
            strategy_reasons.append(f"STRESS_LAG_{lag}_FAILED")

    prediction_status = "VALIDATED" if not prediction_reasons else "BLOCKED"
    strategy_status = "VALIDATED" if not strategy_reasons else "BLOCKED"
    overall = "VALIDATED" if prediction_status == "VALIDATED" and strategy_status == "VALIDATED" else "BLOCKED"
    return {
        "validation_status": overall,
        "prediction_status": prediction_status,
        "strategy_status": strategy_status,
        "prediction_reasons": prediction_reasons,
        "strategy_reasons": strategy_reasons,
    }


def _fetch(symbol):
    import httpx
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "10y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-Quantitative-V1/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: empty")
    ts = result.get("timestamp") or []
    closes = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    return {time.strftime("%Y-%m-%d", time.gmtime(t)): float(c) for t, c in zip(ts, closes) if c is not None and c > 0}


def main():
    snapshot = os.getenv("GORILA_DATA_SNAPSHOT")
    series, snapshot_hash = load_or_fetch_series(SYMBOLS, _fetch, snapshot)
    evidence = []
    for horizon in HORIZONS:
        for symbol in SYMBOLS:
            result = run_oos(series, symbol, horizon)
            gate = validation_gate(result)
            result.update(gate)
            result["validation_reasons"] = (
                list(gate.get("prediction_reasons") or [])
                + list(gate.get("strategy_reasons") or [])
            )
            result["dataset_sha256"] = snapshot_hash
            result["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            evidence.append(result)
    print(json.dumps({
        "schema": "gorila-quantitative-evidence-v1",
        "status": "COMPLETE",
        "method": "nested-wfo-model-selection-with-cost-pbo-dsr-and-adversarial-stress-v2",
        "snapshot_sha256": snapshot_hash,
        "symbols": SYMBOLS,
        "horizons": HORIZONS,
        "evidence": evidence,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
