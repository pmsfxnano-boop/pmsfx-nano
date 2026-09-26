from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from dataclasses import replace

from quant.research_validation import audit_returns
from research.gorila_data_snapshot import load_or_fetch_series
from research.gorila_quantitative_v1_oos import (
    FEATURE_GROUPS,
    INNER_TEST,
    TRAIN_MIN,
    Row,
    _prior_rate,
    _shift_probability,
    dataset,
    fit_logistic,
    predict,
    select_candidate,
)

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_LOCKBOX_HORIZONS", "5,10").split(",") if x.strip()]
LOCKBOX_DAYS = int(os.getenv("GORILA_LOCKBOX_DAYS", "252"))
COSTS_BPS_PER_LEG = [25, 50, 100]
MIN_SPREAD_Z = float(os.getenv("GORILA_LOCKBOX_MIN_SPREAD_Z", "1.0"))
TARGET_ANNUAL_VOL = float(os.getenv("GORILA_LOCKBOX_TARGET_VOL", "0.10"))
MAX_GROSS = float(os.getenv("GORILA_LOCKBOX_MAX_GROSS", "1.0"))
PLACEBO_PERM = int(os.getenv("GORILA_LOCKBOX_PLACEBO_PERM", "64"))
SEED = int(os.getenv("GORILA_LOCKBOX_SEED", "20260926"))


def _rank_corr(values, returns):
    if len(values) < 3:
        return 0.0
    pv = {i: r for r, i in enumerate(sorted(range(len(values)), key=lambda j: values[j]))}
    rv = {i: r for r, i in enumerate(sorted(range(len(returns)), key=lambda j: returns[j]))}
    xs = [pv[i] for i in range(len(values))]
    ys = [rv[i] for i in range(len(returns))]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def _ci(values, z=1.96):
    if not values:
        return None
    m = statistics.mean(values)
    if len(values) < 2:
        return [m, m]
    se = statistics.pstdev(values) / math.sqrt(len(values))
    return [m - z * se, m + z * se]


def _zscore_map(values: dict[str, float]) -> dict[str, float]:
    xs = list(values.values())
    if len(xs) < 2:
        return {k: 0.0 for k in values}
    m = statistics.mean(xs)
    sd = statistics.pstdev(xs)
    if sd <= 1e-12:
        return {k: 0.0 for k in values}
    return {k: max(-2.0, min(2.0, (v - m) / sd)) for k, v in values.items()}


def _ensemble_score(scores: dict[str, float]) -> dict[str, float]:
    components = []
    for values in scores.values():
        components.append(_zscore_map(values))
    return {
        symbol: statistics.mean(component.get(symbol, 0.0) for component in components)
        for symbol in SYMBOLS
    }


def _pair_return(scores, rows, cost_bps_per_leg, use_vol_target):
    ranked = sorted(scores, key=scores.get)
    if len(ranked) < 2:
        return None
    short_symbol, long_symbol = ranked[0], ranked[-1]
    long_row = rows[long_symbol]
    short_row = rows[short_symbol]
    gross = 0.5 * math.expm1(long_row.forward_return) - 0.5 * math.expm1(short_row.forward_return)

    gross_per_day_vol = math.sqrt(
        max(
            1e-12,
            0.5 * long_row.x["vol20"] ** 2 + 0.5 * short_row.x["vol20"] ** 2,
        )
    )
    annualized_vol = gross_per_day_vol * math.sqrt(252.0)
    leverage = 1.0
    if use_vol_target:
        leverage = min(MAX_GROSS, TARGET_ANNUAL_VOL / max(annualized_vol, 1e-8))
    return leverage * gross - 2.0 * leverage * cost_bps_per_leg / 10000.0, leverage, long_symbol, short_symbol


def evaluate_lockbox(series, symbol_rows, horizon):
    common_dates = sorted(set.intersection(*(set(symbol_rows[s]) for s in SYMBOLS)))
    if len(common_dates) <= LOCKBOX_DAYS + TRAIN_MIN:
        return {"status": "INSUFFICIENT_DATA", "horizon_days": horizon}

    lockbox_dates = common_dates[-LOCKBOX_DAYS:]
    dev_dates = common_dates[:-LOCKBOX_DAYS]
    model_specs = {}
    model_objects = {}

    for symbol in SYMBOLS:
        rows = symbol_rows[symbol]
        dev_rows = [r for r in rows if r.date in set(dev_dates)]
        if len(dev_rows) < TRAIN_MIN:
            return {"status": "INSUFFICIENT_DATA", "symbol": symbol, "horizon_days": horizon}
        group, l2, prior_window, selection = select_candidate(dev_rows, horizon)
        names = FEATURE_GROUPS[group]
        model = fit_logistic(dev_rows, names, l2)
        train_rate = sum(r.y for r in dev_rows) / len(dev_rows)
        prior = _prior_rate(dev_rows, prior_window)
        model_specs[symbol] = {
            "group": group,
            "l2": l2,
            "prior_window": prior_window,
            "train_rate": train_rate,
            "prior": prior,
            "selection": selection,
            "train_end": dev_rows[-1].date,
        }
        model_objects[symbol] = (model, names)

    lock_rows = {s: {r.date: r for r in symbol_rows[s] if r.date in set(lockbox_dates)} for s in SYMBOLS}
    rank_ics = []
    fixed_returns = {c: [] for c in COSTS_BPS_PER_LEG}
    vol_returns = {c: [] for c in COSTS_BPS_PER_LEG}
    momentum_returns = {c: [] for c in COSTS_BPS_PER_LEG}
    trades = []
    selected_dates = []

    for date_idx, date in enumerate(lockbox_dates):
        rows = {}
        pdelta = {}
        vol_scaled = {}
        blend = {}
        for symbol in SYMBOLS:
            row = lock_rows[symbol].get(date)
            if row is None:
                continue
            model, names = model_objects[symbol]
            spec = model_specs[symbol]
            p_raw = predict(model, row, names)
            p = _shift_probability(p_raw, spec["train_rate"], spec["prior"])
            delta = p - spec["prior"]
            vol = max(row.x["vol20"], 1e-8)
            pdelta[symbol] = delta
            vol_scaled[symbol] = delta / vol
            blend[symbol] = delta + 0.25 * row.x["r5"] / vol
            rows[symbol] = row

        common = sorted(set(rows) & set(pdelta) & set(vol_scaled) & set(blend))
        if len(common) < 3:
            continue

        scores = {
            "probability_delta": {s: pdelta[s] for s in common},
            "vol_scaled_delta": {s: vol_scaled[s] for s in common},
            "blend_momentum": {s: blend[s] for s in common},
        }
        ensemble = _ensemble_score(scores)
        returns = {s: rows[s].forward_return for s in common}
        rank_ics.append(_rank_corr([ensemble[s] for s in common], [returns[s] for s in common]))

        if date_idx % horizon != 0:
            continue

        if max(ensemble.values()) - min(ensemble.values()) < MIN_SPREAD_Z:
            continue

        selected_dates.append(date)
        for cost in COSTS_BPS_PER_LEG:
            base = _pair_return(ensemble, rows, cost, False)
            vol_targeted = _pair_return(ensemble, rows, cost, True)
            momentum_scores = {s: rows[s].x["r5"] for s in common}
            momentum = _pair_return(momentum_scores, rows, cost, False)
            if base:
                fixed_returns[cost].append(base[0])
            if vol_targeted:
                vol_returns[cost].append(vol_targeted[0])
            if momentum:
                momentum_returns[cost].append(momentum[0])
        trades.append(
            {
                "date": date,
                "long": max(ensemble, key=ensemble.get),
                "short": min(ensemble, key=ensemble.get),
                "score_spread": max(ensemble.values()) - min(ensemble.values()),
            }
        )

    target = vol_returns[50]
    fixed = fixed_returns[50]
    mom = momentum_returns[50]
    perf = audit_returns(target, trials=1, periods_per_year=252.0 / horizon)
    fixed_perf = audit_returns(fixed, trials=1, periods_per_year=252.0 / horizon)
    mom_perf = audit_returns(mom, trials=1, periods_per_year=252.0 / horizon)

    rng = random.Random(SEED + horizon)
    placebo_means = []
    for _ in range(PLACEBO_PERM):
        one = []
        for date_idx, date in enumerate(lockbox_dates):
            if date_idx % horizon != 0:
                continue
            vals = []
            rets = []
            for symbol in SYMBOLS:
                row = lock_rows[symbol].get(date)
                if row is None:
                    continue
                vals.append(row.x["r5"])
                rets.append(row.forward_return)
            if len(vals) < 3:
                continue
            rng.shuffle(rets)
            one.append(_rank_corr(vals, rets))
        if one:
            placebo_means.append(statistics.mean(one))
    placebo_p95 = None
    if placebo_means:
        ordered = sorted(placebo_means)
        placebo_p95 = ordered[min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)]

    mean_rank = statistics.mean(rank_ics) if rank_ics else None
    rank_ci = _ci(rank_ics)
    lockbox_net_50 = statistics.prod(1.0 + r for r in target) - 1.0 if target else 0.0
    momentum_net_50 = statistics.prod(1.0 + r for r in mom) - 1.0 if mom else 0.0
    fixed_net_50 = statistics.prod(1.0 + r for r in fixed) - 1.0 if fixed else 0.0

    prediction_ok = bool(rank_ci and rank_ci[0] > 0 and mean_rank is not None)
    strategy_ok = (
        len(target) >= 20
        and lockbox_net_50 > 0
        and (lockbox_net_50 - momentum_net_50) > 0
        and (perf.get("deflated_sharpe_probability") or 0.0) >= 0.95
        and (placebo_p95 is None or mean_rank > placebo_p95)
    )

    reasons = []
    if not prediction_ok:
        reasons.append("LOCKBOX_RANK_IC_CI_NOT_POSITIVE")
    if len(target) < 20:
        reasons.append("LOCKBOX_MIN_TRADES")
    if lockbox_net_50 <= 0:
        reasons.append("LOCKBOX_NET_RETURN_50BPS_NOT_POSITIVE")
    if lockbox_net_50 - momentum_net_50 <= 0:
        reasons.append("LOCKBOX_DELTA_VS_MOMENTUM_NOT_POSITIVE")
    if (perf.get("deflated_sharpe_probability") or 0.0) < 0.95:
        reasons.append("LOCKBOX_DSR_LT_0_95")
    if placebo_p95 is not None and mean_rank <= placebo_p95:
        reasons.append("LOCKBOX_PLACEBO_NOT_BEATEN")

    return {
        "status": "COMPLETE",
        "method": "frozen-cross-sectional-ensemble-lockbox-v1",
        "horizon_days": horizon,
        "development_end": dev_dates[-1],
        "lockbox_start": lockbox_dates[0],
        "lockbox_end": lockbox_dates[-1],
        "lockbox_days": len(lockbox_dates),
        "model_specs": model_specs,
        "mean_rank_ic": mean_rank,
        "rank_ic_ci95": rank_ci,
        "placebo_rank_ic_p95": placebo_p95,
        "trade_count_50bps": len(target),
        "net_return_50bps": lockbox_net_50,
        "fixed_ensemble_net_return_50bps": fixed_net_50,
        "momentum_net_return_50bps": momentum_net_50,
        "delta_vs_momentum_50bps": lockbox_net_50 - momentum_net_50,
        "performance_audit": perf,
        "fixed_performance_audit": fixed_perf,
        "momentum_performance_audit": mom_perf,
        "validation_status": "VALIDATED" if prediction_ok and strategy_ok else "BLOCKED",
        "validation_reasons": reasons,
        "execution_assumptions": {
            "cost_bps_per_leg": 50,
            "min_score_spread_z": MIN_SPREAD_Z,
            "target_annual_vol": TARGET_ANNUAL_VOL,
            "max_gross": MAX_GROSS,
            "non_overlapping_rebalance": True,
        },
        "trades": trades,
        "point_in_time": True,
    }


def _fetch(symbol):
    import httpx
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "10y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-Lockbox-V1/1.0"},
    )
    response.raise_for_status()
    result = (response.json().get("chart", {}).get("result") or [None])[0]
    timestamps = result.get("timestamp") or []
    closes = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    return {
        time.strftime("%Y-%m-%d", time.gmtime(ts)): float(close)
        for ts, close in zip(timestamps, closes)
        if close is not None and close > 0
    }


def main():
    snapshot = os.getenv("GORILA_DATA_SNAPSHOT")
    series, snapshot_hash = load_or_fetch_series(SYMBOLS, _fetch, snapshot)
    symbol_rows = {symbol: dataset(series, symbol, h) for h in HORIZONS for symbol in SYMBOLS}
    evidence = []
    for horizon in HORIZONS:
        rows = {symbol: symbol_rows[(symbol, horizon)] for symbol in SYMBOLS}
        evidence.append(evaluate_lockbox(series, rows, horizon))
    print(json.dumps(
        {
            "schema": "gorila-lockbox-evidence-v1",
            "status": "COMPLETE",
            "snapshot_sha256": snapshot_hash,
            "evidence": evidence,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
