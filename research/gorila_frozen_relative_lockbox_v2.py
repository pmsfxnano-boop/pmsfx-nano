from __future__ import annotations

import json
import math
import os
import random
import statistics
import time

from quant.research_validation import audit_returns
from research.gorila_data_snapshot import load_or_fetch_series
from research.gorila_quantitative_v1_oos import (
    FEATURE_GROUPS,
    TRAIN_MIN,
    _prior_rate,
    _shift_probability,
    fit_logistic,
    predict,
    dataset,
    select_candidate,
)

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_FROZEN_RELATIVE_HORIZONS", "5,10").split(",") if x.strip()]
LOCKBOX_DAYS = int(os.getenv("GORILA_FROZEN_RELATIVE_LOCKBOX_DAYS", "756"))
SELECTION_DAYS = int(os.getenv("GORILA_FROZEN_RELATIVE_SELECTION_DAYS", "252"))
COSTS_BPS_PER_LEG = [25, 50, 100]
PLACEBO_PERM = int(os.getenv("GORILA_FROZEN_RELATIVE_PLACEBO_PERM", "128"))
SEED = int(os.getenv("GORILA_FROZEN_RELATIVE_SEED", "20260926"))
SCORE_VARIANTS = ("probability_delta", "vol_scaled_delta", "blend_momentum")


def rank_ic(values, returns):
    if len(values) < 3:
        return 0.0
    pv = {i: r for r, i in enumerate(sorted(range(len(values)), key=lambda j: values[j]))}
    rv = {i: r for r, i in enumerate(sorted(range(len(returns)), key=lambda j: returns[j]))}
    xs = [pv[i] for i in range(len(values))]
    ys = [rv[i] for i in range(len(returns))]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def newey_west_ci(values, max_lag=3, z=1.96):
    values = [float(v) for v in values if math.isfinite(float(v))]
    n = len(values)
    if n < 5:
        return [None, None]
    m = statistics.mean(values)
    centered = [v - m for v in values]
    gamma0 = sum(v * v for v in centered) / n
    bandwidth = min(max_lag, n - 1)
    long_run = gamma0
    for lag in range(1, bandwidth + 1):
        gamma = sum(centered[t] * centered[t - lag] for t in range(lag, n)) / n
        long_run += 2.0 * (1.0 - lag / (bandwidth + 1.0)) * gamma
    se = math.sqrt(max(long_run, 0.0) / n)
    return [m - z * se, m + z * se]


def block_bootstrap_ci(values, seed, replicates=2000, block=None):
    values = [float(v) for v in values if math.isfinite(float(v))]
    n = len(values)
    if n < 10:
        return [None, None]
    rng = random.Random(seed)
    block = block or max(2, int(math.sqrt(n)))
    means = []
    for _ in range(replicates):
        draw = []
        while len(draw) < n:
            start = rng.randrange(n)
            for j in range(block):
                draw.append(values[(start + j) % n])
                if len(draw) >= n:
                    break
        means.append(statistics.mean(draw[:n]))
    means.sort()
    return [
        means[int(0.025 * (len(means) - 1))],
        means[int(0.975 * (len(means) - 1))],
    ]


def score_variant(variant, delta, row):
    vol = max(float(row["vol20"]), 1e-8)
    if variant == "probability_delta":
        return delta
    if variant == "vol_scaled_delta":
        return delta / vol
    return delta + 0.25 * float(row["r5"]) / vol


def pair_trade(scores, returns, symbols, cost_per_leg_bps):
    ranked = sorted(symbols, key=scores.get)
    if len(ranked) < 2:
        return None
    short_symbol, long_symbol = ranked[0], ranked[-1]
    gross = 0.5 * math.expm1(float(returns[long_symbol])) - 0.5 * math.expm1(float(returns[short_symbol]))
    net = gross - 2.0 * cost_per_leg_bps / 10000.0
    return net


def choose_variant_on_development(series, horizon, common_dates, dev_dates, symbol_models):
    selection_start = max(0, len(dev_dates) - SELECTION_DAYS)
    selection_dates = dev_dates[selection_start:]
    if len(selection_dates) < 60:
        return "probability_delta", {"status": "FALLBACK", "selection_n": len(selection_dates)}

    rank_values = {variant: [] for variant in SCORE_VARIANTS}
    for date in selection_dates:
        scores = {v: {} for v in SCORE_VARIANTS}
        returns = {}
        for symbol in SYMBOLS:
            model, names, train_rate, prior, _ = symbol_models[symbol]
            rows = symbol_models[symbol][5]
            row = rows.get(date)
            if row is None:
                continue
            p = _shift_probability(predict(model, row, names), train_rate, prior)
            delta = p - prior
            for variant in SCORE_VARIANTS:
                scores[variant][symbol] = score_variant(
                    variant,
                    delta,
                    row.x,
                )
            returns[symbol] = row.forward_return
        common = [s for s in SYMBOLS if s in returns and s in scores["probability_delta"]]
        if len(common) < 3:
            continue
        for variant in SCORE_VARIANTS:
            rank_values[variant].append(
                rank_ic([scores[variant][s] for s in common], [returns[s] for s in common])
            )
    means = {
        variant: statistics.mean(vals) if vals else float("-inf")
        for variant, vals in rank_values.items()
    }
    selected = max(SCORE_VARIANTS, key=lambda v: (means[v], v))
    return selected, {
        "status": "OK",
        "selection_start": selection_dates[0],
        "selection_end": selection_dates[-1],
        "selection_n": len(selection_dates),
        "development_rank_ic_by_variant": means,
    }


def evaluate_symbol_horizon(series, horizon):
    rows = {s: dataset(series, s, horizon) for s in SYMBOLS}
    date_sets = [{r.date for r in rows[s]} for s in SYMBOLS]
    if not all(date_sets):
        return {"status": "INSUFFICIENT_DATA", "horizon_days": horizon}
    common_dates = sorted(set.intersection(*date_sets))
    if len(common_dates) <= LOCKBOX_DAYS + TRAIN_MIN:
        return {"status": "INSUFFICIENT_DATA", "horizon_days": horizon, "common_dates": len(common_dates)}

    dev_dates = common_dates[:-LOCKBOX_DAYS]
    lock_dates = common_dates[-LOCKBOX_DAYS:]

    symbol_models = {}
    for symbol in SYMBOLS:
        dev_rows = [r for r in rows[symbol] if r.date in set(dev_dates)]
        if len(dev_rows) < TRAIN_MIN:
            return {"status": "INSUFFICIENT_DATA", "symbol": symbol, "horizon_days": horizon}
        group, l2, prior_window, selection = select_candidate(dev_rows, horizon)
        names = FEATURE_GROUPS[group]
        model = fit_logistic(dev_rows, names, l2)
        train_rate = sum(r.y for r in dev_rows) / len(dev_rows)
        prior = _prior_rate(dev_rows, prior_window)
        symbol_models[symbol] = (
            model,
            names,
            train_rate,
            prior,
            selection,
            {r.date: r for r in rows[symbol]},
        )

    variant, variant_meta = choose_variant_on_development(
        series, horizon, common_dates, dev_dates, symbol_models
    )

    rank_all = []
    rank_rebalance = []
    yearly = {}
    returns_by_cost = {c: [] for c in COSTS_BPS_PER_LEG}
    momentum_by_cost = {c: [] for c in COSTS_BPS_PER_LEG}

    for i, date in enumerate(lock_dates):
        scores = {}
        returns = {}
        rows_today = {}
        for symbol in SYMBOLS:
            model, names, train_rate, prior, _sel, mapping = symbol_models[symbol]
            row = mapping.get(date)
            if row is None:
                continue
            p = _shift_probability(predict(model, row, names), train_rate, prior)
            delta = p - prior
            scores[symbol] = score_variant(variant, delta, row.x)
            returns[symbol] = row.forward_return
            rows_today[symbol] = row
        common = [s for s in SYMBOLS if s in scores and s in returns]
        if len(common) < 3:
            continue
        rank = rank_ic([scores[s] for s in common], [returns[s] for s in common])
        rank_all.append(rank)
        year = date[:4]
        yearly.setdefault(year, []).append(rank)
        if i % horizon != 0:
            continue
        rank_rebalance.append(rank)
        for cost in COSTS_BPS_PER_LEG:
            trade = pair_trade(scores, returns, common, cost)
            mom_scores = {s: rows_today[s].x["r5"] for s in common}
            mom = pair_trade(mom_scores, returns, common, cost)
            if trade is not None:
                returns_by_cost[cost].append(trade)
            if mom is not None:
                momentum_by_cost[cost].append(mom)

    if len(rank_rebalance) < 50:
        return {"status": "INSUFFICIENT_DATA", "horizon_days": horizon, "rebalance_rank_observations": len(rank_rebalance)}

    rank_mean = statistics.mean(rank_rebalance)
    rank_hac = newey_west_ci(rank_rebalance)
    rank_boot = block_bootstrap_ci(rank_rebalance, SEED + horizon)
    yearly_mean = {y: statistics.mean(v) for y, v in yearly.items() if v}
    positive_years = sum(v > 0 for v in yearly_mean.values())

    rng = random.Random(SEED + horizon)
    placebo_means = []
    for _ in range(PLACEBO_PERM):
        vals = []
        for i, date in enumerate(lock_dates):
            if i % horizon != 0:
                continue
            date_returns = []
            date_scores = []
            for symbol in SYMBOLS:
                row = symbol_models[symbol][5].get(date)
                if row is None:
                    continue
                model, names, train_rate, prior, _sel, _mapping = symbol_models[symbol]
                p = _shift_probability(predict(model, row, names), train_rate, prior)
                date_scores.append(score_variant(variant, p - prior, row.x))
                date_returns.append(row.forward_return)
            if len(date_scores) < 3:
                continue
            rng.shuffle(date_returns)
            vals.append(rank_ic(date_scores, date_returns))
        if vals:
            placebo_means.append(statistics.mean(vals))
    placebo_p95 = None
    placebo_p = None
    if placebo_means:
        placebo_p95 = sorted(placebo_means)[min(len(placebo_means)-1, math.ceil(0.95*len(placebo_means))-1)]
        placebo_p = sum(v >= rank_mean for v in placebo_means) / len(placebo_means)

    strategy = {
        str(cost): {
            "net_return": (math.prod(1.0 + r for r in returns_by_cost[c]) - 1.0) if returns_by_cost[c] else 0.0,
            "trades": len(returns_by_cost[c]),
            "mean_trade_return_ci95": block_bootstrap_ci(returns_by_cost[c], SEED + horizon + c),
            "performance_audit": audit_returns(
                returns_by_cost[c],
                trials=1,
                periods_per_year=252.0 / horizon,
            ),
        }
        for c in COSTS_BPS_PER_LEG
    }
    momentum_net = {
        str(cost): (math.prod(1.0 + r for r in momentum_by_cost[c]) - 1.0) if momentum_by_cost[c] else 0.0
        for c in COSTS_BPS_PER_LEG
    }

    prediction_ok = (
        rank_hac[0] > 0
        and rank_boot[0] > 0
        and len(yearly_mean) >= 3
        and positive_years >= max(2, math.ceil(0.6 * len(yearly_mean)))
        and placebo_p is not None
        and placebo_p < 0.05
    )
    execution_ci = strategy["50"]["mean_trade_return_ci95"]
    execution_net = strategy["50"]["net_return"]
    execution_delta = execution_net - momentum_net["50"]
    execution_ok = (
        execution_net > 0
        and execution_ci[0] is not None
        and execution_ci[0] > 0
        and execution_delta > 0
    )

    prediction_reasons = []
    execution_reasons = []
    if rank_hac[0] <= 0 or rank_boot[0] <= 0:
        prediction_reasons.append("RANK_IC_CI_NOT_POSITIVE")
    if len(yearly_mean) < 3:
        prediction_reasons.append("MIN_YEAR_BUCKETS")
    if positive_years < max(2, math.ceil(0.6 * len(yearly_mean))):
        prediction_reasons.append("YEARLY_STABILITY_FAILED")
    if placebo_p is None or placebo_p >= 0.05:
        prediction_reasons.append("PLACEBO_P_VALUE_GE_0_05")
    if execution_net <= 0:
        execution_reasons.append("NET_RETURN_50BPS_NOT_POSITIVE")
    if execution_ci[0] is None or execution_ci[0] <= 0:
        execution_reasons.append("MEAN_TRADE_RETURN_CI_NOT_POSITIVE")
    if execution_delta <= 0:
        execution_reasons.append("DELTA_VS_MOMENTUM_NOT_POSITIVE")

    return {
        "status": "COMPLETE",
        "method": "frozen-relative-alpha-lockbox-v2",
        "horizon_days": horizon,
        "development_end": dev_dates[-1],
        "lockbox_start": lock_dates[0],
        "lockbox_end": lock_dates[-1],
        "lockbox_days": len(lock_dates),
        "variant": variant,
        "variant_selection": variant_meta,
        "mean_rank_ic": rank_mean,
        "rank_ic_hac_ci95": rank_hac,
        "rank_ic_block_bootstrap_ci95": rank_boot,
        "yearly_rank_ic_mean": yearly_mean,
        "positive_years": positive_years,
        "year_count": len(yearly_mean),
        "placebo_rank_ic_p95": placebo_p95,
        "placebo_rank_ic_p_value": placebo_p,
        "strategy": strategy,
        "momentum_net_return_50bps": momentum_net["50"],
        "delta_vs_momentum_50bps": execution_delta,
        "prediction_status": "VALIDATED" if prediction_ok else "BLOCKED",
        "prediction_reasons": prediction_reasons,
        "execution_status": "VALIDATED" if execution_ok else "BLOCKED",
        "execution_reasons": execution_reasons,
        "validation_status": "VALIDATED" if prediction_ok and execution_ok else "PREDICTOR_VALIDATED_EXECUTION_BLOCKED" if prediction_ok else "BLOCKED",
        "validation_reasons": prediction_reasons + execution_reasons,
        "point_in_time": True,
        "model_frozen_before_lockbox": True,
    }


def _fetch(symbol):
    import httpx
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "10y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-Frozen-Relative-Lockbox-V2/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    result = (payload.get("chart", {}).get("result") or [None])[0]
    ts = result.get("timestamp") or []
    closes = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    return {
        time.strftime("%Y-%m-%d", time.gmtime(t)): float(c)
        for t, c in zip(ts, closes)
        if c is not None and c > 0
    }


def main():
    snapshot = os.getenv("GORILA_DATA_SNAPSHOT")
    series, snapshot_hash = load_or_fetch_series(SYMBOLS, _fetch, snapshot)
    evidence = []
    for horizon in HORIZONS:
        evidence.append(evaluate_symbol_horizon(series, horizon))
    print(json.dumps({
        "schema": "gorila-frozen-relative-lockbox-v2",
        "status": "COMPLETE",
        "snapshot_sha256": snapshot_hash,
        "symbols": SYMBOLS,
        "horizons": HORIZONS,
        "evidence": evidence,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
