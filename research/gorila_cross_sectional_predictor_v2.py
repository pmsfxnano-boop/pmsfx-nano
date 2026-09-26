from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from dataclasses import dataclass

import numpy as np

from quant.research_validation import audit_returns
from research.gorila_data_snapshot import load_or_fetch_series

SYMBOLS = ("GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU")
HORIZON_DAYS = int(os.getenv("GORILA_V2_HORIZON_DAYS", "5"))
TRAIN_MIN_DATES = int(os.getenv("GORILA_V2_TRAIN_MIN_DATES", "400"))
TEST_BLOCK_DATES = int(os.getenv("GORILA_V2_TEST_BLOCK_DATES", "126"))
PURGE_DATES = HORIZON_DAYS
ONE_WAY_COST_BPS = float(os.getenv("GORILA_V2_ONE_WAY_COST_BPS", "10"))
BOOTSTRAPS = int(os.getenv("GORILA_V2_BOOTSTRAPS", "1000"))
PLACEBOS = int(os.getenv("GORILA_V2_PLACEBOS", "24"))
BLOCK_LENGTH = int(os.getenv("GORILA_V2_BLOCK_LENGTH", "20"))
SEED = int(os.getenv("GORILA_V2_SEED", "20260926"))

FEATURE_NAMES = ("r1", "r3", "r5")
L2 = 0.001
LEARNING_RATE = 0.05
ITERATIONS = 100


@dataclass(frozen=True)
class PanelRow:
    date_index: int
    symbol: str
    features: tuple[float, float, float]
    target_residual_log_return: float
    target_up: int


def _fit_logistic(rows: list[PanelRow]):
    x = np.asarray([r.features for r in rows], dtype=np.float64)
    y = np.asarray([r.target_up for r in rows], dtype=np.float64)
    mean = x.mean(axis=0)
    scale = np.maximum(x.std(axis=0), 1e-12)
    z = (x - mean) / scale
    rate = float(np.clip(y.mean(), 1e-6, 1.0 - 1e-6))
    w = np.zeros(z.shape[1], dtype=np.float64)
    b = float(np.log(rate / (1.0 - rate)))
    for _ in range(ITERATIONS):
        p = 1.0 / (1.0 + np.exp(-np.clip(b + z @ w, -30.0, 30.0)))
        err = p - y
        w -= LEARNING_RATE * ((z.T @ err) / len(y) + L2 * w)
        b -= LEARNING_RATE * float(err.mean())
    return mean, scale, w, b


def _predict(model, features):
    mean, scale, w, b = model
    z = (np.asarray(features, dtype=np.float64) - mean) / scale
    return 1.0 / (1.0 + np.exp(-np.clip(b + z @ w, -30.0, 30.0)))


def _build_panel(series: dict[str, dict[str, float]]):
    dates = sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))
    if len(dates) <= TRAIN_MIN_DATES + PURGE_DATES + TEST_BLOCK_DATES:
        raise RuntimeError("INSUFFICIENT_COMMON_DATE_HISTORY")

    panel: dict[int, list[PanelRow]] = {}
    for i in range(65, len(dates) - HORIZON_DAYS):
        date = dates[i]
        future = np.asarray(
            [
                math.log(series[s][dates[i + HORIZON_DAYS]] / series[s][date])
                for s in SYMBOLS
            ],
            dtype=np.float64,
        )
        median_future = float(np.median(future))
        rows = []
        for j, symbol in enumerate(SYMBOLS):
            price = series[symbol][date]
            features = (
                math.log(price / series[symbol][dates[i - 1]]),
                math.log(price / series[symbol][dates[i - 3]]),
                math.log(price / series[symbol][dates[i - 5]]),
            )
            residual = float(future[j] - median_future)
            rows.append(
                PanelRow(
                    date_index=i,
                    symbol=symbol,
                    features=features,
                    target_residual_log_return=residual,
                    target_up=int(residual > 0.0),
                )
            )
        panel[i] = rows
    return dates, panel


def _folds(panel: dict[int, list[PanelRow]]):
    start = TRAIN_MIN_DATES
    last = max(panel)
    while start + TEST_BLOCK_DATES <= last:
        train_dates = [i for i in panel if i < start - PURGE_DATES]
        test_dates = [i for i in range(start, start + TEST_BLOCK_DATES) if i in panel]
        if len(train_dates) >= TRAIN_MIN_DATES and test_dates:
            yield train_dates, test_dates
        start += TEST_BLOCK_DATES


def _rank_ic(scores: np.ndarray, returns: np.ndarray) -> float:
    if len(scores) < 3:
        return 0.0
    srank = np.argsort(np.argsort(scores))
    rrank = np.argsort(np.argsort(returns))
    if np.std(srank) == 0 or np.std(rrank) == 0:
        return 0.0
    return float(np.corrcoef(srank, rrank)[0, 1])


def _portfolio_row(scores: np.ndarray, returns: np.ndarray, cost_bps: float):
    top = int(np.argmax(scores))
    bottom = int(np.argmin(scores))
    gross_log_spread = float(returns[top] - returns[bottom])
    gross_simple = math.expm1(gross_log_spread)
    round_trip_cost = 2.0 * cost_bps / 10000.0
    return gross_simple - round_trip_cost


def _block_ci(values: list[float], block_length: int, iterations: int, seed: int):
    values = np.asarray(values, dtype=np.float64)
    if len(values) == 0:
        return None
    rng = np.random.default_rng(seed)
    means = []
    n = len(values)
    for _ in range(iterations):
        indices = []
        while len(indices) < n:
            start = int(rng.integers(0, n))
            indices.extend((start + np.arange(block_length)) % n)
        means.append(float(values[np.asarray(indices[:n])].mean()))
    return [float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))]


def _paired_block_ci(a: list[float], b: list[float]):
    diff = [x - y for x, y in zip(a, b)]
    return _block_ci(diff, BLOCK_LENGTH, BOOTSTRAPS, SEED + 19)


def _run_once(panel, *, placebo=False):
    all_rank_ic = []
    strategy_returns = []
    momentum_returns = []
    mean_prob = []
    oos_up = []
    fold_count = 0

    for train_dates, test_dates in _folds(panel):
        train_rows = [row for i in train_dates for row in panel[i]]
        if placebo:
            rng = random.Random(SEED + fold_count + HORIZON_DAYS)
            targets = [row.target_up for row in train_rows]
            rng.shuffle(targets)
            train_rows = [
                PanelRow(
                    date_index=row.date_index,
                    symbol=row.symbol,
                    features=row.features,
                    target_residual_log_return=row.target_residual_log_return,
                    target_up=target,
                )
                for row, target in zip(train_rows, targets)
            ]
        model = _fit_logistic(train_rows)
        fold_count += 1

        for date_index in test_dates:
            test = panel[date_index]
            features = [row.features for row in test]
            scores = _predict(model, features)
            future_returns = np.asarray(
                [row.target_residual_log_return for row in test],
                dtype=np.float64,
            )
            all_rank_ic.append(_rank_ic(scores, future_returns))
            mean_prob.append(float(scores.mean()))
            oos_up.extend(row.target_up for row in test)

            strategy_returns.append(
                _portfolio_row(scores, future_returns, ONE_WAY_COST_BPS)
            )
            r5 = np.asarray([row.features[2] for row in test])
            momentum_returns.append(
                _portfolio_row(r5, future_returns, ONE_WAY_COST_BPS)
            )

    return {
        "rank_ic": all_rank_ic,
        "strategy_returns": strategy_returns,
        "momentum_returns": momentum_returns,
        "mean_probability": float(np.mean(mean_prob)),
        "oos_up_rate": float(np.mean(oos_up)),
        "folds": fold_count,
    }


def evaluate(series: dict[str, dict[str, float]]):
    dates, panel = _build_panel(series)
    result = _run_once(panel)

    rank_ci = _block_ci(
        result["rank_ic"], BLOCK_LENGTH, BOOTSTRAPS, SEED + 7
    )
    strategy_ci = _block_ci(
        result["strategy_returns"], BLOCK_LENGTH, BOOTSTRAPS, SEED + 11
    )
    benchmark_ci = _block_ci(
        result["momentum_returns"], BLOCK_LENGTH, BOOTSTRAPS, SEED + 13
    )
    delta_ci = _paired_block_ci(
        result["strategy_returns"], result["momentum_returns"]
    )

    placebo_ics = []
    for k in range(PLACEBOS):
        placebo = _run_once(panel, placebo=True)
        placebo_ics.append(float(np.mean(placebo["rank_ic"])))

    model_mean_ic = float(np.mean(result["rank_ic"]))
    placebo_p95 = float(np.quantile(placebo_ics, 0.95)) if placebo_ics else None
    strategy_mean = float(np.mean(result["strategy_returns"]))
    benchmark_mean = float(np.mean(result["momentum_returns"]))

    performance = audit_returns(
        result["strategy_returns"],
        trials=1,
        periods_per_year=252.0 / HORIZON_DAYS,
    )

    stress = {}
    for lag in (0, 1, 2):
        lag_panel = _shift_targets(panel, lag)
        lag_result = _run_once(lag_panel)
        stress[str(lag)] = {
            "rank_ic_mean": float(np.mean(lag_result["rank_ic"])),
            "rank_ic_ci95": _block_ci(
                lag_result["rank_ic"], BLOCK_LENGTH, BOOTSTRAPS // 2, SEED + 100 + lag
            ),
            "net_return_mean": float(np.mean(lag_result["strategy_returns"])),
            "net_return_ci95": _block_ci(
                lag_result["strategy_returns"], BLOCK_LENGTH, BOOTSTRAPS // 2, SEED + 120 + lag
            ),
        }

    reasons = []
    if len(result["rank_ic"]) < 500:
        reasons.append("MIN_OOS_CROSS_SECTIONAL_OBSERVATIONS")
    if not rank_ci or rank_ci[0] <= 0:
        reasons.append("RANK_IC_CI_NOT_ABOVE_ZERO")
    if placebo_p95 is None or model_mean_ic <= placebo_p95:
        reasons.append("PLACEBO_NOT_BEATEN")
    if not delta_ci or delta_ci[0] <= 0:
        reasons.append("DELTA_VS_MOMENTUM_CI_NOT_ABOVE_ZERO")
    if not strategy_ci or strategy_ci[0] <= 0:
        reasons.append("NET_RETURN_CI_NOT_ABOVE_ZERO")
    for lag, item in stress.items():
        if not item["rank_ic_ci95"] or item["rank_ic_ci95"][0] <= 0:
            reasons.append(f"STRESS_LAG_{lag}_RANK_IC_FAILED")

    status = "VALIDATED_RESEARCH" if not reasons else "BLOCKED"

    return {
        "schema": "gorila-cross-sectional-evidence-v2",
        "status": "COMPLETE",
        "validation_status": status,
        "validation_reasons": reasons,
        "predictor": {
            "model": "fixed-pooled-logit-v1",
            "target": "cross-sectional residual future return sign",
            "features": FEATURE_NAMES,
            "l2": L2,
            "iterations": ITERATIONS,
            "selection_trials": 1,
            "horizon_days": HORIZON_DAYS,
        },
        "dataset": {
            "symbols": list(SYMBOLS),
            "common_dates": len(dates),
            "dataset_sha256": os.getenv("GORILA_SNAPSHOT_SHA256"),
        },
        "oos": {
            "folds": result["folds"],
            "observations": len(result["rank_ic"]),
            "mean_rank_ic": model_mean_ic,
            "median_rank_ic": float(np.median(result["rank_ic"])),
            "positive_rank_ic_fraction": float(np.mean(np.asarray(result["rank_ic"]) > 0)),
            "rank_ic_ci95": rank_ci,
            "mean_probability": result["mean_probability"],
            "oos_residual_up_rate": result["oos_up_rate"],
        },
        "economics": {
            "one_way_cost_bps": ONE_WAY_COST_BPS,
            "round_trip_cost_bps": 2.0 * ONE_WAY_COST_BPS,
            "mean_net_return_per_rebalance": strategy_mean,
            "net_return_ci95": strategy_ci,
            "benchmark_momentum_mean_net_return": benchmark_mean,
            "benchmark_momentum_ci95": benchmark_ci,
            "delta_vs_momentum_mean": strategy_mean - benchmark_mean,
            "delta_vs_momentum_ci95": delta_ci,
            "performance_audit": performance,
        },
        "placebo": {
            "permutations": PLACEBOS,
            "rank_ic_mean_distribution_p95": placebo_p95,
            "rank_ic_mean_distribution": placebo_ics,
        },
        "stress": stress,
        "point_in_time": True,
        "frozen_spec": True,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _shift_targets(panel, lag):
    if lag == 0:
        return panel
    keys = sorted(panel)
    shifted = {}
    for pos, key in enumerate(keys):
        source = keys[max(0, pos - lag)]
        shifted[key] = []
        for row, source_row in zip(panel[key], panel[source]):
            shifted[key].append(
                PanelRow(
                    date_index=row.date_index,
                    symbol=row.symbol,
                    features=row.features,
                    target_residual_log_return=source_row.target_residual_log_return,
                    target_up=int(source_row.target_residual_log_return > 0),
                )
            )
    return shifted


def _fetch(symbol):
    import httpx
    response = httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range": "10y", "interval": "1d", "events": "history"},
        timeout=30,
        headers={"User-Agent": "Gorila-CrossSectional-V2/1.0"},
    )
    response.raise_for_status()
    result = (response.json().get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: empty")
    timestamps = result.get("timestamp") or []
    closes = ((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {
        time.strftime("%Y-%m-%d", time.gmtime(ts)): float(close)
        for ts, close in zip(timestamps, closes)
        if close is not None and close > 0
    }


def main():
    series, snapshot_hash = load_or_fetch_series(
        list(SYMBOLS), _fetch, os.getenv("GORILA_DATA_SNAPSHOT")
    )
    os.environ["GORILA_SNAPSHOT_SHA256"] = snapshot_hash
    result = evaluate(series)
    result["dataset"]["dataset_sha256"] = snapshot_hash
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
