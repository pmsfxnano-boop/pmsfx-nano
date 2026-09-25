from __future__ import annotations

"""Gorila Argentum — search-aware nested WFO audit.

This is deliberately separate from the production/backtest path. It computes:
1) candidate-by-fold inner-validation scores;
2) candidate-by-fold outer OOS scores;
3) nested-WF probability of backtest overfitting (PBO);
4) DSR audit on the OOS returns of the candidate selected within each outer fold.

It is NOT CPCV: the folds are the project's purged expanding WFO folds.
"""

import json
import math
import statistics
import time
from itertools import combinations

from gorila_nested_pairwise_rank_oos import (
    COSTS_BPS,
    FEATURE_GROUPS,
    INNER_MIN_TRAIN,
    INNER_TEST_SIZE,
    L2_VALUES,
    OUTER_TEST_SIZE,
    OUTER_TRAIN_MIN,
    build_dates,
    fit,
    make_dataset,
    pair_prob,
    pair_rows,
    rank_score,
    yahoo,
)
from quant.research_validation import audit_returns, pbo_from_train_test

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [5, 10]
SEARCH_TRIALS = len(FEATURE_GROUPS) * len(L2_VALUES)
DEFAULT_COST_BPS = 25


def candidate_specs():
    return [
        (group, l2)
        for group in FEATURE_GROUPS
        for l2 in L2_VALUES
    ]


def split_inner(train_dates, horizon):
    inner_train_end = max(
        INNER_MIN_TRAIN,
        len(train_dates) - 2 * INNER_TEST_SIZE,
    )
    inner_train = train_dates[:inner_train_end - horizon]
    inner_test = train_dates[inner_train_end:inner_train_end + INNER_TEST_SIZE]
    if len(inner_train) < INNER_MIN_TRAIN or len(inner_test) < 20:
        inner_train = train_dates[:max(INNER_MIN_TRAIN, len(train_dates) - INNER_TEST_SIZE)]
        inner_test = train_dates[-INNER_TEST_SIZE:]
    return inner_train, inner_test


def brier_for_candidate(data, train_dates, test_dates, group, l2):
    idxs = FEATURE_GROUPS[group]
    x_train, y_train = pair_rows(data, train_dates, idxs)
    if len(x_train) < 500 or len(set(y_train)) < 2 or not test_dates:
        return None
    model = fit(x_train, y_train, l2=l2)
    probs, labels = [], []
    for d in test_dates:
        for a, b in combinations(SYMBOLS, 2):
            xa, xb = data[d]["x"][a], data[d]["x"][b]
            probs.append(pair_prob(model, [xa[i] - xb[i] for i in idxs]))
            labels.append(1 if data[d]["fwd"][a] > data[d]["fwd"][b] else 0)
    return sum((p - y) ** 2 for p, y in zip(probs, labels)) / len(labels)


def strategy_returns(data, test_dates, model, group, cost_bps):
    idxs = FEATURE_GROUPS[group]
    returns = []
    for d in test_dates:
        scores = {
            s: rank_score(model, [data[d]["x"][s][i] for i in idxs])
            for s in SYMBOLS
        }
        ranked = sorted(SYMBOLS, key=lambda s: scores[s])
        lo, sh = ranked[-1], ranked[0]
        returns.append(
            0.5 * (data[d]["fwd"][lo] - data[d]["fwd"][sh])
            - cost_bps / 10000.0
        )
    return returns


def run_horizon(series, horizon):
    usable, data = make_dataset(series, horizon)
    specs = candidate_specs()
    inner_matrix = {spec: [] for spec in specs}
    outer_matrix = {spec: [] for spec in specs}
    selected = []
    selected_returns = []
    fold_records = []

    start = OUTER_TRAIN_MIN
    fold_id = 0
    while start + OUTER_TEST_SIZE <= len(usable):
        train_dates = usable[:start]
        test_dates = usable[start:start + OUTER_TEST_SIZE]
        inner_train, inner_test = split_inner(train_dates, horizon)

        inner_scores = {}
        outer_scores = {}

        for group, l2 in specs:
            inner_brier = brier_for_candidate(
                data, inner_train, inner_test, group, l2
            )
            outer_brier = brier_for_candidate(
                data,
                train_dates[:-horizon],
                test_dates,
                group,
                l2,
            )
            if inner_brier is not None:
                inner_scores[(group, l2)] = inner_brier
            if outer_brier is not None:
                outer_scores[(group, l2)] = outer_brier

        valid = [
            spec for spec in specs
            if spec in inner_scores and spec in outer_scores
        ]
        if not valid:
            start += OUTER_TEST_SIZE
            continue

        winner = min(
            valid,
            key=lambda spec: (inner_scores[spec], spec[0], spec[1]),
        )
        selected.append(winner)

        for spec in specs:
            inner_matrix[spec].append(
                -inner_scores[spec] if spec in inner_scores else float("nan")
            )
            outer_matrix[spec].append(
                -outer_scores[spec] if spec in outer_scores else float("nan")
            )

        idxs = FEATURE_GROUPS[winner[0]]
        x_train, y_train = pair_rows(
            data, train_dates[:-horizon], idxs
        )
        model = fit(x_train, y_train, l2=winner[1])
        rets = strategy_returns(
            data, test_dates, model, winner[0], DEFAULT_COST_BPS
        )
        selected_returns.extend(rets)

        fold_records.append({
            "fold": fold_id,
            "train_end": train_dates[-1],
            "test_start": test_dates[0],
            "test_end": test_dates[-1],
            "candidate_count": len(valid),
            "selected_group": winner[0],
            "selected_l2": winner[1],
            "inner_brier_selected": inner_scores[winner],
            "outer_brier_selected": outer_scores[winner],
            "inner_scores": {
                f"{g}|l2={l2}": inner_scores.get((g, l2))
                for g, l2 in specs
            },
            "outer_scores": {
                f"{g}|l2={l2}": outer_scores.get((g, l2))
                for g, l2 in specs
            },
            "selected_trade_return_count": len(rets),
        })
        fold_id += 1
        start += OUTER_TEST_SIZE

    if not fold_records:
        return {
            "status": "INSUFFICIENT_DATA",
            "validated": False,
            "horizon": horizon,
        }

    # Remove candidates with any missing fold so PBO uses a rectangular matrix.
    complete_specs = [
        spec for spec in specs
        if all(math.isfinite(x) for x in inner_matrix[spec])
        and all(math.isfinite(x) for x in outer_matrix[spec])
    ]
    train_matrix = [inner_matrix[spec] for spec in complete_specs]
    test_matrix = [outer_matrix[spec] for spec in complete_specs]

    pbo = pbo_from_train_test(
        train_matrix,
        test_matrix,
    ) if len(complete_specs) >= 2 else {
        "status": "INSUFFICIENT_CANDIDATES",
        "candidate_count": len(complete_specs),
        "split_count": len(fold_records),
    }

    dsr = audit_returns(
        selected_returns,
        trials=SEARCH_TRIALS,
        periods_per_year=252.0 / max(1, horizon),
    )

    return {
        "status": "COMPLETE",
        "method": "nested-wfo-search-aware-pbo-dsr-v1",
        "validation_type": "purged-expanding-wfo",
        "horizon": horizon,
        "symbols": SYMBOLS,
        "outer_folds": len(fold_records),
        "candidate_count_total": SEARCH_TRIALS,
        "candidate_count_complete": len(complete_specs),
        "candidate_specs": [f"{g}|l2={l2}" for g, l2 in complete_specs],
        "selected_candidates": [
            f"{g}|l2={l2}" for g, l2 in selected
        ],
        "pbo": pbo,
        "dsr": dsr,
        "selected_return_count": len(selected_returns),
        "selected_cost_bps": DEFAULT_COST_BPS,
        "folds": fold_records,
        "limitations": [
            "PBO here is nested-WFO/CSCV-style over the actual candidate-by-fold inner/outer matrices; it is not CPCV.",
            "DSR uses the stitched OOS trade-return series of the candidate selected independently inside each outer fold.",
            "Production eligibility is not modified by this audit.",
        ],
    }


def main():
    series = {symbol: yahoo(symbol) for symbol in SYMBOLS}
    results = {
        str(horizon): run_horizon(series, horizon)
        for horizon in HORIZONS
    }
    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "method": "nested-wfo-search-aware-pbo-dsr-v1",
                "symbols": SYMBOLS,
                "horizons": HORIZONS,
                "search_trials": SEARCH_TRIALS,
                "generated_at": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                ),
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
