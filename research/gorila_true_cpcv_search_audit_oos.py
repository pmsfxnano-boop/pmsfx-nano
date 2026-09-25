from __future__ import annotations

import hashlib
import json
import os
import statistics
import time

from quant.research_validation import audit_returns, combinatorial_splits, pbo_from_train_test
from research.gorila_nested_pairwise_rank_oos import (
    FEATURE_GROUPS,
    L2_VALUES,
    SYMBOLS,
    fit,
    pair_prob,
    pair_rows,
    rank_score,
    yahoo,
    make_dataset,
)

HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
CPCV_GROUPS = int(os.getenv("GORILA_CPCV_GROUPS", "6"))
CPCV_TEST_GROUPS = int(os.getenv("GORILA_CPCV_TEST_GROUPS", "2"))
PURGE = int(os.getenv("GORILA_CPCV_PURGE", "5"))
EMBARGO = int(os.getenv("GORILA_CPCV_EMBARGO", "5"))
MIN_TRAIN = int(os.getenv("GORILA_CPCV_MIN_TRAIN", "252"))
FIT_EPOCHS = int(os.getenv("GORILA_CPCV_FIT_EPOCHS", "80"))
COST_BPS = int(os.getenv("GORILA_CPCV_COST_BPS", "25"))


def contiguous_groups(n: int, groups: int) -> list[list[int]]:
    if groups <= 1 or groups > n:
        raise ValueError("invalid CPCV group count")
    base, rem = divmod(n, groups)
    out = []
    start = 0
    for g in range(groups):
        size = base + (1 if g < rem else 0)
        out.append(list(range(start, start + size)))
        start += size
    return out


def purge_train_indices(n: int, test_indices: list[int], purge: int, embargo: int) -> list[int]:
    excluded = set()
    radius = max(0, purge) + max(0, embargo)
    for idx in test_indices:
        lo = max(0, idx - radius)
        hi = min(n - 1, idx + radius)
        excluded.update(range(lo, hi + 1))
    return [i for i in range(n) if i not in excluded]


def brier_from_model(data, dates, model, idxs):
    losses = []
    for d in dates:
        fwd = data[d]["fwd"]
        for a in SYMBOLS:
            for b in SYMBOLS:
                if a >= b:
                    continue
                xa, xb = data[d]["x"][a], data[d]["x"][b]
                p = pair_prob(model, [xa[i] - xb[i] for i in idxs])
                y = 1 if fwd[a] > fwd[b] else 0
                losses.append((p - y) ** 2)
    return statistics.mean(losses) if losses else float("inf")


def execute_selected(data, test_dates, model, idxs, horizon, cost_bps):
    position = {d: i for i, d in enumerate(test_dates)}
    last = -10**9
    returns = []
    for d in test_dates:
        if position[d] - last < horizon:
            continue
        ranked = sorted(
            SYMBOLS,
            key=lambda s: rank_score(model, [data[d]["x"][s][i] for i in idxs]),
        )
        lo, sh = ranked[-1], ranked[0]
        returns.append(
            0.5 * (data[d]["fwd"][lo] - data[d]["fwd"][sh]) - cost_bps / 10000
        )
        last = position[d]
    return returns


def data_hash(series) -> str:
    payload = json.dumps(series, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def run_horizon(series, horizon):
    dates, data = make_dataset(series, horizon)
    groups = contiguous_groups(len(dates), CPCV_GROUPS)
    split_specs = combinatorial_splits(CPCV_GROUPS, CPCV_TEST_GROUPS)

    train_matrix = [[] for _ in range(len(FEATURE_GROUPS) * len(L2_VALUES))]
    test_matrix = [[] for _ in range(len(FEATURE_GROUPS) * len(L2_VALUES))]
    candidate_meta = [
        (group_name, l2)
        for group_name in FEATURE_GROUPS
        for l2 in L2_VALUES
    ]
    split_records = []

    for train_groups, test_groups in split_specs:
        test_indices = sorted(i for g in test_groups for i in groups[g])
        train_indices = purge_train_indices(
            len(dates), test_indices, PURGE, EMBARGO
        )
        train_dates = [dates[i] for i in train_indices]
        test_dates = [dates[i] for i in test_indices]
        if len(train_dates) < MIN_TRAIN or len(test_dates) < horizon + 2:
            continue

        split_train = []
        split_test = []
        for group_name, l2 in candidate_meta:
            idxs = FEATURE_GROUPS[group_name]
            X, y = pair_rows(data, train_dates, idxs)
            if len(X) < 500 or len(set(y)) < 2:
                split_train.append(float("-inf"))
                split_test.append(float("-inf"))
                continue
            model = fit(X, y, l2=l2, epochs=FIT_EPOCHS)
            train_brier = brier_from_model(data, train_dates, model, idxs)
            test_brier = brier_from_model(data, test_dates, model, idxs)
            split_train.append(-train_brier)
            split_test.append(-test_brier)

        if not all(x > float("-inf") for x in split_train + split_test):
            continue

        for i, value in enumerate(split_train):
            train_matrix[i].append(value)
        for i, value in enumerate(split_test):
            test_matrix[i].append(value)

        winner = max(range(len(candidate_meta)), key=lambda i: split_train[i])
        winner_group, winner_l2 = candidate_meta[winner]
        winner_model = fit(
            *pair_rows(data, train_dates, FEATURE_GROUPS[winner_group]),
            l2=winner_l2,
            epochs=FIT_EPOCHS,
        )
        winner_returns = execute_selected(
            data,
            test_dates,
            winner_model,
            FEATURE_GROUPS[winner_group],
            horizon,
            COST_BPS,
        )
        dsr = audit_returns(
            winner_returns,
            trials=len(candidate_meta),
            periods_per_year=252.0 / horizon,
        )
        split_records.append(
            {
                "train_groups": list(train_groups),
                "test_groups": list(test_groups),
                "train_size": len(train_dates),
                "test_size": len(test_dates),
                "selected_group": winner_group,
                "selected_l2": winner_l2,
                "selected_train_brier": -split_train[winner],
                "selected_test_brier": -split_test[winner],
                "selected_returns": winner_returns,
                "selected_dsr": dsr,
            }
        )

    pbo = pbo_from_train_test(train_matrix, test_matrix)
    dsr_values = [
        r["selected_dsr"]["deflated_sharpe_probability"]
        for r in split_records
        if r["selected_dsr"].get("status") == "COMPLETE"
        and r["selected_dsr"].get("deflated_sharpe_probability") is not None
    ]
    compounds = [
        r["selected_dsr"]["compound_return"]
        for r in split_records
        if r["selected_dsr"].get("status") == "COMPLETE"
    ]
    return {
        "outer_splits": len(split_records),
        "candidate_count": len(candidate_meta),
        "cpcv_group_count": CPCV_GROUPS,
        "cpcv_test_group_count": CPCV_TEST_GROUPS,
        "purge_days": PURGE,
        "embargo_days": EMBARGO,
        "pbo": pbo,
        "selected_dsr_summary": {
            "splits_with_dsr": len(dsr_values),
            "mean_deflated_sharpe_probability": statistics.mean(dsr_values) if dsr_values else None,
            "median_deflated_sharpe_probability": statistics.median(dsr_values) if dsr_values else None,
            "positive_dsr_95pct_splits": sum(v >= 0.95 for v in dsr_values),
            "mean_compound_return": statistics.mean(compounds) if compounds else None,
            "positive_compound_splits": sum(v > 0 for v in compounds),
        },
        "selected_candidates": [
            {
                "test_groups": r["test_groups"],
                "selected_group": r["selected_group"],
                "selected_l2": r["selected_l2"],
                "selected_train_brier": r["selected_train_brier"],
                "selected_test_brier": r["selected_test_brier"],
                "compound_return": r["selected_dsr"].get("compound_return"),
                "sharpe": r["selected_dsr"].get("sharpe"),
                "dsr_probability": r["selected_dsr"].get("deflated_sharpe_probability"),
            }
            for r in split_records
        ],
        "candidate_meta": [
            {"group": g, "l2": l2}
            for g, l2 in candidate_meta
        ],
    }


def main():
    series = {s: yahoo(s) for s in SYMBOLS}
    snapshot_hash = data_hash(series)
    results = {str(h): run_horizon(series, h) for h in HORIZONS}
    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "method": "true-combinatorial-purged-cv-search-audit-v1",
                "symbols": SYMBOLS,
                "horizons": HORIZONS,
                "data_snapshot_sha256": snapshot_hash,
                "candidate_search_space": len(FEATURE_GROUPS) * len(L2_VALUES),
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
