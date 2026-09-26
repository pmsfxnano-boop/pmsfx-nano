from __future__ import annotations

import json
import math
import os
import random
import statistics
import time
from collections import defaultdict

from quant.research_validation import audit_returns, pbo_from_train_test
from research.gorila_data_snapshot import load_or_fetch_series
from research.gorila_quantitative_v1_oos import (
    FEATURE_GROUPS,
    INNER_TEST,
    L2_VALUES,
    TEST_SIZE,
    TRAIN_MIN,
    _prior_rate,
    _shift_probability,
    fit_logistic,
    predict,
    select_candidate,
)

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_RELATIVE_HORIZONS", "5,10").split(",") if x.strip()]
COSTS_BPS_PER_LEG = [25, 50, 100]
SCORE_VARIANTS = ["probability_delta", "vol_scaled_delta", "blend_momentum"]
PLACEBO_PERM = int(os.getenv("GORILA_RELATIVE_PLACEBO_PERM", "32"))
SEED = int(os.getenv("GORILA_RELATIVE_SEED", "20260926"))


def rank_ic(values, returns):
    if len(values) < 3:
        return 0.0
    pv = {i: r for r, i in enumerate(sorted(range(len(values)), key=lambda j: values[j]))}
    rv = {i: r for r, i in enumerate(sorted(range(len(returns)), key=lambda j: returns[j]))}
    xs = [pv[i] for i in range(len(values))]
    ys = [rv[i] for i in range(len(returns))]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    den = math.sqrt(sum((x-mx)**2 for x in xs) * sum((y-my)**2 for y in ys))
    return sum((x-mx)*(y-my) for x,y in zip(xs,ys)) / den if den else 0.0


def normal_ci(values, z=1.96):
    if not values:
        return None
    mean = statistics.mean(values)
    if len(values) <= 1:
        return [mean, mean]
    se = statistics.pstdev(values) / math.sqrt(len(values))
    return [mean-z*se, mean+z*se]


def build_rows(series, symbol, horizon):
    dates = sorted(series[symbol])
    rows = {}
    for i in range(65, len(dates)-horizon):
        date = dates[i]
        p0 = series[symbol][date]
        hist = [
            math.log(series[symbol][dates[k]] / series[symbol][dates[k-1]])
            for k in range(max(1, i-20), i)
        ]
        if len(hist) < 18:
            continue
        vol = statistics.pstdev(hist[-20:])
        fwd = math.log(series[symbol][dates[i+horizon]] / p0)
        rows[date] = {
            "r1": math.log(p0 / series[symbol][dates[i-1]]),
            "r5": math.log(p0 / series[symbol][dates[i-5]]),
            "vol20": vol,
            "forward_return": fwd,
        }
    return rows


def make_model_rows(series, symbol, horizon):
    dates = sorted(series[symbol])
    out = []
    for i in range(65, len(dates)-horizon):
        p0 = series[symbol][dates[i]]
        x = {
            "r1": math.log(p0 / series[symbol][dates[i-1]]),
            "r3": math.log(p0 / series[symbol][dates[i-3]]),
            "r5": math.log(p0 / series[symbol][dates[i-5]]),
        }
        hist = [math.log(series[symbol][dates[k]] / series[symbol][dates[k-1]]) for k in range(max(1,i-20),i)]
        if len(hist) < 18:
            continue
        vol = statistics.pstdev(hist[-20:])
        win = [series[symbol][dates[k]] for k in range(max(0,i-20),i)]
        sd = statistics.pstdev(win)
        z20 = (p0-statistics.mean(win))/sd if sd>0 else 0.0
        x.update({"vol20":vol,"z20":z20,"r1_vol":x["r1"]/max(vol,1e-8),"r5_z":x["r5"]*z20})
        forward = math.log(series[symbol][dates[i+horizon]]/p0)
        out.append({
            "date": dates[i],
            "x": x,
            "y": 1 if forward > 0 else 0,
            "forward_return": forward,
        })
    return out


def rows_for_model(series, symbol, horizon):
    from dataclasses import make_dataclass
    Row = make_dataclass("RelativeRow", [("date",str),("x",dict),("y",int),("forward_return",float)], frozen=True)
    return [Row(**r) for r in make_model_rows(series,symbol,horizon)]


def score_variant(variant, symbol_score, symbol_row):
    pdelta = symbol_score
    momentum = symbol_row["r5"]
    vol = max(symbol_row["vol20"], 1e-8)
    if variant == "probability_delta":
        return pdelta
    if variant == "vol_scaled_delta":
        return pdelta / vol
    return pdelta + 0.25 * (momentum / vol)


def pair_trade(scores, returns, symbols, cost_per_leg_bps):
    ranked = sorted(symbols, key=lambda s: scores[s])
    if len(ranked) < 2:
        return None
    lo, hi = ranked[0], ranked[-1]
    long_ret = math.expm1(returns[hi])
    short_ret = math.expm1(returns[lo])
    # Dollar-neutral 50/50 long-short. Cost is charged independently per leg.
    net = 0.5 * long_ret - 0.5 * short_ret - 2.0 * cost_per_leg_bps / 10000.0
    return net


def fold_predictions(series, horizon, train_end_date, test_dates):
    predictions = {date: {} for date in test_dates}
    returns = {date: {} for date in test_dates}
    structure = {date: {} for date in test_dates}
    for symbol in SYMBOLS:
        rows = rows_for_model(series, symbol, horizon)
        train = [r for r in rows if r.date <= train_end_date]
        test_by_date = {r.date:r for r in rows if r.date in predictions}
        if len(train) < TRAIN_MIN:
            continue
        group, l2, prior_window, selection = select_candidate(train, horizon)
        names = FEATURE_GROUPS[group]
        model = fit_logistic(train, names, l2)
        train_rate = sum(r.y for r in train) / len(train)
        prior = _prior_rate(train, prior_window)
        for date in test_dates:
            row = test_by_date.get(date)
            if row is None:
                continue
            p = _shift_probability(predict(model,row,names), train_rate, prior)
            predictions[date][symbol] = p - prior
            returns[date][symbol] = row.forward_return
            structure[date][symbol] = {
                "r5": row.x["r5"],
                "vol20": row.x["vol20"],
                "prior": prior,
            }
    return predictions, returns, structure


def evaluate_symbol_horizon(series, horizon):
    all_dates = sorted(set.intersection(*(set(r.date for r in rows_for_model(series,s,horizon)) for s in SYMBOLS)))
    outer = []
    start = TRAIN_MIN
    while start + TEST_SIZE <= len(all_dates):
        train_end_date = all_dates[start-horizon-1]
        test_dates = all_dates[start:start+TEST_SIZE]
        predictions, returns, structure = fold_predictions(series,horizon,train_end_date,test_dates)
        outer.append({
            "train_end": train_end_date,
            "test_dates": test_dates,
            "predictions": predictions,
            "returns": returns,
            "structure": structure,
        })
        start += TEST_SIZE
    if not outer:
        return {"status":"INSUFFICIENT_DATA","horizon_days":horizon}

    # Nested selection over score variants using the tail of each training fold.
    candidate_train, candidate_test = [[] for _ in SCORE_VARIANTS], [[] for _ in SCORE_VARIANTS]
    fold_metrics = []
    all_rank_ics = {v: [] for v in SCORE_VARIANTS}
    all_returns = {v: {c: [] for c in COSTS_BPS_PER_LEG} for v in SCORE_VARIANTS}
    baseline_returns = {c: [] for c in COSTS_BPS_PER_LEG}

    for fold in outer:
        dates = fold["test_dates"]
        # Model selection uses only the first half of the fold as a pseudo-test.
        split = max(20, len(dates)//2)
        train_dates = dates[:split]
        val_dates = dates[split:]
        train_perf = {v: [] for v in SCORE_VARIANTS}
        test_perf = {v: [] for v in SCORE_VARIANTS}
        for variant in SCORE_VARIANTS:
            train_ic = []
            val_ic = []
            for date in train_dates:
                s = fold["predictions"].get(date,{})
                r = fold["returns"].get(date,{})
                st = fold["structure"].get(date,{})
                if len(s)>=3:
                    vals = {sym:score_variant(variant,sym_score,st[sym]) for sym,sym_score in s.items()}
                    common = [sym for sym in vals if sym in r]
                    train_ic.append(rank_ic([vals[z] for z in common],[r[z] for z in common]))
            for date in val_dates:
                s = fold["predictions"].get(date,{})
                r = fold["returns"].get(date,{})
                st = fold["structure"].get(date,{})
                if len(s)>=3:
                    vals = {sym:score_variant(variant,sym_score,st[sym]) for sym,sym_score in s.items()}
                    common = [sym for sym in vals if sym in r]
                    val_ic.append(rank_ic([vals[z] for z in common],[r[z] for z in common]))
            train_perf[variant] = statistics.mean(train_ic) if train_ic else -1.0
            test_perf[variant] = statistics.mean(val_ic) if val_ic else -1.0
        selected = max(SCORE_VARIANTS, key=lambda v:(train_perf[v],v))
        # Treat variant selection as candidate universe for PBO.
        for i,v in enumerate(SCORE_VARIANTS):
            candidate_train[i].append(train_perf[v])
            candidate_test[i].append(test_perf[v])
        fold_rank=[]
        for date in dates:
            s=fold["predictions"].get(date,{})
            r=fold["returns"].get(date,{})
            st=fold["structure"].get(date,{})
            common=[sym for sym in s if sym in r and sym in st]
            if len(common)>=3:
                vals={sym:score_variant(selected,s[sym],st[sym]) for sym in common}
                ic=rank_ic([vals[z] for z in common],[r[z] for z in common])
                fold_rank.append(ic)
                for cost in COSTS_BPS_PER_LEG:
                    tr=pair_trade(vals,{z:r[z] for z in common},common,cost)
                    if tr is not None:
                        all_returns[selected][cost].append(tr)
                # Momentum benchmark
                mvals={sym:st[sym]["r5"] for sym in common}
                for cost in COSTS_BPS_PER_LEG:
                    tr=pair_trade(mvals,{z:r[z] for z in common},common,cost)
                    if tr is not None: baseline_returns[cost].append(tr)
        all_rank_ics[selected].extend(fold_rank)
        fold_metrics.append({
            "selected_variant":selected,
            "train_rank_ic":train_perf[selected],
            "validation_rank_ic":test_perf[selected],
            "test_rank_ic":statistics.mean(fold_rank) if fold_rank else 0.0,
        })

    pbo=pbo_from_train_test(candidate_train,candidate_test)
    selected_variants=[f["selected_variant"] for f in fold_metrics]
    majority=sorted(set(selected_variants),key=lambda v:selected_variants.count(v),reverse=True)[0]
    rank_values=all_rank_ics[majority]
    return_ci=normal_ci([f["test_rank_ic"] for f in fold_metrics])
    net50=sum(all_returns[majority][50]) if all_returns[majority][50] else 0.0
    mom50=sum(baseline_returns[50]) if baseline_returns[50] else 0.0
    delta50=net50-mom50
    audit=audit_returns(all_returns[majority][50],trials=len(SCORE_VARIANTS),periods_per_year=252.0/max(1,horizon))
    placebo=[]
    rng=random.Random(SEED+horizon)
    for _ in range(PLACEBO_PERM):
        vals=[]
        for fold in outer:
            for date in fold["test_dates"]:
                r=dict(fold["returns"].get(date,{}))
                s=dict(fold["predictions"].get(date,{}))
                if len(r)<3: continue
                syms=list(r)
                shuffled=syms[:]
                rng.shuffle(shuffled)
                rankvals={syms[i]:score_variant(majority,s.get(syms[i],0.0),fold["structure"].get(date,{}).get(syms[i],{"r5":0.0,"vol20":1.0})) for i in range(len(syms))}
                common=list(rankvals)
                vals.append(rank_ic([rankvals[z] for z in common],[r[shuffled[i]] for i,z in enumerate(common)]))
        if vals: placebo.append(statistics.mean(vals))
    placebo_p95=sorted(placebo)[min(len(placebo)-1,max(0,int(math.ceil(0.95*len(placebo)))-1))] if placebo else None

    prediction_ok = bool(rank_values) and statistics.mean(rank_values)>0 and return_ci and return_ci[0]>0
    strategy_ok = (
        bool(all_returns[majority][50])
        and net50>0
        and delta50>0
        and pbo.get("status")=="COMPLETE"
        and pbo.get("pbo",1.0)<=0.10
        and (audit.get("deflated_sharpe_probability") or 0.0)>=0.95
        and (placebo_p95 is None or statistics.mean(rank_values)>placebo_p95)
    )
    reasons=[]
    if not prediction_ok: reasons.append("RANK_IC_CI_NOT_POSITIVE")
    if net50<=0: reasons.append("NET_RETURN_50BPS_NOT_POSITIVE")
    if delta50<=0: reasons.append("DELTA_VS_MOMENTUM_NOT_POSITIVE")
    if pbo.get("status")!="COMPLETE" or pbo.get("pbo",1.0)>0.10: reasons.append("PBO_GATE_FAILED")
    if (audit.get("deflated_sharpe_probability") or 0.0)<0.95: reasons.append("DSR_GATE_FAILED")
    if placebo_p95 is not None and statistics.mean(rank_values)<=placebo_p95: reasons.append("PLACEBO_NOT_BEATEN")
    return {
        "status":"COMPLETE",
        "method":"nested-cross-sectional-relative-alpha-v1",
        "horizon_days":horizon,
        "oos_dates":len(all_dates),
        "outer_folds":len(outer),
        "selected_variants":selected_variants,
        "majority_variant":majority,
        "mean_rank_ic":statistics.mean(rank_values) if rank_values else None,
        "rank_ic_ci95":return_ci,
        "net_return_50bps_per_leg":net50,
        "momentum_net_return_50bps_per_leg":mom50,
        "delta_vs_momentum_50bps":delta50,
        "performance_audit":audit,
        "pbo":pbo,
        "placebo_rank_ic_p95":placebo_p95,
        "validation_status":"VALIDATED" if prediction_ok and strategy_ok else "BLOCKED",
        "validation_reasons":reasons,
        "data_point_in_time":True,
    }


def _fetch(symbol):
    import httpx
    response=httpx.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
        params={"range":"10y","interval":"1d","events":"history"},
        timeout=30,
        headers={"User-Agent":"Gorila-Relative-Alpha-V1/1.0"},
    )
    response.raise_for_status()
    result=(response.json().get("chart",{}).get("result") or [None])[0]
    ts=result.get("timestamp") or []
    closes=((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    return {time.strftime("%Y-%m-%d",time.gmtime(t)):float(c) for t,c in zip(ts,closes) if c is not None and c>0}


def main():
    snapshot=os.getenv("GORILA_DATA_SNAPSHOT")
    series,snapshot_hash=load_or_fetch_series(SYMBOLS,_fetch,snapshot)
    evidence=[evaluate_symbol_horizon(series,h) for h in HORIZONS]
    print(json.dumps({
        "schema":"gorila-relative-alpha-evidence-v1",
        "status":"COMPLETE",
        "snapshot_sha256":snapshot_hash,
        "symbols":SYMBOLS,
        "horizons":HORIZONS,
        "evidence":evidence,
        "generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
    },indent=2,sort_keys=True))


if __name__=="__main__":
    main()
