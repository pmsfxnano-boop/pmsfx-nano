from __future__ import annotations

import json
import os
import math
import statistics
import time
from itertools import combinations

import httpx

from research.gorila_data_snapshot import load_or_fetch_series

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
EXECUTION_LAGS = [0, 1, 2]
COSTS_BPS = [25, 50, 100, 150]
TRAIN_MIN = 504
TEST_SIZE = 126
INNER_TEST = 63
INNER_MIN = 252
FEATURES = {
    "momentum": [0, 1, 2],
    "base_vol_z": [0, 1, 2, 3, 4],
    "relative": [5, 6, 7, 8, 9, 10],
    "full": list(range(11)),
}
L2S = [0.001, 0.002, 0.01]


def get_json(url, params=None):
    r = httpx.get(url, params=params, timeout=30,
                  headers={"User-Agent": "Gorila-Argentum-NestedRank-Stress/0.1"})
    r.raise_for_status()
    return r.json()


def yahoo(symbol):
    j = get_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
                 {"range": "5y", "interval": "1d", "events": "history"})
    res = (j.get("chart", {}).get("result") or [None])[0]
    if not res:
        raise RuntimeError(symbol + ": empty")
    ts = res.get("timestamp") or []
    close = ((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {time.strftime("%Y-%m-%d", time.gmtime(t)): float(c)
            for t, c in zip(ts, close) if c is not None and c > 0}


def logret(series, dates, i, n=1):
    if i < n:
        return None
    a, b = series.get(dates[i - n]), series.get(dates[i])
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return math.log(b / a)


def pct(v, xs):
    n = len(xs)
    if n <= 1:
        return 0.5
    less = sum(x < v for x in xs)
    equal = sum(x == v for x in xs)
    return (less + 0.5 * equal) / n


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(X, y, l2=0.002, epochs=80, lr=0.035):
    means = [sum(r[j] for r in X) / len(X) for j in range(len(X[0]))]
    scales = [max(1e-12, math.sqrt(sum((r[j]-means[j])**2 for r in X)/len(X)))
              for j in range(len(X[0]))]
    w, b = [0.0] * len(X[0]), 0.0
    for _ in range(epochs):
        for row, target in zip(X, y):
            z = b + sum(a * (v-m)/s for a,v,m,s in zip(w,row,means,scales))
            p = sigmoid(z)
            e = p - target
            for j in range(len(w)):
                w[j] -= lr * (e * (row[j]-means[j])/scales[j] + l2*w[j])
            b -= lr * e
    return means, scales, w, b


def pair_prob(model, row):
    means, scales, w, b = model
    return sigmoid(b + sum(a*(v-m)/s for a,v,m,s in zip(w,row,means,scales)))


def rank_score(model, row):
    _means, scales, w, _b = model
    return sum(a*v/s for a,v,s in zip(w,row,scales))


def feature_snapshot(series, dates, idx):
    r1={s:logret(series[s],dates,idx,1) for s in SYMBOLS}
    r3={s:logret(series[s],dates,idx,3) for s in SYMBOLS}
    r5={s:logret(series[s],dates,idx,5) for s in SYMBOLS}
    if any(r1[s] is None or r3[s] is None or r5[s] is None for s in SYMBOLS):
        return None
    v1,v3,v5=list(r1.values()),list(r3.values()),list(r5.values())
    out={}
    for s in SYMBOLS:
        hist=[logret(series[s],dates,k,1) for k in range(max(25,idx-60),idx)]
        hist=[v for v in hist if v is not None]
        if len(hist)<30:
            return None
        vol=statistics.pstdev(hist[-20:]) if len(hist[-20:])>1 else 0.0
        prices=[series[s][dates[k]] for k in range(max(0,idx-20),idx)]
        sd=statistics.pstdev(prices) if len(prices)>1 else 0.0
        z=(series[s][dates[idx]]-statistics.mean(prices))/sd if sd>0 else 0.0
        out[s]=[
            r1[s],r3[s],r5[s],vol,z,
            pct(r1[s],v1),pct(r3[s],v3),pct(r5[s],v5),
            r1[s]-statistics.median(v1),
            r3[s]-statistics.median(v3),
            r5[s]-statistics.median(v5),
        ]
    return out


def make_dataset(series, horizon, lag):
    dates=sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))
    last_needed=horizon+lag
    data={}
    for i in range(60,len(dates)-last_needed):
        snap=feature_snapshot(series,dates,i)
        if snap is None:
            continue
        fwd={s:logret(series[s],dates,i+lag+horizon,horizon) for s in SYMBOLS}
        if any(v is None for v in fwd.values()):
            continue
        data[dates[i]]={"x":snap,"fwd":fwd}
    return sorted(data),data


def pair_rows(data, ds, idxs):
    X=[]; y=[]
    for d in ds:
        for a,b in combinations(SYMBOLS,2):
            xa,xb=data[d]["x"][a],data[d]["x"][b]
            X.append([xa[i]-xb[i] for i in idxs])
            y.append(1 if data[d]["fwd"][a] > data[d]["fwd"][b] else 0)
    return X,y


def choose(data, train_dates, horizon, lag):
    end=max(INNER_MIN,len(train_dates)-2*INNER_TEST)
    inner_train=train_dates[:max(1,end-horizon-lag)]
    inner_test=train_dates[end:end+INNER_TEST]
    best=None
    for group,idxs in FEATURES.items():
        for l2 in L2S:
            X,y=pair_rows(data,inner_train,idxs)
            if len(X)<500 or len(set(y))<2:
                continue
            model=fit(X,y,l2=l2)
            losses=[]
            for d in inner_test:
                for a,b in combinations(SYMBOLS,2):
                    xa,xb=data[d]["x"][a],data[d]["x"][b]
                    p=pair_prob(model,[xa[i]-xb[i] for i in idxs])
                    yy=1 if data[d]["fwd"][a]>data[d]["fwd"][b] else 0
                    losses.append((p-yy)**2)
            score=sum(losses)/len(losses)
            cand=(score,group,l2)
            if best is None or cand<best:
                best=cand
    return (best[1],best[2]) if best else ("full",0.002)


def run(series,horizon,lag):
    dates,data=make_dataset(series,horizon,lag)
    folds=[]
    start=TRAIN_MIN
    while start+TEST_SIZE<=len(dates):
        train_dates=dates[:start]
        test_dates=dates[start:start+TEST_SIZE]
        group,l2=choose(data,train_dates,horizon,lag)
        idxs=FEATURES[group]
        X,y=pair_rows(data,train_dates[:max(1,len(train_dates)-horizon-lag)],idxs)
        if len(X)<1000 or len(set(y))<2:
            start+=TEST_SIZE
            continue
        model=fit(X,y,l2=l2)
        day_rows={}
        for d in test_dates:
            scores={s:rank_score(model,[data[d]["x"][s][i] for i in idxs]) for s in SYMBOLS}
            day_rows[d]={s:{"score":scores[s],"momentum":data[d]["x"][s][2],"fwd":data[d]["fwd"][s]} for s in SYMBOLS}

        def ret_for(universe,cost,mode):
            rs=[]
            last=-10**9
            pos={d:i for i,d in enumerate(test_dates)}
            for d in test_dates:
                if pos[d]-last<horizon:
                    continue
                ranked=sorted(universe,key=lambda s:day_rows[d][s][mode])
                lo,sh=ranked[-1],ranked[0]
                rs.append(0.5*(day_rows[d][lo]["fwd"]-day_rows[d][sh]["fwd"])-cost/10000)
                last=pos[d]
            eq=1.0
            for r in rs:
                eq*=1+r
            return eq-1.0

        full_exec={str(c):{"model":ret_for(SYMBOLS,c,"score"),
                           "momentum":ret_for(SYMBOLS,c,"momentum")}
                   for c in COSTS_BPS}
        for c in COSTS_BPS:
            full_exec[str(c)]["delta"]=full_exec[str(c)]["model"]-full_exec[str(c)]["momentum"]

        leave_one={}
        for excluded in SYMBOLS:
            universe=[s for s in SYMBOLS if s!=excluded]
            leave_one[excluded]={
                str(c):ret_for(universe,c,"score") for c in COSTS_BPS
            }

        pair_acc=[]
        for d in test_dates:
            for a,b in combinations(SYMBOLS,2):
                xa,xb=data[d]["x"][a],data[d]["x"][b]
                p=pair_prob(model,[xa[i]-xb[i] for i in idxs])
                pair_acc.append((p>=0.5)==(data[d]["fwd"][a]>data[d]["fwd"][b]))
        folds.append({
            "train_end":train_dates[-1],
            "test_start":test_dates[0],
            "test_end":test_dates[-1],
            "selected_group":group,
            "selected_l2":l2,
            "pairwise_accuracy":sum(pair_acc)/len(pair_acc),
            "execution":full_exec,
            "leave_one_symbol_out":leave_one,
        })
        start+=TEST_SIZE

    summary={}
    for c in COSTS_BPS:
        vals=[f["execution"][str(c)]["delta"] for f in folds]
        summary[str(c)]={"positive_folds":sum(v>0 for v in vals),
                         "folds":len(vals),
                         "positive_rate":sum(v>0 for v in vals)/len(vals) if vals else 0.0,
                         "delta_values":vals}
    lag_summary={s:{} for s in SYMBOLS}
    for excluded in SYMBOLS:
        for c in COSTS_BPS:
            vals=[f["leave_one_symbol_out"][excluded][str(c)] for f in folds]
            lag_summary[excluded][str(c)]={
                "mean_fold_return":statistics.mean(vals) if vals else 0.0,
                "positive_folds":sum(v>0 for v in vals),
                "folds":len(vals),
            }
    return {
        "outer_folds":len(folds),
        "execution_delta":summary,
        "pairwise_accuracy":statistics.mean([f["pairwise_accuracy"] for f in folds]) if folds else 0.0,
        "selected_groups":[f["selected_group"] for f in folds],
        "lag_days":lag,
        "universe_stress":lag_summary,
        "folds":folds,
    }


series, SNAPSHOT_SHA256 = load_or_fetch_series(SYMBOLS, yahoo, os.getenv("GORILA_DATA_SNAPSHOT"))
results={}
for h in HORIZONS:
    results[str(h)]={}
    for lag in EXECUTION_LAGS:
        results[str(h)][str(lag)]=run(series,h,lag)

print(json.dumps({
    "status":"COMPLETE",
    "method":"nested-purged-relative-ranking-execution-and-universe-stress-v1",
    "symbols":SYMBOLS,
    "data_snapshot_sha256": SNAPSHOT_SHA256,
    "horizons":HORIZONS,
    "execution_lags_days":EXECUTION_LAGS,
    "costs_bps_roundtrip":COSTS_BPS,
    "feature_groups":FEATURES,
    "l2_values":L2S,
    "generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
    "results":results,
},indent=2))
