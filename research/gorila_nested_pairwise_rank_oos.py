from __future__ import annotations

import json
import os
import os
import math
import statistics
import time
from itertools import combinations

import httpx

from research.gorila_data_snapshot import load_or_fetch_series

SYMBOLS = ["GGAL", "BMA", "YPFD", "PAMP", "TGSU2", "CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
COSTS_BPS = [25, 50, 100]
OUTER_TRAIN_MIN = 504
OUTER_TEST_SIZE = 126
INNER_TEST_SIZE = 63
INNER_MIN_TRAIN = 252

FEATURE_GROUPS = {
    "base_momentum": [0, 1, 2],
    "base_plus_vol_z": [0, 1, 2, 3, 4],
    "relative_percentiles": [5, 6, 7, 8, 9, 10],
    "full": list(range(11)),
}
L2_VALUES = [0.001, 0.002, 0.01]


def get_json(url, params=None):
    r = httpx.get(url, params=params, timeout=30,
                  headers={"User-Agent": "Gorila-Argentum-NestedRank/0.1"})
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
    a, b = series.get(dates[i-n]), series.get(dates[i])
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return math.log(b / a)


def percentile_rank(value, values):
    n = len(values)
    if n <= 1:
        return 0.5
    less = sum(v < value for v in values)
    equal = sum(v == value for v in values)
    return (less + 0.5 * equal) / n


def sigmoid(z):
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, z))))


def fit(X, y, l2=0.002, epochs=80, lr=0.035):
    means = [sum(row[j] for row in X) / len(X) for j in range(len(X[0]))]
    scales = [max(1e-12, math.sqrt(sum((row[j]-means[j])**2 for row in X)/len(X)))
              for j in range(len(X[0]))]
    Z = [[(v-m)/s for v,m,s in zip(row,means,scales)] for row in X]
    w, b = [0.0]*len(Z[0]), 0.0
    for _ in range(epochs):
        for row, target in zip(Z, y):
            p = sigmoid(b + sum(a*v for a,v in zip(w,row)))
            e = p-target
            for j in range(len(w)):
                w[j] -= lr*(e*row[j] + l2*w[j])
            b -= lr*e
    return means, scales, w, b


def pair_prob(model, row):
    means, scales, w, b = model
    return sigmoid(b + sum(a*(v-m)/s for a,v,m,s in zip(w,row,means,scales)))


def rank_score(model, row):
    _means, scales, w, _b = model
    return sum(a*v/s for a,v,s in zip(w,row,scales))


def build_dates(series):
    return sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))


def feature_snapshot(series, dates, idx):
    r1 = {s: logret(series[s],dates,idx,1) for s in SYMBOLS}
    r3 = {s: logret(series[s],dates,idx,3) for s in SYMBOLS}
    r5 = {s: logret(series[s],dates,idx,5) for s in SYMBOLS}
    if any(r1[s] is None or r3[s] is None or r5[s] is None for s in SYMBOLS):
        return None
    vals1, vals3, vals5 = list(r1.values()), list(r3.values()), list(r5.values())
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
            percentile_rank(r1[s],vals1),
            percentile_rank(r3[s],vals3),
            percentile_rank(r5[s],vals5),
            r1[s]-statistics.median(vals1),
            r3[s]-statistics.median(vals3),
            r5[s]-statistics.median(vals5),
        ]
    return out


def make_dataset(series,horizon):
    dates=build_dates(series)
    data={}
    for i in range(60,len(dates)-horizon):
        snap=feature_snapshot(series,dates,i)
        if snap is None:
            continue
        fwd={s:logret(series[s],dates,i+horizon,horizon) for s in SYMBOLS}
        if any(v is None for v in fwd.values()):
            continue
        data[dates[i]]={"x":snap,"fwd":fwd}
    return sorted(data),data


def pair_rows(data,ds,idxs):
    # Build pairwise feature differences for the selected feature positions.
    X=[]; y=[]
    for d in ds:
        for a,b in combinations(SYMBOLS,2):
            xa,xb=data[d]["x"][a],data[d]["x"][b]
            X.append([xa[i]-xb[i] for i in idxs])
            y.append(1 if data[d]["fwd"][a]>data[d]["fwd"][b] else 0)
    return X,y


def choose_nested(data,train_dates,horizon):
    # Inner validation is completely contained in the outer training history.
    inner_train_end=max(INNER_MIN_TRAIN, len(train_dates)-2*INNER_TEST_SIZE)
    inner_train=train_dates[:inner_train_end-horizon]
    inner_test=train_dates[inner_train_end:inner_train_end+INNER_TEST_SIZE]
    if len(inner_train)<INNER_MIN_TRAIN or len(inner_test)<20:
        inner_train=train_dates[:max(INNER_MIN_TRAIN,len(train_dates)-INNER_TEST_SIZE)]
        inner_test=train_dates[-INNER_TEST_SIZE:]
    candidates=[]
    for group,idxs in FEATURE_GROUPS.items():
        for l2 in L2_VALUES:
            X,y=pair_rows(data,inner_train,idxs)
            if len(X)<500 or len(set(y))<2:
                continue
            model=fit(X,y,l2=l2)
            probs=[]; labels=[]
            for d in inner_test:
                for a,b in combinations(SYMBOLS,2):
                    xa,xb=data[d]["x"][a],data[d]["x"][b]
                    probs.append(pair_prob(model,[xa[i]-xb[i] for i in idxs]))
                    labels.append(1 if data[d]["fwd"][a]>data[d]["fwd"][b] else 0)
            brier=sum((p-y)**2 for p,y in zip(probs,labels))/len(labels)
            candidates.append((brier,group,l2))
    if not candidates:
        return "full",0.002
    candidates.sort(key=lambda z:(z[0],z[1],z[2]))
    return candidates[0][1],candidates[0][2]


def run_horizon(series,horizon):
    usable,data=make_dataset(series,horizon)
    outer=[]; all_pair=[]
    start=OUTER_TRAIN_MIN
    while start+OUTER_TEST_SIZE<=len(usable):
        train_dates=usable[:start]
        test_dates=usable[start:start+OUTER_TEST_SIZE]
        group,l2=choose_nested(data,train_dates,horizon)
        idxs=FEATURE_GROUPS[group]
        X,y=pair_rows(data,train_dates[:-horizon],idxs)
        if len(X)<1000 or len(set(y))<2:
            start+=OUTER_TEST_SIZE; continue
        model=fit(X,y,l2=l2)
        oos={}
        pair_hits=[]
        for d in test_dates:
            scores={}
            for s in SYMBOLS:
                scores[s]=rank_score(model,[data[d]["x"][s][i] for i in idxs])
            oos[d]={s:{"score":scores[s],"momentum":data[d]["x"][s][2],"fwd":data[d]["fwd"][s]}
                    for s in SYMBOLS}
            for a,b in combinations(SYMBOLS,2):
                xa,xb=data[d]["x"][a],data[d]["x"][b]
                p=pair_prob(model,[xa[i]-xb[i] for i in idxs])
                hit = (p >= 0.5) == (data[d]["fwd"][a] > data[d]["fwd"][b])
                all_pair.append(hit)
                pair_hits.append(hit)
        def execute(mode,cost):
            ret=[]
            last=-10**9
            pos={d:i for i,d in enumerate(test_dates)}
            for d in test_dates:
                if pos[d]-last<horizon: continue
                ranked=sorted(SYMBOLS,key=lambda s:oos[d][s]["score"] if mode=="model" else oos[d][s]["momentum"])
                lo,sh=ranked[-1],ranked[0]
                ret.append(0.5*(oos[d][lo]["fwd"]-oos[d][sh]["fwd"])-cost/10000)
                last=pos[d]
            eq=1.0
            for r in ret: eq*=1+r
            return eq-1.0
        fold={str(c):{"model":execute("model",c),"momentum":execute("momentum",c)} for c in COSTS_BPS}
        for c in COSTS_BPS:
            fold[str(c)]["delta"]=fold[str(c)]["model"]-fold[str(c)]["momentum"]
        outer.append({
            "train_end":train_dates[-1],
            "test_start":test_dates[0],
            "test_end":test_dates[-1],
            "selected_group":group,
            "selected_l2":l2,
            "pairwise_accuracy":sum(pair_hits)/len(pair_hits),
            "execution":fold,
        })
        start+=OUTER_TEST_SIZE

    summary={}
    for c in COSTS_BPS:
        vals=[f["execution"][str(c)]["delta"] for f in outer]
        summary[str(c)]={"folds":len(vals),
                         "positive_folds":sum(v>0 for v in vals),
                         "positive_rate":sum(v>0 for v in vals)/len(vals) if vals else 0.0,
                         "delta_values":vals}
    return {"outer_folds":len(outer),"folds":outer,
            "pairwise_accuracy":sum(all_pair)/len(all_pair) if all_pair else 0.0,
            "execution_delta":summary,
            "selected_groups":[f["selected_group"] for f in outer]}


if __name__ == "__main__":
    series, SNAPSHOT_SHA256 = load_or_fetch_series(SYMBOLS, yahoo, os.getenv("GORILA_DATA_SNAPSHOT"))
    results={str(h):run_horizon(series,h) for h in HORIZONS}
    print(json.dumps({
        "status":"COMPLETE",
        "method":"nested-purged-pairwise-ranking-feature-selection-v1",
        "symbols":SYMBOLS,
    "data_snapshot_sha256": SNAPSHOT_SHA256,
        "horizons":HORIZONS,
        "costs_bps_roundtrip":COSTS_BPS,
        "outer_train_min":OUTER_TRAIN_MIN,
        "outer_test_size":OUTER_TEST_SIZE,
        "inner_test_size":INNER_TEST_SIZE,
        "feature_groups":FEATURE_GROUPS,
        "l2_values":L2_VALUES,
        "generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
        "results":results,
    },indent=2))
