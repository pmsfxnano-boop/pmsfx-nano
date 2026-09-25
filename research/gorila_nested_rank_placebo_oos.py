from __future__ import annotations

import json
import os
import math
import random
import statistics
import time
from itertools import combinations

import httpx

SYMBOLS=["GGAL","BMA","YPFD","PAMP","TGSU2","CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
TRAIN_MIN=504
TEST_SIZE=126
INNER_TEST=63
INNER_MIN=252
N_PERM=12
SEED=20260925
FEATURE_GROUPS={
    "momentum":[0,1,2],
    "base_vol_z":[0,1,2,3,4],
    "relative":[5,6,7,8,9,10],
    "full":list(range(11)),
}
L2S=[0.001,0.002,0.01]


def get_json(url,params=None):
    r=httpx.get(url,params=params,timeout=30,headers={"User-Agent":"Gorila-Argentum-Temporal-Placebo/0.1"})
    r.raise_for_status()
    return r.json()


def yahoo(symbol):
    j=get_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",{"range":"5y","interval":"1d","events":"history"})
    res=(j.get("chart",{}).get("result") or [None])[0]
    if not res: raise RuntimeError(symbol+": empty")
    ts=res.get("timestamp") or []
    close=((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {time.strftime("%Y-%m-%d",time.gmtime(t)):float(c) for t,c in zip(ts,close) if c is not None and c>0}


def logret(series,dates,i,n=1):
    if i<n:return None
    a,b=series.get(dates[i-n]),series.get(dates[i])
    if a is None or b is None or a<=0 or b<=0:return None
    return math.log(b/a)


def pct(v,xs):
    n=len(xs)
    if n<=1:return 0.5
    return (sum(x<v for x in xs)+0.5*sum(x==v for x in xs))/n


def sigmoid(z):
    return 1.0/(1.0+math.exp(-max(-30.0,min(30.0,z))))


def fit(X,y,l2=0.002,epochs=80,lr=0.035):
    means=[sum(r[j] for r in X)/len(X) for j in range(len(X[0]))]
    scales=[max(1e-12,math.sqrt(sum((r[j]-means[j])**2 for r in X)/len(X))) for j in range(len(X[0]))]
    w=[0.0]*len(X[0]); b=0.0
    for _ in range(epochs):
        for row,target in zip(X,y):
            z=b+sum(a*(v-m)/s for a,v,m,s in zip(w,row,means,scales))
            p=sigmoid(z); e=p-target
            for j in range(len(w)):
                w[j]-=lr*(e*(row[j]-means[j])/scales[j]+l2*w[j])
            b-=lr*e
    return means,scales,w,b


def pair_prob(model,row):
    means,scales,w,b=model
    return sigmoid(b+sum(a*(v-m)/s for a,v,m,s in zip(w,row,means,scales)))


def rank_score(model,row):
    _means,scales,w,_b=model
    return sum(a*v/s for a,v,s in zip(w,row,scales))


def feature_snapshot(series,dates,idx):
    r1={s:logret(series[s],dates,idx,1) for s in SYMBOLS}
    r3={s:logret(series[s],dates,idx,3) for s in SYMBOLS}
    r5={s:logret(series[s],dates,idx,5) for s in SYMBOLS}
    if any(r1[s] is None or r3[s] is None or r5[s] is None for s in SYMBOLS):return None
    v1,v3,v5=list(r1.values()),list(r3.values()),list(r5.values())
    out={}
    for s in SYMBOLS:
        hist=[logret(series[s],dates,k,1) for k in range(max(25,idx-60),idx)]
        hist=[v for v in hist if v is not None]
        if len(hist)<30:return None
        vol=statistics.pstdev(hist[-20:]) if len(hist[-20:])>1 else 0.0
        prices=[series[s][dates[k]] for k in range(max(0,idx-20),idx)]
        sd=statistics.pstdev(prices) if len(prices)>1 else 0.0
        z=(series[s][dates[idx]]-statistics.mean(prices))/sd if sd>0 else 0.0
        out[s]=[r1[s],r3[s],r5[s],vol,z,pct(r1[s],v1),pct(r3[s],v3),pct(r5[s],v5),
                r1[s]-statistics.median(v1),r3[s]-statistics.median(v3),r5[s]-statistics.median(v5)]
    return out


def make_dataset(series,horizon):
    dates=sorted(set.intersection(*(set(series[s]) for s in SYMBOLS)))
    data={}
    for i in range(60,len(dates)-horizon):
        snap=feature_snapshot(series,dates,i)
        if snap is None:continue
        fwd={s:logret(series[s],dates,i+horizon,horizon) for s in SYMBOLS}
        if any(v is None for v in fwd.values()):continue
        data[dates[i]]={"x":snap,"fwd":fwd}
    return sorted(data),data


def pair_rows(data,ds,idxs,fwd_override=None):
    X=[];y=[]
    for d in ds:
        f=fwd_override[d] if fwd_override is not None else data[d]["fwd"]
        for a,b in combinations(SYMBOLS,2):
            xa,xb=data[d]["x"][a],data[d]["x"][b]
            X.append([xa[i]-xb[i] for i in idxs])
            y.append(1 if f[a]>f[b] else 0)
    return X,y


def choose(data,train_dates,horizon,fwd_override=None):
    end=max(INNER_MIN,len(train_dates)-2*INNER_TEST)
    inner_train=train_dates[:max(1,end-horizon)]
    inner_test=train_dates[end:end+INNER_TEST]
    best=None
    for group,idxs in FEATURE_GROUPS.items():
        for l2 in L2S:
            X,y=pair_rows(data,inner_train,idxs,fwd_override)
            if len(X)<500 or len(set(y))<2:continue
            model=fit(X,y,l2=l2)
            losses=[]
            for d in inner_test:
                f=fwd_override[d] if fwd_override is not None else data[d]["fwd"]
                for a,b in combinations(SYMBOLS,2):
                    xa,xb=data[d]["x"][a],data[d]["x"][b]
                    p=pair_prob(model,[xa[i]-xb[i] for i in idxs])
                    yy=1 if f[a]>f[b] else 0
                    losses.append((p-yy)**2)
            score=sum(losses)/len(losses)
            cand=(score,group,l2)
            if best is None or cand<best:best=cand
    return (best[1],best[2]) if best else ("full",0.002)


def fold_metrics(data,test_dates,model,idxs):
    hits=[];ic=[]
    for d in test_dates:
        # Cross-sectional rank IC: Pearson corr of ranks of model score and realized return.
        sc={};fr=data[d]["fwd"]
        for s in SYMBOLS:
            sc[s]=rank_score(model,[data[d]["x"][s][i] for i in idxs])
        ranked_s=sorted(SYMBOLS,key=lambda s:sc[s])
        ranked_r=sorted(SYMBOLS,key=lambda s:fr[s])
        pos_s={s:i for i,s in enumerate(ranked_s)};pos_r={s:i for i,s in enumerate(ranked_r)}
        xs=[pos_s[s] for s in SYMBOLS];ys=[pos_r[s] for s in SYMBOLS]
        mx,my=statistics.mean(xs),statistics.mean(ys)
        den=math.sqrt(sum((v-mx)**2 for v in xs)*sum((v-my)**2 for v in ys))
        if den>0:ic.append(sum((a-mx)*(b-my) for a,b in zip(xs,ys))/den)
        for a,b in combinations(SYMBOLS,2):
            p=pair_prob(model,[data[d]["x"][a][i]-data[d]["x"][b][i] for i in idxs])
            hits.append((p>=0.5)==(fr[a]>fr[b]))
    return statistics.mean(hits) if hits else 0.0, statistics.mean(ic) if ic else 0.0


def run_horizon(series,horizon):
    dates,data=make_dataset(series,horizon)
    outer=[]
    rng=random.Random(SEED+horizon)
    start=TRAIN_MIN
    while start+TEST_SIZE<=len(dates):
        train_dates=dates[:start];test_dates=dates[start:start+TEST_SIZE]
        group,l2=choose(data,train_dates,horizon)
        idxs=FEATURE_GROUPS[group]
        X,y=pair_rows(data,train_dates[:-horizon],idxs)
        model=fit(X,y,l2=l2)
        acc,ic=fold_metrics(data,test_dates,model,idxs)
        # Build date-level null labels by permuting complete forward-return vectors across training dates.
        perm_briers=[]
        perm_accs=[]
        train_fwd={d:data[d]["fwd"] for d in train_dates}
        dates_train=list(train_fwd)
        for _ in range(N_PERM):
            shuffled=dates_train[:]
            rng.shuffle(shuffled)
            override={d:train_fwd[src] for d,src in zip(dates_train,shuffled)}
            pgroup,pl2=choose(data,train_dates,horizon,override)
            pidx=FEATURE_GROUPS[pgroup]
            pX,py=pair_rows(data,train_dates[:-horizon],pidx,override)
            pm=fit(pX,py,l2=pl2)
            probs=[];labels=[]
            for d in test_dates:
                fr=data[d]["fwd"]
                for a,b in combinations(SYMBOLS,2):
                    p=pair_prob(pm,[data[d]["x"][a][i]-data[d]["x"][b][i] for i in pidx])
                    probs.append(p);labels.append(1 if fr[a]>fr[b] else 0)
            perm_accs.append(sum((p>=0.5)==bool(y) for p,y in zip(probs,labels))/len(labels))
            perm_briers.append(sum((p-y)**2 for p,y in zip(probs,labels))/len(labels))
        outer.append({
            "train_end":train_dates[-1],"test_start":test_dates[0],"test_end":test_dates[-1],
            "selected_group":group,"selected_l2":l2,
            "pairwise_accuracy":acc,"rank_ic":ic,
            "placebo_accuracy_mean":statistics.mean(perm_accs),
            "placebo_accuracy_p95":sorted(perm_accs)[max(0,int(0.95*len(perm_accs))-1)],
            "placebo_brier_mean":statistics.mean(perm_briers),
            "placebo_brier_p05":sorted(perm_briers)[max(0,int(0.05*len(perm_briers))-1)],
        })
        start+=TEST_SIZE
    return {
        "outer_folds":len(outer),
        "folds":outer,
        "mean_pairwise_accuracy":statistics.mean([f["pairwise_accuracy"] for f in outer]) if outer else 0.0,
        "mean_rank_ic":statistics.mean([f["rank_ic"] for f in outer]) if outer else 0.0,
        "positive_rank_ic_folds":sum(f["rank_ic"]>0 for f in outer),
        "placebo_accuracy_mean":statistics.mean([f["placebo_accuracy_mean"] for f in outer]) if outer else 0.0,
    }


series={s:yahoo(s) for s in SYMBOLS}
results={str(h):run_horizon(series,h) for h in HORIZONS}
print(json.dumps({
    "status":"COMPLETE",
    "method":"nested-purged-ranking-temporal-placebo-and-rank-IC-v1",
    "symbols":SYMBOLS,"horizons":HORIZONS,
    "outer_train_min":TRAIN_MIN,"outer_test_size":TEST_SIZE,
    "n_permutations_per_fold":N_PERM,
    "seed":SEED,
    "generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),
    "results":results,
},indent=2))
