from __future__ import annotations

import json, math, os, statistics, time
from itertools import combinations
import httpx

ALL_SYMBOLS=["GGAL","BMA","YPFD","PAMP","TGSU2","CEPU"]
HORIZONS = [int(x) for x in os.getenv("GORILA_HORIZONS", "5,10").split(",") if x.strip()]
UNIVERSES={"full":ALL_SYMBOLS,**{f"minus_{s}":[x for x in ALL_SYMBOLS if x!=s] for s in ALL_SYMBOLS}}
TRAIN_MIN=504
TEST_SIZE=126
INNER_TEST=63
INNER_MIN=252
COSTS_BPS=[25,50,100,150]
FEATURE_GROUPS={"momentum":[0,1,2],"base_vol_z":[0,1,2,3,4],"relative":[5,6,7,8,9,10],"full":list(range(11))}
L2S=[0.001,0.002,0.01]

def get_json(url,params=None):
    r=httpx.get(url,params=params,timeout=30,headers={"User-Agent":"Gorila-Argentum-Universe-LOO/0.1"})
    r.raise_for_status(); return r.json()

def yahoo(symbol):
    j=get_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",{"range":"5y","interval":"1d","events":"history"})
    res=(j.get("chart",{}).get("result") or [None])[0]
    if not res: raise RuntimeError(symbol+": empty")
    ts=res.get("timestamp") or []
    close=((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {time.strftime("%Y-%m-%d",time.gmtime(t)):float(c) for t,c in zip(ts,close) if c is not None and c>0}

def logret(series,dates,i,n=1):
    if i<n:return None
    a,b=series[dates[i-n]],series[dates[i]]
    if a<=0 or b<=0:return None
    return math.log(b/a)

def pct(v,xs):
    n=len(xs)
    return 0.5 if n<=1 else (sum(x<v for x in xs)+0.5*sum(x==v for x in xs))/n

def sigmoid(z): return 1/(1+math.exp(-max(-30,min(30,z))))

def fit(X,y,l2=0.002,epochs=80,lr=0.035):
    means=[sum(r[j] for r in X)/len(X) for j in range(len(X[0]))]
    scales=[max(1e-12,math.sqrt(sum((r[j]-means[j])**2 for r in X)/len(X))) for j in range(len(X[0]))]
    w=[0.0]*len(X[0]); b=0.0
    for _ in range(epochs):
        for row,t in zip(X,y):
            p=sigmoid(b+sum(a*(v-m)/s for a,v,m,s in zip(w,row,means,scales))); e=p-t
            for j in range(len(w)): w[j]-=lr*(e*(row[j]-means[j])/scales[j]+l2*w[j])
            b-=lr*e
    return means,scales,w,b

def pair_prob(model,row):
    means,scales,w,b=model
    return sigmoid(b+sum(a*(v-m)/s for a,v,m,s in zip(w,row,means,scales)))

def rank_score(model,row):
    _m,scales,w,_b=model
    return sum(a*v/s for a,v,s in zip(w,row,scales))

def feature_snapshot(series,dates,idx,syms):
    r1={s:logret(series[s],dates,idx,1) for s in syms}
    r3={s:logret(series[s],dates,idx,3) for s in syms}
    r5={s:logret(series[s],dates,idx,5) for s in syms}
    if any(r1[s] is None or r3[s] is None or r5[s] is None for s in syms): return None
    v1,v3,v5=list(r1.values()),list(r3.values()),list(r5.values())
    out={}
    for s in syms:
        hist=[logret(series[s],dates,k,1) for k in range(max(25,idx-60),idx)]
        if len([v for v in hist if v is not None])<30:return None
        hist=[v for v in hist if v is not None]
        vol=statistics.pstdev(hist[-20:]) if len(hist[-20:])>1 else 0.0
        prices=[series[s][dates[k]] for k in range(max(0,idx-20),idx)]
        sd=statistics.pstdev(prices) if len(prices)>1 else 0.0
        z=(series[s][dates[idx]]-statistics.mean(prices))/sd if sd>0 else 0.0
        out[s]=[r1[s],r3[s],r5[s],vol,z,pct(r1[s],v1),pct(r3[s],v3),pct(r5[s],v5),
                r1[s]-statistics.median(v1),r3[s]-statistics.median(v3),r5[s]-statistics.median(v5)]
    return out

def dataset(series,horizon,syms):
    dates=sorted(set.intersection(*(set(series[s]) for s in syms)))
    data={}
    for i in range(60,len(dates)-horizon):
        snap=feature_snapshot(series,dates,i,syms)
        if snap is None:continue
        fwd={s:logret(series[s],dates,i+horizon,horizon) for s in syms}
        if any(v is None for v in fwd.values()):continue
        data[dates[i]]={"x":snap,"fwd":fwd}
    return sorted(data),data

def pair_rows(data,ds,idxs,syms):
    X=[];y=[]
    for d in ds:
        for a,b in combinations(syms,2):
            xa,xb=data[d]["x"][a],data[d]["x"][b]
            X.append([xa[i]-xb[i] for i in idxs]);y.append(1 if data[d]["fwd"][a]>data[d]["fwd"][b] else 0)
    return X,y

def choose(data,train_dates,horizon,syms):
    end=max(INNER_MIN,len(train_dates)-2*INNER_TEST)
    inner_train=train_dates[:max(1,end-horizon)];inner_test=train_dates[end:end+INNER_TEST]
    best=None
    for group,idxs in FEATURE_GROUPS.items():
        for l2 in L2S:
            X,y=pair_rows(data,inner_train,idxs,syms)
            if len(X)<400 or len(set(y))<2:continue
            model=fit(X,y,l2=l2);loss=[]
            for d in inner_test:
                for a,b in combinations(syms,2):
                    xa,xb=data[d]["x"][a],data[d]["x"][b]
                    p=pair_prob(model,[xa[i]-xb[i] for i in idxs]); yy=1 if data[d]["fwd"][a]>data[d]["fwd"][b] else 0
                    loss.append((p-yy)**2)
            cand=(sum(loss)/len(loss),group,l2)
            if best is None or cand<best:best=cand
    return (best[1],best[2]) if best else ("full",0.002)

def run(series,horizon,syms):
    dates,data=dataset(series,horizon,syms)
    folds=[]; start=TRAIN_MIN
    while start+TEST_SIZE<=len(dates):
        train=dates[:start];test=dates[start:start+TEST_SIZE]
        group,l2=choose(data,train,horizon,syms);idxs=FEATURE_GROUPS[group]
        X,y=pair_rows(data,train[:-horizon],idxs,syms);model=fit(X,y,l2=l2)
        pair_hits=[];rank_ics=[]
        for d in test:
            scores={s:rank_score(model,[data[d]["x"][s][i] for i in idxs]) for s in syms}
            rv=data[d]["fwd"]
            rs=sorted(syms,key=lambda s:scores[s]);rr=sorted(syms,key=lambda s:rv[s])
            ps={s:i for i,s in enumerate(rs)};pr={s:i for i,s in enumerate(rr)}
            xs=[ps[s] for s in syms];ys=[pr[s] for s in syms];mx,my=statistics.mean(xs),statistics.mean(ys)
            den=math.sqrt(sum((v-mx)**2 for v in xs)*sum((v-my)**2 for v in ys))
            if den>0:rank_ics.append(sum((a-mx)*(b-my) for a,b in zip(xs,ys))/den)
            for a,b in combinations(syms,2):
                p=pair_prob(model,[data[d]["x"][a][i]-data[d]["x"][b][i] for i in idxs])
                pair_hits.append((p>=0.5)==(rv[a]>rv[b]))
        def exec_ret(cost):
            vals=[];last=-10**9;pos={d:i for i,d in enumerate(test)}
            for d in test:
                if pos[d]-last<horizon:continue
                ranked=sorted(syms,key=lambda s:rank_score(model,[data[d]["x"][s][i] for i in idxs]))
                lo,sh=ranked[-1],ranked[0]
                vals.append(0.5*(data[d]["fwd"][lo]-data[d]["fwd"][sh])-cost/10000);last=pos[d]
            eq=1.0
            for v in vals:eq*=1+v
            return eq-1
        execs={str(c):exec_ret(c) for c in COSTS_BPS}
        folds.append({"train_end":train[-1],"test_start":test[0],"test_end":test[-1],"group":group,"l2":l2,
                      "pairwise_accuracy":sum(pair_hits)/len(pair_hits),"rank_ic":statistics.mean(rank_ics) if rank_ics else 0.0,
                      "execution":execs})
        start+=TEST_SIZE
    return {
        "symbols":syms,"outer_folds":len(folds),
        "mean_pairwise_accuracy":statistics.mean([f["pairwise_accuracy"] for f in folds]) if folds else 0.0,
        "positive_rank_ic_folds":sum(f["rank_ic"]>0 for f in folds),
        "mean_rank_ic":statistics.mean([f["rank_ic"] for f in folds]) if folds else 0.0,
        "execution":[{"cost_bps":c,"positive_folds":sum(f["execution"][str(c)]>0 for f in folds),
                      "folds":len(folds),"mean_fold_return":statistics.mean([f["execution"][str(c)] for f in folds]) if folds else 0.0}
                     for c in COSTS_BPS],
        "folds":folds,
    }

series={s:yahoo(s) for s in ALL_SYMBOLS}
results={}
for h in HORIZONS:
    results[str(h)]={}
    for name,syms in UNIVERSES.items():
        results[str(h)][name]=run(series,h,syms)
print(json.dumps({"status":"COMPLETE","method":"true-universe-leave-one-out-nested-relative-ranking-v1",
                  "universes":UNIVERSES,"horizons":HORIZONS,"costs_bps_roundtrip":COSTS_BPS,
                  "generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"results":results},indent=2))
