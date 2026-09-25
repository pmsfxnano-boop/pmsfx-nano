from __future__ import annotations
import json, math, statistics, time
import httpx

SYMBOLS=["GGAL","BMA","YPFD","PAMP","TGSU2","CEPU"]

def get_json(url,params=None):
    r=httpx.get(url,params=params,timeout=30,headers={"User-Agent":"Gorila-Argentum-Coupling-OOS/0.1"})
    r.raise_for_status()
    return r.json()

def yahoo(symbol):
    j=get_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",{"range":"5y","interval":"1d","events":"history"})
    res=(j.get("chart",{}).get("result") or [None])[0]
    if not res: raise RuntimeError(symbol+": empty")
    ts=res.get("timestamp") or []
    close=((res.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    return {time.strftime("%Y-%m-%d",time.gmtime(t)):float(c) for t,c in zip(ts,close) if c is not None and c>0}

def argdatos_dollar(casa):
    j=get_json(f"https://api.argentinadatos.com/v1/cotizaciones/dolares/{casa}")
    if isinstance(j,dict): j=[j]
    return {str(x["fecha"])[:10]:float(x["venta"]) for x in j if x.get("fecha") and x.get("venta") is not None}

def argdatos_risk():
    j=get_json("https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais")
    return {str(x["fecha"])[:10]:float(x["valor"]) for x in j if x.get("fecha") and x.get("valor") is not None}

def ret(series,dates,i,n=1):
    if i<n: return None
    a=series.get(dates[i-n]); b=series.get(dates[i])
    if a is None or b is None or a<=0 or b<=0: return None
    return math.log(b/a)

def corr(xs,ys):
    n=min(len(xs),len(ys))
    if n<20: return 0.0
    xs=xs[-n:]; ys=ys[-n:]
    mx=sum(xs)/n; my=sum(ys)/n
    dx=[x-mx for x in xs]; dy=[y-my for y in ys]
    den=math.sqrt(sum(x*x for x in dx)*sum(y*y for y in dy))
    return 0.0 if den==0 else sum(x*y for x,y in zip(dx,dy))/den

def features_for(target,series,fx,risk,dates,i):
    own=[ret(series[target],dates,i,k) for k in (1,3,5)]
    if any(v is None for v in own): return None
    hist=[]
    for k in range(max(60,i-80),i):
        rv=ret(series[target],dates,k,1)
        if rv is not None: hist.append(rv)
    if len(hist)<30: return None
    vol=statistics.pstdev(hist[-20:]) if len(hist[-20:])>1 else 0.0
    vals=[series[target].get(dates[k]) for k in range(max(0,i-20),i) if series[target].get(dates[k]) is not None]
    if len(vals)<10: return None
    sd=statistics.pstdev(vals)
    z=(series[target][dates[i]]-statistics.mean(vals))/sd if sd>0 else 0.0
    peer_terms=[]
    for peer in SYMBOLS:
        if peer==target: continue
        pr=[ret(series[peer],dates,k,1) for k in range(max(60,i-60),i)]
        tr=[ret(series[target],dates,k,1) for k in range(max(60,i-60),i)]
        aligned=[(a,b) for a,b in zip(pr,tr) if a is not None and b is not None]
        if len(aligned)>=30:
            c=corr([a for a,b in aligned],[b for a,b in aligned])
            prev=ret(series[peer],dates,i-1,1)
            if prev is not None: peer_terms.append(c*prev)
    peer_pressure=sum(peer_terms)/max(1.0,sum(abs(x) for x in peer_terms)) if peer_terms else 0.0
    fxr=(ret(fx,dates,i-1,1) or 0.0) if fx else 0.0
    embi_delta=(risk.get(dates[i-1],0.0)-risk.get(dates[i-2],0.0)) if risk and i>=2 and dates[i-1] in risk and dates[i-2] in risk else 0.0
    baseline=[own[0],own[1],own[2],vol,z]
    advanced=baseline+[peer_pressure,fxr,embi_delta/100.0]
    return baseline,advanced

def fit(X,y):
    means=[sum(r[j] for r in X)/len(X) for j in range(len(X[0]))]
    scales=[max(1e-12,math.sqrt(sum((r[j]-means[j])**2 for r in X)/len(X))) for j in range(len(X[0]))]
    Z=[[(v-m)/s for v,m,s in zip(r,means,scales)] for r in X]
    w=[0.0]*len(Z[0]); b=0.0
    for _ in range(260):
        for r,t in zip(Z,y):
            z=b+sum(a*v for a,v in zip(w,r)); p=1/(1+math.exp(-max(-30,min(30,z)))); e=p-t
            for j in range(len(w)): w[j]-=0.025*(e*r[j]+0.002*w[j])
            b-=0.025*e
    return means,scales,w,b

def predict(m,row):
    means,scales,w,b=m
    z=b+sum(a*(v-m0)/s for a,v,m0,s in zip(w,row,means,scales))
    return 1/(1+math.exp(-max(-30,min(30,z))))

def evaluate(samples):
    split=int(len(samples)*0.8)
    tr,te=samples[:split],samples[split:]
    for name,idx in [("baseline",0),("advanced",1)]:
        pass
    out={}
    for name,idx in [("baseline",0),("advanced",1)]:
        X=[fs[idx] for fs,_ in tr]; y=[label for _,label in tr]
        m=fit(X,y)
        probs=[predict(m,a[idx]) for a in te]
        labels=[b for _,b in te]
        acc=sum((p>=.5)==bool(y) for p,y in zip(probs,labels))/len(labels)
        brier=sum((p-y)**2 for p,y in zip(probs,labels))/len(labels)
        base=sum(labels)/len(labels)
        base_brier=sum((base-y)**2 for y in labels)/len(labels)
        out[name]={"accuracy":acc,"brier":brier,"baseline_brier":base_brier,"brier_skill":1-brier/base_brier if base_brier>0 else None}
    return {"samples":len(samples),"train":len(tr),"test":len(te),"baseline":out["baseline"],"advanced":out["advanced"],
            "delta_accuracy":out["advanced"]["accuracy"]-out["baseline"]["accuracy"],
            "delta_brier":out["advanced"]["brier"]-out["baseline"]["brier"]}

series={s:yahoo(s) for s in SYMBOLS}
fx=argdatos_dollar("bolsa")
risk=argdatos_risk()
results={}
for target in SYMBOLS:
    dates=sorted(set(series[target]))
    samples=[]
    for i in range(80,len(dates)-1):
        fs=features_for(target,series,fx,risk,dates,i)
        if fs is None: continue
        label=1 if series[target][dates[i+1]]>series[target][dates[i]] else 0
        samples.append((fs,label))
    results[target]=evaluate(samples) if len(samples)>=300 else {"status":"INSUFFICIENT_DATA","samples":len(samples)}
print(json.dumps({"status":"COMPLETE","generated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"fx_series_rows":len(fx),"risk_rows":len(risk),"results":results},indent=2))
