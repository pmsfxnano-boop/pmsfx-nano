from __future__ import annotations
import math
from .storage import Store

def _vals(series):
    return [v for _,v in series]

def log_return(series,n=1):
    vals=_vals(series)
    if len(vals)<=n or vals[-1]<=0 or vals[-1-n]<=0: return None
    return math.log(vals[-1]/vals[-1-n])

def realized_vol(series,window=20):
    vals=_vals(series)
    if len(vals)<window+1: return None
    rets=[math.log(b/a) for a,b in zip(vals[-window-1:],vals[-window:]) if a>0 and b>0]
    if len(rets)<2: return None
    m=sum(rets)/len(rets)
    return math.sqrt(sum((r-m)**2 for r in rets)/(len(rets)-1))

def zscore_last(series,window=20):
    vals=_vals(series)
    if len(vals)<window: return None
    w=vals[-window:]; m=sum(w)/len(w); s=math.sqrt(sum((x-m)**2 for x in w)/len(w))
    return 0.0 if s==0 else (w[-1]-m)/s

def build_features(symbol,field="close"):
    store=Store(); store.init()
    s=store.recent_series(symbol,field,limit=250)
    features={
        "symbol":symbol,
        "field":field,
        "r1":log_return(s,1),
        "r3":log_return(s,3),
        "r5":log_return(s,5),
        "vol5":realized_vol(s,5),
        "vol20":realized_vol(s,20),
        "z20":zscore_last(s,20),
        "samples":len(s),
        "latest_event":s[-1][0] if s else None
    }
    return features
