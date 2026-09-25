from __future__ import annotations
import math
from .storage import Store

def _returns(series):
    vals=[v for _,v in series]
    if len(vals)<2: return []
    out=[]
    for a,b in zip(vals,vals[1:]):
        if a > 0 and b > 0:
            out.append(math.log(b/a))
    return out

def _pearson(xs,ys):
    n=min(len(xs),len(ys))
    if n<8: return None
    xs=xs[-n:]; ys=ys[-n:]
    mx=sum(xs)/n; my=sum(ys)/n
    dx=[x-mx for x in xs]; dy=[y-my for y in ys]
    den=math.sqrt(sum(x*x for x in dx)*sum(y*y for y in dy))
    if den==0: return 0.0
    return sum(x*y for x,y in zip(dx,dy))/den

def _lagged(xs,ys,max_lag=5):
    # Positive lag means X leads Y by that many aligned observations.
    best=None
    for lag in range(0,max_lag+1):
        if lag==0:
            x,y=xs,ys
        else:
            if len(xs)<=lag or len(ys)<=lag: continue
            x,y=xs[:-lag],ys[lag:]
        c=_pearson(x,y)
        if c is None: continue
        item={"lag":lag,"corr":c,"n":min(len(x),len(y))}
        if best is None or abs(c)>abs(best["corr"]):
            best=item
    return best

def _date_key(ts): return str(ts)[:10]

def _align_by_date(a,b):
    da={_date_key(t):v for t,v in a}
    db={_date_key(t):v for t,v in b}
    keys=sorted(set(da).intersection(db))
    return [(k,da[k]) for k in keys],[(k,db[k]) for k in keys]

def _edge(store,a,af,b,bf):
    sa=store.recent_series(a,af)
    sb=store.recent_series(b,bf)
    aa,bb=_align_by_date(sa,sb)
    if len(aa)<10 or len(bb)<10:
        return None
    ra=_returns(aa); rb=_returns(bb)
    n=min(len(ra),len(rb))
    if n<8: return None
    fit=_lagged(ra[-n:],rb[-n:],max_lag=5)
    if fit is None: return None
    return {"from":a,"to":b,"coupling":round(fit["corr"],4),"best_lag":fit["lag"],"n":fit["n"],"method":"aligned-log-return-lagged-pearson-v1"}

def build_matrix(pairs):
    store=Store(); store.init()
    matrix={}; edges=[]
    for a,af,b,bf in pairs:
        e=_edge(store,a,af,b,bf)
        if e is None: continue
        matrix.setdefault(a,{})[b]={"coupling":e["coupling"],"best_lag":e["best_lag"],"n":e["n"]}
        # Reverse edge is computed independently, not assumed symmetric.
        rev=_edge(store,b,bf,a,af)
        if rev is not None:
            matrix.setdefault(b,{})[a]={"coupling":rev["coupling"],"best_lag":rev["best_lag"],"n":rev["n"]}
        edges.append(e)
    store.save_coupling("V0",matrix,{"edges":len(edges),"method":"aligned-log-return-lagged-pearson-v1"})
    return {"matrix":matrix,"edges":edges,"status":"READY" if edges else "WAITING_FOR_DEPTH"}


def current_coupling_state(pairs):
    store=Store(); store.init()
    edges=[]
    for a,af,b,bf in pairs:
        sa=store.recent_series(a,af)
        sb=store.recent_series(b,bf)
        aa,bb=_align_by_date(sa,sb)
        if len(aa)<10 or len(bb)<10:
            continue
        ra=_returns(aa); rb=_returns(bb)
        n=min(len(ra),len(rb))
        if n<8:
            continue
        fit=_lagged(ra[-n:],rb[-n:],max_lag=5)
        if fit is None:
            continue
        edges.append({
            "from":a,
            "to":b,
            "coupling":round(fit["corr"],4),
            "lag":fit["lag"],
            "n":fit["n"]
        })
    abs_mean=(sum(abs(e["coupling"]) for e in edges)/len(edges)) if edges else 0.0
    signed_mean=(sum(e["coupling"] for e in edges)/len(edges)) if edges else 0.0
    return {
        "status":"READY" if edges else "WAITING_FOR_DEPTH",
        "edge_count":len(edges),
        "mean_abs_coupling":round(abs_mean,4),
        "mean_signed_coupling":round(signed_mean,4),
        "structural_state":"HIGH_SYNCHRONIZATION" if abs_mean>=0.55 else "NORMAL",
        "edges":sorted(edges,key=lambda e:abs(e["coupling"]),reverse=True)[:12],
        "method":"aligned-log-return-lagged-pearson-v1"
    }
