from __future__ import annotations
import math
from .storage import Store

def _corr(xs,ys):
    n=min(len(xs),len(ys))
    if n<8: return None
    xs=xs[-n:]; ys=ys[-n:]
    mx=sum(xs)/n; my=sum(ys)/n
    dx=[x-mx for x in xs]; dy=[y-my for y in ys]
    den=math.sqrt(sum(x*x for x in dx)*sum(y*y for y in dy))
    return (sum(x*y for x,y in zip(dx,dy))/den) if den else 0.0

def build_matrix(pairs):
    store=Store(); store.init(); matrix={}; edges=[]
    for a,af,b,bf in pairs:
        sa=store.recent_series(a,af); sb=store.recent_series(b,bf)
        if len(sa)<8 or len(sb)<8: continue
        ca=_corr([v for _,v in sa],[v for _,v in sb])
        if ca is None: continue
        matrix.setdefault(a,{})[b]=round(ca,4)
        matrix.setdefault(b,{})[a]=round(ca,4)
        edges.append({"from":a,"to":b,"coupling":round(ca,4),"n":min(len(sa),len(sb))})
    store.save_coupling("V0",matrix,{"edges":len(edges),"method":"ordinal-correlation-v0"})
    return {"matrix":matrix,"edges":edges,"status":"READY" if edges else "WAITING_FOR_DEPTH"}
