from __future__ import annotations
from datetime import datetime, timezone
from .storage import Store

def _latest(store,symbol,field):
    rows=store.recent_series(symbol,field,limit=1)
    return rows[-1] if rows else None

def _spread(a,b):
    if a is None or b in (None,0): return None
    return (a/b)-1.0

def build_market_state():
    store=Store(); store.init()
    values={
        "USD_BCRA":_latest(store,"USD_BCRA","reference"),
        "USD_MEP":_latest(store,"USD_MEP","sell"),
        "USD_CCL":_latest(store,"USD_CCL","sell"),
        "USD_BLUE":_latest(store,"USD_BLUE","sell"),
        "USD_MAYORISTA":_latest(store,"USD_MAYORISTA","sell"),
        "USD_CRYPTO":_latest(store,"USD_CRIPTO","sell"),
        "EMBI_ARG":_latest(store,"EMBI_ARG","embi_bps"),
    }
    mep=values["USD_MEP"][1] if values["USD_MEP"] else None
    ccl=values["USD_CCL"][1] if values["USD_CCL"] else None
    blue=values["USD_BLUE"][1] if values["USD_BLUE"] else None
    oficial=values["USD_BCRA"][1] if values["USD_BCRA"] else None
    embi=values["EMBI_ARG"][1] if values["EMBI_ARG"] else None
    return {
        "fx":{
            "official":oficial,
            "mep":mep,
            "ccl":ccl,
            "blue":blue,
            "crypto":values["USD_CRIPTO"][1] if values["USD_CRIPTO"] else None,
            "spreads":{
                "mep_official":_spread(mep,oficial),
                "ccl_official":_spread(ccl,oficial),
                "blue_official":_spread(blue,oficial),
                "ccl_mep":_spread(ccl,mep),
            }
        },
        "risk":{"embi_bps":embi},
        "as_of":{
            k:(v[0] if v else None) for k,v in values.items()
        },
        "sources":store.health()
    }
