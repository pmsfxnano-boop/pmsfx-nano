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
    values={}
    errors={}
    requested = {
        "USD_BCRA": ("USD_BCRA", "reference"),
        "USD_MEP": ("USD_MEP", "sell"),
        "USD_CCL": ("USD_CCL", "sell"),
        "USD_BLUE": ("USD_BLUE", "sell"),
        "USD_MAYORISTA": ("USD_MAYORISTA", "sell"),
        "USD_CRYPTO": ("USD_CRIPTO", "sell"),
        "EMBI_ARG": ("EMBI_ARG", "embi_bps"),
    }
    for key, (symbol, field) in requested.items():
        try:
            values[key] = _latest(store, symbol, field)
        except Exception as exc:
            values[key] = None
            errors[key] = f"{type(exc).__name__}: {exc}"

    mep=values["USD_MEP"][1] if values["USD_MEP"] else None
    ccl=values["USD_CCL"][1] if values["USD_CCL"] else None
    blue=values["USD_BLUE"][1] if values["USD_BLUE"] else None
    oficial=values["USD_BCRA"][1] if values["USD_BCRA"] else None
    embi=values["EMBI_ARG"][1] if values["EMBI_ARG"] else None
    return {
        "status": "READY" if not errors else "DEGRADED",
        "fx":{
            "official":oficial,
            "mep":mep,
            "ccl":ccl,
            "blue":blue,
            "crypto":values["USD_CRYPTO"][1] if values["USD_CRYPTO"] else None,
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
        "errors": errors,
        "sources":store.health()
    }
