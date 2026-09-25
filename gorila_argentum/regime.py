from __future__ import annotations
import math
from statistics import mean,pstdev

def _clip(x,lo=0.0,hi=1.0): return max(lo,min(hi,float(x)))

def classify_regime(return_bps, fx_stress=None, risk_delta_bps=None, window=20):
    r=list(map(float,return_bps[-window:]))
    if len(r)<10:
        return {"regime":"UNKNOWN","confidence":0.0,"features":{"n":len(r)}}
    mu=mean(r); vol=pstdev(r)
    trend=_clip(abs(mu)/5.0)
    vol_score=_clip(vol/25.0)
    fx=0.0 if fx_stress is None else _clip(abs(float(fx_stress)))
    risk=0.0 if risk_delta_bps is None else _clip(abs(float(risk_delta_bps))/50.0)
    if vol_score>=0.85:
        regime="HIGH_VOL"
    elif fx>=0.75 and risk>=0.50:
        regime="FX_RISK_STRESS"
    elif mu>=2.0 and vol<15.0:
        regime="TREND_UP"
    elif mu<=-2.0 and vol<15.0:
        regime="TREND_DOWN"
    elif risk>=0.75:
        regime="RISK_OFF"
    else:
        regime="MEAN_REVERT"
    confidence=round(max(trend,vol_score,fx,risk),4)
    return {"regime":regime,"confidence":confidence,"features":{"return_mean_bps":round(mu,4),"return_vol_bps":round(vol,4),"fx_stress":round(fx,4),"risk_stress":round(risk,4),"n":len(r)}}
