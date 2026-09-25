from gorila_argentum.regime import classify_regime
from gorila_argentum.validation import purged_walk_forward

def test_regime_trend_up():
    r=classify_regime([3.0]*20,fx_stress=0.0,risk_delta_bps=0.0)
    assert r["regime"]=="TREND_UP"

def test_regime_high_vol():
    r=classify_regime([40,-40,35,-35,30,-30,45,-45,50,-50,25,-25,40,-40,35,-35,30,-30,45,-45])
    assert r["regime"]=="HIGH_VOL"

def test_purged_walk_forward():
    X=[]; y=[]
    for i in range(120):
        X.append([float(i), float(i%3)])
        y.append(1 if i>55 else 0)
    result=purged_walk_forward(X,y,train_size=50,test_size=10,purge=2)
    assert result["status"]=="READY"
    assert result["oos_n"]>0
