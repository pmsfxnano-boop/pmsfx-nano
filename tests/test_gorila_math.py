from gorila_argentum.coupling import _returns, _pearson, _lagged

def test_returns():
    r=_returns([("2026-01-01",100),("2026-01-02",110),("2026-01-03",121)])
    assert len(r)==2 and abs(r[0]-r[1])<1e-12

def test_corr():
    c=_pearson([1,2,3,4,5,6,7,8],[2,4,6,8,10,12,14,16])
    assert c > 0.999

def test_lag():
    xs=[1,2,3,4,5,7,6,8,9,11,10,12]
    ys=[40]+xs[:-1]
    item=_lagged(xs,ys,max_lag=2)
    assert item["lag"]==1 and item["corr"]>0.999
