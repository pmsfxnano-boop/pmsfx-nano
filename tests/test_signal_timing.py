from gorila_argentum.signal import compose_signal
from gorila_argentum.timing import make_window

def test_signal_window():
    s=compose_signal("GGAL",0.72,900,regime="TREND_UP")
    assert s["direction"]=="UP"
    assert 0<s["confidence"]<=1
    assert s["timing"]["horizon_seconds"]==900
    assert s["status"]=="LIVE"

def test_timing_positive():
    w=make_window(60,30)
    assert w.validity_seconds==30
