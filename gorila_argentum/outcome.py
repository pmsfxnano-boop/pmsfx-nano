from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class Outcome:
    prediction_id:str
    realized_return:float
    realized_direction:str
    correct:bool|None
    brier:float|None

def resolve(prediction_id, p_up, entry_price, exit_price, threshold_bps=0.5):
    entry=float(entry_price); exit_=float(exit_price)
    if entry<=0: raise ValueError("invalid_entry_price")
    ret_bps=(exit_/entry-1.0)*10000.0
    if ret_bps>=threshold_bps: direction="UP"
    elif ret_bps<=-threshold_bps: direction="DOWN"
    else: direction="FLAT"
    correct=None
    if direction in ("UP","DOWN") and p_up is not None:
        predicted="UP" if float(p_up)>=0.5 else "DOWN"
        correct=(predicted==direction)
    label=1 if direction=="UP" else 0 if direction=="DOWN" else None
    brier=None if label is None or p_up is None else (float(p_up)-label)**2
    return Outcome(str(prediction_id),ret_bps,direction,correct,brier).__dict__
