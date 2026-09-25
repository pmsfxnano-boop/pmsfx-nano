from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta
from typing import Sequence
from .prediction import fit_logistic,predict

@dataclass(frozen=True)
class FoldResult:
    fold:int
    train_n:int
    test_n:int
    accuracy:float
    brier:float
    baseline_brier:float

def purged_walk_forward(X:Sequence[Sequence[float]], y:Sequence[int], train_size:int, test_size:int, purge:int=0, step:int|None=None):
    if len(X)!=len(y): raise ValueError("length_mismatch")
    step=step or test_size
    folds=[]; start=train_size
    fold=0
    while start+test_size<=len(X):
        train_end=max(0,start-purge)
        train_X=X[:train_end]; train_y=y[:train_end]
        test_X=X[start:start+test_size]; test_y=y[start:start+test_size]
        if len(train_X)>=20 and len(set(train_y))>=2:
            model=fit_logistic(train_X,train_y)
            probs=[predict(model,row) for row in test_X]
            acc=sum((p>=0.5)==bool(t) for p,t in zip(probs,test_y))/len(test_y)
            rate=sum(train_y)/len(train_y)
            baseline=[rate]*len(test_y)
            brier=sum((p-t)**2 for p,t in zip(probs,test_y))/len(test_y)
            bb=sum((p-t)**2 for p,t in zip(baseline,test_y))/len(test_y)
            folds.append(FoldResult(fold,len(train_X),len(test_X),acc,brier,bb))
        start+=step; fold+=1
    if not folds: return {"status":"INSUFFICIENT_DATA","folds":[]}
    return {
        "status":"READY",
        "folds":[f.__dict__ for f in folds],
        "oos_n":sum(f.test_n for f in folds),
        "accuracy":sum(f.accuracy*f.test_n for f in folds)/sum(f.test_n for f in folds),
        "brier":sum(f.brier*f.test_n for f in folds)/sum(f.test_n for f in folds),
        "baseline_brier":sum(f.baseline_brier*f.test_n for f in folds)/sum(f.test_n for f in folds),
        "brier_skill":1.0-sum(f.brier*f.test_n for f in folds)/sum(f.baseline_brier*f.test_n for f in folds) if sum(f.baseline_brier*f.test_n for f in folds)>0 else None
    }
