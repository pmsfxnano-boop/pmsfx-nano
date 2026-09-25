from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Sequence

def sigmoid(z: float) -> float:
    z=max(-30.0,min(30.0,z))
    return 1.0/(1.0+math.exp(-z))

@dataclass
class Model:
    mean: list[float]
    scale: list[float]
    weights: list[float]
    bias: float

def _standardize_fit(X):
    p=len(X[0])
    mean=[sum(row[j] for row in X)/len(X) for j in range(p)]
    scale=[]
    for j in range(p):
        var=sum((row[j]-mean[j])**2 for row in X)/len(X)
        scale.append(math.sqrt(var) if var>1e-12 else 1.0)
    return mean,scale

def _transform(row,mean,scale):
    return [(x-m)/s for x,m,s in zip(row,mean,scale)]

def fit_logistic(X: Sequence[Sequence[float]], y: Sequence[int], epochs=220, lr=0.04) -> Model:
    if len(X)<20 or len(X)!=len(y) or len({int(v) for v in y})<2:
        raise ValueError("insufficient_training_data")
    X=[list(map(float,r)) for r in X]; y=list(map(int,y))
    mean,scale=_standardize_fit(X)
    Z=[_transform(r,mean,scale) for r in X]
    w=[0.0]*len(Z[0]); b=0.0
    for _ in range(epochs):
        for x,target in zip(Z,y):
            p=sigmoid(b+sum(a*v for a,v in zip(w,x)))
            err=p-target
            for j in range(len(w)): w[j]-=lr*err*x[j]
            b-=lr*err
    return Model(mean,scale,w,b)

def predict(model: Model, row: Sequence[float]) -> float:
    z=model.bias+sum(a*v for a,v in zip(model.weights,_transform(list(map(float,row)),model.mean,model.scale)))
    return sigmoid(z)

def chronological_oos(X: Sequence[Sequence[float]], y: Sequence[int], train_frac=0.8):
    if len(X)!=len(y): raise ValueError("length_mismatch")
    split=max(10,min(len(X)-1,int(len(X)*train_frac)))
    model=fit_logistic(X[:split],y[:split])
    probs=[predict(model,row) for row in X[split:]]
    labels=list(y[split:])
    acc=sum((p>=0.5)==bool(t) for p,t in zip(probs,labels))/len(labels)
    brier=sum((p-t)**2 for p,t in zip(probs,labels))/len(labels)
    return {"model":model,"accuracy_oos":acc,"brier_oos":brier,"n_train":split,"n_test":len(labels)}
