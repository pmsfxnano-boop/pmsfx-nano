from gorila_argentum.prediction import fit_logistic, predict, chronological_oos

def test_prediction_smoke():
    X=[]; y=[]
    for i in range(80):
        x=float(i)
        X.append([x])
        y.append(1 if x>40 else 0)
    result=chronological_oos(X,y,train_frac=0.8)
    assert 0 <= result["brier_oos"] <= 1
    assert result["n_test"] > 0
    assert predict(result["model"],[90.0]) > 0.5

def test_constant_scale_feature():
    X=[[1.0,2.0],[2.0,2.0],[3.0,2.0],[4.0,2.0],[5.0,2.0],[6.0,2.0],[7.0,2.0],[8.0,2.0],[9.0,2.0],[10.0,2.0],
       [11.0,2.0],[12.0,2.0],[13.0,2.0],[14.0,2.0],[15.0,2.0],[16.0,2.0],[17.0,2.0],[18.0,2.0],[19.0,2.0],[20.0,2.0]]
    y=[0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1]
    model=fit_logistic(X,y)
    assert len(model.weights)==2
