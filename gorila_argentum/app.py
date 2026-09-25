from fastapi import FastAPI
from .storage import Store
from .ingest import run_batch
from .coupling import build_matrix
from .config import settings
from .state import build_market_state
from .features import build_features

app=FastAPI(title="Gorila Argentum",version="0.1.0")

@app.on_event("startup")
def startup():
    Store().init()

@app.get("/health")
def health():
    return {"ok":True,"service":"gorila-argentum","version":"0.1.0","mode":"RESEARCH","storage":"postgres" if settings.database_url else "sqlite-fallback","sources":Store().health()}

@app.get("/api/state")
def state():
    return {"sources":Store().health(),"symbols":list(settings.symbols)}

@app.get("/api/state/live")
def live_state():
    return build_market_state()

@app.post("/api/ingest")
def ingest():
    return run_batch()

@app.get("/api/features/{symbol}")
def features(symbol: str):
    return build_features(symbol)

@app.post("/api/coupling")
def coupling():
    pairs=[("USD_MEP","sell","USD_CCL","sell"),("USD_BLUE","sell","USD_MEP","sell"),("USD_MEP","sell","EMBI_ARG","embi_bps"),("USD_CCL","sell","EMBI_ARG","embi_bps"),("USD_MEP","sell","USD_BCRA","reference")]
    return build_matrix(pairs)

@app.get("/")
def root():
    return {"name":"Gorila Argentum","status":"ONLINE","mode":"RESEARCH","next":"data-fabric → coupling → features → regime → prediction → timing"}
