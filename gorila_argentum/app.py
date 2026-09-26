from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from .storage import Store
from .ingest import run_batch
from .coupling import build_matrix
from .config import settings
from .signal import compose_signal
from .regime import classify_regime
from .dashboard import HTML as DASHBOARD_HTML
from .state import build_market_state
from .features import build_features
from .drift import rolling_drift

app=FastAPI(title="Gorila Argentum",version="0.1.0")

@app.on_event("startup")
def startup():
    Store().init()

@app.get("/health")
def health():
    return {"ok":True,"service":"gorila-argentum","version":"0.1.0","mode":"RESEARCH","storage":"postgres" if settings.database_url else "sqlite-fallback","sources":Store().health()}

@app.get("/dashboard",response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML

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

@app.get("/api/drift/{symbol}/{field}")
def drift(symbol: str, field: str, current_size: int = 30, reference_size: int = 90):
    current_size = max(10, min(120, int(current_size)))
    reference_size = max(20, min(180, int(reference_size)))
    store = Store(); store.init()
    series = store.recent_series(symbol, field, limit=current_size + reference_size)
    return rolling_drift([value for _, value in series], current_size=current_size, reference_size=reference_size)

@app.get("/api/signal/{symbol}")
def signal(symbol: str, probability_up: float, horizon_seconds: int = 900, regime: str = "UNKNOWN"):
    return compose_signal(symbol, probability_up, horizon_seconds, regime=regime)

@app.get("/api/coupling/current")
def coupling_current():
    from .coupling import current_coupling_state
    pairs=[("USD_MEP","sell","USD_CCL","sell"),("USD_BLUE","sell","USD_MEP","sell"),("USD_MEP","sell","EMBI_ARG","embi_bps"),("USD_CCL","sell","EMBI_ARG","embi_bps"),("USD_MEP","sell","USD_BCRA","reference")]
    return current_coupling_state(pairs)

@app.post("/api/coupling")
def coupling():
    pairs=[("USD_MEP","sell","USD_CCL","sell"),("USD_BLUE","sell","USD_MEP","sell"),("USD_MEP","sell","EMBI_ARG","embi_bps"),("USD_CCL","sell","EMBI_ARG","embi_bps"),("USD_MEP","sell","USD_BCRA","reference")]
    return build_matrix(pairs)

@app.get("/")
def root():
    return {"name":"Gorila Argentum","status":"ONLINE","mode":"RESEARCH","next":"data-fabric → coupling → features → regime → prediction → timing"}
