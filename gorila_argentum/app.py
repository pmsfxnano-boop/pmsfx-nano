from fastapi import FastAPI, HTTPException
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
from .control import build_control_state
from .shadow import validate_shadow_prediction, compute_shadow_outcome, validate_observed_at
from .promotion import evaluate_promotion, CURRENT_BATCH10_EVIDENCE
from .learning import run_learning_cycle
from .calibration import build_recalibration_candidate
from .audit import build_audit_state

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

@app.get("/api/drift")
def drift_summary(symbol: str | None = None, field: str | None = None, limit: int = 100):
    store=Store(); store.init()
    return {"items":store.latest_drift(symbol=symbol,field=field,limit=limit)}

@app.get("/api/control")
def control():
    return build_control_state()

@app.get("/api/audit")
def audit():
    return build_audit_state()

@app.get("/api/shadow")
def shadow(limit: int = 50, symbol: str | None = None, status: str | None = None):
    store = Store(); store.init()
    return {"items": store.latest_shadow(symbol=symbol, status=status, limit=limit)}

@app.get("/api/shadow/summary")
def shadow_summary():
    store = Store(); store.init()
    return store.shadow_summary()

@app.post("/api/shadow/settle-due")
def shadow_settle_due(max_lateness_seconds: int = 3600, limit: int = 50):
    store = Store(); store.init()
    return store.settle_due_shadow_from_observations(
        max_lateness_seconds=max(60, min(172800, int(max_lateness_seconds))),
        limit=max(1, min(100, int(limit))),
    )

@app.get("/api/promotion")
def promotion():
    store = Store(); store.init()
    return {
        "current_evaluation": evaluate_promotion(CURRENT_BATCH10_EVIDENCE),
        "latest_decision": store.latest_promotion_decision(),
    }

@app.post("/api/promotion/evaluate")
def promotion_evaluate():
    store = Store(); store.init()
    decision = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    persisted = store.save_promotion_decision("multihorizon-meta-research-v1", "V2", decision)
    return {"decision": decision, "persisted": persisted}

@app.post("/api/learning/run")
def learning_run(symbol: str, horizon_days: int = 5):
    return run_learning_cycle(symbol, horizon_days=max(1, min(20, int(horizon_days))))

@app.get("/api/recalibration")
def recalibration(limit: int = 10):
    store = Store(); store.init()
    return {"items": store.latest_calibration(limit=limit)}

@app.post("/api/recalibration/evaluate")
def recalibration_evaluate():
    store = Store(); store.init()
    rows = store.latest_shadow(status="SETTLED", limit=500)
    result = build_recalibration_candidate(rows)
    persisted = store.save_calibration_run("shadow-probability-v0", result)
    return {
        "candidate": result,
        "persisted": persisted,
        "automatic_apply": False,
        "apply_gate": "PROMOTION_AND_DURABILITY_REQUIRED",
    }

@app.get("/api/learning")
def learning(symbol: str | None = None, limit: int = 20):
    store = Store(); store.init()
    return {"items": store.latest_learning(symbol=symbol, limit=limit)}

@app.post("/api/shadow/prediction")
def create_shadow_prediction(
    symbol: str,
    probability_up: float,
    horizon_seconds: int = 900,
    model_version: str = "V0",
    regime: str = "UNKNOWN",
    entry_price: float = 0.0,
    feature_hash: str = "",
):
    try:
        values = validate_shadow_prediction(
            symbol=symbol,
            probability_up=probability_up,
            horizon_seconds=horizon_seconds,
            entry_price=entry_price,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    store = Store(); store.init()
    control_state = build_control_state(store)
    if control_state["runtime"]["circuit_breaker"] == "HALTED":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "RESEARCH_CIRCUIT_BREAKER_HALTED",
                "reasons": control_state["runtime"]["circuit_breaker_reasons"],
            },
        )
    return store.save_shadow_prediction(
        values["symbol"], model_version, values["probability_up"], values["horizon_seconds"],
        regime, values["entry_price"], feature_hash=feature_hash,
    )

@app.post("/api/shadow/{prediction_id}/settle")
def settle_shadow_prediction(prediction_id: str, observed_price: float, observed_at: str):
    store = Store(); store.init()
    prediction = store.get_shadow_prediction(prediction_id)
    if not prediction:
        raise HTTPException(status_code=404, detail="shadow_prediction_not_found")
    try:
        normalized_observed_at = validate_observed_at(
            prediction["created_at"], prediction["horizon_seconds"], observed_at
        )
        outcome = compute_shadow_outcome(
            prediction["probability_up"], prediction["entry_price"], observed_price
        )
        return store.settle_shadow_prediction(
            prediction_id, outcome, normalized_observed_at
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

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
