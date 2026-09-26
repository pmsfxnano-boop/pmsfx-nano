from fastapi import FastAPI, HTTPException, Header
import os
import threading
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
from .security import require_internal_key, require_runtime_tick_key
from scripts.gorila_runtime_tick import run_tick as run_runtime_tick

app=FastAPI(title="Gorila Argentum",version="0.1.0")

@app.on_event("startup")
def startup():
    Store().init()

    def persistence_probe():
        try:
            probe = Store()
            probe.init()
            proof = probe.verify_persistence()
            print(
                "GORILA_PERSISTENCE_ROUNDTRIP",
                proof["backend"],
                proof["verified"],
                proof["heartbeat_id"],
                flush=True,
            )
        except Exception as exc:
            print(
                "GORILA_PERSISTENCE_ROUNDTRIP_FAILED",
                type(exc).__name__,
                str(exc),
                flush=True,
            )

    def bootstrap_cycle():
        run_id = os.getenv("GORILA_BOOTSTRAP_TICK_RUN_ID", "").strip()
        if not run_id:
            return
        store = Store()
        store.init()
        if not store.pg:
            print(
                "GORILA_BOOTSTRAP_TICK_SKIPPED",
                "durable_storage_required",
                flush=True,
            )
            return
        if not store.claim_runtime_run(run_id, "bootstrap_operational"):
            print(
                "GORILA_BOOTSTRAP_TICK_SKIPPED",
                "already_claimed",
                run_id,
                flush=True,
            )
            return
        try:
            payload = run_runtime_tick()
            store.finish_runtime_run(run_id, "COMPLETED", payload)
            print(
                "GORILA_BOOTSTRAP_TICK_COMPLETED",
                run_id,
                payload.get("status"),
                flush=True,
            )
        except Exception as exc:
            error = {"error": type(exc).__name__, "message": str(exc)}
            store.finish_runtime_run(run_id, "FAILED", error)
            print(
                "GORILA_BOOTSTRAP_TICK_FAILED",
                run_id,
                type(exc).__name__,
                str(exc),
                flush=True,
            )

    threading.Thread(
        target=persistence_probe,
        name="gorila-persistence-probe",
        daemon=True,
    ).start()
    threading.Thread(
        target=bootstrap_cycle,
        name="gorila-bootstrap-cycle",
        daemon=True,
    ).start()

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
def ingest(x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key")):
    require_internal_key(x_gorila_internal_key)
    return run_batch()

@app.get("/api/features/{symbol}")
def features(symbol: str):
    return build_features(symbol)

@app.get("/api/regime/{symbol}")
def regime(symbol: str):
    from math import log
    store = Store(); store.init()
    series = store.recent_series(symbol, "close", limit=40)
    returns_bps = [10000.0 * log(b/a) for (_,a),(_,b) in zip(series, series[1:]) if a > 0 and b > 0]
    fx_series = store.recent_series("USD_MEP", "sell", limit=2)
    risk_series = store.recent_series("EMBI_ARG", "embi_bps", limit=2)
    fx_stress = None
    risk_delta = None
    if len(fx_series) == 2 and fx_series[-2][1] != 0:
        fx_stress = (fx_series[-1][1] / fx_series[-2][1]) - 1.0
    if len(risk_series) == 2:
        risk_delta = risk_series[-1][1] - risk_series[-2][1]
    return classify_regime(returns_bps, fx_stress=fx_stress, risk_delta_bps=risk_delta)

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

@app.get("/api/runtime/runs")
def runtime_runs(kind: str | None = None, limit: int = 20):
    store = Store()
    store.init()
    return {"items": store.latest_runtime_run(kind=kind, limit=max(1, min(100, int(limit))))}

@app.post("/api/runtime/tick")
def runtime_tick(
    x_gorila_runtime_key: str | None = Header(default=None, alias="X-Gorila-Runtime-Key"),
):
    require_runtime_tick_key(x_gorila_runtime_key)
    try:
        return run_runtime_tick()
    except RuntimeError as exc:
        if str(exc) == "durable_storage_required":
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise

@app.get("/api/shadow")
def shadow(limit: int = 50, symbol: str | None = None, status: str | None = None):
    store = Store(); store.init()
    return {"items": store.latest_shadow(symbol=symbol, status=status, limit=limit)}

@app.get("/api/shadow/summary")
def shadow_summary():
    store = Store(); store.init()
    return store.shadow_summary()

@app.post("/api/shadow/settle-due")
def shadow_settle_due(
    max_lateness_seconds: int = 3600,
    limit: int = 50,
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
    store = Store(); store.init()
    if not store.pg:
        raise HTTPException(status_code=409, detail="durable_storage_required")
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
def promotion_evaluate(
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
    store = Store(); store.init()
    decision = evaluate_promotion(CURRENT_BATCH10_EVIDENCE)
    persisted = store.save_promotion_decision("multihorizon-meta-research-v1", "V2", decision)
    return {"decision": decision, "persisted": persisted}

@app.post("/api/learning/run")
def learning_run(
    symbol: str,
    horizon_days: int = 5,
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
    store = Store(); store.init()
    if not store.pg:
        raise HTTPException(status_code=409, detail="durable_storage_required")
    return run_learning_cycle(
        symbol,
        horizon_days=max(1, min(20, int(horizon_days))),
        store=store,
    )

@app.get("/api/recalibration")
def recalibration(limit: int = 10):
    store = Store(); store.init()
    return {"items": store.latest_calibration(limit=limit)}

@app.post("/api/recalibration/evaluate")
def recalibration_evaluate(
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
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
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
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
def settle_shadow_prediction(
    prediction_id: str,
    observed_price: float,
    observed_at: str,
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
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
def coupling(
    x_gorila_internal_key: str | None = Header(default=None, alias="X-Gorila-Internal-Key"),
):
    require_internal_key(x_gorila_internal_key)
    pairs=[("USD_MEP","sell","USD_CCL","sell"),("USD_BLUE","sell","USD_MEP","sell"),("USD_MEP","sell","EMBI_ARG","embi_bps"),("USD_CCL","sell","EMBI_ARG","embi_bps"),("USD_MEP","sell","USD_BCRA","reference")]
    return build_matrix(pairs)

@app.get("/", response_class=HTMLResponse)
def root():
    return DASHBOARD_HTML
