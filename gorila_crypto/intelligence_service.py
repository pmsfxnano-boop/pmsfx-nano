"""HTTP wrapper for the isolated adaptive Opportunity Clock worker."""

from __future__ import annotations

import threading
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .intelligence_runner import run_once
from .intelligence_store import IntelligenceStore

app = FastAPI(title="Gorila Crypto Opportunity Intelligence", version="1")
_state = {
    "status": "STARTING",
    "last_run": None,
    "last_result": None,
    "error": None,
}
_lock = threading.Lock()


def _worker() -> None:
    store = IntelligenceStore()
    store.init()
    while True:
        try:
            result = run_once(store)
            with _lock:
                _state["status"] = result.get("status", "RUNNING")
                _state["last_run"] = time.time()
                _state["last_result"] = result
                _state["error"] = None
        except Exception as exc:
            with _lock:
                _state["status"] = "ERROR"
                _state["error"] = f"{type(exc).__name__}: {exc}"
        time.sleep(0.25)


@app.on_event("startup")
def startup() -> None:
    thread = threading.Thread(target=_worker, name="opportunity-intelligence", daemon=True)
    thread.start()


@app.get("/api/crypto/health")
def health() -> JSONResponse:
    with _lock:
        snapshot = dict(_state)
    return JSONResponse(
        {
            "service": "gorila-crypto-opportunity-intelligence",
            "model_version": "opportunity-clock-intelligence-v1",
            **snapshot,
        }
    )
