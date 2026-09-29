"""Isolated Crypto application boundary.

A2 deliberately contains no external market network client and no background
worker. Those are introduced in later cleanroom phases after the boundary tests
are proven.
"""

from __future__ import annotations

from fastapi import FastAPI

from gorila_core.market_freshness import (
    DELAYED_MAX_AGE_SECONDS,
    LIVE_MAX_AGE_SECONDS,
)
from gorila_crypto.config import settings


app = FastAPI(
    title="Gorila Crypto Cleanroom",
    version="0.1.0-cleanroom",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "service": "gorila-crypto",
        "domain": "crypto",
        "status": "READY",
        "runtime_isolated": True,
        "network_adapters": 0,
        "background_workers": 0,
        "forecast_status": "BLOCKED_NO_VALIDATED_MODEL",
    }


@app.get("/api/crypto/health")
def health() -> dict:
    return {
        "service": "gorila-crypto",
        "domain": "crypto",
        "status": "READY",
        "runtime_isolated": True,
        "network_adapters": 0,
        "background_workers": 0,
        "symbols": list(settings.symbols),
        "forecast": {
            "status": "BLOCKED_NO_VALIDATED_MODEL",
            "semantics": "P(SIGNED_TARGET_RETURN_BPS_POSITIVE)",
            "automatic_promotion": False,
        },
        "freshness_contract": {
            "live_max_age_seconds": LIVE_MAX_AGE_SECONDS,
            "delayed_max_age_seconds": DELAYED_MAX_AGE_SECONDS,
        },
    }


@app.get("/api/crypto/config")
def config_snapshot() -> dict:
    return {
        "environment": settings.environment,
        "symbols": list(settings.symbols),
        "network_adapters": 0,
        "background_workers": 0,
    }
