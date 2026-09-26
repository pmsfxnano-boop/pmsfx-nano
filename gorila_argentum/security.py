from __future__ import annotations

import hmac
import os

from fastapi import HTTPException


def require_internal_key(provided_key: str | None) -> None:
    expected = os.getenv("GORILA_INTERNAL_API_KEY", "").strip()
    if not expected:
        return
    if not provided_key or not hmac.compare_digest(provided_key, expected):
        raise HTTPException(status_code=401, detail="invalid_internal_key")
