"""Release-lineage identity for Cryptonita.

Every operational/evidence surface exposes the same immutable identity so a
research result can be traced to the exact deployed commit and contracts.
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from .execution import ExecutionCostModel
from .protocol import PREREGISTERED_CRYPTO_PROTOCOL
from .research_contract import research_truth_decision


def release_identity() -> dict[str, Any]:
    code_sha = (
        os.getenv("RENDER_GIT_COMMIT")
        or os.getenv("GORILA_CRYPTO_CODE_VERSION")
        or "unknown"
    )
    truth = {
        str(h): research_truth_decision(
            PREREGISTERED_CRYPTO_PROTOCOL,
            horizon_ms=int(h),
        ).contract_hash
        for h in PREREGISTERED_CRYPTO_PROTOCOL.forecast_horizons_ms
    }
    execution_model = ExecutionCostModel()
    payload = {
        "code_sha": code_sha,
        "study_id": PREREGISTERED_CRYPTO_PROTOCOL.study_id,
        "protocol_hash": PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash,
        "persistence_contract_version": PREREGISTERED_CRYPTO_PROTOCOL.persistence_contract_version,
        "research_truth_contracts": truth,
        "execution_model_hash": execution_model.model_hash,
        "execution_model_validated": execution_model.validated_execution_model,
    }
    release_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        **payload,
        "release_hash": release_hash,
    }
