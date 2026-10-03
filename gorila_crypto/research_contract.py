"""Research truth contract for Cryptonita.

This module prevents the OOS engine from treating a sampled/compact durable ledger
as equivalent to the lossless hot market plane. Unsupported research resolutions are
hard-blocked rather than silently approximated.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from .protocol import CryptoStudyProtocol


@dataclass(frozen=True)
class ResearchTruthDecision:
    status: str
    requested_horizon_ms: int
    capture_contract: str
    exact_trade_coverage: bool
    exact_book_coverage: bool
    reasons: tuple[str, ...]
    contract_hash: str


def research_truth_decision(
    protocol: CryptoStudyProtocol,
    *,
    horizon_ms: int,
) -> ResearchTruthDecision:
    if horizon_ms <= 0:
        raise ValueError("horizon_ms must be positive")

    reasons: list[str] = []
    exact_trade = float(protocol.trade_persistence_sample_rate) >= 1.0
    exact_book = float(protocol.bookticker_persistence_interval_seconds) * 1000.0 <= float(horizon_ms)

    if not exact_trade:
        reasons.append(
            "RAW_TRADE_LEDGER_NOT_LOSSLESS:"
            f"{protocol.trade_persistence_sample_rate:.6f}"
        )
    if not exact_book:
        reasons.append(
            "BOOKTICKER_LEDGER_INTERVAL_EXCEEDS_HORIZON:"
            f"{protocol.bookticker_persistence_interval_seconds * 1000.0:.0f}ms>{horizon_ms}ms"
        )
    if protocol.persistence_contract_version.strip() == "":
        reasons.append("EMPTY_PERSISTENCE_CONTRACT")

    payload: dict[str, Any] = {
        "protocol_hash": protocol.protocol_hash,
        "study_id": protocol.study_id,
        "protocol_version": protocol.version,
        "persistence_contract_version": protocol.persistence_contract_version,
        "trade_persistence_sample_rate": protocol.trade_persistence_sample_rate,
        "bookticker_persistence_interval_seconds": protocol.bookticker_persistence_interval_seconds,
        "requested_horizon_ms": horizon_ms,
        "exact_trade_coverage": exact_trade,
        "exact_book_coverage": exact_book,
        "reasons": tuple(reasons),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()

    return ResearchTruthDecision(
        status="PASS" if not reasons else "BLOCKED",
        requested_horizon_ms=horizon_ms,
        capture_contract=protocol.persistence_contract_version,
        exact_trade_coverage=exact_trade,
        exact_book_coverage=exact_book,
        reasons=tuple(reasons),
        contract_hash=digest,
    )


def require_research_truth(
    protocol: CryptoStudyProtocol,
    *,
    horizon_ms: int,
) -> ResearchTruthDecision:
    decision = research_truth_decision(protocol, horizon_ms=horizon_ms)
    if decision.status != "PASS":
        raise RuntimeError(
            "research_truth_contract_blocked:"
            f"horizon={horizon_ms} reasons={decision.reasons}"
        )
    return decision
