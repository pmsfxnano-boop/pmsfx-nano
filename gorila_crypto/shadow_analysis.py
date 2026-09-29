"""Research-only orchestration for A6 shadow analysis.

This layer bridges the immutable replay ledger to the descriptive lead/lag and
Opportunity Clock engine. It requires ingest-order replay so the information set
is reconstructed from receipt order, and persists the exact replay fingerprint.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable

from .lead_lag import LeadLagConfig, run_lead_lag_shadow, summarize_opportunity_clock
from .ledger import ReplaySpec, replay, replay_manifest
from .storage import CryptoStore


def run_shadow_analysis(
    store: CryptoStore,
    replay_spec: ReplaySpec,
    config: LeadLagConfig,
    *,
    symbols: Iterable[str] | None = None,
) -> dict[str, Any]:
    if replay_spec.order != "ingest":
        raise ValueError("A6 shadow analysis requires ingest-order PIT replay")

    result = replay(store, replay_spec)
    scan = run_lead_lag_shadow(
        result.rows,
        result.fingerprint,
        config,
        symbols=symbols,
    )

    observation_rows = [
        {
            "replay_fingerprint": result.fingerprint,
            **asdict(item),
            "leader_event_time": item.leader_event_time.isoformat(),
            "leader_received_time": item.leader_received_time.isoformat(),
            "target_event_time": item.target_event_time.isoformat(),
            "target_received_time": item.target_received_time.isoformat(),
        }
        for item in scan.observations
    ]
    opportunity_rows = [
        {
            **asdict(item),
            "detection_event_time": item.detection_event_time.isoformat(),
            "detection_received_time": item.detection_received_time.isoformat(),
            "first_reaction_event_time": (
                item.first_reaction_event_time.isoformat()
                if item.first_reaction_event_time else None
            ),
            "first_reaction_received_time": (
                item.first_reaction_received_time.isoformat()
                if item.first_reaction_received_time else None
            ),
            "convergence_event_time": (
                item.convergence_event_time.isoformat()
                if item.convergence_event_time else None
            ),
            "convergence_received_time": (
                item.convergence_received_time.isoformat()
                if item.convergence_received_time else None
            ),
        }
        for item in scan.opportunities
    ]

    stored_observations = store.save_lead_lag_observations(observation_rows)
    stored_opportunities = store.save_opportunity_clocks(opportunity_rows)

    manifest = replay_manifest(replay_spec, result)
    manifest["analysis"] = "A6_DESCRIPTIVE_SHADOW"
    manifest["symbols"] = list(scan.symbols)
    manifest["observation_count"] = scan.observation_count
    manifest["pair_summary_count"] = scan.pair_summary_count
    manifest["opportunity_count"] = scan.opportunity_count
    store.save_replay_manifest(manifest)

    return {
        "status": "DESCRIPTIVE_SHADOW",
        "replay_fingerprint": result.fingerprint,
        "symbols": list(scan.symbols),
        "observation_count": scan.observation_count,
        "pair_summary_count": scan.pair_summary_count,
        "opportunity_count": scan.opportunity_count,
        "stored_observations": stored_observations,
        "stored_opportunities": stored_opportunities,
        "summaries": [asdict(summary) for summary in scan.summaries],
        "opportunity_summary": summarize_opportunity_clock(scan.opportunities),
        "automatic_promotion": False,
        "forecast": False,
        "execution": False,
    }
