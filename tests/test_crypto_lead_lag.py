"""A6 synthetic tests for lead/lag and the Opportunity Clock."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gorila_crypto.lead_lag import (
    LeadLagConfig,
    LeadLagObservation,
    PricePoint,
    build_opportunity_clock,
    build_price_points,
    measure_lead_lag,
    run_lead_lag_shadow,
    summarize_opportunity_clock,
)
from gorila_crypto.storage import CryptoStore


def point(seq: int, symbol: str, t: int, received: int, price: float) -> PricePoint:
    base = datetime(2026, 9, 29, 15, 0, 0, tzinfo=timezone.utc)
    return PricePoint(
        ledger_seq=seq,
        symbol=symbol,
        event_time=base + timedelta(milliseconds=t),
        received_time=base + timedelta(milliseconds=received),
        price=price,
        size=1.0,
        event_id=f'{symbol}-{seq}',
    )


def test_build_price_points_reads_only_trade_payloads() -> None:
    rows = [
        {
            'event_id': '1',
            'ledger_seq': 1,
            'symbol': 'BTCUSDT',
            'event_type': 'trade',
            'event_time': '2026-09-29T15:00:00+00:00',
            'received_time': '2026-09-29T15:00:00+00:00',
            'payload': {'p': '100.0', 'q': '0.1'},
        },
        {
            'event_id': '2',
            'ledger_seq': 2,
            'symbol': 'BTCUSDT',
            'event_type': 'bookTicker',
            'event_time': '2026-09-29T15:00:00.010000+00:00',
            'received_time': '2026-09-29T15:00:00.010000+00:00',
            'payload': {'b': '100', 'a': '101'},
        },
    ]
    points = build_price_points(rows)
    assert list(points) == ['BTCUSDT']
    assert len(points['BTCUSDT']) == 1
    assert points['BTCUSDT'][0].price == 100.0


def test_lead_lag_uses_received_time_for_information_causality() -> None:
    leader = [
        point(1, 'BTCUSDT', 0, 0, 100.00),
        point(2, 'BTCUSDT', 1000, 1000, 100.06),
    ]
    target = [
        point(10, 'ETHUSDT', 0, 0, 200.00),
        point(11, 'ETHUSDT', 1500, 1500, 200.05),
    ]
    config = LeadLagConfig(
        lookback_seconds=1.0,
        shock_min_bps=5.0,
        response_delays_ms=(500,),
    )
    obs, summaries = measure_lead_lag(
        leader, target, config, leader_symbol='BTCUSDT', target_symbol='ETHUSDT'
    )
    assert len(obs) == 1
    assert isinstance(obs[0], LeadLagObservation)
    assert obs[0].market_lag_ms == 500.0
    assert obs[0].information_lag_ms == 500.0
    assert summaries[0].n == 1


def test_late_received_target_cannot_be_used_as_pit_baseline() -> None:
    leader = [
        point(1, 'BTCUSDT', 0, 0, 100.00),
        point(2, 'BTCUSDT', 1000, 1000, 100.06),
    ]
    target = [
        point(10, 'ETHUSDT', 0, 2000, 200.00),
        point(11, 'ETHUSDT', 1500, 2500, 200.05),
    ]
    config = LeadLagConfig(shock_min_bps=5.0, response_delays_ms=(500,))
    obs, _ = measure_lead_lag(leader, target, config)
    assert obs == []


def test_opportunity_clock_tracks_reaction_convergence_and_excursions() -> None:
    leader = [
        point(1, 'BTCUSDT', 0, 0, 100.00),
        point(2, 'BTCUSDT', 1000, 1000, 100.08),
    ]
    target = [
        point(10, 'ETHUSDT', 0, 0, 200.00),
        point(11, 'ETHUSDT', 1200, 1200, 199.96),
        point(12, 'ETHUSDT', 2000, 2000, 200.08),
        point(13, 'ETHUSDT', 3000, 3000, 200.10),
    ]
    config = LeadLagConfig(
        shock_min_bps=5.0,
        reaction_threshold_bps=1.0,
        convergence_fraction=0.5,
        max_response_seconds=5.0,
    )
    result = build_opportunity_clock(
        leader, target, config, 'abc123',
        leader_symbol='BTCUSDT', target_symbol='ETHUSDT'
    )
    assert len(result) == 1
    row = result[0]
    assert row.status == 'CONVERGED'
    assert row.first_reaction_information_lag_ms == 1000.0
    assert row.convergence_information_lag_ms == 2000.0
    assert row.max_adverse_excursion_bps < 0
    assert row.max_favorable_excursion_bps > 0


def test_opportunity_summary_is_descriptive_shadow_only() -> None:
    leader = [point(1, 'BTCUSDT', 0, 0, 100.0), point(2, 'BTCUSDT', 1000, 1000, 100.06)]
    target = [point(10, 'ETHUSDT', 0, 0, 200.0), point(11, 'ETHUSDT', 2000, 2000, 200.04)]
    result = build_opportunity_clock(
        leader, target, LeadLagConfig(shock_min_bps=5.0), 'fingerprint',
        leader_symbol='BTCUSDT', target_symbol='ETHUSDT'
    )
    summary = summarize_opportunity_clock(result)
    assert summary['status'] == 'DESCRIPTIVE_SHADOW'
    assert summary['count'] == 1
    assert 'forecast' not in summary
    assert 'signal' not in summary


def test_storage_persists_a6_shadow_artifacts(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / 'a6.sqlite3'))
    observation = {
        'replay_fingerprint': 'abc',
        'leader_symbol': 'BTCUSDT',
        'target_symbol': 'ETHUSDT',
        'leader_ledger_seq': 10,
        'leader_event_time': '2026-09-29T15:00:01+00:00',
        'leader_received_time': '2026-09-29T15:00:01+00:00',
        'delay_ms': 500,
        'target_ledger_seq': 11,
        'target_event_time': '2026-09-29T15:00:01.5+00:00',
        'target_received_time': '2026-09-29T15:00:01.5+00:00',
        'leader_return_bps': 8.0,
        'target_return_bps': 2.0,
        'signed_target_response_bps': 2.0,
        'market_lag_ms': 500.0,
        'information_lag_ms': 500.0,
    }
    assert store.save_lead_lag_observations([observation]) == 1
    opportunity = {
        'opportunity_id': 'opp-1',
        'replay_fingerprint': 'abc',
        'leader_symbol': 'BTCUSDT',
        'target_symbol': 'ETHUSDT',
        'leader_ledger_seq': 10,
        'direction': 1,
        'leader_return_bps': 8.0,
        'detection_event_time': '2026-09-29T15:00:01+00:00',
        'detection_received_time': '2026-09-29T15:00:01+00:00',
        'baseline_target_price': 200.0,
        'first_reaction_ledger_seq': 11,
        'first_reaction_event_time': '2026-09-29T15:00:01.5+00:00',
        'first_reaction_received_time': '2026-09-29T15:00:01.5+00:00',
        'convergence_ledger_seq': 12,
        'convergence_event_time': '2026-09-29T15:00:02+00:00',
        'convergence_received_time': '2026-09-29T15:00:02+00:00',
        'first_reaction_market_lag_ms': 500.0,
        'first_reaction_information_lag_ms': 500.0,
        'convergence_market_lag_ms': 1000.0,
        'convergence_information_lag_ms': 1000.0,
        'max_favorable_excursion_bps': 3.0,
        'max_adverse_excursion_bps': -2.0,
        'status': 'CONVERGED',
    }
    assert store.save_opportunity_clocks([opportunity]) == 1


def test_pairwise_shadow_scan_never_emits_self_pairs() -> None:
    leader = [
        point(1, "BTCUSDT", 0, 0, 100.0),
        point(2, "BTCUSDT", 1000, 1000, 100.06),
    ]
    target = [
        point(10, "ETHUSDT", 0, 0, 200.0),
        point(11, "ETHUSDT", 2000, 2000, 200.08),
    ]
    rows = []
    for p in [leader[0], leader[1], target[0], target[1]]:
        rows.append(
            {
                "event_id": p.event_id,
                "ledger_seq": p.ledger_seq,
                "symbol": p.symbol,
                "event_type": "trade",
                "event_time": p.event_time.isoformat(),
                "received_time": p.received_time.isoformat(),
                "payload": {"p": str(p.price), "q": "1"},
            }
        )

    scan = run_lead_lag_shadow(
        rows,
        "fingerprint",
        LeadLagConfig(
            shock_min_bps=5.0,
            reaction_threshold_bps=1.0,
            convergence_fraction=0.5,
        ),
        symbols=("BTCUSDT", "ETHUSDT"),
    )
    assert scan.symbols == ("BTCUSDT", "ETHUSDT")
    assert scan.opportunity_count >= 1
    assert all(s.leader_symbol != s.target_symbol for s in scan.summaries)
