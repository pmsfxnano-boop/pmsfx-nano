from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gorila_crypto.opportunity_intelligence import (
    AdaptiveOpportunityClock,
    HORIZONS_MS,
    FEATURE_NAMES,
    SymbolMicrostructure,
)


def ts(ms: int) -> datetime:
    return datetime(2026, 10, 1, 2, 0, 0, tzinfo=timezone.utc) + timedelta(milliseconds=ms)


def test_microstructure_snapshot_contains_real_book_features() -> None:
    state = SymbolMicrostructure("BTCUSDT")
    state.feed(
        "bookTicker",
        {"b": "100.0", "a": "100.1", "B": "5.0", "A": "1.0"},
        ts(0),
    )
    state.feed(
        "trade",
        {"p": "100.05", "q": "2.0", "m": False},
        ts(10),
    )
    snapshot = state.snapshot(ts(20))
    assert snapshot["spread_bps"] > 0
    assert snapshot["imbalance"] > 0
    assert snapshot["trade_flow_z"] > 0
    assert snapshot["transport_age_ms"] >= 0


def test_one_second_shock_is_causal_in_receive_time() -> None:
    state = SymbolMicrostructure("BTCUSDT")
    state.feed("trade", {"p": "100.0", "q": "1", "m": False}, ts(0))
    state.feed("trade", {"p": "100.06", "q": "1", "m": False}, ts(1000))
    shock = state.return_over_ms(ts(1000), 100.06, 1000)
    assert shock is not None
    assert shock > 5.0


def test_hazard_probability_is_monotone_and_expected_time_is_bounded() -> None:
    engine = AdaptiveOpportunityClock()
    features = [0.1] * len(FEATURE_NAMES)
    forecast = engine.predict("BTCUSDT", "ETHUSDT", features)
    values = [forecast.probability_by_horizon[h] for h in HORIZONS_MS]
    assert values == sorted(values)
    assert 0.0 <= values[0] <= values[-1] <= 1.0
    assert 0.0 < forecast.expected_reaction_ms <= HORIZONS_MS[-1]


def test_online_updates_change_probability_after_observed_reaction() -> None:
    engine = AdaptiveOpportunityClock()
    base = ts(0)

    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.00", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="b0",
        replay_fingerprint="live:test",
    )
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200.00", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="e0",
        replay_fingerprint="live:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.60", "q": "1", "m": False},
        event_time=ts(1000),
        received_time=ts(1000),
        event_id="b1",
        replay_fingerprint="live:test",
    )
    before = engine.snapshot("BTCUSDT", "ETHUSDT", ts(1000), 6.0)
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200.08", "q": "1", "m": False},
        event_time=ts(1200),
        received_time=ts(1200),
        event_id="e1",
        replay_fingerprint="live:test",
    )
    assert engine.training_updates >= 1
    after = engine.snapshot("BTCUSDT", "ETHUSDT", ts(1200), 6.0)
    assert before["training_updates"] < after["training_updates"]


def test_state_round_trip_preserves_pending_opportunity() -> None:
    engine = AdaptiveOpportunityClock()
    base = ts(0)
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.00", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="b0",
        replay_fingerprint="live:test",
    )
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200.00", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="e0",
        replay_fingerprint="live:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.60", "q": "1", "m": False},
        event_time=ts(1000),
        received_time=ts(1000),
        event_id="b1",
        replay_fingerprint="live:test",
    )
    assert engine.pending
    restored = AdaptiveOpportunityClock.from_state(engine.state())
    assert set(restored.pending) == set(engine.pending)
    assert restored.training_updates == engine.training_updates


def test_warm_replay_does_not_train() -> None:
    engine = AdaptiveOpportunityClock()
    base = ts(0)
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.00", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="b0",
        replay_fingerprint="warm:test",
    )
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200.00", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="e0",
        replay_fingerprint="warm:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.60", "q": "1", "m": False},
        event_time=ts(1000),
        received_time=ts(1000),
        event_id="b1",
        replay_fingerprint="warm:test",
        learn=False,
    )
    assert engine.training_updates == 0


def test_depth_updates_do_not_fake_best_bid_ask() -> None:
    state = SymbolMicrostructure("BTCUSDT")
    state.feed(
        "bookTicker",
        {"b": "100.0", "a": "100.1", "B": "5", "A": "4"},
        ts(0),
    )
    state.feed(
        "depthUpdate",
        {"b": [["99.0", "10"]], "a": [["101.0", "10"]]},
        ts(10),
    )
    snapshot = state.snapshot(ts(20))
    assert snapshot["mid"] == 100.05
    assert snapshot["depth_activity"] > 0


def test_late_target_reaction_after_five_seconds_is_not_learned_as_success() -> None:
    engine = AdaptiveOpportunityClock()
    base = ts(0)
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="e0",
        replay_fingerprint="late:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="b0",
        replay_fingerprint="late:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.60", "q": "1", "m": False},
        event_time=ts(1000),
        received_time=ts(1000),
        event_id="b1",
        replay_fingerprint="late:test",
    )
    assert engine.pending
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200.10", "q": "1", "m": False},
        event_time=ts(6000),
        received_time=ts(6000),
        event_id="e1",
        replay_fingerprint="late:test",
    )
    assert engine.resolved == 0


def test_pairwise_overlap_is_suppressed() -> None:
    engine = AdaptiveOpportunityClock()
    base = ts(0)
    engine.feed_event(
        symbol="ETHUSDT",
        event_type="trade",
        payload={"p": "200", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="e0",
        replay_fingerprint="overlap:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100", "q": "1", "m": False},
        event_time=base,
        received_time=base,
        event_id="b0",
        replay_fingerprint="overlap:test",
    )
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "100.60", "q": "1", "m": False},
        event_time=ts(1000),
        received_time=ts(1000),
        event_id="b1",
        replay_fingerprint="overlap:test",
    )
    started = engine.opportunities_started
    engine.feed_event(
        symbol="BTCUSDT",
        event_type="trade",
        payload={"p": "101.20", "q": "1", "m": False},
        event_time=ts(2000),
        received_time=ts(2000),
        event_id="b2",
        replay_fingerprint="overlap:test",
    )
    assert engine.opportunities_started == started
