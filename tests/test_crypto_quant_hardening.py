from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gorila_crypto.forecast import DetectionFeatureSnapshot, ForecastTargetSpec
from gorila_crypto.lead_lag import LeadLagConfig, PricePoint, detect_leader_impulses
from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.quant_store import QuantCryptoStore
from gorila_crypto.research_gates import combinatorial_pbo, deflated_sharpe_p_value


def _point(seq: int, symbol: str, seconds: float, price: float, event_id: str) -> PricePoint:
    base = datetime(2026, 9, 30, tzinfo=timezone.utc)
    dt = base + timedelta(seconds=seconds)
    return PricePoint(seq, symbol, dt, dt, price, 1.0, event_id)


def test_preregistered_protocol_is_immutable() -> None:
    assert PREREGISTERED_CRYPTO_PROTOCOL.purge_ms == 5000
    assert PREREGISTERED_CRYPTO_PROTOCOL.embargo_ms == 5000
    assert PREREGISTERED_CRYPTO_PROTOCOL.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert PREREGISTERED_CRYPTO_PROTOCOL.streams == ("trade", "bookTicker")
    assert len(PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash) == 64
    assert PREREGISTERED_CRYPTO_PROTOCOL.matches_runtime(
        provider="binance",
        symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        streams=("trade", "bookTicker"),
    )
    assert not PREREGISTERED_CRYPTO_PROTOCOL.matches_runtime(
        provider="binance",
        symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        streams=("trade", "bookTicker", "depth"),
    )


def test_leader_shocks_are_declustered() -> None:
    series = [
        _point(1, "BTCUSDT", 0.0, 100.0, "a"),
        _point(2, "BTCUSDT", 1.0, 100.0, "b"),
        _point(3, "BTCUSDT", 2.0, 101.0, "c"),
        _point(4, "BTCUSDT", 2.2, 102.0, "d"),
        _point(5, "BTCUSDT", 4.0, 103.0, "e"),
    ]
    shocks = detect_leader_impulses(
        series,
        LeadLagConfig(
            lookback_seconds=1.0,
            shock_min_bps=5.0,
            refractory_seconds=1.0,
        ),
    )
    times = [point.event_time for point, _ in shocks]
    assert len(times) == len(set(times))
    assert all(
        (right - left).total_seconds() >= 1.0
        for left, right in zip(times, times[1:])
    )


def test_cscv_pbo_requires_a_candidate_family() -> None:
    assert combinatorial_pbo([[0.1, 0.2, 0.0]]).status == "INSUFFICIENT_CANDIDATES"


def test_dsr_refuses_short_samples() -> None:
    assert deflated_sharpe_p_value([0.0] * 29, n_trials=36).status == "INSUFFICIENT_DATA"


def test_quant_store_scopes_events_and_runtime_sessions(tmp_path) -> None:
    store = QuantCryptoStore(sqlite_path=str(tmp_path / "crypto.sqlite3"))
    study = PREREGISTERED_CRYPTO_PROTOCOL
    store.register_study(study)
    session_id = store.start_capture_session(
        study_id=study.study_id,
        protocol_hash=study.protocol_hash,
        provider="binance",
        venue="binance_spot",
        symbols=study.symbols,
        streams=study.streams,
        region="test",
        instance_id="pytest",
        code_version="test",
    )
    run_id = store.start_runtime_run_scoped(kind="TEST", session_id=session_id)
    event_id = store.append_scoped_event(
        study_id=study.study_id,
        capture_session_id=session_id,
        symbol="BTCUSDT",
        event_type="trade",
        event_time="2026-09-30T12:00:00+00:00",
        received_time="2026-09-30T12:00:00.001000+00:00",
        source="binance.websocket.trade",
        payload={"p": "100.0", "q": "1.0"},
        sequence_start=1,
        sequence_end=1,
    )["event_id"]
    rows = store.read_scoped_events(
        study_id=study.study_id,
        capture_session_id=session_id,
        source_prefix="binance.websocket.",
    )
    assert rows and rows[0]["event_id"] == event_id
    assert rows[0]["metadata"]["crypto_study_id"] == study.study_id
    assert rows[0]["metadata"]["capture_session_id"] == session_id
    assert store.active_capture_session(study.study_id) == session_id
    store.finish_runtime_run_scoped(
        run_id=run_id,
        session_id=session_id,
        status="STOPPED",
        result={"events_inserted": 1},
    )
    store.set_capture_session_status(session_id, "STOPPED")
    assert store.active_capture_session(study.study_id) is None
