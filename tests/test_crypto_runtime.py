"""A9 prospective ingestion runtime tests."""

from __future__ import annotations

from datetime import datetime, timezone

from gorila_crypto.binance import BinanceStreamConfig, NormalizedMarketEvent
from gorila_crypto.runtime import (
    IngestRuntimeConfig,
    ProspectiveCryptoIngestor,
    SequenceContinuityMonitor,
)
from gorila_crypto.storage import CryptoStore


BASE = datetime(2026, 9, 29, 15, 0, tzinfo=timezone.utc)


def event(
    *,
    symbol: str = "BTCUSDT",
    trade_id: int = 1,
    second: int = 0,
) -> NormalizedMarketEvent:
    t = BASE.replace(second=BASE.second + second)
    return NormalizedMarketEvent(
        symbol=symbol,
        event_type="trade",
        event_time=t,
        received_time=t,
        source="binance.websocket.trade",
        payload={
            "e": "trade",
            "s": symbol,
            "t": trade_id,
            "p": "100.0",
            "q": "0.01",
        },
        provider_time=t,
        sequence_start=trade_id,
        sequence_end=trade_id,
        sequence_kind="trade_id",
        receive_time_ns=1_000_000_000 + second,
        quality="OK",
    )


def depth_event(*, first_id: int, final_id: int, second: int = 0) -> NormalizedMarketEvent:
    t = BASE.replace(second=BASE.second + second)
    return NormalizedMarketEvent(
        symbol="BTCUSDT",
        event_type="depthUpdate",
        event_time=t,
        received_time=t,
        source="binance.websocket.depth",
        payload={
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "U": first_id,
            "u": final_id,
            "b": [],
            "a": [],
        },
        provider_time=t,
        sequence_start=first_id,
        sequence_end=final_id,
        sequence_kind="book_update_id",
        receive_time_ns=1_000_000_000 + second,
        quality="OK",
    )


class FakeAdapter:
    def __init__(self, events: list[NormalizedMarketEvent], *, reconnect_events: list[list[NormalizedMarketEvent]] | None = None) -> None:
        self.config = BinanceStreamConfig(symbols=("BTCUSDT",), streams=("trade",))
        self._events = events
        self._reconnect_events = reconnect_events

    def iter_forever(self, *, stop_event, on_connection):
        on_connection("CONNECTED", {"at": BASE.isoformat()})
        for item in self._events:
            yield item
        if self._reconnect_events:
            for batch in self._reconnect_events:
                on_connection("DISCONNECTED", {"at": BASE.isoformat()})
                on_connection("CONNECTED", {"at": BASE.isoformat()})
                for item in batch:
                    yield item


def test_sequence_monitor_flags_depth_gap_only_within_connection() -> None:
    monitor = SequenceContinuityMonitor()
    monitor.new_connection()
    assert monitor.observe(depth_event(first_id=1, final_id=1)) is None
    assert monitor.observe(depth_event(first_id=3, final_id=3)) == (2, 3)

    monitor.new_connection()
    assert monitor.observe(depth_event(first_id=100, final_id=100)) is None


def test_sequence_monitor_does_not_infer_trade_id_gaps() -> None:
    monitor = SequenceContinuityMonitor()
    monitor.new_connection()
    assert monitor.observe(event(trade_id=1)) is None
    assert monitor.observe(event(trade_id=100)) is None


def test_ingestor_persists_events_gaps_and_runtime_result(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "runtime.sqlite3"))
    adapter = FakeAdapter(
        [
            event(trade_id=1),
            depth_event(first_id=1, final_id=1, second=1),
            depth_event(first_id=3, final_id=3, second=2),
        ]
    )
    ingestor = ProspectiveCryptoIngestor(
        store,
        adapter,
        now=lambda: BASE.replace(second=BASE.second + 1),
    )

    result = ingestor.run()

    assert result["status"] == "STREAM_ENDED"
    assert result["events_inserted"] == 3
    assert result["gaps_detected"] == 1
    assert result["automatic_promotion"] is False
    assert result["forecast"] is False
    assert result["execution"] is False

    conn = store.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM crypto_events").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM crypto_data_gaps").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM crypto_connection_events").fetchone()[0] >= 3
        runtime = conn.execute(
            "SELECT status,result FROM crypto_runtime_runs WHERE run_id=?",
            (result["run_id"],),
        ).fetchone()
        assert runtime["status"] == "STREAM_ENDED"
        assert conn.execute(
            "SELECT status FROM crypto_source_health WHERE source=?",
            ("binance.websocket.trade",),
        ).fetchone()["status"] == "LIVE"
    finally:
        conn.close()


def test_ingestor_does_not_infer_continuity_across_reconnect_boundary(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "reconnect.sqlite3"))
    adapter = FakeAdapter(
        [event(trade_id=1)],
        reconnect_events=[[event(trade_id=100, second=1)]],
    )
    result = ProspectiveCryptoIngestor(store, adapter).run()

    assert result["status"] == "STREAM_ENDED"
    assert result["gaps_detected"] == 0
    assert result["events_inserted"] == 2


def test_feed_watchdog_survives_first_restart_request(tmp_path) -> None:
    import threading
    import time

    store = CryptoStore(sqlite_path=str(tmp_path / "watchdog.sqlite3"))
    adapter = FakeAdapter([event(trade_id=1)])
    ingestor = ProspectiveCryptoIngestor(store, adapter)
    ingestor._feed_stale_timeout_seconds = 0.05
    ingestor._feed_watchdog_interval_seconds = 0.01
    ingestor._feed_stop_event = threading.Event()
    ingestor._capture_started_at = BASE
    ingestor._last_event = event(trade_id=1, second=0)
    ingestor._feed_watchdog_grace_until = 0.0

    thread = threading.Thread(target=ingestor._feed_watchdog, daemon=True)
    thread.start()
    deadline = time.monotonic() + 2.0
    while not ingestor._feed_stop_event.is_set() and time.monotonic() < deadline:
        time.sleep(0.01)

    assert ingestor._feed_stop_event.is_set()
    assert thread.is_alive()

    ingestor.stop_event.set()
    thread.join(timeout=1.0)
    assert not thread.is_alive()
    store.close()


def test_runtime_config_rejects_empty_kind() -> None:
    config = IngestRuntimeConfig(kind="   ")
    try:
        config.validate()
    except ValueError as exc:
        assert "runtime kind" in str(exc)
    else:
        raise AssertionError("empty runtime kind was accepted")


def test_build_prospective_runtime_fails_closed_without_durable_database(monkeypatch) -> None:
    import gorila_crypto.runtime as runtime

    monkeypatch.setattr(runtime, "CRYPTO_DATABASE_URL", "")
    try:
        runtime.build_prospective_runtime()
    except RuntimeError as exc:
        assert "durable prospective ingestion" in str(exc)
    else:
        raise AssertionError("runtime accepted ephemeral storage")
