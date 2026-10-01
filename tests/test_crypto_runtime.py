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
    source_family = "binance.websocket.market"

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


def test_ingestor_batches_event_persistence(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "batch-runtime.sqlite3"))
    adapter = FakeAdapter([event(trade_id=i, second=i) for i in range(1, 8)])
    ingestor = ProspectiveCryptoIngestor(
        store,
        adapter,
        config=IngestRuntimeConfig(
            event_batch_size=3,
            event_batch_flush_interval_seconds=60.0,
        ),
    )

    result = ingestor.run()

    assert result["events_inserted"] == 7
    conn = store.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM crypto_events").fetchone()[0] == 7
    finally:
        conn.close()


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
        metadata = conn.execute(
            "SELECT metadata FROM crypto_events WHERE event_type='trade' ORDER BY ledger_seq LIMIT 1"
        ).fetchone()[0]
        assert "transport_latency_seconds" in metadata
        assert "received_age_seconds" in metadata
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
        transport_row = conn.execute(
            "SELECT status FROM crypto_source_health WHERE source=?",
            ("binance.websocket.market",),
        ).fetchone()
        assert transport_row is None
        assert conn.execute(
            "SELECT COUNT(*) FROM crypto_connection_events WHERE source=? AND status='CONNECTED'",
            ("binance.websocket.market",),
        ).fetchone()[0] >= 1
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


def test_symbol_health_requires_fresh_data_for_every_required_symbol(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "symbol-health.sqlite3"))
    adapter = FakeAdapter([event(symbol="BTCUSDT", trade_id=1)])
    adapter.config = BinanceStreamConfig(
        symbols=("BTCUSDT", "ETHUSDT"),
        streams=("trade",),
    )
    clock = {"now": BASE}
    ingestor = ProspectiveCryptoIngestor(
        store,
        adapter,
        now=lambda: clock["now"],
    )
    ingestor._ingest(event(symbol="BTCUSDT", trade_id=1))

    live = ingestor.symbol_health()
    assert live[0]["symbol"] == "BTCUSDT"
    assert live[0]["status"] == "LIVE"
    assert live[1]["symbol"] == "ETHUSDT"
    assert live[1]["status"] == "STARTING"

    clock["now"] = BASE.replace(minute=2, second=1)
    stale = ingestor.symbol_health()
    assert stale[0]["status"] == "DELAYED"
    assert stale[1]["status"] == "NO_DATA"
    assert not all(row["healthy"] for row in stale)


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

def test_spool_drain_replays_fifo_and_deletes_after_success(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "drain.sqlite3"))
    adapter = FakeAdapter([event(trade_id=1)])
    ingestor = ProspectiveCryptoIngestor(store, adapter)
    ingestor.session_id = "session-recovery"
    row = {
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": BASE.isoformat(),
        "received_time": BASE.isoformat(),
        "source": "binance.websocket.trade",
        "payload": {"p": "100.0", "q": "0.01"},
        "quality": "OK",
        "metadata": {},
        "event_key": "recovery-test-key",
    }
    ingestor._evidence_spool.append([row])

    calls = []

    def writer(rows):
        calls.append(rows)
        return [{
            "inserted": True,
            "ledger_seq": 1,
            "event_id": "durable-event-1",
            "event_key": "recovery-test-key",
        }]

    ingestor._write_durable_rows = writer

    assert ingestor._drain_one_spool_batch() is True
    assert len(calls) == 1
    assert calls[0][0]["event_key"] == "recovery-test-key"
    assert ingestor._evidence_spool.stats()["batches"] == 0
    assert ingestor.events_inserted == 1


def test_spool_drain_keeps_batch_on_durable_write_failure(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "drain-failure.sqlite3"))
    adapter = FakeAdapter([event(trade_id=1)])
    ingestor = ProspectiveCryptoIngestor(store, adapter)
    ingestor.session_id = "session-recovery"
    row = {
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": BASE.isoformat(),
        "received_time": BASE.isoformat(),
        "source": "binance.websocket.trade",
        "payload": {"p": "100.0", "q": "0.01"},
        "quality": "OK",
        "metadata": {},
        "event_key": "recovery-failure-key",
    }
    ingestor._evidence_spool.append([row])

    def failing_writer(rows):
        raise RuntimeError("database_unavailable")

    ingestor._write_durable_rows = failing_writer

    assert ingestor._drain_one_spool_batch() is False
    assert ingestor._evidence_spool.stats()["batches"] == 1
    assert ingestor._evidence_spool.peek().rows[0]["event_key"] == "recovery-failure-key"


def test_recovery_gate_blocks_pending_production_evidence(tmp_path) -> None:
    from gorila_crypto.quant_store import QuantCryptoStore

    store = QuantCryptoStore(sqlite_path=str(tmp_path / "gate.sqlite3"))
    adapter = FakeAdapter([event(trade_id=1)])
    ingestor = ProspectiveCryptoIngestor(store, adapter)
    ingestor.session_id = "session-recovery"
    ingestor._evidence_spool.append([{
        "symbol": "BTCUSDT",
        "event_type": "trade",
        "event_time": BASE.isoformat(),
        "received_time": BASE.isoformat(),
        "source": "binance.websocket.trade",
        "payload": {"p": "100.0", "q": "0.01"},
        "quality": "OK",
        "metadata": {},
        "event_key": "gate-pending-key",
    }])

    gate = ingestor.recovery_gate()
    assert gate["status"] == "BLOCKED"
    assert "EVIDENCE_SPOOL_PENDING" in gate["reasons"]


def test_source_health_is_deferred_off_market_event_path(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "deferred-health.sqlite3"))
    adapter = FakeAdapter([event(trade_id=1)])
    ingestor = ProspectiveCryptoIngestor(store, adapter)
    store.init()

    ingestor._ingest(event(trade_id=1))

    conn = store.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM crypto_source_health").fetchone()[0] == 0
    finally:
        conn.close()

    ingestor._flush_source_health(force=True)

    conn = store.connect()
    try:
        row = conn.execute(
            "SELECT status,rows_last_batch FROM crypto_source_health WHERE source=?",
            ("binance.websocket.trade",),
        ).fetchone()
        assert row["status"] == "LIVE"
        assert row["rows_last_batch"] == 1
    finally:
        conn.close()
