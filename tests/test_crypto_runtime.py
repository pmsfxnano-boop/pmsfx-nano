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


def test_symbol_health_accepts_explicit_reference(tmp_path) -> None:
    store = CryptoStore(sqlite_path=str(tmp_path / "symbol-health.sqlite3"))
    ingestor = ProspectiveCryptoIngestor(
        store,
        FakeAdapter([event(trade_id=1)]),
        now=lambda: BASE,
    )
    health = ingestor.symbol_health(now=BASE)
    assert len(health) == 1
    assert health[0]["status"] == "STARTING"
    store.close()


def test_feed_watchdog_restarts_when_one_required_symbol_is_stale(tmp_path) -> None:
    import threading
    import time
    from datetime import timedelta

    store = CryptoStore(sqlite_path=str(tmp_path / "symbol-stale-watchdog.sqlite3"))
    class MultiSymbolFakeAdapter:
        config = BinanceStreamConfig(
            symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
            streams=("trade", "bookTicker"),
        )

    ingestor = ProspectiveCryptoIngestor(
        store,
        MultiSymbolFakeAdapter(),
        now=lambda: BASE + timedelta(seconds=100),
    )
    ingestor._feed_stale_timeout_seconds = 90.0
    ingestor._feed_watchdog_interval_seconds = 0.01
    ingestor._feed_stop_event = threading.Event()
    ingestor._capture_started_at = BASE

    ingestor._symbol_first_received = {
        "BTCUSDT": BASE,
        "ETHUSDT": BASE,
        "SOLUSDT": BASE,
    }
    ingestor._symbol_last_received = {
        "BTCUSDT": BASE + timedelta(seconds=99),
        "ETHUSDT": BASE,
        "SOLUSDT": BASE + timedelta(seconds=98),
    }

    thread = threading.Thread(target=ingestor._feed_watchdog, daemon=True)
    thread.start()
    deadline = time.monotonic() + 2.0
    while not ingestor._feed_stop_event.is_set() and time.monotonic() < deadline:
        time.sleep(0.01)

    assert ingestor._feed_stop_event.is_set()
    assert "ETHUSDT" in (ingestor.last_error or "")

    ingestor.stop_event.set()
    thread.join(timeout=1.0)
    assert not thread.is_alive()
    store.close()


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


def test_production_feed_exception_restarts_with_same_session(tmp_path, monkeypatch) -> None:
    import gorila_crypto.runtime as runtime

    from gorila_crypto.quant_store import QuantCryptoStore

    class FlakyProductionAdapter:
        source_family = "binance.websocket.market"

        def __init__(self, calls: dict[str, int], control: dict[str, object]) -> None:
            self.config = BinanceStreamConfig(
                symbols=tuple(runtime.PREREGISTERED_CRYPTO_PROTOCOL.symbols),
                streams=tuple(runtime.settings.streams),
            )
            self.calls = calls
            self.control = control

        def iter_forever(self, *, stop_event, on_connection):
            self.calls["n"] += 1
            on_connection("CONNECTED", {"at": BASE.isoformat()})
            yield event(trade_id=self.calls["n"])
            if self.calls["n"] == 1:
                raise RuntimeError("simulated transport failure")
            overall_stop = self.control["overall_stop"]
            assert hasattr(overall_stop, "set")
            overall_stop.set()

    calls = {"n": 0}
    control: dict[str, object] = {"overall_stop": None}
    first = FlakyProductionAdapter(calls, control)
    second = FlakyProductionAdapter(calls, control)
    adapters = iter([second])

    store = QuantCryptoStore(sqlite_path=str(tmp_path / "production-restart.sqlite3"))

    monkeypatch.setattr(runtime, "build_market_adapter", lambda: next(adapters))

    ingestor = ProspectiveCryptoIngestor(
        store,
        first,
        now=lambda: BASE,
    )
    control["overall_stop"] = ingestor.stop_event

    result = ingestor.run()

    assert result["status"] == "STOPPED"
    assert result["events_inserted"] == 2
    assert calls["n"] == 2

    conn = store.connect()
    try:
        sessions = conn.execute(
            "SELECT session_id,status FROM crypto_capture_sessions ORDER BY started_at"
        ).fetchall()
        assert len(sessions) == 1
        assert sessions[0]["status"] == "STOPPED"

        metadata_rows = conn.execute(
            "SELECT status,metadata FROM crypto_connection_events ORDER BY created_at"
        ).fetchall()
        statuses = [row["status"] for row in metadata_rows]
        assert "FEED_ERROR" in statuses
        assert "FEED_RESTARTED" in statuses
        run_ids = {
            __import__("json").loads(row["metadata"])["run_id"]
            for row in metadata_rows
            if row["metadata"]
        }
        assert len(run_ids) == 1
    finally:
        conn.close()
        store.close()


def test_runtime_config_rejects_empty_kind() -> None:
    config = IngestRuntimeConfig(kind="   ")
    try:
        config.validate()
    except ValueError as exc:
        assert "runtime kind" in str(exc)
    else:
        raise AssertionError("empty runtime kind was accepted")


def test_terminal_capture_status_fences_running_session_leases(tmp_path) -> None:
    from gorila_crypto.quant_store import QuantCryptoStore

    store = QuantCryptoStore(sqlite_path=str(tmp_path / "lease-fence.sqlite3"))
    store.init()
    conn = store.connect()
    try:
        conn.execute(
            """
            INSERT INTO crypto_capture_sessions(
                session_id,study_id,provider,venue,region,instance_id,
                code_version,symbols_json,streams_json,protocol_hash,
                started_at,ended_at,status,metadata
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "session-1",
                "study-1",
                "binance",
                "BINANCE_SPOT",
                "test",
                "instance",
                "code",
                "[\"BTCUSDT\"]",
                "[\"trade\"]",
                "protocol",
                "2026-09-29T15:00:00+00:00",
                None,
                "RUNNING",
                "{}",
            ),
        )
        conn.execute(
            """
            INSERT INTO crypto_runtime_leases(
                run_id,session_id,started_at,heartbeat_at,status
            ) VALUES(?,?,?,?,?)
            """,
            (
                "run-1",
                "session-1",
                "2026-09-29T15:00:00+00:00",
                "2026-09-29T15:00:01+00:00",
                "RUNNING",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    store.set_capture_session_status("session-1", "ABORTED_STALE")

    conn = store.connect()
    try:
        session = conn.execute(
            "SELECT status,ended_at FROM crypto_capture_sessions WHERE session_id=?",
            ("session-1",),
        ).fetchone()
        lease = conn.execute(
            "SELECT status FROM crypto_runtime_leases WHERE run_id=?",
            ("run-1",),
        ).fetchone()
        assert session["status"] == "ABORTED_STALE"
        assert session["ended_at"] is not None
        assert lease["status"] == "ABORTED_STALE"
    finally:
        conn.close()
        store.close()


def test_start_capture_session_fences_terminal_lease(tmp_path) -> None:
    from gorila_crypto.quant_store import QuantCryptoStore

    store = QuantCryptoStore(sqlite_path=str(tmp_path / "start-capture-fence.sqlite3"))
    store.init()
    conn = store.connect()
    try:
        conn.execute(
            """
            INSERT INTO crypto_studies(
                study_id,protocol_hash,created_at,status,protocol_json
            ) VALUES(?,?,?,?,?)
            """,
            ("study-1", "protocol-1", "2026-09-29T15:00:00+00:00", "REGISTERED", "{}"),
        )
        conn.execute(
            """
            INSERT INTO crypto_capture_sessions(
                session_id,study_id,provider,venue,region,instance_id,
                code_version,symbols_json,streams_json,protocol_hash,
                started_at,ended_at,status,metadata
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "terminal-session",
                "study-1",
                "binance",
                "BINANCE_SPOT",
                "test",
                "instance",
                "old",
                "[\"BTCUSDT\"]",
                "[\"trade\"]",
                "protocol-1",
                "2026-09-29T15:00:00+00:00",
                "2026-09-29T15:01:00+00:00",
                "ABORTED_STALE",
                "{}",
            ),
        )
        conn.execute(
            """
            INSERT INTO crypto_runtime_leases(
                run_id,session_id,started_at,heartbeat_at,status
            ) VALUES(?,?,?,?,?)
            """,
            (
                "terminal-run",
                "terminal-session",
                "2026-09-29T15:00:00+00:00",
                "2026-09-29T15:00:59+00:00",
                "RUNNING",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    new_session = store.start_capture_session(
        study_id="study-1",
        protocol_hash="protocol-1",
        provider="binance",
        venue="BINANCE_SPOT",
        symbols=("BTCUSDT",),
        streams=("trade",),
        region="test",
        instance_id="new-instance",
        code_version="new-code",
    )

    conn = store.connect()
    try:
        lease = conn.execute(
            "SELECT status FROM crypto_runtime_leases WHERE run_id=?",
            ("terminal-run",),
        ).fetchone()
        session = conn.execute(
            "SELECT status FROM crypto_capture_sessions WHERE session_id=?",
            (new_session,),
        ).fetchone()
        assert lease["status"] == "ABORTED_STALE"
        assert session["status"] == "RUNNING"
    finally:
        conn.close()
        store.close()


def test_start_capture_session_is_running_atomically(tmp_path) -> None:
    from gorila_crypto.quant_store import QuantCryptoStore

    store = QuantCryptoStore(sqlite_path=str(tmp_path / "atomic-start.sqlite3"))
    store.init()
    conn = store.connect()
    try:
        conn.execute(
            """
            INSERT INTO crypto_studies(
                study_id,protocol_hash,created_at,status,protocol_json
            ) VALUES(?,?,?,?,?)
            """,
            ("study-atomic", "protocol-atomic", "2026-09-29T15:00:00+00:00", "REGISTERED", "{}"),
        )
        conn.commit()
    finally:
        conn.close()

    session_id = store.start_capture_session(
        study_id="study-atomic",
        protocol_hash="protocol-atomic",
        provider="binance",
        venue="BINANCE_SPOT",
        symbols=("BTCUSDT",),
        streams=("trade",),
        region="test",
        instance_id="atomic-instance",
        code_version="atomic-code",
    )

    conn = store.connect()
    try:
        row = conn.execute(
            "SELECT status,ended_at FROM crypto_capture_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
        assert row["status"] == "RUNNING"
        assert row["ended_at"] is None
    finally:
        conn.close()
        store.close()


def test_reconcile_fences_lease_from_terminal_capture_session(tmp_path) -> None:
    from gorila_crypto.quant_store import QuantCryptoStore

    store = QuantCryptoStore(sqlite_path=str(tmp_path / "reconcile-lease.sqlite3"))
    store.init()
    conn = store.connect()
    try:
        conn.execute(
            """
            INSERT INTO crypto_capture_sessions(
                session_id,study_id,provider,venue,region,instance_id,
                code_version,symbols_json,streams_json,protocol_hash,
                started_at,ended_at,status,metadata
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "session-terminal",
                "study-1",
                "binance",
                "BINANCE_SPOT",
                "test",
                "instance",
                "code",
                "[\"BTCUSDT\"]",
                "[\"trade\"]",
                "protocol",
                "2026-09-29T15:00:00+00:00",
                "2026-09-29T15:00:05+00:00",
                "ABORTED_STALE",
                "{}",
            ),
        )
        conn.execute(
            """
            INSERT INTO crypto_runtime_runs(
                run_id,kind,created_at,status,result
            ) VALUES(?,?,?,?,?)
            """,
            (
                "run-terminal",
                "test",
                "2026-09-29T15:00:00+00:00",
                "RUNNING",
                "{}",
            ),
        )
        conn.execute(
            """
            INSERT INTO crypto_runtime_leases(
                run_id,session_id,started_at,heartbeat_at,status
            ) VALUES(?,?,?,?,?)
            """,
            (
                "run-terminal",
                "session-terminal",
                "2026-09-29T15:00:00+00:00",
                "2026-09-29T15:00:04+00:00",
                "RUNNING",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    store.reconcile_stale_runtime_runs(stale_after_seconds=120.0)

    conn = store.connect()
    try:
        lease = conn.execute(
            "SELECT status FROM crypto_runtime_leases WHERE run_id=?",
            ("run-terminal",),
        ).fetchone()
        runtime = conn.execute(
            "SELECT status FROM crypto_runtime_runs WHERE run_id=?",
            ("run-terminal",),
        ).fetchone()
        assert lease["status"] == "ABORTED_STALE"
        assert runtime["status"] == "ABORTED_STALE"
    finally:
        conn.close()
        store.close()


def test_build_prospective_runtime_fails_closed_without_durable_database(monkeypatch) -> None:
    import gorila_crypto.runtime as runtime

    monkeypatch.setattr(runtime, "CRYPTO_DATABASE_URL", "")
    try:
        runtime.build_prospective_runtime()
    except RuntimeError as exc:
        assert "durable prospective ingestion" in str(exc)
    else:
        raise AssertionError("runtime accepted ephemeral storage")
