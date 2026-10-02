"""A11 replay data-quality gate tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from gorila_crypto.quality import (
    DataQualityConfig,
    evaluate_replay_quality,
    quality_fingerprint,
)
from gorila_crypto.storage import CryptoStore


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_rows(symbol: str, count: int = 100, *, epoch: int = 1) -> list[dict]:
    rows = []
    for i in range(count):
        t = BASE + timedelta(seconds=i * 40)
        rows.append(
            {
                "ledger_seq": i + 1,
                "event_id": f"{symbol}-{i}",
                "symbol": symbol,
                "source": "binance.websocket.trade",
                "event_type": "trade",
                "event_time": t.isoformat(),
                "received_time": (t + timedelta(milliseconds=10)).isoformat(),
                "metadata": {"ingest_epoch": epoch},
                "payload": {"p": "100.0", "q": "1"},
            }
        )
    return rows


def test_quality_gate_passes_clean_prospective_replay() -> None:
    rows = make_rows("BTCUSDT") + make_rows("ETHUSDT")
    report = evaluate_replay_quality(
        rows,
        replay_fingerprint="fp-good",
        config=DataQualityConfig(
            min_rows_per_symbol=100,
            min_duration_seconds=3600,
            max_p99_transport_latency_ms=100,
        ),
        reference_time=BASE + timedelta(hours=2),
    )
    assert report.status == "PASS"
    assert report.passed is True
    assert report.invalid_timestamp_count == 0
    assert report.future_event_count == 0
    assert report.future_received_count == 0
    assert report.negative_transport_latency_count == 0
    assert report.required_source_gap_count == 0
    assert report.symbol_stats["BTCUSDT"].duration_seconds >= 3600
    assert quality_fingerprint(report)


def test_quality_gate_rejects_future_events_and_required_gaps() -> None:
    rows = make_rows("BTCUSDT")
    future = dict(rows[-1])
    future["event_time"] = (BASE + timedelta(days=2)).isoformat()
    future["received_time"] = (BASE + timedelta(days=2)).isoformat()
    rows[-1] = future
    report = evaluate_replay_quality(
        rows,
        replay_fingerprint="fp-bad",
        config=DataQualityConfig(
            min_rows_per_symbol=100,
            min_duration_seconds=3600,
            max_required_source_gaps=0,
        ),
        gap_rows=[
            {
                "source": "binance.websocket.trade",
                "metadata": {"event_type": "trade"},
            }
        ],
        reference_time=BASE + timedelta(hours=2),
    )
    assert report.status == "FAIL"
    assert "FUTURE_EVENTS" in report.reasons
    assert "REQUIRED_SOURCE_GAPS" in report.reasons


def test_quality_gate_normalizes_replay_order_before_latency_checks() -> None:
    rows = make_rows("BTCUSDT")
    descending = list(reversed(rows))
    report = evaluate_replay_quality(
        descending,
        replay_fingerprint="fp-descending",
        config=DataQualityConfig(
            min_rows_per_symbol=100,
            min_duration_seconds=3600,
            max_receive_time_reversals=0,
        ),
        reference_time=BASE + timedelta(hours=2),
    )
    assert report.status == "PASS"
    assert report.receive_time_reversal_count == 0


def test_quality_gate_detects_receive_time_reversal() -> None:
    rows = make_rows("BTCUSDT")
    rows[50]["received_time"] = (
        BASE + timedelta(seconds=10)
    ).isoformat()
    report = evaluate_replay_quality(
        rows,
        replay_fingerprint="fp-reversal",
        config=DataQualityConfig(
            min_rows_per_symbol=100,
            min_duration_seconds=3600,
            max_receive_time_reversals=0,
        ),
        reference_time=BASE + timedelta(hours=2),
    )
    assert report.status == "FAIL"
    assert report.receive_time_reversal_count >= 1
    assert "RECEIVE_TIME_REVERSAL" in report.reasons


def test_quality_report_persists_with_deterministic_hash(tmp_path) -> None:
    rows = make_rows("BTCUSDT")
    report = evaluate_replay_quality(
        rows,
        replay_fingerprint="fp-storage",
        config=DataQualityConfig(
            min_rows_per_symbol=100,
            min_duration_seconds=3600,
        ),
        reference_time=BASE + timedelta(hours=2),
    )
    digest = quality_fingerprint(report)
    store = CryptoStore(sqlite_path=str(tmp_path / "quality.sqlite3"))
    report_id = store.save_quality_report(
        {
            "status": report.status,
            "replay_fingerprint": report.replay_fingerprint,
            "rows": report.rows,
            "symbols": list(report.symbols),
            "reasons": list(report.reasons),
        },
        digest,
    )
    conn = store.connect()
    try:
        row = conn.execute(
            "SELECT status,report_hash,report_json FROM crypto_quality_reports WHERE report_id=?",
            (report_id,),
        ).fetchone()
        assert row["status"] == "PASS"
        assert row["report_hash"] == digest
        assert "BTCUSDT" in row["report_json"]
    finally:
        conn.close()


def test_quality_gate_rejects_negative_transport_clock() -> None:
    rows = make_rows("BTCUSDT")
    rows[10]["received_time"] = (BASE - timedelta(seconds=1)).isoformat()
    report = evaluate_replay_quality(
        rows,
        replay_fingerprint="fp-negative-latency",
        config=DataQualityConfig(
            min_rows_per_symbol=100,
            min_duration_seconds=3600,
        ),
        reference_time=BASE + timedelta(hours=2),
    )
    assert report.status == "FAIL"
    assert report.negative_transport_latency_count >= 1
    assert "NEGATIVE_TRANSPORT_LATENCY" in report.reasons


def test_quality_gate_rejects_insufficient_required_event_type_coverage() -> None:
    rows = make_rows("BTCUSDT", count=100)
    # One book update is present, but the configured minimum is 10.
    rows.append({
        "ledger_seq": 101,
        "event_id": "BTCUSDT-book-1",
        "symbol": "BTCUSDT",
        "source": "binance.websocket.bookTicker",
        "event_type": "bookTicker",
        "event_time": (BASE + timedelta(seconds=4000)).isoformat(),
        "received_time": (BASE + timedelta(seconds=4000, milliseconds=10)).isoformat(),
        "metadata": {"ingest_epoch": 1},
        "payload": {"b": "100.0", "a": "101.0"},
    })
    report = evaluate_replay_quality(
        rows,
        replay_fingerprint="fp-coverage",
        config=DataQualityConfig(
            min_rows_per_symbol=1,
            min_duration_seconds=0,
            required_event_types=("trade", "bookTicker", "depthUpdate"),
            required_event_type_min_rows={
                "trade": 100,
                "bookTicker": 10,
                "depthUpdate": 10,
            },
        ),
        reference_time=BASE + timedelta(hours=2),
    )
    assert report.status == "FAIL"
    assert report.event_type_counts["trade"] == 100
    assert report.event_type_counts["bookTicker"] == 1
    assert report.event_type_counts.get("depthUpdate", 0) == 0
    assert "EVENT_TYPE_INSUFFICIENT:bookTicker:1<10" in report.reasons
    assert "EVENT_TYPE_INSUFFICIENT:depthUpdate:0<10" in report.reasons
