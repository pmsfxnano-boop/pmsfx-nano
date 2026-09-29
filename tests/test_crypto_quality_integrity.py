from __future__ import annotations

from datetime import datetime, timezone

from gorila_crypto.quality import DataQualityConfig, evaluate_replay_quality


def _row(quality: str) -> dict:
    return {
        "symbol": "BTC/USD",
        "event_type": "bookUpdate",
        "event_time": "2026-09-29T20:00:00Z",
        "received_time": "2026-09-29T20:00:00.010000Z",
        "source": "kraken.websocket.book",
        "quality": quality,
        "metadata": {"ingest_epoch": 1},
    }


def test_quality_gate_rejects_unverified_required_book_integrity() -> None:
    report = evaluate_replay_quality(
        [_row("INTEGRITY_UNVERIFIED")],
        replay_fingerprint="x",
        reference_time=datetime(2026, 9, 29, 20, 1, tzinfo=timezone.utc),
        config=DataQualityConfig(
            min_rows_per_symbol=1,
            min_duration_seconds=0,
            required_event_types=("bookUpdate",),
            required_event_type_min_rows={"bookUpdate": 1},
            required_integrity_event_types=("bookUpdate",),
        ),
    )
    assert report.status == "FAIL"
    assert any("INTEGRITY_UNVERIFIED_OR_FAILED:bookUpdate" in r for r in report.reasons)
    assert report.event_quality_counts["bookUpdate"]["INTEGRITY_UNVERIFIED"] == 1


def test_quality_gate_accepts_verified_required_book_integrity() -> None:
    report = evaluate_replay_quality(
        [_row("INTEGRITY_VERIFIED")],
        replay_fingerprint="x",
        reference_time=datetime(2026, 9, 29, 20, 1, tzinfo=timezone.utc),
        config=DataQualityConfig(
            min_rows_per_symbol=1,
            min_duration_seconds=0,
            required_event_types=("bookUpdate",),
            required_event_type_min_rows={"bookUpdate": 1},
            required_integrity_event_types=("bookUpdate",),
        ),
    )
    assert report.status == "PASS"
    assert report.event_quality_counts["bookUpdate"]["INTEGRITY_VERIFIED"] == 1
