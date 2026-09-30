import os

from gorila_argentum.canonical_data import (
    canonical_content_hash,
    canonical_daily_series,
    reconcile_daily_symbol,
)
from gorila_argentum.storage import Store


def _store(tmp_path, monkeypatch, rows):
    db = tmp_path / "canonical.sqlite3"
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(db))
    store = Store()
    store.init()
    store.insert_observations(rows)
    return store


def test_same_session_collision_is_one_reconciled_row(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.0,
                "event_time": "2026-09-25T03:00:00+00:00",
                "received_time": "2026-09-28T12:00:00+00:00",
                "source": "BYMADATA/GGAL/historical",
            },
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.1,
                "event_time": "2026-09-25T04:00:00+00:00",
                "received_time": "2026-09-28T12:00:01+00:00",
                "source": "Rava/GGAL",
            },
        ],
    )
    result = reconcile_daily_symbol(store, "GGAL")
    assert result["accepted"] == 1
    assert result["quarantined"] == 0
    assert canonical_daily_series(store, "GGAL") == [("2026-09-25", 100.0)]


def test_non_primary_vendor_disagreement_goes_to_quarantine(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.0,
                "event_time": "2026-09-25T03:00:00+00:00",
                "received_time": "2026-09-28T12:00:00+00:00",
                "source": "Rava/GGAL",
            },
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 101.0,
                "event_time": "2026-09-25T04:00:00+00:00",
                "received_time": "2026-09-28T12:00:01+00:00",
                "source": "TwelveData/GGAL",
            },
        ],
    )
    result = reconcile_daily_symbol(store, "GGAL")
    assert result["accepted"] == 0
    assert result["quarantined"] == 1
    assert canonical_daily_series(store, "GGAL") == []


def test_daily_session_keeps_declared_primary_source(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.0,
                "event_time": "2026-09-25T03:00:00+00:00",
                "received_time": "2026-09-28T12:00:00+00:00",
                "source": "BYMADATA/GGAL/historical",
            },
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.1,
                "event_time": "2026-09-25T20:00:00+00:00",
                "received_time": "2026-09-28T12:00:01+00:00",
                "source": "Rava/GGAL",
            },
        ],
    )
    reconcile_daily_symbol(store, "GGAL")
    assert canonical_daily_series(store, "GGAL") == [("2026-09-25", 100.0)]


def test_yahoo_is_excluded_from_model_fabric(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 999.0,
                "event_time": "2026-09-25T04:00:00+00:00",
                "received_time": "2026-09-28T12:00:00+00:00",
                "source": "YahooChart/GGAL.BA",
            },
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.0,
                "event_time": "2026-09-25T05:00:00+00:00",
                "received_time": "2026-09-28T12:00:01+00:00",
                "source": "BYMADATA/GGAL/historical",
            },
        ],
    )
    reconcile_daily_symbol(store, "GGAL")
    assert canonical_daily_series(store, "GGAL") == [("2026-09-25", 100.0)]


def test_session_normalization_is_timezone_aware(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.0,
                "event_time": "2026-09-26T00:30:00+00:00",
                "received_time": "2026-09-28T12:00:00+00:00",
                "source": "BYMADATA/GGAL/historical",
            },
        ],
    )
    reconcile_daily_symbol(store, "GGAL")
    assert canonical_daily_series(store, "GGAL") == [("2026-09-25", 100.0)]


def test_canonical_content_hash_changes_with_accepted_value(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [{
            "symbol": "GGAL",
            "field": "close",
            "value": 100.0,
            "event_time": "2026-09-25T03:00:00+00:00",
            "received_time": "2026-09-28T12:00:00+00:00",
            "source": "BYMADATA/GGAL/historical",
        }],
    )
    reconcile_daily_symbol(store, "GGAL")
    first = canonical_content_hash(store, "GGAL")
    assert first

    store.insert_observations([{
        "symbol": "GGAL",
        "field": "close",
        "value": 101.0,
        "event_time": "2026-09-25T04:00:00+00:00",
        "received_time": "2026-09-28T12:01:00+00:00",
        "source": "BYMADATA/GGAL/historical",
    }])
    reconcile_daily_symbol(store, "GGAL")
    second = canonical_content_hash(store, "GGAL")
    assert second
    assert first != second


def test_rava_public_source_is_admitted_to_rava_family(tmp_path, monkeypatch):
    store = _store(
        tmp_path,
        monkeypatch,
        [
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.0,
                "event_time": "2026-09-25T03:00:00+00:00",
                "received_time": "2026-09-28T12:00:00+00:00",
                "source": "BYMADATA/GGAL/historical",
            },
            {
                "symbol": "GGAL",
                "field": "close",
                "value": 100.1,
                "event_time": "2026-09-25T04:00:00+00:00",
                "received_time": "2026-09-28T12:00:01+00:00",
                "source": "RavaPublic/GGAL",
            },
        ],
    )
    result = reconcile_daily_symbol(store, "GGAL")
    assert result["accepted"] == 1
    assert result["quarantined"] == 0
