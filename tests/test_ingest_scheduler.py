from datetime import datetime, timedelta, timezone

from gorila_argentum.ingest import _source_due, _due_sources


class _FakeConn:
    pass


def test_failed_source_uses_retry_backoff(tmp_path, monkeypatch):
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "scheduler.sqlite3"))
    from gorila_argentum.storage import Store

    store = Store()
    store.init()
    source = "RavaPublic/GGAL"
    store.upsert_health(
        source,
        "DEGRADED",
        last_error="test",
        rows=0,
        success=False,
    )

    # The just-recorded failed attempt must not make the autonomous loop
    # immediately hammer the failed provider again.
    assert _source_due(
        store,
        {source},
        success_interval_seconds=86400,
        retry_interval_seconds=3600,
    ) is False


def test_healthy_source_becomes_due_after_success_interval(tmp_path, monkeypatch):
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "scheduler2.sqlite3"))
    from gorila_argentum.storage import Store

    store = Store()
    store.init()
    source = "BYMADATA/GGAL/historical"
    store.upsert_health(source, "HEALTHY", rows=269, success=True)

    conn = store.connect()
    old = (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat()
    conn.execute(
        "UPDATE source_health SET last_success_at=?, last_attempt_at=? WHERE source=?",
        (old, old, source),
    )
    conn.commit()
    conn.close()
    store.conn = None

    assert _source_due(
        store,
        {source},
        success_interval_seconds=6 * 3600,
        retry_interval_seconds=900,
    ) is True


def test_due_sources_are_independent(tmp_path, monkeypatch):
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "scheduler3.sqlite3"))
    from gorila_argentum.storage import Store

    store = Store()
    store.init()
    fresh = "RavaPublic/GGAL"
    stale = "RavaPublic/BMA"
    store.upsert_health(fresh, "HEALTHY", rows=243, success=True)
    store.upsert_health(stale, "DEGRADED", last_error="test", rows=0, success=False)

    conn = store.connect()
    recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    old = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    conn.execute(
        "UPDATE source_health SET last_success_at=?, last_attempt_at=? WHERE source=?",
        (recent, recent, fresh),
    )
    conn.execute(
        "UPDATE source_health SET last_attempt_at=? WHERE source=?",
        (old, stale),
    )
    conn.commit()
    conn.close()
    store.conn = None

    due = _due_sources(
        store,
        {fresh, stale},
        success_interval_seconds=24 * 3600,
        retry_interval_seconds=3600,
    )
    assert due == {stale}


def test_optional_twelve_data_source_uses_independent_backoff(tmp_path, monkeypatch):
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "scheduler3.sqlite3"))
    from gorila_argentum.storage import Store

    store = Store()
    store.init()
    source = "TwelveData/GGAL"
    store.upsert_health(source, "DEGRADED", last_error="rate_limit", rows=0, success=False)

    assert _source_due(
        store,
        {source},
        success_interval_seconds=86400,
        retry_interval_seconds=3600,
    ) is False
