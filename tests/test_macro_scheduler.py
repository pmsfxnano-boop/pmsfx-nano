from datetime import datetime, timedelta, timezone

from gorila_argentum.storage import Store
from gorila_argentum.app import _macro_source_due


def test_macro_source_due_uses_durable_freshness(tmp_path, monkeypatch):
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "macro.sqlite3"))
    store = Store()
    store.init()
    source = "BCRA/MonetaryV4"
    store.upsert_health(source, "HEALTHY", rows=594, success=True)
    conn = store.connect()
    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(hours=1)).isoformat()
    stale = (now - timedelta(hours=7)).isoformat()
    conn.execute(
        "UPDATE source_health SET last_success_at=?, last_attempt_at=? WHERE source=?",
        (fresh, fresh, source),
    )
    conn.commit()
    conn.close()
    store.conn = None
    due, reason = _macro_source_due(store, source)
    assert due is False
    assert reason.startswith("FRESH_")

    store = Store()
    store.init()
    conn = store.connect()
    conn.execute(
        "UPDATE source_health SET last_success_at=?, last_attempt_at=? WHERE source=?",
        (stale, stale, source),
    )
    conn.commit()
    conn.close()
    store.conn = None
    due, reason = _macro_source_due(store, source)
    assert due is True
    assert reason.startswith("STALE_")
