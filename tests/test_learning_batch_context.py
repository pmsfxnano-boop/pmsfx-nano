from datetime import datetime, timezone

from gorila_argentum.canonical_data import canonical_content_hash_batch
from gorila_argentum.learning import build_learning_context


def test_batched_learning_context_returns_all_symbols(tmp_path, monkeypatch):
    monkeypatch.setenv("GORILA_SQLITE_PATH", str(tmp_path / "batch-learning.sqlite3"))
    from gorila_argentum.storage import Store

    store = Store()
    store.init()
    symbols = ("GGAL", "BMA", "YPFD")
    for symbol in symbols:
        store.insert_observations([{
            "symbol": symbol,
            "field": "close",
            "value": 100.0,
            "event_time": "2026-09-25T03:00:00+00:00",
            "received_time": datetime.now(timezone.utc).isoformat(),
            "source": "BYMADATA/test",
            "quality": "OK",
        }])

    # No prior learning/shadow state is required; context construction should
    # still produce deterministic cache-key containers for every symbol.
    context = build_learning_context(store, symbols, horizon_days=5)

    assert tuple(context) == symbols
    for symbol in symbols:
        assert context[symbol]["canonical_content_hash"]
        assert context[symbol]["feedback_state"]["sample_count"] == 0
        assert context[symbol]["previous"] is None
