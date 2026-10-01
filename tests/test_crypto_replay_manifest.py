from __future__ import annotations

import sqlite3

from gorila_crypto.storage import CryptoStore


def test_replay_manifest_stays_in_crypto_storage(tmp_path) -> None:
    db = tmp_path / "crypto.sqlite3"
    store = CryptoStore(sqlite_path=str(db))
    manifest_id = store.save_replay_manifest(
        {
            "replay_version": "1",
            "order": "ingest",
            "symbol": "BTCUSDT",
            "row_count": 1,
            "first_ledger_seq": 1,
            "last_ledger_seq": 1,
            "fingerprint_sha256": "abc123",
        }
    )
    assert manifest_id

    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT fingerprint_sha256 FROM crypto_replay_manifests "
            "WHERE manifest_id=?",
            (manifest_id,),
        ).fetchone()
        assert row == ("abc123",)
    finally:
        conn.close()
