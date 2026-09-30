"""Final persistence/runtime release gate for the current Crypto tree."""
from pathlib import Path

from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.quant_store import QuantCryptoStore


def test_current_crypto_persistence_contract(tmp_path: Path) -> None:
    p = PREREGISTERED_CRYPTO_PROTOCOL
    store = QuantCryptoStore(sqlite_path=str(tmp_path / "crypto.sqlite3"))
    store.register_study(p)
    session_id = store.start_capture_session(
        study_id=p.study_id,
        protocol_hash=p.protocol_hash,
        provider=p.provider,
        venue=p.venue,
        symbols=p.symbols,
        streams=p.streams,
        region="test",
        instance_id="pytest",
        code_version="v9",
    )
    assert store.active_capture_session(p.study_id) == session_id
    assert store.scoped_stats(
        study_id=p.study_id,
        capture_session_id=session_id,
    )["total_rows"] == 0
