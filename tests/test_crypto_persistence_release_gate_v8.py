"""Final persistence integrity gate for Crypto cleanroom."""
from pathlib import Path

from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.quant_store import QuantCryptoStore


def test_quant_store_exposes_scoped_cohort_integrity_contract(tmp_path: Path) -> None:
    store = QuantCryptoStore(sqlite_path=str(tmp_path / "crypto.sqlite3"))
    protocol = PREREGISTERED_CRYPTO_PROTOCOL
    store.register_study(protocol)

    session_id = store.start_capture_session(
        study_id=protocol.study_id,
        protocol_hash=protocol.protocol_hash,
        provider=protocol.provider,
        venue=protocol.venue,
        symbols=protocol.symbols,
        streams=protocol.streams,
        region="test",
        instance_id="pytest",
        code_version="release-gate-v8",
    )

    assert store.active_capture_session(protocol.study_id) == session_id
    assert store.scoped_stats(
        study_id=protocol.study_id,
        capture_session_id=session_id,
    )["total_rows"] == 0

    store.set_capture_session_status(session_id, "STOPPED")
    assert store.active_capture_session(protocol.study_id) is None


def test_protocol_hash_is_not_replaceable(tmp_path: Path) -> None:
    store = QuantCryptoStore(sqlite_path=str(tmp_path / "crypto.sqlite3"))
    protocol = PREREGISTERED_CRYPTO_PROTOCOL
    store.register_study(protocol)

    mutated = protocol.canonical_dict() | {"version": "tampered"}
    class TamperedProtocol:
        def validate(self): return None
        def canonical_dict(self): return mutated
        study_id = protocol.study_id
        protocol_hash = "0" * 64

    try:
        store.register_study(TamperedProtocol())
    except RuntimeError as exc:
        assert str(exc) == "protocol_hash_conflict"
    else:
        raise AssertionError("protocol identity was mutable")
