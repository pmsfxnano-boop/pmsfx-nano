"""Final Crypto release gate v10."""
from pathlib import Path

from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL
from gorila_crypto.quant_store import QuantCryptoStore


def test_scoped_event_metadata_defaults_safely(tmp_path: Path) -> None:
    p = PREREGISTERED_CRYPTO_PROTOCOL
    store = QuantCryptoStore(sqlite_path=str(tmp_path / "crypto.sqlite3"))
    store.register_study(p)
    session = store.start_capture_session(
        study_id=p.study_id,
        protocol_hash=p.protocol_hash,
        provider=p.provider,
        venue=p.venue,
        symbols=p.symbols,
        streams=p.streams,
        region="test",
        instance_id="pytest",
        code_version="v10",
    )
    row = store.append_scoped_event(
        study_id=p.study_id,
        capture_session_id=session,
        symbol="BTCUSDT",
        event_type="trade",
        event_time="2026-09-30T12:00:00+00:00",
        received_time="2026-09-30T12:00:00.001000+00:00",
        source="binance.websocket.trade",
        payload={"p":"100.0","q":"1.0"},
        sequence_start=1,
        sequence_end=1,
    )
    assert row["event_id"]
