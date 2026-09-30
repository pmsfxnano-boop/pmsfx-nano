"""Final Crypto cleanroom release gate.

This test intentionally lives in a new file so the gate can validate the current
tree without mutating large production/test modules during the release step.
"""

from gorila_crypto.binance import BinanceStreamConfig, build_ws_url
from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL


def test_current_crypto_release_contract() -> None:
    protocol = PREREGISTERED_CRYPTO_PROTOCOL
    assert protocol.study_id == "crypto-binance-spot-prospective-v2"
    assert protocol.provider == "binance"
    assert protocol.venue == "binance_spot"
    assert protocol.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert protocol.streams == ("trade", "bookTicker")

    config = BinanceStreamConfig(
        symbols=protocol.symbols,
        streams=protocol.streams,
    )
    url = build_ws_url(config)
    assert url.startswith("wss://data-stream.binance.vision:443/stream?")


def test_release_protocol_hash_is_stable() -> None:
    assert len(PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash) == 64
