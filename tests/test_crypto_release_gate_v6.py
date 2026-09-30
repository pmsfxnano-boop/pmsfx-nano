"""Final release gate for the current Crypto cleanroom tree."""
from urllib.parse import parse_qs, urlparse

from gorila_crypto.binance import BinanceStreamConfig, build_ws_url
from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL


def test_current_crypto_release_contract() -> None:
    protocol = PREREGISTERED_CRYPTO_PROTOCOL
    assert protocol.provider == "binance"
    assert protocol.venue == "binance_spot"
    assert protocol.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert protocol.streams == ("trade", "bookTicker")

    cfg = BinanceStreamConfig(
        symbols=protocol.symbols,
        streams=protocol.streams,
    )
    query = parse_qs(urlparse(build_ws_url(cfg)).query)
    assert query["streams"][0] == (
        "btcusdt@trade/btcusdt@bookTicker/"
        "ethusdt@trade/ethusdt@bookTicker/"
        "solusdt@trade/solusdt@bookTicker"
    )


def test_protocol_hash_is_stable_shape() -> None:
    assert len(PREREGISTERED_CRYPTO_PROTOCOL.protocol_hash) == 64
