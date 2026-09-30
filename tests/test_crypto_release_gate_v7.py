"""Final current-tree Crypto cleanroom release gate."""
from urllib.parse import parse_qs, urlparse

from gorila_crypto.binance import BinanceStreamConfig, build_ws_url
from gorila_crypto.protocol import PREREGISTERED_CRYPTO_PROTOCOL


def test_current_tree_crypto_contract() -> None:
    p = PREREGISTERED_CRYPTO_PROTOCOL
    assert p.provider == "binance"
    assert p.venue == "binance_spot"
    assert p.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert p.streams == ("trade", "bookTicker")

    cfg = BinanceStreamConfig(symbols=p.symbols, streams=p.streams)
    query = parse_qs(urlparse(build_ws_url(cfg)).query)
    assert query["streams"][0] == (
        "btcusdt@trade/btcusdt@bookTicker/"
        "ethusdt@trade/ethusdt@bookTicker/"
        "solusdt@trade/solusdt@bookTicker"
    )
