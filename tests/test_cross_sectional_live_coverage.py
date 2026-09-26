from gorila_argentum import cross_sectional_live


class FakeStore:
    def init(self):
        return None


def test_cross_sectional_returns_structured_missing_data(monkeypatch):
    data = {symbol: {} for symbol in cross_sectional_live.SYMBOLS}
    data["YPFD"] = {"2026-09-25": 100.0}
    monkeypatch.setattr(
        cross_sectional_live,
        "_series_from_store",
        lambda store, limit=2500: data,
    )
    result = cross_sectional_live.score_universe(FakeStore())
    assert result["status"] == "INSUFFICIENT_DATA"
    assert "GGAL" in result["missing_symbols"]
    assert "YPFD" not in result["missing_symbols"]
    assert result["no_execution_authority"] is True
