from gorila_argentum.sources import rava_public_historical_daily


def test_rava_public_parser_extracts_daily_close(monkeypatch):
    html = """
    <html><body>
      <table>
        <tr><th>Fecha</th><th>Apertura</th><th>Máximo</th><th>Mínimo</th><th>Cierre</th><th>Volumen</th></tr>
        <tr><td>25/09/2026</td><td>6.405,00</td><td>6.470,00</td><td>6.270,00</td><td>6.290,00</td><td>1.997.725</td></tr>
        <tr><td>24/09/2026</td><td>6.450,00</td><td>6.495,00</td><td>6.315,00</td><td>6.395,00</td><td>1.959.017</td></tr>
      </table>
    </body></html>
    """

    class FakeResponse:
        text = html

        def raise_for_status(self):
            return None

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            assert url.endswith("/perfil/GGAL")
            return FakeResponse()

    monkeypatch.setattr("gorila_argentum.sources._client", lambda: FakeClient())

    result = rava_public_historical_daily("GGAL", limit_rows=10)

    assert result.error is None
    assert len(result.rows) == 2
    assert result.rows[-1]["value"] == 6395.0
    assert result.rows[-1]["source"] == "RavaPublic/GGAL"
