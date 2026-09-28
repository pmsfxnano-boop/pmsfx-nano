from __future__ import annotations
import re
import time
import httpx
from html.parser import HTMLParser
from datetime import datetime, timezone

from zoneinfo import ZoneInfo
from .config import settings

def now(): return datetime.now(timezone.utc)
def iso(dt): return dt.astimezone(timezone.utc).isoformat()

class SourceResult:
    def __init__(self, source, rows=None, error=None, latency_ms=None):
        self.source=source; self.rows=rows or []; self.error=error; self.latency_ms=latency_ms

class _RavaProfileTableParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._in_td = False

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []
            self._in_td = True

    def handle_data(self, data):
        if self._in_td and self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            value = " ".join("".join(self._cell).split())
            self._row.append(value)
            self._cell = None
            self._in_td = False
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


def _parse_rava_number(value: str) -> float | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def rava_public_historical_daily(symbol: str, limit_rows: int = 120):
    source = f"RavaPublic/{symbol}"
    t0 = time.perf_counter()
    received = now()
    try:
        url = f"https://www.rava.com/perfil/{symbol}"
        with _client() as client:
            response = client.get(url)
            response.raise_for_status()
            html = response.text

        parser = _RavaProfileTableParser()
        parser.feed(html)
        date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")
        rows = []
        for cells in parser.rows:
            if len(cells) < 5 or not date_pattern.match(cells[0]):
                continue
            close = _parse_rava_number(cells[4])
            if close is None or close <= 0:
                continue
            day, month, year = cells[0].split("/")
            local_stamp = f"{year}-{month}-{day}T23:59:59-03:00"
            rows.append(
                {
                    "symbol": str(symbol).upper(),
                    "field": "close",
                    "value": close,
                    "event_time": local_stamp,
                    "received_time": iso(received),
                    "source": source,
                    "latency_ms": (time.perf_counter() - t0) * 1000,
                    "metadata": {
                        "provider": "Rava",
                        "transport": "public_profile_html",
                        "url": url,
                    },
                }
            )
        rows = rows[-max(1, int(limit_rows)):]
        if not rows:
            raise RuntimeError("RAVA_PUBLIC_HISTORICAL_ROWS_NOT_FOUND")
        return SourceResult(source, rows, latency_ms=(time.perf_counter() - t0) * 1000)
    except Exception as e:
        return SourceResult(
            source,
            error=f"{type(e).__name__}: {e}",
            latency_ms=(time.perf_counter() - t0) * 1000,
        )

def _client():
    return httpx.Client(timeout=settings.http_timeout_s, headers={"User-Agent":"Gorila-Argentum/0.1"})

def argentina_datos_fx():
    source="ArgentinaDatos/FX"; t0=time.perf_counter(); received=now()
    try:
        with _client() as c:
            data=c.get(settings.argentina_datos_fx_url); data.raise_for_status(); payload=data.json()
        items=[x for x in payload if isinstance(x,dict)] if isinstance(payload,list) else []
        # The public endpoint returns a long daily history. The runtime only
        # needs a small recent window; the durable research store already
        # contains the historical archive.
        items.sort(key=lambda x: str(x.get("fecha") or ""))
        items=items[-120:]
        rows=[]
        for x in items:
            casa=str(x.get("casa") or "").lower()
            stamp=x.get("fecha") or received.isoformat()
            for field,key in [("buy","compra"),("sell","venta")]:
                val=x.get(key)
                if val is not None:
                    rows.append({"symbol":f"USD_{casa.upper()}","field":field,"value":float(val),"event_time":str(stamp),"received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000,"metadata":{"casa":casa}})
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)
def argentina_datos_risk():
    source="ArgentinaDatos/EMBI+"; t0=time.perf_counter(); received=now()
    try:
        with _client() as c:
            data=c.get(settings.argentina_datos_risk_url); data.raise_for_status(); payload=data.json()
        rows=[]
        for x in payload[-250:] if isinstance(payload,list) else []:
            v=x.get("valor"); d=x.get("fecha")
            if v is not None and d:
                rows.append({"symbol":"EMBI_ARG","field":"embi_bps","value":float(v),"event_time":f"{d}T23:59:59+00:00","received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000})
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)

def bcra_fx():
    source="BCRA/FX"; t0=time.perf_counter(); received=now()
    try:
        with _client() as c:
            data=c.get(settings.bcra_url); data.raise_for_status(); payload=data.json()
        raw=payload.get("results") if isinstance(payload,dict) else []
        results=[raw] if isinstance(raw,dict) else [x for x in raw if isinstance(x,dict)]
        rows=[]
        for item in results:
            stamp=item.get("fecha") or item.get("Fecha") or received.date().isoformat()
            details=item.get("detalle") or item.get("Detalle") or []
            details=[details] if isinstance(details,dict) else [x for x in details if isinstance(x,dict)]
            for detail in details:
                code=str(detail.get("codigoMoneda") or detail.get("CodigoMoneda") or "").upper()
                if code and code != "USD":
                    continue
                value=detail.get("tipoCotizacion") or detail.get("tipoCambio") or detail.get("valor")
                if value is None:
                    continue
                rows.append({
                    "symbol":"USD_BCRA",
                    "field":"reference",
                    "value":float(value),
                    "event_time":str(stamp),
                    "received_time":iso(received),
                    "source":source,
                    "latency_ms":(time.perf_counter()-t0)*1000,
                    "metadata":{"codigoMoneda":code or "USD","description":detail.get("descripcion")},
                })
        if not rows:
            raise RuntimeError("BCRA payload parsed but no USD reference quote found")
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)
def twelve_data_daily(symbol):
    source=f"TwelveData/{symbol}"; t0=time.perf_counter(); received=now()
    if not settings.twelve_data_api_key: return SourceResult(source,error="TWELVE_DATA_API_KEY_MISSING")
    try:
        params={"symbol":symbol,"interval":settings.twelve_data_interval,"outputsize":5000,"apikey":settings.twelve_data_api_key}
        with _client() as c:
            data=c.get("https://api.twelvedata.com/time_series",params=params); data.raise_for_status(); payload=data.json()
        if payload.get("status")=="error": raise RuntimeError(payload.get("message","Twelve Data error"))
        rows=[]
        for x in payload.get("values") or []:
            if x.get("close") is None: continue
            rows.append({"symbol":symbol,"field":"close","value":float(x["close"]),"event_time":str(x["datetime"]),"received_time":iso(received),"source":source,"latency_ms":(time.perf_counter()-t0)*1000})
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=str(e),latency_ms=(time.perf_counter()-t0)*1000)

def twelve_data_intraday(symbol, interval="1min"):
    source=f"TwelveDataLive/{symbol}"; t0=time.perf_counter(); received=now()
    if not settings.twelve_data_api_key:
        return SourceResult(source,error="TWELVE_DATA_API_KEY_MISSING",latency_ms=(time.perf_counter()-t0)*1000)
    try:
        params={
            "symbol": f"{symbol}:BCBA",
            "interval": interval,
            "outputsize": 120,
            "order": "asc",
            "apikey": settings.twelve_data_api_key,
        }
        with _client() as client:
            response=client.get("https://api.twelvedata.com/time_series",params=params)
            response.raise_for_status()
            payload=response.json()
        if payload.get("status") == "error":
            raise RuntimeError(payload.get("message","Twelve Data error"))
        values=payload.get("values") or []
        rows=[]
        for item in values:
            close=item.get("close")
            stamp=item.get("datetime")
            if close is None or not stamp:
                continue
            rows.append({
                "symbol":symbol,
                "field":"close_1m" if interval=="1min" else f"close_{interval}",
                "value":float(close),
                "event_time":str(stamp),
                "received_time":iso(received),
                "source":source,
                "latency_ms":(time.perf_counter()-t0)*1000,
                "metadata":{"interval":interval,"outputsize":120,"venue":"BCBA","provider":"TwelveData"},
            })
        if not rows:
            raise RuntimeError("TWELVE_DATA_NO_INTRADAY_ROWS")
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=f"{type(e).__name__}: {e}",latency_ms=(time.perf_counter()-t0)*1000)

def twelve_data_live_quote(symbol, interval=None):
    source=f"TwelveDataLive/{symbol}"; t0=time.perf_counter(); received=now()
    if not settings.twelve_data_api_key:
        return SourceResult(source,error="TWELVE_DATA_API_KEY_MISSING",latency_ms=(time.perf_counter()-t0)*1000)
    try:
        params={"symbol":symbol,"mic_code":"XBUE","apikey":settings.twelve_data_api_key}
        with _client() as client:
            response=client.get("https://api.twelvedata.com/quote",params=params)
            response.raise_for_status()
            payload=response.json()
        if payload.get("status") == "error":
            raise RuntimeError(payload.get("message","Twelve Data error"))
        price=payload.get("close") or payload.get("last") or payload.get("price")
        if price is None:
            raise RuntimeError("TWELVE_DATA_QUOTE_NO_PRICE")
        stamp=payload.get("datetime") or payload.get("timestamp") or iso(received)
        rows=[{
            "symbol":symbol,
            "field":"close_1m",
            "value":float(price),
            "event_time":str(stamp),
            "received_time":iso(received),
            "source":source,
            "latency_ms":(time.perf_counter()-t0)*1000,
            "metadata":{"endpoint":"quote","venue":"BCBA","mic_code":"XBUE","provider":"TwelveData"},
        }]
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=f"{type(e).__name__}: {e}",latency_ms=(time.perf_counter()-t0)*1000)

def _byma_client():
    """Create an HTTP client for BYMADATA Open Access.

    BYMA's public endpoint is unauthenticated. Some environments reject the
    server certificate chain, so verification is controlled explicitly by
    BYMA_VERIFY_SSL and defaults to false for this public, credential-free feed.
    """
    verify_ssl = str(getattr(settings, "byma_verify_ssl", False)).strip().lower() in {"1", "true", "yes"}
    return httpx.Client(
        timeout=settings.http_timeout_s,
        verify=verify_ssl,
        headers={"User-Agent": "Gorila-Argentum/1.0", "Accept": "application/json"},
    )


def _byma_symbol(symbol: str) -> str:
    return f"{str(symbol).strip().upper()} 24HS"


def byma_historical_daily(symbol: str):
    """Fetch recent daily OHLCV for a BYMA-listed instrument from BYMADATA.

    Endpoint: /chart/historical-series/history
    The public endpoint expects the 24HS settlement suffix for equities.
    """
    source=f"BYMADATA/{symbol}/historical"
    t0=time.perf_counter()
    received=now()
    from datetime import timedelta
    start=received - timedelta(days=max(90, int(getattr(settings, "byma_history_days", 400))))
    end=received + timedelta(days=1)
    params={
        "symbol": _byma_symbol(symbol),
        "resolution": "D",
        "from": str(int(start.timestamp())),
        "to": str(int(end.timestamp())),
    }
    url=f"{settings.byma_open_access_base_url.rstrip('/')}/chart/historical-series/history"
    try:
        with _byma_client() as c:
            response=c.get(url, params=params)
            response.raise_for_status()
            payload=response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("BYMA_HISTORICAL_INVALID_PAYLOAD")
        status=str(payload.get("s") or "").lower()
        if status not in {"ok", "no_data"}:
            raise RuntimeError(f"BYMA_HISTORICAL_STATUS_{status or 'UNKNOWN'}")
        ts=payload.get("t") or []
        opens=payload.get("o") or []
        highs=payload.get("h") or []
        lows=payload.get("l") or []
        closes=payload.get("c") or []
        volumes=payload.get("v") or []
        n=min(len(ts), len(closes))
        rows=[]
        for idx in range(n):
            if closes[idx] is None:
                continue
            stamp=datetime.fromtimestamp(float(ts[idx]), timezone.utc).isoformat()
            metadata={
                "provider":"BYMA",
                "endpoint":"bymadata_free/chart/historical-series/history",
                "resolution":"D",
                "symbol_query":params["symbol"],
            }
            if idx < len(opens): metadata["open"]=opens[idx]
            if idx < len(highs): metadata["high"]=highs[idx]
            if idx < len(lows): metadata["low"]=lows[idx]
            if idx < len(volumes): metadata["volume"]=volumes[idx]
            rows.append({
                "symbol":symbol,
                "field":"close",
                "value":float(closes[idx]),
                "event_time":stamp,
                "received_time":iso(received),
                "source":source,
                "latency_ms":(time.perf_counter()-t0)*1000,
                "metadata":metadata,
            })
        if not rows:
            raise RuntimeError("BYMA_HISTORICAL_NO_ROWS")
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=f"{type(e).__name__}: {e}",latency_ms=(time.perf_counter()-t0)*1000)


def byma_live_panel():
    """Fetch the BYMADATA leading-equity panel in one request.

    Returns one latest observation per core ticker and respects the public feed's
    settlement convention. The caller should rate-limit repeated requests.
    """
    source="BYMADATA/leading-equity"
    t0=time.perf_counter()
    received=now()
    url=f"{settings.byma_open_access_base_url.rstrip('/')}/leading-equity"
    try:
        with _byma_client() as c:
            response=c.post(url, json={"T1": True, "page_size": 100})
            response.raise_for_status()
            payload=response.json()
        data=(payload.get("data") if isinstance(payload, dict) else None) or []
        core=set(settings.core_symbols)
        local_now=received.astimezone(ZoneInfo("America/Argentina/Buenos_Aires"))
        rows=[]
        for item in data:
            symbol=str(item.get("symbol") or "").upper()
            if symbol not in core:
                continue
            price=item.get("trade")
            if price is None or float(price) <= 0:
                price=item.get("closingPrice") or item.get("settlementPrice")
            if price is None or float(price) <= 0:
                continue
            trade_hour=str(item.get("tradeHour") or "").strip()
            try:
                hh,mm,ss=[int(x) for x in trade_hour.split(":")]
                event_local=datetime(local_now.year,local_now.month,local_now.day,hh,mm,ss,tzinfo=local_now.tzinfo)
                event_time=event_local.astimezone(timezone.utc).isoformat()
            except Exception:
                event_time=iso(received)
            rows.append({
                "symbol":symbol,
                "field":"close_1m",
                "value":float(price),
                "event_time":event_time,
                "received_time":iso(received),
                "source":source,
                "latency_ms":(time.perf_counter()-t0)*1000,
                "metadata":{
                    "provider":"BYMA",
                    "endpoint":"bymadata_free/leading-equity",
                    "settlement":"24HS",
                    "trade_hour":trade_hour,
                    "closing_price":item.get("closingPrice"),
                    "previous_closing_price":item.get("previousClosingPrice"),
                    "volume":item.get("volume"),
                    "vwap":item.get("vwap"),
                },
            })
        if not rows:
            raise RuntimeError("BYMA_LIVE_NO_CORE_ROWS")
        return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=f"{type(e).__name__}: {e}",latency_ms=(time.perf_counter()-t0)*1000)

def byma_status():
    source="BYMADATA/OpenAccess"
    t0=time.perf_counter()
    try:
        with _byma_client() as c:
            response=c.get(settings.byma_open_access_url)
            response.raise_for_status()
        return SourceResult(source,rows=[{"symbol":"BYMA_STATUS","field":"status","value":1.0,"event_time":iso(now()),"received_time":iso(now()),"source":source,"latency_ms":(time.perf_counter()-t0)*1000,"metadata":{"provider":"BYMA","endpoint":"open.bymadata.com.ar"}}],latency_ms=(time.perf_counter()-t0)*1000)
    except Exception as e:
        return SourceResult(source,error=f"{type(e).__name__}: {e}",latency_ms=(time.perf_counter()-t0)*1000)

def yahoo_chart_daily(symbol):
    source=f"YahooChart/{symbol}.BA"; t0=time.perf_counter(); received=now()
    ticker=f"{symbol}.BA"
    urls=[
        f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
    ]
    params={"range":"5y","interval":"1d","events":"history"}
    last_error=None
    # Yahoo rate-limits parallel historical pulls. Use bounded retry/backoff and
    # alternate query hosts; ingestion code also serializes these calls.
    for attempt in range(4):
        url=urls[attempt % len(urls)]
        try:
            with _client() as c:
                data=c.get(url,params=params)
                if data.status_code == 429:
                    last_error=f"YAHOO_RATE_LIMIT_429 host={url.split('/')[2]} attempt={attempt+1}"
                    time.sleep(min(8.0, 0.75 * (2 ** attempt)))
                    continue
                data.raise_for_status()
                payload=data.json()
            result=(payload.get("chart",{}).get("result") or [None])[0]
            if not result:
                raise RuntimeError("YAHOO_EMPTY_RESULT")
            timestamps=result.get("timestamp") or []
            quote=((result.get("indicators") or {}).get("quote") or [{}])[0]
            closes=quote.get("close") or []
            rows=[]
            for ts,close in zip(timestamps,closes):
                if close is None: continue
                rows.append({
                    "symbol":symbol,
                    "field":"close",
                    "value":float(close),
                    "event_time":datetime.fromtimestamp(ts,timezone.utc).isoformat(),
                    "received_time":iso(received),
                    "source":source,
                    "latency_ms":(time.perf_counter()-t0)*1000,
                    "metadata":{"interval":"1d","range":"5y","ticker":ticker},
                })
            if not rows:
                raise RuntimeError("YAHOO_NO_USABLE_ROWS")
            return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
        except httpx.HTTPStatusError as e:
            status=e.response.status_code if e.response is not None else None
            last_error=f"HTTP_{status}_{url.split('/')[2]}"
            if status in {429,500,502,503,504}:
                time.sleep(min(8.0, 0.75 * (2 ** attempt)))
                continue
            break
        except Exception as e:
            last_error=f"{type(e).__name__}: {e}"
            if attempt < 3:
                time.sleep(min(4.0, 0.5 * (2 ** attempt)))
                continue
            break
    return SourceResult(source,error=last_error or "YAHOO_HISTORY_FAILED",latency_ms=(time.perf_counter()-t0)*1000)


def yahoo_chart_intraday(symbol, interval="1m"):
    """Fetch the latest intraday chart for a Buenos Aires listing.

    This is a research-data heartbeat, not an execution feed. It is intentionally
    serialized by the caller and returns a bounded snapshot so vendor throttling
    cannot take the API process down.
    """
    source=f"YahooChartLive/{symbol}.BA"; t0=time.perf_counter(); received=now()
    ticker=f"{symbol}.BA"
    urls=[
        f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
    ]
    params={"range":"1d","interval":interval,"events":"history"}
    last_error=None
    for attempt in range(3):
        url=urls[attempt % len(urls)]
        try:
            with _client() as c:
                data=c.get(url,params=params)
                if data.status_code == 429:
                    last_error=f"YAHOO_RATE_LIMIT_429 host={url.split('/')[2]} attempt={attempt+1}"
                    time.sleep(min(5.0, 0.75 * (2 ** attempt)))
                    continue
                data.raise_for_status()
                payload=data.json()
            result=(payload.get("chart",{}).get("result") or [None])[0]
            if not result:
                raise RuntimeError("YAHOO_EMPTY_INTRADAY_RESULT")
            timestamps=result.get("timestamp") or []
            quote=((result.get("indicators") or {}).get("quote") or [{}])[0]
            closes=quote.get("close") or []
            rows=[]
            for ts,close in zip(timestamps,closes):
                if close is None:
                    continue
                rows.append({
                    "symbol":symbol,
                    "field":"close_1m" if interval == "1m" else f"close_{interval}",
                    "value":float(close),
                    "event_time":datetime.fromtimestamp(ts,timezone.utc).isoformat(),
                    "received_time":iso(received),
                    "source":source,
                    "latency_ms":(time.perf_counter()-t0)*1000,
                    "metadata":{"interval":interval,"range":"1d","ticker":ticker},
                })
            if not rows:
                raise RuntimeError("YAHOO_NO_INTRADAY_ROWS")
            return SourceResult(source,rows,latency_ms=(time.perf_counter()-t0)*1000)
        except httpx.HTTPStatusError as e:
            status=e.response.status_code if e.response is not None else None
            last_error=f"HTTP_{status}_{url.split('/')[2]}"
            if status in {429,500,502,503,504}:
                time.sleep(min(5.0, 0.75 * (2 ** attempt)))
                continue
            break
        except Exception as e:
            last_error=f"{type(e).__name__}: {e}"
            if attempt < 2:
                time.sleep(min(3.0, 0.5 * (2 ** attempt)))
                continue
            break
    return SourceResult(source,error=last_error or "YAHOO_INTRADAY_FAILED",latency_ms=(time.perf_counter()-t0)*1000)

