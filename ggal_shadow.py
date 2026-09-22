"""Isolated GGAL + Argentina sovereign-risk shadow specialist."""
from __future__ import annotations

import json, math, os, sqlite3, time
from datetime import datetime, timezone
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DB_PATH = Path(os.getenv("GGAL_SHADOW_DB", "/tmp/ggal_shadow.sqlite3"))
GGAL_SYMBOL = "GGAL"
EMBI_URL = "https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais"
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS ggal_shadow_market_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
      event_time TEXT NOT NULL, received_time TEXT NOT NULL,
      close REAL NOT NULL, volume REAL, source TEXT NOT NULL,
      UNIQUE(symbol,event_time,source)
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_risk_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,
      event_time TEXT NOT NULL, received_time TEXT NOT NULL,
      embi_bps REAL NOT NULL, UNIQUE(source,event_time)
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_forecasts (
      id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
      event_time TEXT NOT NULL, model_id TEXT NOT NULL,
      probability_up REAL, direction TEXT, accuracy_oos REAL,
      brier_oos REAL, sample_count INTEGER, status TEXT NOT NULL,
      metadata TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_registry (
      id INTEGER PRIMARY KEY AUTOINCREMENT, registered_at TEXT NOT NULL,
      model_id TEXT NOT NULL, version TEXT NOT NULL,
      status TEXT NOT NULL, mode TEXT NOT NULL,
      validation_type TEXT, engineering_thresholds TEXT NOT NULL,
      metadata TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_source_health (
      source TEXT PRIMARY KEY, status TEXT NOT NULL,
      last_success_at TEXT, last_attempt_at TEXT,
      last_error TEXT, usable_rows INTEGER NOT NULL DEFAULT 0
    );
    """)
    return conn

def fetch_json(url: str) -> dict | list:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"User-Agent":"PMSF-X-Nano-GGAL-Shadow/1.0"})
            with urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise RuntimeError(f"SOURCE_REQUEST_FAILED after {MAX_RETRIES} attempts: {last_error}")

def _set_health(source: str, status: str, *, success_at=None, error=None, usable_rows=None):
    conn = db()
    now = now_iso()
    current = conn.execute("SELECT * FROM ggal_shadow_source_health WHERE source=?", (source,)).fetchone()
    if usable_rows is None:
        usable_rows = int(current["usable_rows"]) if current else 0
    conn.execute("""
      INSERT INTO ggal_shadow_source_health(source,status,last_success_at,last_attempt_at,last_error,usable_rows)
      VALUES(?,?,?,?,?,?)
      ON CONFLICT(source) DO UPDATE SET
        status=excluded.status,last_success_at=COALESCE(excluded.last_success_at,ggal_shadow_source_health.last_success_at),
        last_attempt_at=excluded.last_attempt_at,last_error=excluded.last_error,usable_rows=excluded.usable_rows
    """, (source, status, success_at, now, error, usable_rows))
    conn.commit(); conn.close()

def source_health() -> dict:
    conn = db()
    rows = conn.execute("SELECT source,status,last_success_at,last_attempt_at,last_error,usable_rows FROM ggal_shadow_source_health").fetchall()
    conn.close()
    result = {}
    for row in rows:
        result[row["source"]] = dict(row)
    return result

def capture_market() -> dict:
    received = now_iso()
    source = "TwelveData/GGAL"
    try:
        if not TWELVE_DATA_API_KEY:
            raise RuntimeError("TWELVE_DATA_API_KEY_MISSING")
        params = urlencode({
            "symbol": GGAL_SYMBOL,
            "exchange": "NASDAQ",
            "interval": "1day",
            "outputsize": "5000",
            "apikey": TWELVE_DATA_API_KEY,
        })
        market = fetch_json(f"{TWELVE_DATA_URL}?{params}")
        if not isinstance(market, dict):
            raise RuntimeError("GGAL_PRICE_SOURCE_INVALID")
        if market.get("status") == "error" or market.get("code"):
            raise RuntimeError(f"TWELVE_DATA_ERROR: {market.get('message', market.get('code'))}")
        values = market.get("values") or []
        if not values:
            raise RuntimeError("GGAL_NO_USABLE_ROWS")
        exchange_tz = (market.get("meta") or {}).get("exchange_timezone") or "America/New_York"
        tz = ZoneInfo(exchange_tz)
        prices = []
        for row in values:
            dt_text = row.get("datetime")
            close = row.get("close")
            if dt_text and close is not None:
                local_dt = datetime.strptime(str(dt_text), "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
                event_time = local_dt.astimezone(timezone.utc).isoformat()
                volume = float(row["volume"]) if row.get("volume") not in (None, "") else None
                prices.append((event_time, float(close), volume))
        if not prices:
            raise RuntimeError("GGAL_NO_USABLE_ROWS")
        conn = db()
        for event_time, close, volume in prices:
            conn.execute("INSERT OR IGNORE INTO ggal_shadow_market_events(symbol,event_time,received_time,close,volume,source) VALUES(?,?,?,?,?,?)",
                         (GGAL_SYMBOL,event_time,received,close,volume,"twelvedata_time_series"))
        conn.commit(); conn.close()
        _set_health(source, "HEALTHY", success_at=received, error=None, usable_rows=len(prices))
        return {"source":source,"status":"HEALTHY","events":len(prices),"received_time":received}
    except Exception as exc:
        previous = source_health().get(source, {})
        status = "DEGRADED" if previous.get("usable_rows", 0) > 0 else "INVALID"
        _set_health(source, status, error=str(exc), usable_rows=previous.get("usable_rows", 0))
        return {"source":source,"status":status,"events":0,"error":str(exc),"received_time":received}
"""Isolated GGAL + Argentina sovereign-risk shadow specialist."""
from __future__ import annotations

import json, math, os, sqlite3, time
from datetime import datetime, timezone
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DB_PATH = Path(os.getenv("GGAL_SHADOW_DB", "/tmp/ggal_shadow.sqlite3"))
GGAL_SYMBOL = "GGAL"
EMBI_URL = "https://api.argentinadatos.com/v1/finanzas/indices/riesgo-pais"
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS ggal_shadow_market_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
      event_time TEXT NOT NULL, received_time TEXT NOT NULL,
      close REAL NOT NULL, volume REAL, source TEXT NOT NULL,
      UNIQUE(symbol,event_time,source)
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_risk_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,
      event_time TEXT NOT NULL, received_time TEXT NOT NULL,
      embi_bps REAL NOT NULL, UNIQUE(source,event_time)
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_forecasts (
      id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
      event_time TEXT NOT NULL, model_id TEXT NOT NULL,
      probability_up REAL, direction TEXT, accuracy_oos REAL,
      brier_oos REAL, sample_count INTEGER, status TEXT NOT NULL,
      metadata TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_registry (
      id INTEGER PRIMARY KEY AUTOINCREMENT, registered_at TEXT NOT NULL,
      model_id TEXT NOT NULL, version TEXT NOT NULL,
      status TEXT NOT NULL, mode TEXT NOT NULL,
      validation_type TEXT, engineering_thresholds TEXT NOT NULL,
      metadata TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ggal_shadow_source_health (
      source TEXT PRIMARY KEY, status TEXT NOT NULL,
      last_success_at TEXT, last_attempt_at TEXT,
      last_error TEXT, usable_rows INTEGER NOT NULL DEFAULT 0
    );
    """)
    return conn

def fetch_json(url: str) -> dict | list:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"User-Agent":"PMSF-X-Nano-GGAL-Shadow/1.0"})
            with urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_SECONDS * (2 ** (attempt - 1)))
    raise RuntimeError(f"SOURCE_REQUEST_FAILED after {MAX_RETRIES} attempts: {last_error}")

def _set_health(source: str, status: str, *, success_at=None, error=None, usable_rows=None):
    conn = db()
    now = now_iso()
    current = conn.execute("SELECT * FROM ggal_shadow_source_health WHERE source=?", (source,)).fetchone()
    if usable_rows is None:
        usable_rows = int(current["usable_rows"]) if current else 0
    conn.execute("""
      INSERT INTO ggal_shadow_source_health(source,status,last_success_at,last_attempt_at,last_error,usable_rows)
      VALUES(?,?,?,?,?,?)
      ON CONFLICT(source) DO UPDATE SET
        status=excluded.status,last_success_at=COALESCE(excluded.last_success_at,ggal_shadow_source_health.last_success_at),
        last_attempt_at=excluded.last_attempt_at,last_error=excluded.last_error,usable_rows=excluded.usable_rows
    """, (source, status, success_at, now, error, usable_rows))
    conn.commit(); conn.close()

def source_health() -> dict:
    conn = db()
    rows = conn.execute("SELECT source,status,last_success_at,last_attempt_at,last_error,usable_rows FROM ggal_shadow_source_health").fetchall()
    conn.close()
    result = {}
    for row in rows:
        result[row["source"]] = dict(row)
    return result

def capture_market() -> dict:
    received = now_iso()
    try:
        y = fetch_json(YAHOO_URL + "?range=5y&interval=1d")
        result = (y.get("chart",{}).get("result") or [None])[0]
        if not result or not result.get("timestamp"):
            raise RuntimeError("GGAL_PRICE_SOURCE_INVALID")
        ts = result["timestamp"]
        q = result["indicators"]["quote"][0]
        volumes = q.get("volume", [None] * len(ts))
        prices = []
        for i, t in enumerate(ts):
            close = q["close"][i]
            if close is not None:
                prices.append((datetime.fromtimestamp(t, timezone.utc).isoformat(), float(close), volumes[i]))
        if not prices:
            raise RuntimeError("GGAL_NO_USABLE_ROWS")
        conn = db()
        for event_time, close, volume in prices:
            conn.execute("INSERT OR IGNORE INTO ggal_shadow_market_events(symbol,event_time,received_time,close,volume,source) VALUES(?,?,?,?,?,?)",
                         (GGAL_SYMBOL,event_time,received,close,volume,"yahoo_chart"))
        conn.commit(); conn.close()
        _set_health("Yahoo/GGAL.BA", "HEALTHY", success_at=received, error=None, usable_rows=len(prices))
        return {"source":"Yahoo/GGAL.BA","status":"HEALTHY","events":len(prices),"received_time":received}
    except Exception as exc:
        previous = source_health().get("Yahoo/GGAL.BA", {})
        status = "DEGRADED" if previous.get("usable_rows", 0) > 0 else "INVALID"
        _set_health("Yahoo/GGAL.BA", status, error=str(exc), usable_rows=previous.get("usable_rows", 0))
        return {"source":"Yahoo/GGAL.BA","status":status,"events":0,"error":str(exc),"received_time":received}

def capture_risk() -> dict:
    received = now_iso()
    try:
        risk = fetch_json(EMBI_URL)
        if not isinstance(risk, list):
            raise RuntimeError("EMBI_SOURCE_INVALID")
        usable = [(str(row["fecha"])+"T23:59:59+00:00", float(row["valor"])) for row in risk if row.get("fecha") and row.get("valor") is not None]
        if not usable:
            raise RuntimeError("EMBI_NO_USABLE_ROWS")
        conn = db()
        for event_time, value in usable:
            conn.execute("INSERT OR IGNORE INTO ggal_shadow_risk_events(source,event_time,received_time,embi_bps) VALUES(?,?,?,?)",
                         ("ArgentinaDatos",event_time,received,value))
        conn.commit(); conn.close()
        _set_health("ArgentinaDatos/EMBI+", "HEALTHY", success_at=received, error=None, usable_rows=len(usable))
        return {"source":"ArgentinaDatos/EMBI+","status":"HEALTHY","events":len(usable),"received_time":received}
    except Exception as exc:
        previous = source_health().get("ArgentinaDatos/EMBI+", {})
        status = "DEGRADED" if previous.get("usable_rows", 0) > 0 else "INVALID"
        _set_health("ArgentinaDatos/EMBI+", status, error=str(exc), usable_rows=previous.get("usable_rows", 0))
        return {"source":"ArgentinaDatos/EMBI+","status":status,"events":0,"error":str(exc),"received_time":received}

def capture() -> dict:
    market = capture_market()
    risk = capture_risk()
    return {"market":market,"risk":risk,"health":source_health()}

def sigmoid(z: float) -> float:
    z=max(-30,min(30,z))
    return 1/(1+math.exp(-z))

def train_and_forecast() -> dict:
    conn=db()
    rows=conn.execute("""
      SELECT m.event_time,m.close,r.embi_bps
      FROM ggal_shadow_market_events m
      JOIN ggal_shadow_risk_events r ON substr(m.event_time,1,10)=substr(r.event_time,1,10)
      ORDER BY m.event_time
    """).fetchall()
    conn.close()
    if len(rows)<40:
        return {"status":"EXPERIMENTAL","mode":"SHADOW","validation":"INSUFFICIENT_DATA","sample_count":len(rows),"forecast":None,"accuracy_oos":None,"brier_oos":None}
    data=[]
    for i in range(6,len(rows)-1):
        c0=float(rows[i]["close"]); c1=float(rows[i+1]["close"]); prev=float(rows[i-1]["close"])
        e=float(rows[i]["embi_bps"]); ep=float(rows[i-5]["embi_bps"])
        ret=(c0/prev-1)*10000; embi_delta=e-ep; future=(c1/c0-1)*10000
        data.append(((ret,embi_delta,e),1 if future>0 else 0))
    split=max(20,int(len(data)*0.8)); train,test=data[:split],data[split:]
    if not test or len(set(y for _,y in train))<2:
        return {"status":"EXPERIMENTAL","mode":"SHADOW","validation":"INSUFFICIENT_CLASS_VARIATION","sample_count":len(data),"forecast":None,"accuracy_oos":None,"brier_oos":None}
    means=[sum(x[j] for x,_ in train)/len(train) for j in range(3)]
    scales=[max(1e-6,(sum((x[j]-means[j])**2 for x,_ in train)/len(train))**0.5) for j in range(3)]
    w=[0.0,0.0,0.0]; b=0.0
    for _ in range(220):
        for x,y in train:
            z=b+sum(w[j]*((x[j]-means[j])/scales[j]) for j in range(3)); p=sigmoid(z); e=p-y
            for j in range(3): w[j]-=0.04*e*((x[j]-means[j])/scales[j])
            b-=0.04*e
    probs=[]; labels=[]
    for x,y in test:
        z=b+sum(w[j]*((x[j]-means[j])/scales[j]) for j in range(3)); probs.append(sigmoid(z)); labels.append(y)
    acc=sum((p>=0.5)==bool(y) for p,y in zip(probs,labels))/len(labels)
    brier=sum((p-y)**2 for p,y in zip(probs,labels))/len(labels)
    x,y=data[-1]; p=sigmoid(b+sum(w[j]*((x[j]-means[j])/scales[j]) for j in range(3)))
    forecast={"probability_up":round(p,4),"direction":"UP" if p>=0.5 else "DOWN"}
    conn=db(); meta={"inputs":["GGAL NASDAQ ADR","EMBI+ Argentina"],"thresholds":"PROVISIONAL_ENGINEERING_DEFAULTS_NOT_VALIDATED_ALPHA","sources":{"price":"Twelve Data / GGAL NASDAQ","risk":"ArgentinaDatos / Ámbito"}}
    conn.execute("INSERT INTO ggal_shadow_forecasts(created_at,event_time,model_id,probability_up,direction,accuracy_oos,brier_oos,sample_count,status,metadata) VALUES(?,?,?,?,?,?,?,?,?,?)",(now_iso(),rows[-1]["event_time"],"sovereign-risk-v1",p,forecast["direction"],acc,brier,len(data),"EXPERIMENTAL_SHADOW",json.dumps(meta)))
    conn.execute("INSERT INTO ggal_shadow_registry(registered_at,model_id,version,status,mode,validation_type,engineering_thresholds,metadata) VALUES(?,?,?,?,?,?,?,?)",(now_iso(),"sovereign-risk-v1","v1","EXPERIMENTAL","SHADOW","chronological_oos_holdout","PROVISIONAL_ENGINEERING_DEFAULTS_NOT_VALIDATED_ALPHA",json.dumps(meta)))
    conn.commit(); conn.close()
    return {"status":"EXPERIMENTAL","mode":"SHADOW","validation":"CHRONOLOGICAL_OOS_HOLDOUT","sample_count":len(data),"forecast":forecast,"accuracy_oos":round(acc,4),"brier_oos":round(brier,4)}

def run_cycle():
    capture_result=capture()
    model=train_and_forecast()
    return {"capture":capture_result,"model":model,"triggers_enabled":False}
