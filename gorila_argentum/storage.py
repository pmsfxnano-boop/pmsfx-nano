from __future__ import annotations
import json, sqlite3, os
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
 id BIGSERIAL PRIMARY KEY,
 symbol TEXT NOT NULL,
 field TEXT NOT NULL,
 value DOUBLE PRECISION,
 event_time TEXT NOT NULL,
 received_time TEXT NOT NULL,
 source TEXT NOT NULL,
 latency_ms DOUBLE PRECISION,
 quality TEXT NOT NULL DEFAULT 'OK',
 metadata TEXT NOT NULL DEFAULT '{}',
 UNIQUE(symbol,field,event_time,source)
);
CREATE INDEX IF NOT EXISTS idx_obs_symbol_time ON observations(symbol,event_time);
CREATE INDEX IF NOT EXISTS idx_obs_source_time ON observations(source,event_time);
CREATE TABLE IF NOT EXISTS source_health (
 source TEXT PRIMARY KEY,
 status TEXT NOT NULL,
 last_success_at TEXT,
 last_attempt_at TEXT,
 last_error TEXT,
 rows_last_batch INTEGER NOT NULL DEFAULT 0,
 latency_ms DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS coupling_snapshots (
 id BIGSERIAL PRIMARY KEY,
 created_at TEXT NOT NULL,
 horizon TEXT NOT NULL,
 matrix_json TEXT NOT NULL,
 metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS drift_snapshots (
 id BIGSERIAL PRIMARY KEY,
 created_at TEXT NOT NULL,
 symbol TEXT NOT NULL,
 field TEXT NOT NULL,
 status TEXT NOT NULL,
 reference_n INTEGER NOT NULL,
 current_n INTEGER NOT NULL,
 psi DOUBLE PRECISION,
 ks DOUBLE PRECISION,
 mean_shift_z DOUBLE PRECISION,
 std_ratio DOUBLE PRECISION,
 reference_mean DOUBLE PRECISION,
 current_mean DOUBLE PRECISION,
 reference_window INTEGER NOT NULL,
 current_window INTEGER NOT NULL,
 metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_drift_symbol_field_time ON drift_snapshots(symbol,field,created_at);
"""

def utc_now(): return datetime.now(timezone.utc).isoformat()

class Store:
    def __init__(self):
        self.pg = bool(os.getenv("DATABASE_URL"))
        self.path = os.getenv("GORILA_SQLITE_PATH","/tmp/gorila_argentum.sqlite3")
        self.conn = None

    def connect(self):
        if self.pg:
            import psycopg
            self.conn = psycopg.connect(os.environ["DATABASE_URL"])
            schema=os.getenv("GORILA_DB_SCHEMA","gorila_argentum").strip()
            if not schema.replace("_","").isalnum():
                raise ValueError("invalid_database_schema")
            with self.conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                cur.execute(f'SET search_path TO "{schema}"')
            return self.conn
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        sqlite_schema = (SCHEMA.replace("BIGSERIAL","INTEGER")
                              .replace("DOUBLE PRECISION","REAL"))
        self.conn.executescript(sqlite_schema)
        return self.conn

    def init(self):
        conn=self.connect()
        if self.pg:
            with conn.cursor() as cur: cur.execute(SCHEMA)
            conn.commit()
        conn.close(); self.conn=None

    def insert_observations(self, rows):
        if not rows: return 0
        conn=self.connect()
        values=[(r["symbol"],r["field"],r.get("value"),r["event_time"],r["received_time"],r["source"],r.get("latency_ms"),r.get("quality","OK"),json.dumps(r.get("metadata",{}))) for r in rows]
        if self.pg:
            q="""INSERT INTO observations(symbol,field,value,event_time,received_time,source,latency_ms,quality,metadata)
                 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
            with conn.cursor() as cur: cur.executemany(q,values)
        else:
            q="""INSERT OR IGNORE INTO observations(symbol,field,value,event_time,received_time,source,latency_ms,quality,metadata)
                 VALUES(?,?,?,?,?,?,?,?,?)"""
            conn.executemany(q,values)
        conn.commit(); n=len(rows); conn.close(); self.conn=None; return n

    def upsert_health(self, source, status, last_error=None, rows=0, latency_ms=None, success=False):
        conn=self.connect(); now=utc_now()
        if self.pg:
            q="""INSERT INTO source_health(source,status,last_success_at,last_attempt_at,last_error,rows_last_batch,latency_ms)
                 VALUES(%s,%s,%s,%s,%s,%s,%s)
                 ON CONFLICT(source) DO UPDATE SET
                 status=EXCLUDED.status,
                 last_success_at=COALESCE(EXCLUDED.last_success_at,source_health.last_success_at),
                 last_attempt_at=EXCLUDED.last_attempt_at,last_error=EXCLUDED.last_error,
                 rows_last_batch=EXCLUDED.rows_last_batch,latency_ms=EXCLUDED.latency_ms"""
            with conn.cursor() as cur: cur.execute(q,(source,status,now if success else None,now,last_error,rows,latency_ms))
        else:
            conn.execute("""INSERT INTO source_health(source,status,last_success_at,last_attempt_at,last_error,rows_last_batch,latency_ms)
                            VALUES(?,?,?,?,?,?,?)
                            ON CONFLICT(source) DO UPDATE SET
                            status=excluded.status,
                            last_success_at=COALESCE(excluded.last_success_at,source_health.last_success_at),
                            last_attempt_at=excluded.last_attempt_at,last_error=excluded.last_error,
                            rows_last_batch=excluded.rows_last_batch,latency_ms=excluded.latency_ms""",
                         (source,status,now if success else None,now,last_error,rows,latency_ms))
        conn.commit(); conn.close(); self.conn=None

    def recent_series(self, symbol, field, limit=250):
        conn=self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("SELECT event_time,value FROM observations WHERE symbol=%s AND field=%s AND value IS NOT NULL ORDER BY event_time DESC LIMIT %s",(symbol,field,limit))
                out=cur.fetchall()
        else:
            out=conn.execute("SELECT event_time,value FROM observations WHERE symbol=? AND field=? AND value IS NOT NULL ORDER BY event_time DESC LIMIT ?",(symbol,field,limit)).fetchall()
        conn.close(); self.conn=None
        return list(reversed([(r[0],float(r[1])) for r in out]))

    def save_coupling(self, horizon, matrix, metadata):
        conn=self.connect(); now=utc_now(); payload=json.dumps(matrix)
        if self.pg:
            with conn.cursor() as cur: cur.execute("INSERT INTO coupling_snapshots(created_at,horizon,matrix_json,metadata) VALUES(%s,%s,%s,%s)",(now,horizon,payload,json.dumps(metadata)))
        else:
            conn.execute("INSERT INTO coupling_snapshots(created_at,horizon,matrix_json,metadata) VALUES(?,?,?,?)",(now,horizon,payload,json.dumps(metadata)))
        conn.commit(); conn.close(); self.conn=None

    def save_drift(self, symbol, field, result, metadata=None):
        conn=self.connect(); now=utc_now()
        values=(
            now, symbol, field, result.get("status","UNKNOWN"),
            int(result.get("reference_n",0)), int(result.get("current_n",0)),
            result.get("psi"), result.get("ks"), result.get("mean_shift_z"),
            result.get("std_ratio"), result.get("reference_mean"), result.get("current_mean"),
            int(result.get("reference_window",0)), int(result.get("current_window",0)),
            json.dumps(metadata or {})
        )
        if self.pg:
            q="""INSERT INTO drift_snapshots(
                created_at,symbol,field,status,reference_n,current_n,psi,ks,mean_shift_z,
                std_ratio,reference_mean,current_mean,reference_window,current_window,metadata)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with conn.cursor() as cur: cur.execute(q,values)
        else:
            q="""INSERT INTO drift_snapshots(
                created_at,symbol,field,status,reference_n,current_n,psi,ks,mean_shift_z,
                std_ratio,reference_mean,current_mean,reference_window,current_window,metadata)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            conn.execute(q,values)
        conn.commit(); conn.close(); self.conn=None

    def latest_drift(self, symbol=None, field=None, limit=100):
        conn=self.connect()
        where=[]; params=[]
        if symbol is not None:
            where.append("symbol=%s" if self.pg else "symbol=?")
            params.append(symbol)
        if field is not None:
            where.append("field=%s" if self.pg else "field=?")
            params.append(field)
        clause=(" WHERE "+" AND ".join(where)) if where else ""
        limit=int(max(1,min(500,limit)))
        if self.pg:
            q=f"""SELECT created_at,symbol,field,status,reference_n,current_n,psi,ks,mean_shift_z,
                         std_ratio,reference_mean,current_mean,reference_window,current_window,metadata
                  FROM drift_snapshots{clause}
                  ORDER BY created_at DESC LIMIT %s"""
            params.append(limit)
            with conn.cursor() as cur:
                cur.execute(q,tuple(params)); rows=cur.fetchall()
            keys=["created_at","symbol","field","status","reference_n","current_n","psi","ks","mean_shift_z",
                  "std_ratio","reference_mean","current_mean","reference_window","current_window","metadata"]
            out=[dict(zip(keys,r)) for r in rows]
        else:
            q=f"""SELECT created_at,symbol,field,status,reference_n,current_n,psi,ks,mean_shift_z,
                         std_ratio,reference_mean,current_mean,reference_window,current_window,metadata
                  FROM drift_snapshots{clause}
                  ORDER BY created_at DESC LIMIT ?"""
            params.append(limit)
            rows=conn.execute(q,tuple(params)).fetchall()
            out=[dict(r) for r in rows]
        conn.close(); self.conn=None
        for row in out:
            try: row["metadata"]=json.loads(row["metadata"] or "{}")
            except Exception: pass
        return out

    def health(self):
        conn=self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("SELECT source,status,last_success_at,last_attempt_at,last_error,rows_last_batch,latency_ms FROM source_health ORDER BY source")
                rows=cur.fetchall()
            keys=["source","status","last_success_at","last_attempt_at","last_error","rows_last_batch","latency_ms"]
            out=[dict(zip(keys,r)) for r in rows]
        else:
            rows=conn.execute("SELECT source,status,last_success_at,last_attempt_at,last_error,rows_last_batch,latency_ms FROM source_health ORDER BY source").fetchall()
            out=[dict(r) for r in rows]
        conn.close(); self.conn=None; return out
