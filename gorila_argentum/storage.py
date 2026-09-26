from __future__ import annotations
import json, sqlite3, os, uuid
from datetime import datetime, timezone, timedelta

def _as_iso(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)

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
CREATE TABLE IF NOT EXISTS shadow_predictions (
 id TEXT PRIMARY KEY,
 created_at TEXT NOT NULL,
 symbol TEXT NOT NULL,
 model_version TEXT NOT NULL,
 probability_up DOUBLE PRECISION NOT NULL,
 direction TEXT NOT NULL,
 horizon_seconds INTEGER NOT NULL,
 regime TEXT NOT NULL,
 entry_price DOUBLE PRECISION NOT NULL,
 feature_hash TEXT NOT NULL DEFAULT '',
 status TEXT NOT NULL DEFAULT 'OPEN',
 settled_at TEXT,
 metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_shadow_symbol_time ON shadow_predictions(symbol,created_at);
CREATE INDEX IF NOT EXISTS idx_shadow_status_time ON shadow_predictions(status,created_at);
CREATE TABLE IF NOT EXISTS shadow_outcomes (
 id TEXT PRIMARY KEY,
 prediction_id TEXT NOT NULL,
 observed_at TEXT NOT NULL,
 observed_price DOUBLE PRECISION NOT NULL,
 realized_direction TEXT NOT NULL,
 return_pct DOUBLE PRECISION,
 correct INTEGER,
 brier DOUBLE PRECISION,
 logloss DOUBLE PRECISION,
 metadata TEXT NOT NULL DEFAULT '{}',
 UNIQUE(prediction_id)
);
CREATE INDEX IF NOT EXISTS idx_shadow_outcome_time ON shadow_outcomes(observed_at);
CREATE TABLE IF NOT EXISTS promotion_decisions (
 id TEXT PRIMARY KEY,
 created_at TEXT NOT NULL,
 model_id TEXT NOT NULL,
 version TEXT NOT NULL,
 status TEXT NOT NULL,
 automatic_promotion INTEGER NOT NULL DEFAULT 0,
 reasons TEXT NOT NULL DEFAULT '[]',
 checks TEXT NOT NULL DEFAULT '{}',
 evidence TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_promotion_model_time ON promotion_decisions(model_id,created_at);
CREATE TABLE IF NOT EXISTS learning_runs (
 id TEXT PRIMARY KEY,
 created_at TEXT NOT NULL,
 symbol TEXT NOT NULL,
 horizon_days INTEGER NOT NULL,
 status TEXT NOT NULL,
 dataset_hash TEXT,
 samples INTEGER NOT NULL DEFAULT 0,
 result TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_learning_symbol_time ON learning_runs(symbol,created_at);
CREATE TABLE IF NOT EXISTS calibration_runs (
 id TEXT PRIMARY KEY,
 created_at TEXT NOT NULL,
 model_id TEXT NOT NULL,
 status TEXT NOT NULL,
 intercept DOUBLE PRECISION,
 result TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_calibration_model_time ON calibration_runs(model_id,created_at);
CREATE TABLE IF NOT EXISTS runtime_heartbeats (
 id TEXT PRIMARY KEY,
 created_at TEXT NOT NULL,
 backend TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runtime_heartbeats_time ON runtime_heartbeats(created_at);
CREATE TABLE IF NOT EXISTS runtime_runs (
 id TEXT PRIMARY KEY,
 kind TEXT NOT NULL,
 started_at TEXT NOT NULL,
 completed_at TEXT,
 status TEXT NOT NULL,
 result TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_runtime_runs_time ON runtime_runs(started_at);
CREATE TABLE IF NOT EXISTS research_evidence (
 id TEXT PRIMARY KEY,
 run_id TEXT NOT NULL,
 created_at TEXT NOT NULL,
 source TEXT NOT NULL,
 model_id TEXT NOT NULL,
 symbol TEXT NOT NULL,
 horizon_days INTEGER NOT NULL,
 dataset_sha256 TEXT,
 sample_count INTEGER NOT NULL DEFAULT 0,
 oos_samples INTEGER NOT NULL DEFAULT 0,
 outer_folds INTEGER NOT NULL DEFAULT 0,
 accuracy DOUBLE PRECISION,
 brier DOUBLE PRECISION,
 baseline_brier DOUBLE PRECISION,
 brier_skill DOUBLE PRECISION,
 brier_skill_ci_low DOUBLE PRECISION,
 brier_skill_ci_high DOUBLE PRECISION,
 logloss DOUBLE PRECISION,
 rank_ic DOUBLE PRECISION,
 net_return_50bps DOUBLE PRECISION,
 placebo_accuracy_p95 DOUBLE PRECISION,
 pbo DOUBLE PRECISION,
 dsr DOUBLE PRECISION,
 execution_delta_50bps DOUBLE PRECISION,
 stress_pass INTEGER NOT NULL DEFAULT 0,
 data_health INTEGER NOT NULL DEFAULT 0,
 point_in_time INTEGER NOT NULL DEFAULT 0,
 validation_status TEXT NOT NULL,
 validation_reasons TEXT NOT NULL DEFAULT '[]',
 manifest_sha256 TEXT NOT NULL,
 metrics TEXT NOT NULL DEFAULT '{}',
 UNIQUE(run_id,symbol,horizon_days)
);
CREATE INDEX IF NOT EXISTS idx_research_evidence_symbol_horizon ON research_evidence(symbol,horizon_days,created_at);
CREATE INDEX IF NOT EXISTS idx_research_evidence_dataset ON research_evidence(dataset_sha256);
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
            with conn.cursor() as cur:
                cur.execute(SCHEMA)
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_shadow_feature_hash ON shadow_predictions(feature_hash)"
                )
                cur.execute(
                    "ALTER TABLE research_evidence ADD COLUMN IF NOT EXISTS pbo DOUBLE PRECISION"
                )
                cur.execute(
                    "ALTER TABLE research_evidence ADD COLUMN IF NOT EXISTS dsr DOUBLE PRECISION"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_research_evidence_validation ON research_evidence(validation_status,created_at)"
                )
            conn.commit()
        conn.close(); self.conn=None

    def claim_runtime_run(self, run_id: str, kind: str) -> bool:
        now = utc_now()
        conn = self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO runtime_runs(id,kind,started_at,status,result) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING",
                    (run_id, kind, now, "RUNNING", "{}"),
                )
                inserted = cur.rowcount == 1
        else:
            cur.execute(
                "INSERT OR IGNORE INTO runtime_runs(id,kind,started_at,status,result) "
                "VALUES(?,?,?,?,?)",
                (run_id, kind, now, "RUNNING", "{}"),
            )
            inserted = cur.rowcount == 1
        conn.commit()
        conn.close()
        self.conn = None
        return bool(inserted)

    def finish_runtime_run(self, run_id: str, status: str, result: dict) -> None:
        conn = self.connect()
        now = utc_now()
        payload = json.dumps(result, sort_keys=True, default=str)
        if self.pg:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE runtime_runs SET completed_at=%s,status=%s,result=%s WHERE id=%s",
                    (now, status, payload, run_id),
                )
        else:
            conn.execute(
                "UPDATE runtime_runs SET completed_at=?,status=?,result=? WHERE id=?",
                (now, status, payload, run_id),
            )
        conn.commit()
        conn.close()
        self.conn = None

    def latest_runtime_run(self, kind: str | None = None, limit: int = 20):
        conn = self.connect()
        params = []
        if self.pg:
            sql = "SELECT id,kind,started_at,completed_at,status,result FROM runtime_runs"
            if kind is not None:
                sql += " WHERE kind=%s"
                params.append(kind)
            sql += " ORDER BY started_at DESC LIMIT %s"
            params.append(max(1, min(100, int(limit))))
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        else:
            sql = "SELECT id,kind,started_at,completed_at,status,result FROM runtime_runs"
            if kind is not None:
                sql += " WHERE kind=?"
                params.append(kind)
            sql += " ORDER BY started_at DESC LIMIT ?"
            params.append(max(1, min(100, int(limit))))
            rows = conn.execute(sql, tuple(params)).fetchall()
        conn.close()
        self.conn = None
        out = []
        for row in rows:
            item = dict(zip(
                ["id","kind","started_at","completed_at","status","result"], row
            ))
            item["started_at"] = _as_iso(item.get("started_at"))
            item["completed_at"] = _as_iso(item.get("completed_at"))
            try:
                item["result"] = json.loads(item["result"] or "{}")
            except Exception:
                pass
            out.append(item)
        return out

    def verify_persistence(self) -> dict:
        heartbeat_id = uuid.uuid4().hex
        created_at = utc_now()
        backend = "postgres" if self.pg else "sqlite-fallback"

        conn = self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO runtime_heartbeats(id,created_at,backend) VALUES(%s,%s,%s)",
                    (heartbeat_id, created_at, backend),
                )
        else:
            conn.execute(
                "INSERT INTO runtime_heartbeats(id,created_at,backend) VALUES(?,?,?)",
                (heartbeat_id, created_at, backend),
            )
        conn.commit()
        conn.close()
        self.conn = None

        # Verify durability with a fresh connection, not the original transaction.
        check = self.connect()
        if self.pg:
            with check.cursor() as cur:
                cur.execute(
                    "SELECT id,backend FROM runtime_heartbeats WHERE id=%s",
                    (heartbeat_id,),
                )
                row = cur.fetchone()
        else:
            row = check.execute(
                "SELECT id,backend FROM runtime_heartbeats WHERE id=?",
                (heartbeat_id,),
            ).fetchone()
        check.close()
        self.conn = None

        verified = bool(row and row[0] == heartbeat_id and row[1] == backend)
        if not verified:
            raise RuntimeError("persistence_roundtrip_failed")
        return {
            "verified": True,
            "backend": backend,
            "heartbeat_id": heartbeat_id,
            "created_at": created_at,
        }

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
        conn = self.connect()
        fetch_limit = max(int(limit) * 3, int(limit))
        if self.pg:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT event_time,value,source,received_time
                       FROM observations
                       WHERE symbol=%s AND field=%s AND value IS NOT NULL
                       ORDER BY event_time DESC, received_time DESC, source ASC
                       LIMIT %s""",
                    (symbol, field, fetch_limit),
                )
                out = cur.fetchall()
        else:
            out = conn.execute(
                """SELECT event_time,value,source,received_time
                   FROM observations
                   WHERE symbol=? AND field=? AND value IS NOT NULL
                   ORDER BY event_time DESC, received_time DESC, source ASC
                   LIMIT ?""",
                (symbol, field, fetch_limit),
            ).fetchall()
        conn.close(); self.conn = None

        dedup = {}
        for row in out:
            event_time = row[0]
            if event_time not in dedup:
                dedup[event_time] = (event_time, float(row[1]))
            if len(dedup) >= int(limit):
                break
        return list(reversed(list(dedup.values())))

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


    def shadow_exists_by_feature_hash(self, feature_hash: str) -> bool:
        if not feature_hash:
            return False
        conn = self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM shadow_predictions WHERE feature_hash=%s LIMIT 1",
                    (feature_hash,),
                )
                row = cur.fetchone()
        else:
            row = conn.execute(
                "SELECT 1 FROM shadow_predictions WHERE feature_hash=? LIMIT 1",
                (feature_hash,),
            ).fetchone()
        conn.close()
        self.conn = None
        return bool(row)

    def save_shadow_prediction(
        self,
        symbol,
        model_version,
        probability_up,
        horizon_seconds,
        regime,
        entry_price,
        feature_hash="",
        metadata=None,
        created_at=None,
    ):
        if feature_hash and self.shadow_exists_by_feature_hash(feature_hash):
            conn = self.connect()
            if self.pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id,created_at,status FROM shadow_predictions WHERE feature_hash=%s LIMIT 1",
                        (feature_hash,),
                    )
                    row = cur.fetchone()
            else:
                row = conn.execute(
                    "SELECT id,created_at,status FROM shadow_predictions WHERE feature_hash=? LIMIT 1",
                    (feature_hash,),
                ).fetchone()
            conn.close(); self.conn = None
            if row:
                return {"id": row[0], "created_at": _as_iso(row[1]), "status": row[2], "existing": True}

        prediction_id = uuid.uuid4().hex
        conn = self.connect(); now = created_at or utc_now()
        values = (
            prediction_id, now, symbol, model_version, float(probability_up),
            "UP" if float(probability_up) >= 0.5 else "DOWN",
            int(horizon_seconds), regime, float(entry_price), feature_hash or "",
            "OPEN", None, json.dumps(metadata or {})
        )
        if self.pg:
            q = """INSERT INTO shadow_predictions(
                id,created_at,symbol,model_version,probability_up,direction,horizon_seconds,
                regime,entry_price,feature_hash,status,settled_at,metadata)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with conn.cursor() as cur: cur.execute(q, values)
        else:
            conn.execute("""INSERT INTO shadow_predictions(
                id,created_at,symbol,model_version,probability_up,direction,horizon_seconds,
                regime,entry_price,feature_hash,status,settled_at,metadata)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", values)
        conn.commit(); conn.close(); self.conn=None
        return {"id": prediction_id, "created_at": _as_iso(now), "status": "OPEN", "existing": False}

    def get_shadow_prediction(self, prediction_id):
        conn = self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("""SELECT id,created_at,symbol,model_version,probability_up,direction,
                                      horizon_seconds,regime,entry_price,feature_hash,status,settled_at,metadata
                               FROM shadow_predictions WHERE id=%s""", (prediction_id,))
                row = cur.fetchone()
            keys = ["id","created_at","symbol","model_version","probability_up","direction",
                    "horizon_seconds","regime","entry_price","feature_hash","status","settled_at","metadata"]
            out = dict(zip(keys, row)) if row else None
        else:
            row = conn.execute("""SELECT id,created_at,symbol,model_version,probability_up,direction,
                                         horizon_seconds,regime,entry_price,feature_hash,status,settled_at,metadata
                                  FROM shadow_predictions WHERE id=?""",(prediction_id,)).fetchone()
            out = dict(row) if row else None
        conn.close(); self.conn=None
        if out:
            out["created_at"] = _as_iso(out.get("created_at"))
            out["settled_at"] = _as_iso(out.get("settled_at"))
            try: out["metadata"] = json.loads(out["metadata"] or "{}")
            except Exception: pass
        return out

    def settle_shadow_prediction(self, prediction_id, outcome, observed_at, metadata=None):
        prediction = self.get_shadow_prediction(prediction_id)
        if not prediction:
            raise KeyError("shadow_prediction_not_found")
        if prediction["status"] != "OPEN":
            raise ValueError("shadow_prediction_not_open")

        created_value = prediction["created_at"]
        observed_value = observed_at
        created_dt = (
            created_value if isinstance(created_value, datetime)
            else datetime.fromisoformat(str(created_value).replace("Z", "+00:00"))
        )
        observed_dt = (
            observed_value if isinstance(observed_value, datetime)
            else datetime.fromisoformat(str(observed_value).replace("Z", "+00:00"))
        )
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)
        if observed_dt.tzinfo is None:
            observed_dt = observed_dt.replace(tzinfo=timezone.utc)
        observed_dt = observed_dt.astimezone(timezone.utc)
        if observed_dt < created_dt.astimezone(timezone.utc) + timedelta(seconds=int(prediction["horizon_seconds"])):
            raise ValueError("observed_before_horizon")
        observed_at = observed_dt.isoformat()

        outcome_id = uuid.uuid4().hex
        values = (
            outcome_id, prediction_id, observed_at, float(outcome["observed_price"]),
            outcome["realized_direction"], outcome.get("return_pct"),
            None if outcome.get("correct") is None else int(bool(outcome["correct"])),
            outcome.get("brier"), outcome.get("logloss"), json.dumps(metadata or {})
        )
        conn = self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO shadow_outcomes(
                    id,prediction_id,observed_at,observed_price,realized_direction,
                    return_pct,correct,brier,logloss,metadata)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", values)
                cur.execute("UPDATE shadow_predictions SET status=%s,settled_at=%s WHERE id=%s",
                            ("SETTLED", observed_at, prediction_id))
        else:
            conn.execute("""INSERT INTO shadow_outcomes(
                id,prediction_id,observed_at,observed_price,realized_direction,
                return_pct,correct,brier,logloss,metadata)
                VALUES(?,?,?,?,?,?,?,?,?,?)""", values)
            conn.execute("UPDATE shadow_predictions SET status=?,settled_at=? WHERE id=?",
                         ("SETTLED", observed_at, prediction_id))
        conn.commit(); conn.close(); self.conn=None
        return {"id": outcome_id, "prediction_id": prediction_id, "observed_at": observed_at}


    def settle_due_shadow_from_observations(self, *, max_lateness_seconds=3600, limit=50):
        import datetime as _dt

        predictions = self.latest_shadow(status="OPEN", limit=limit)
        settled = []
        for prediction in predictions:
            created = _dt.datetime.fromisoformat(prediction["created_at"].replace("Z", "+00:00"))
            due = created + _dt.timedelta(seconds=int(prediction["horizon_seconds"]))
            conn = self.connect()
            if self.pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT event_time,value
                           FROM observations
                           WHERE symbol=%s AND field='close' AND value IS NOT NULL
                           ORDER BY event_time ASC""",
                        (prediction["symbol"],),
                    )
                    rows = cur.fetchall()
            else:
                rows = conn.execute(
                    """SELECT event_time,value
                       FROM observations
                       WHERE symbol=? AND field='close' AND value IS NOT NULL
                       ORDER BY event_time ASC""",
                    (prediction["symbol"],),
                ).fetchall()
            conn.close(); self.conn = None

            chosen = None
            for event_time, value in rows:
                observed = (
                    event_time
                    if isinstance(event_time, _dt.datetime)
                    else _dt.datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
                )
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=_dt.timezone.utc)
                observed = observed.astimezone(_dt.timezone.utc)
                if observed < due:
                    continue
                if observed > due + _dt.timedelta(seconds=int(max_lateness_seconds)):
                    break
                chosen = (observed, float(value))
                break
            if chosen is None:
                continue

            from .shadow import compute_shadow_outcome
            outcome = compute_shadow_outcome(
                prediction["probability_up"], prediction["entry_price"], chosen[1]
            )
            settled.append(
                self.settle_shadow_prediction(
                    prediction["id"], outcome, chosen[0].isoformat(),
                    metadata={"resolution": "observation_event_time", "field": "close"},
                )
            )
        return {"attempted": len(predictions), "settled": len(settled), "items": settled}

    def latest_shadow(self, symbol=None, status=None, limit=100):
        conn = self.connect()
        where = []; params = []
        if symbol is not None:
            where.append("p.symbol=%s" if self.pg else "p.symbol=?"); params.append(symbol)
        if status is not None:
            where.append("p.status=%s" if self.pg else "p.status=?"); params.append(status)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        limit = int(max(1, min(500, limit)))
        sql = f"""SELECT p.id,p.created_at,p.symbol,p.model_version,p.probability_up,p.direction,
                         p.horizon_seconds,p.regime,p.entry_price,p.feature_hash,p.status,p.settled_at,
                         o.observed_at,o.observed_price,o.realized_direction,o.return_pct,o.correct,o.brier,o.logloss
                  FROM shadow_predictions p
                  LEFT JOIN shadow_outcomes o ON o.prediction_id=p.id
                  {clause} ORDER BY p.created_at DESC LIMIT {"%s" if self.pg else "?"}"""
        params.append(limit)
        keys = ["id","created_at","symbol","model_version","probability_up","direction","horizon_seconds",
                "regime","entry_price","feature_hash","status","settled_at","observed_at","observed_price",
                "realized_direction","return_pct","correct","brier","logloss"]
        if self.pg:
            with conn.cursor() as cur: cur.execute(sql, tuple(params)); rows = cur.fetchall()
            out = [dict(zip(keys,r)) for r in rows]
        else:
            rows = conn.execute(sql, tuple(params)).fetchall()
            out = [dict(r) for r in rows]
        conn.close(); self.conn=None
        return out

    def shadow_summary(self):
        conn = self.connect()
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("""SELECT COUNT(*)::int,
                                      COALESCE(SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END),0)::int
                               FROM shadow_predictions""")
                counts = cur.fetchone()
                cur.execute("""SELECT COUNT(*)::int,
                                      AVG(correct),
                                      AVG(brier),
                                      AVG(logloss),
                                      AVG(return_pct)
                               FROM shadow_outcomes""")
                metrics = cur.fetchone()
        else:
            counts = conn.execute("""SELECT COUNT(*),
                                             COALESCE(SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END),0)
                                      FROM shadow_predictions""").fetchone()
            metrics = conn.execute("""SELECT COUNT(*),AVG(correct),AVG(brier),AVG(logloss),AVG(return_pct)
                                      FROM shadow_outcomes""").fetchone()
        conn.close(); self.conn=None
        return {
            "predictions": int(counts[0]),
            "open": int(counts[1]),
            "settled": int(metrics[0]),
            "accuracy": metrics[1],
            "mean_brier": metrics[2],
            "mean_logloss": metrics[3],
            "mean_return_pct": metrics[4],
        }


    def save_promotion_decision(self, model_id, version, decision):
        decision_id = uuid.uuid4().hex
        conn = self.connect(); now = utc_now()
        values = (
            decision_id, now, model_id, version, decision.get("status","BLOCKED"),
            int(bool(decision.get("automatic_promotion", False))),
            json.dumps(decision.get("reasons", [])),
            json.dumps(decision.get("checks", {})),
            json.dumps(decision.get("evidence", {})),
        )
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO promotion_decisions(
                    id,created_at,model_id,version,status,automatic_promotion,reasons,checks,evidence)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""", values)
        else:
            conn.execute("""INSERT INTO promotion_decisions(
                id,created_at,model_id,version,status,automatic_promotion,reasons,checks,evidence)
                VALUES(?,?,?,?,?,?,?,?,?)""", values)
        conn.commit(); conn.close(); self.conn=None
        return {"id": decision_id, "created_at": now, "status": decision.get("status","BLOCKED")}

    def latest_promotion_decision(self, model_id=None):
        conn=self.connect()
        if self.pg:
            sql="SELECT id,created_at,model_id,version,status,automatic_promotion,reasons,checks,evidence FROM promotion_decisions"
            params=[]
            if model_id is not None:
                sql += " WHERE model_id=%s"; params.append(model_id)
            sql += " ORDER BY created_at DESC LIMIT 1"
            with conn.cursor() as cur:
                cur.execute(sql,tuple(params)); row=cur.fetchone()
        else:
            sql="SELECT id,created_at,model_id,version,status,automatic_promotion,reasons,checks,evidence FROM promotion_decisions"
            params=[]
            if model_id is not None:
                sql += " WHERE model_id=?"; params.append(model_id)
            sql += " ORDER BY created_at DESC LIMIT 1"
            row=conn.execute(sql,tuple(params)).fetchone()
        conn.close(); self.conn=None
        if not row: return None
        out=dict(zip(["id","created_at","model_id","version","status","automatic_promotion","reasons","checks","evidence"],row))
        for key in ("reasons","checks","evidence"):
            try: out[key]=json.loads(out[key] or ("[]" if key=="reasons" else "{}"))
            except Exception: pass
        out["automatic_promotion"]=bool(out["automatic_promotion"])
        return out


    def save_learning_run(self, symbol, horizon_days, result):
        run_id = uuid.uuid4().hex
        conn = self.connect(); now = utc_now()
        values = (
            run_id, now, symbol, int(horizon_days), result.get("status","UNKNOWN"),
            result.get("dataset_hash"), int(result.get("samples",0)), json.dumps(result)
        )
        if self.pg:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO learning_runs(
                    id,created_at,symbol,horizon_days,status,dataset_hash,samples,result)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""", values)
        else:
            conn.execute("""INSERT INTO learning_runs(
                id,created_at,symbol,horizon_days,status,dataset_hash,samples,result)
                VALUES(?,?,?,?,?,?,?,?)""", values)
        conn.commit(); conn.close(); self.conn=None
        return {"id": run_id, "created_at": now, "status": result.get("status","UNKNOWN")}

    def latest_learning(self, symbol=None, limit=20):
        conn=self.connect()
        params=[]
        if self.pg:
            sql="SELECT id,created_at,symbol,horizon_days,status,dataset_hash,samples,result FROM learning_runs"
            if symbol is not None:
                sql += " WHERE symbol=%s"; params.append(symbol)
            sql += " ORDER BY created_at DESC LIMIT %s"; params.append(max(1,min(100,int(limit))))
            with conn.cursor() as cur:
                cur.execute(sql,tuple(params)); rows=cur.fetchall()
        else:
            sql="SELECT id,created_at,symbol,horizon_days,status,dataset_hash,samples,result FROM learning_runs"
            if symbol is not None:
                sql += " WHERE symbol=?"; params.append(symbol)
            sql += " ORDER BY created_at DESC LIMIT ?"; params.append(max(1,min(100,int(limit))))
            rows=conn.execute(sql,tuple(params)).fetchall()
        conn.close(); self.conn=None
        out=[]
        for row in rows:
            item=dict(zip(["id","created_at","symbol","horizon_days","status","dataset_hash","samples","result"],row))
            try: item["result"]=json.loads(item["result"] or "{}")
            except Exception: pass
            out.append(item)
        return out

    def save_calibration_run(self, model_id, result):
        run_id = uuid.uuid4().hex
        conn = self.connect()
        now = utc_now()
        values = (
            run_id,
            now,
            model_id,
            result.get("status", "UNKNOWN"),
            result.get("intercept"),
            json.dumps(result),
        )
        if self.pg:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO calibration_runs(
                        id,created_at,model_id,status,intercept,result)
                        VALUES(%s,%s,%s,%s,%s,%s)""",
                    values,
                )
        else:
            conn.execute(
                """INSERT INTO calibration_runs(
                    id,created_at,model_id,status,intercept,result)
                    VALUES(?,?,?,?,?,?)""",
                values,
            )
        conn.commit()
        conn.close()
        self.conn = None
        return {"id": run_id, "created_at": now, "status": result.get("status", "UNKNOWN")}

    def latest_calibration(self, model_id=None, limit=20):
        conn = self.connect()
        params = []
        if self.pg:
            sql = "SELECT id,created_at,model_id,status,intercept,result FROM calibration_runs"
            if model_id is not None:
                sql += " WHERE model_id=%s"
                params.append(model_id)
            sql += " ORDER BY created_at DESC LIMIT %s"
            params.append(max(1, min(100, int(limit))))
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        else:
            sql = "SELECT id,created_at,model_id,status,intercept,result FROM calibration_runs"
            if model_id is not None:
                sql += " WHERE model_id=?"
                params.append(model_id)
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(max(1, min(100, int(limit))))
            rows = conn.execute(sql, tuple(params)).fetchall()
        conn.close()
        self.conn = None
        out = []
        for row in rows:
            item = dict(zip(["id","created_at","model_id","status","intercept","result"], row))
            try:
                item["result"] = json.loads(item["result"] or "{}")
            except Exception:
                pass
            out.append(item)
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
