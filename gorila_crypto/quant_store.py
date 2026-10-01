                    """,
                    (now, json.dumps({"reason": reason}, sort_keys=True), study_id),
                )
                conn.execute(
                    """
                    UPDATE crypto_capture_sessions
                    SET status='ABORTED_REPLACED', ended_at=?
                    WHERE study_id=? AND status='RUNNING'
                    """,
                    (now, study_id),
                )
            conn.commit()
            return int(revoked)
        finally:
            conn.close()
    def start_capture_session(
        self,
        *,
        study_id: str,
        protocol_hash: str,
        provider: str,
        venue: str,
        symbols: tuple[str, ...],
        streams: tuple[str, ...],
        region: str | None,
        instance_id: str | None,
        code_version: str | None,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT protocol_hash FROM crypto_studies WHERE study_id=%s",
                        (study_id,),
                    )
                    study_row = cur.fetchone()
            else:
                study_row = conn.execute(
                    "SELECT protocol_hash FROM crypto_studies WHERE study_id=?",
                    (study_id,),
                ).fetchone()
            if study_row is None:
                raise RuntimeError("study_not_registered")
            if str(study_row[0]) != protocol_hash:
                raise RuntimeError("protocol_hash_conflict")
        finally:
            conn.close()
        session_id = str(uuid.uuid4())
        stale_after_seconds = 120.0
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.session_id,
                               s.status,
                               s.started_at,
                               COALESCE(MAX(r.heartbeat_at), '') AS heartbeat_at
                        FROM crypto_capture_sessions s
                        LEFT JOIN crypto_runtime_leases r
                          ON r.session_id = s.session_id
                        WHERE s.study_id=%s
                          AND s.status IN ('STARTING','RUNNING')
                        GROUP BY s.session_id,s.status,s.started_at
                        """,
                        (study_id,),
                    )
                    active = cur.fetchone()
                    if active is not None:
                        active_session_id, active_status, started_at, heartbeat_at = active
                        age_reference = heartbeat_at or started_at
                        cur.execute(
                            "SELECT EXTRACT(EPOCH FROM (NOW() - %s::timestamptz))",
                            (age_reference,),
                        )
                        age_seconds = float(cur.fetchone()[0] or 0.0)
                        if age_seconds > stale_after_seconds:
                            cur.execute(
                                """
                                UPDATE crypto_capture_sessions
                                SET status='ABORTED_STALE',ended_at=NOW()::text,
                                    metadata=jsonb_set(
                                        COALESCE(metadata::jsonb,'{}'::jsonb),
                                        '{stale_reconciliation}',
                                        %s::jsonb,
                                        true
                                    )::text
                                WHERE session_id=%s
                                  AND status IN ('STARTING','RUNNING')
                                """,
                                (
                                    json.dumps(
                                        {
                                            "reconciled_at": _utc_now(),
                                            "age_seconds": age_seconds,
                                            "previous_status": active_status,
                                            "reason": "no_recent_runtime_lease_heartbeat",
                                        },
                                        sort_keys=True,
                                    ),
                                    active_session_id,
                                ),
                            )
                            conn.commit()
                        else:
                            conn.rollback()
                            raise RuntimeError(
                                "active_capture_session_exists:"
                                f"session={active_session_id}:status={active_status}:"
                                f"age_seconds={age_seconds:.1f}"
                            )
                    else:
                        conn.rollback()
            else:
                conn.rollback()
        finally:
            conn.close()

        values = (
            session_id,
            study_id,
            provider,
            venue,
            region,
            instance_id,
            code_version,
            json.dumps(list(symbols), sort_keys=True),
            json.dumps(list(streams), sort_keys=True),
            protocol_hash,
            _utc_now(),
            "STARTING",
            json.dumps(dict(metadata or {}), sort_keys=True, default=str),
        )
        conn = self.connect()
        try:
            try:
                if self._pg:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO crypto_capture_sessions(
                                session_id,study_id,provider,venue,region,instance_id,
                                code_version,symbols_json,streams_json,protocol_hash,
                                started_at,status,metadata)
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            values,
                        )
                else:
                    conn.execute(
                        """
                        INSERT INTO crypto_capture_sessions(
                            session_id,study_id,provider,venue,region,instance_id,
                            code_version,symbols_json,streams_json,protocol_hash,
                            started_at,status,metadata)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        values,