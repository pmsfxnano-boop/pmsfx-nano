                        INSERT INTO crypto_capture_sessions(
                            session_id,study_id,provider,venue,region,instance_id,
                            code_version,symbols_json,streams_json,protocol_hash,
                            started_at,status,metadata)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        values,
                    )
                conn.commit()
            except Exception as exc:
                conn.rollback()
                # A preregistered study is single-writer. A second active
                # capture is not allowed because it would contaminate the cohort.
                raise RuntimeError(
                    f"active_capture_session_exists_or_session_creation_failed:{type(exc).__name__}:{exc}"
                ) from exc
        finally:
            conn.close()
        self.set_capture_session_status(session_id, "RUNNING")
        return session_id

    def set_capture_session_status(self, session_id: str, status: str) -> None:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE crypto_capture_sessions SET status=%s, ended_at=%s WHERE session_id=%s AND status IN ('STARTING','RUNNING')",
                        (
                            status,
                            _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                            session_id,
                        ),
                    )
            else:
                conn.execute(
                    "UPDATE crypto_capture_sessions SET status=?, ended_at=? WHERE session_id=? AND status IN ('STARTING','RUNNING')",
                    (
                        status,
                        _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                        session_id,
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def append_scoped_event(self, *, study_id: str, capture_session_id: str, **kwargs: Any) -> dict[str, Any]:
        metadata = dict(kwargs.pop("metadata", None) or {})
        metadata.update(
            {
                "crypto_study_id": study_id,
                "capture_session_id": capture_session_id,
            }
        )
        return super().append_event(metadata=metadata, **kwargs)

    def start_runtime_run_scoped(self, *, kind: str, session_id: str | None = None) -> str:
        run_id = super().start_runtime_run(kind=kind)
        self.init()
        now = _utc_now()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO crypto_runtime_leases(run_id,session_id,started_at,heartbeat_at,status)
                        VALUES(%s,%s,%s,%s,'RUNNING')
                        ON CONFLICT(run_id) DO UPDATE SET