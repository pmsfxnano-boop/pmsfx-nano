                            _utc_now() if status not in {"STARTING", "RUNNING"} else None,
                            session_id,
                        ),
                    )
            else:
                conn.execute(
                    "UPDATE crypto_capture_sessions SET status=?, ended_at=? WHERE session_id=?",
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