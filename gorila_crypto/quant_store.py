                          )
                      )
                    """,
                    (_utc_now(), cutoff),
                )
            conn.commit()
        finally:
            conn.close()
        return int(count)

    def active_capture_session(self, study_id: str) -> str | None:
        self.init()
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT session_id FROM crypto_capture_sessions "
                        "WHERE study_id=%s AND status='RUNNING' ORDER BY started_at DESC LIMIT 1",
                        (study_id,),
                    )
                    row = cur.fetchone()
            else:
                row = conn.execute(
                    "SELECT session_id FROM crypto_capture_sessions "
                    "WHERE study_id=? AND status='RUNNING' ORDER BY started_at DESC LIMIT 1",
                    (study_id,),
                ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()

    def scoped_stats(self, *, study_id: str, capture_session_id: str | None = None) -> dict[str, Any]:
        self.init()
        params: list[Any] = [study_id]
        placeholder = "%s" if self._pg else "?"
        clauses = ["e.metadata::jsonb->>'crypto_study_id'=%s"] if self._pg else [
            "json_extract(e.metadata, '$.crypto_study_id')=?"
        ]
        if capture_session_id is not None:
            clauses.append(
                "e.metadata::jsonb->>'capture_session_id'=%s"
                if self._pg
                else "json_extract(e.metadata, '$.capture_session_id')=?"
            )
            params.append(capture_session_id)
        where = " AND ".join(clauses)
        query = (
            "SELECT e.symbol,e.event_type,count(*) AS rows,"
            "min(e.event_time) AS first_event,max(e.event_time) AS last_event "
            f"FROM crypto_events e WHERE {where} "
            "GROUP BY e.symbol,e.event_type ORDER BY e.symbol,e.event_type"
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
            else:
                rows = conn.execute(query, params).fetchall()
            grouped = [
                {
                    "symbol": str(row[0]),
                    "event_type": str(row[1]),
                    "rows": int(row[2]),
                    "first_event": str(row[3]),
                    "last_event": str(row[4]),
                }
                for row in rows
            ]
            return {
                "study_id": study_id,
                "capture_session_id": capture_session_id,
                "backend": self.backend,
                "event_counts": grouped,
                "total_rows": sum(item["rows"] for item in grouped),
            }
        finally:
            conn.close()

    def read_scoped_events(
        self,
        *,
        study_id: str,
        capture_session_id: str | None = None,
        symbol: str | None = None,
        source: str | None = None,
        source_prefix: str | None = None,
        start_received_time: str | None = None,
        end_received_time: str | None = None,
        start_event_time: str | None = None,
        end_event_time: str | None = None,
        order: str = "ingest",
        limit: int = 100_000,
        include_payload: bool = True,
    ) -> list[dict[str, Any]]:
        """Read exactly one study/session cohort using the immutable metadata scope."""
        self.init()
        if limit < 1:
            raise ValueError("limit must be positive")
        order_by = {
            "ingest": "e.ledger_seq ASC",
            "event_time": "e.event_time ASC, e.received_time ASC, e.ledger_seq ASC",
        }.get(order)
        if order_by is None:
            raise ValueError("invalid replay order")
        params: list[Any] = [study_id]
        clauses = ["e.metadata::jsonb->>'crypto_study_id'=%s"] if self._pg else [
            "json_extract(e.metadata, '$.crypto_study_id')=?"
        ]
        if capture_session_id is not None:
            clauses.append(
                "e.metadata::jsonb->>'capture_session_id'=%s"
                if self._pg
                else "json_extract(e.metadata, '$.capture_session_id')=?"
            )
            params.append(capture_session_id)
        if symbol is not None:
            clauses.append(f"e.symbol={placeholder}")
            params.append(symbol.upper())
        if source is not None:
            clauses.append(f"e.source={placeholder}")
            params.append(source)
        if source_prefix:
            clauses.append(f"e.source LIKE {placeholder}")
            params.append(source_prefix.rstrip("%") + "%")
        if start_received_time is not None:
            clauses.append(f"e.received_time>={placeholder}")
            params.append(start_received_time)
        if end_received_time is not None:
            clauses.append(f"e.received_time<={placeholder}")
            params.append(end_received_time)
        if start_event_time is not None:
            clauses.append(f"e.event_time>={placeholder}")
            params.append(start_event_time)
        if end_event_time is not None:
            clauses.append(f"e.event_time<={placeholder}")
            params.append(end_event_time)
        where = " AND ".join(clauses)
        payload_column = "e.payload_json" if include_payload else "NULL AS payload_json"
        query = (
            "SELECT e.ledger_seq,e.event_id,e.event_key,e.symbol,e.event_type,"
            "e.event_time,e.received_time,e.provider_time,e.source,e.sequence_start,"
            "e.sequence_end,e.payload_hash,"
            f"{payload_column},e.quality,e.metadata,e.recorded_at "
            "FROM crypto_events e WHERE "
            f"{where} ORDER BY {order_by} LIMIT {int(limit)}"
        )
        conn = self.connect()
        try:
            if self._pg:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    return self._rows_to_dict(rows)
            rows = conn.execute(query, params).fetchall()
            return self._rows_to_dict(rows, sqlite=True)
        finally:
            conn.close()

    @staticmethod
    def _rows_to_dict(rows: Any, sqlite: bool = False) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in rows:
            if sqlite:
                values = [row[key] for key in (
                    "ledger_seq","event_id","event_key","symbol","event_type",
                    "event_time","received_time","provider_time","source",
                    "sequence_start","sequence_end","payload_hash","payload_json",
                    "quality","metadata","recorded_at"
                )]
            else:
                values = list(row)
            output.append(
                {
                    "ledger_seq": int(values[0]),
                    "event_id": str(values[1]),
                    "event_key": str(values[2]),
                    "symbol": str(values[3]),
                    "event_type": str(values[4]),
                    "event_time": str(values[5]),
                    "received_time": str(values[6]),
                    "provider_time": values[7],
                    "source": str(values[8]),
                    "sequence_start": values[9],
                    "sequence_end": values[10],
                    "payload_hash": str(values[11]),
                    "payload": json.loads(values[12]) if values[12] is not None else None,
                    "quality": str(values[13]),
                    "metadata": json.loads(values[14]),
                    "recorded_at": str(values[15]),
                }
            )
        return output