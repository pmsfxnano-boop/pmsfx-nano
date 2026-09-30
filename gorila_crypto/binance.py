            payload = response.json()
        if not isinstance(payload, dict) or "lastUpdateId" not in payload:
            raise BinanceAdapterError("invalid Binance depth snapshot response")
        return payload


class BinanceSpotMarketAdapter:
    """Single-connection market stream adapter.

    This object is a transport component. It does not start automatically and it
    does not make forecasts or trading decisions.
    """

    source_family = "binance.websocket.market"

    def __init__(
        self,
        config: BinanceStreamConfig,
        *,
        rest_client: BinanceRestClient | None = None,
        event_sink: Callable[[NormalizedMarketEvent], None] | None = None,
    ) -> None:
        self.config = config
        self.rest_client = rest_client or BinanceRestClient(
            base_url=config.rest_base_url,
            timeout_s=config.connect_timeout_s,
        )
        self.event_sink = event_sink

    def connection_url(self) -> str:
        return build_ws_url(self.config)

    def subscription_request(self) -> dict[str, Any]:
        return {
            "method": "SUBSCRIBE",
            "params": list(build_stream_names(self.config)),
            "id": uuid.uuid4().hex[:32],
        }

    def connect(self):
        ws = websocket.create_connection(
            self.config.ws_base_url,
            timeout=self.config.recv_timeout_s,
            ping_interval=self.config.ping_interval_s,
            enable_multithread=True,
        )
        ws.send(json.dumps(self.subscription_request(), separators=(",", ":")))
        return ws

    def iter_events_once(self, *, ws) -> Iterator[NormalizedMarketEvent]:
        connected_ns = time.time_ns()
        while (time.time_ns() - connected_ns) / 1_000_000_000 < self.config.connection_max_seconds:
            raw = ws.recv()
            if raw is None:
                break
            received_ns = time.time_ns()
            received_time = datetime.fromtimestamp(
                received_ns / 1_000_000_000,
                tz=timezone.utc,
            )
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                message = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise InvalidMarketEvent("Binance websocket payload is not valid JSON") from exc
            if not isinstance(message, Mapping):
                raise InvalidMarketEvent("Binance websocket message must be an object")
            event = normalize_market_message(
                message,
                received_ns=received_ns,
                received_time=received_time,
            )
            if event is None:
                continue
            if self.event_sink is not None:
                self.event_sink(event)
            yield event

    def close(self, ws) -> None:
        try:
            ws.close()
        except Exception:
            pass

    def iter_forever(
        self,
        *,
        stop_event=None,
        on_connection: Callable[[str, dict[str, Any]], None] | None = None,
        initial_backoff_s: float = 1.0,
        max_backoff_s: float = 60.0,
    ) -> Iterator[NormalizedMarketEvent]:
        """Reconnect with bounded exponential backoff.

        The socket connects to the public combined-stream endpoint and then
        sends an explicit SUBSCRIBE request. This makes the requested stream
        set observable at the protocol layer instead of relying only on the URL.
        A controlled reconnect is also the normal path before Binance's
        documented 24-hour connection boundary.
        """
        backoff = max(0.1, float(initial_backoff_s))
        while stop_event is None or not stop_event.is_set():
            ws = None
            connected_at = datetime.now(timezone.utc)
            try:
                if on_connection:
                    on_connection("CONNECTING", {"at": connected_at.isoformat()})
                ws = self.connect()
                backoff = max(0.1, float(initial_backoff_s))
                if on_connection:
                    on_connection(
                        "SUBSCRIBE_SENT",
                        {
                            "at": datetime.now(timezone.utc).isoformat(),
                            "streams": list(build_stream_names(self.config)),
                        },
                    )
                    on_connection("CONNECTED", {"at": datetime.now(timezone.utc).isoformat()})
                for event in self.iter_events_once(ws=ws):
                    yield event
                    if stop_event is not None and stop_event.is_set():
                        return
                if on_connection:
                    on_connection(
                        "ROTATE",
                        {
                            "at": datetime.now(timezone.utc).isoformat(),
                            "connection_max_seconds": self.config.connection_max_seconds,
                        },
                    )
            except Exception as exc:
                if on_connection:
                    on_connection(
                        "ERROR",
                        {
                            "at": datetime.now(timezone.utc).isoformat(),
                            "error": f"{type(exc).__name__}: {exc}",
                        },
                    )
                if stop_event is not None and stop_event.is_set():
                    return
                time.sleep(backoff)
                backoff = min(max_backoff_s, backoff * 2.0)
            finally:
                if ws is not None:
                    self.close(ws)
                if on_connection:
                    on_connection(
                        "DISCONNECTED",
                        {"at": datetime.now(timezone.utc).isoformat()},
                    )