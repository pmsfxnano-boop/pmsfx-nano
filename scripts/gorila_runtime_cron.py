from __future__ import annotations

import json
import os
import urllib.request


def main() -> int:
    base_url = os.environ["GORILA_SERVICE_URL"].rstrip("/")
    key = os.environ["GORILA_RUNTIME_TICK_KEY"].strip()
    request = urllib.request.Request(
        f"{base_url}/api/runtime/tick",
        method="POST",
        headers={
            "X-Gorila-Runtime-Key": key,
            "User-Agent": "Gorila-Argentum-Render-Cron/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=900) as response:
        body = response.read().decode("utf-8")
        if response.status != 200:
            raise RuntimeError(f"runtime_tick_http_{response.status}: {body}")
    payload = json.loads(body)
    if payload.get("status") != "COMPLETED":
        raise RuntimeError(f"runtime_tick_incomplete: {payload}")
    print(json.dumps(payload, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
