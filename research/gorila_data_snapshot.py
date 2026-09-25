from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Callable, Mapping


def canonical_hash(series: Mapping[str, Mapping[str, float]]) -> str:
    payload = json.dumps(series, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def load_or_fetch_series(
    symbols: list[str],
    fetcher: Callable[[str], Mapping[str, float]],
    snapshot_path: str | None = None,
) -> tuple[dict[str, dict[str, float]], str | None]:
    if snapshot_path:
        path = Path(snapshot_path)
        if path.exists():
            payload = json.loads(path.read_text())
            series = payload.get("series", payload)
            loaded = {s: {str(k): float(v) for k, v in series[s].items()} for s in symbols}
            expected = payload.get("sha256")
            actual = canonical_hash(loaded)
            if expected and expected != actual:
                raise RuntimeError(
                    f"snapshot hash mismatch: expected {expected}, got {actual}"
                )
            return loaded, actual

    series = {s: {str(k): float(v) for k, v in fetcher(s).items()} for s in symbols}
    return series, canonical_hash(series)


def write_snapshot(
    path: str,
    series: Mapping[str, Mapping[str, float]],
    metadata: Mapping[str, object],
) -> str:
    normalized = {
        s: {str(k): float(v) for k, v in series[s].items()}
        for s in sorted(series)
    }
    digest = canonical_hash(normalized)
    payload = {
        "schema": "gorila-market-snapshot-v1",
        "sha256": digest,
        "metadata": dict(metadata),
        "series": normalized,
    }
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return digest
