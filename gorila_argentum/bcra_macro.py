from __future__ import annotations

import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .config import settings
from .sources import SourceResult, _client, iso, now

CATALOG_URL = settings.bcra_monetary_url
CATALOG_TTL_SECONDS = max(900, int(settings.bcra_catalog_ttl_seconds))
RANGE_DAYS = max(30, int(settings.bcra_monetary_range_days))

# The watchlist is intentionally small and semantic. It is a market-context layer,
# not a second prediction model. IDs are discovered from the BCRA catalog so a
# future catalog change does not silently bind Gorila to stale numeric IDs.
WATCHLIST: tuple[dict[str, Any], ...] = (
    {
        "symbol": "BCRA_RESERVAS_USD",
        "field": "value",
        "labels": ("reservas internacionales",),
        "periodicidad": "D",
        "kind": "stock",
    },
    {
        "symbol": "BCRA_WHOLESALE_FX",
        "field": "value",
        "labels": ("tipo de cambio mayorista", "3500"),
        "periodicidad": "D",
        "kind": "level",
    },
    {
        "symbol": "BCRA_BASE_MONETARIA",
        "field": "value",
        "labels": ("base monetaria - total",),
        "periodicidad": "D",
        "kind": "stock",
    },
    {
        "symbol": "BCRA_BADLAR_NA",
        "field": "value",
        "labels": ("badlar en pesos de bancos privados",),
        "periodicidad": "D",
        "kind": "rate",
    },
    {
        "symbol": "BCRA_TAMAR_NA",
        "field": "value",
        "labels": ("tamar en pesos de bancos privados",),
        "periodicidad": "D",
        "kind": "rate",
    },
)

_catalog_cache: tuple[float, list[dict[str, Any]]] | None = None
_catalog_lock = threading.Lock()


def _request_json(path: str, *, params: dict[str, Any] | None = None) -> tuple[dict[str, Any], float]:
    started = time.perf_counter()
    with _client() as client:
        response = client.get(path, params=params)
        response.raise_for_status()
        payload = response.json()
    return payload, (time.perf_counter() - started) * 1000.0


def _catalog_page(offset: int) -> tuple[list[dict[str, Any]], int, float]:
    payload, latency_ms = _request_json(
        CATALOG_URL,
        params={"offset": offset, "limit": 1000, "Accept-Language": "es-AR"},
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    resultset = metadata.get("resultset", {}) if isinstance(metadata, dict) else {}
    count = int(resultset.get("count") or 0)
    rows = [row for row in (results or []) if isinstance(row, dict)]
    return rows, count, latency_ms


def bcra_monetary_catalog(*, force: bool = False) -> list[dict[str, Any]]:
    global _catalog_cache
    now_mono = time.monotonic()
    if not force and _catalog_cache and now_mono - _catalog_cache[0] < CATALOG_TTL_SECONDS:
        return list(_catalog_cache[1])

    with _catalog_lock:
        now_mono = time.monotonic()
        if not force and _catalog_cache and now_mono - _catalog_cache[0] < CATALOG_TTL_SECONDS:
            return list(_catalog_cache[1])

        rows: list[dict[str, Any]] = []
        offset = 0
        expected = None
        while True:
            page, count, _ = _catalog_page(offset)
            if expected is None:
                expected = count
            rows.extend(page)
            if not page or len(rows) >= expected or len(page) < 1000:
                break
            offset += len(page)

        _catalog_cache = (time.monotonic(), rows)
        return list(rows)


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _score_candidate(candidate: dict[str, Any], spec: dict[str, Any]) -> float:
    desc = _normalize_text(candidate.get("descripcion"))
    periodicity = _normalize_text(candidate.get("periodicidad")).upper()
    unit = _normalize_text(candidate.get("unidadExpresion"))

    score = 0.0
    labels = tuple(_normalize_text(x) for x in spec["labels"])
    if labels and all(label in desc for label in labels):
        score += 10.0
    elif labels:
        score += sum(2.0 for label in labels if label in desc)

    wanted_period = str(spec.get("periodicidad") or "").upper()
    if wanted_period and periodicity == wanted_period:
        score += 4.0
    elif wanted_period:
        score -= 3.0

    if spec["kind"] == "rate" and "%" in unit:
        score += 2.0
    if spec["symbol"] == "BCRA_WHOLESALE_FX" and ("usd" in unit or "por usd" in unit):
        score += 2.0
    if spec["symbol"] == "BCRA_RESERVAS_USD" and "usd" in unit:
        score += 2.0
    if spec["symbol"] == "BCRA_BASE_MONETARIA" and ("pesos" in unit or "millones" in unit):
        score += 2.0

    return score


def resolve_watchlist(catalog: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    resolved: dict[str, dict[str, Any]] = {}
    for spec in WATCHLIST:
        candidates = [
            item for item in catalog
            if isinstance(item.get("idVariable"), int)
        ]
        ranked = sorted(
            ((candidate, _score_candidate(candidate, spec)) for candidate in candidates),
            key=lambda pair: pair[1],
            reverse=True,
        )
        if ranked and ranked[0][1] >= 10.0:
            row = dict(ranked[0][0])
            row["semantic_symbol"] = spec["symbol"]
            row["semantic_field"] = spec["field"]
            row["semantic_kind"] = spec["kind"]
            row["match_score"] = round(ranked[0][1], 2)
            resolved[spec["symbol"]] = row
    return resolved


def _fetch_series(variable: dict[str, Any], *, days: int = RANGE_DAYS) -> tuple[list[dict[str, Any]], float]:
    today = date.today()
    start = today - timedelta(days=days)
    params = {
        "desde": start.isoformat(),
        "hasta": today.isoformat(),
        "limit": 3000,
        "offset": 0,
        "Accept-Language": "es-AR",
    }
    payload, latency_ms = _request_json(
        f"{CATALOG_URL}/{int(variable['idVariable'])}",
        params=params,
    )
    results = payload.get("results") if isinstance(payload, dict) else []
    results = [item for item in (results or []) if isinstance(item, dict)]
    details: list[dict[str, Any]] = []
    for result in results:
        raw_details = result.get("detalle") if isinstance(result, dict) else None
        if isinstance(raw_details, list):
            details.extend(item for item in raw_details if isinstance(item, dict))
        elif isinstance(raw_details, dict):
            details.append(raw_details)
    return details, latency_ms


def bcra_macro_cycle() -> SourceResult:
    source = "BCRA/MonetaryV4"
    started = time.perf_counter()
    received = now()
    try:
        catalog_started = time.perf_counter()
        catalog = bcra_monetary_catalog()
        catalog_latency_ms = (time.perf_counter() - catalog_started) * 1000.0
        resolved = resolve_watchlist(catalog)

        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        for spec in WATCHLIST:
            symbol = spec["symbol"]
            variable = resolved.get(symbol)
            if variable is None:
                errors.append(f"{symbol}: WATCHLIST_VARIABLE_NOT_RESOLVED")
                continue
            details, _ = _fetch_series(variable)
            for detail in details:
                stamp = detail.get("fecha")
                value = detail.get("valor")
                if stamp is None or value is None:
                    continue
                try:
                    numeric = float(value)
                except (TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "field": spec["field"],
                        "value": numeric,
                        "event_time": str(stamp),
                        "received_time": iso(received),
                        "source": source,
                        "latency_ms": (time.perf_counter() - started) * 1000.0,
                        "metadata": {
                            "provider": "BCRA",
                            "api_version": "v4.0",
                            "idVariable": int(variable["idVariable"]),
                            "description": variable.get("descripcion"),
                            "category": variable.get("categoria"),
                            "series_type": variable.get("tipoSerie"),
                            "periodicity": variable.get("periodicidad"),
                            "unit": variable.get("unidadExpresion"),
                            "currency": variable.get("moneda"),
                            "semantic_symbol": symbol,
                            "semantic_kind": spec["kind"],
                            "knowledge_time": iso(received),
                            "catalog_match_score": variable.get("match_score"),
                        },
                    }
                )

        if not rows:
            raise RuntimeError(
                "BCRA_MACRO_NO_ROWS"
                + (f": {'; '.join(errors)}" if errors else "")
            )

        result = SourceResult(
            source,
            rows,
            error=("; ".join(errors) if errors else None),
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        print("GORILA_BCRA_MACRO_CYCLE", {
            "status": "HEALTHY" if rows else "DEGRADED",
            "rows": len(rows),
            "resolved_variables": {spec["symbol"]: resolved.get(spec["symbol"], {}).get("idVariable") for spec in WATCHLIST},
            "errors": errors[:8],
            "latency_ms": round(float(result.latency_ms or 0), 2),
        }, flush=True)
        return result
    except Exception as exc:
        return SourceResult(
            source,
            error=f"{type(exc).__name__}: {exc}",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )


def _safe_latest(store, symbol: str) -> tuple[str | None, float | None]:
    rows = store.recent_series(symbol, "value", limit=1)
    if not rows:
        return None, None
    event_time, value = rows[-1]
    return str(event_time), float(value)


def _direction(change: float | None, *, epsilon: float = 1e-9) -> str:
    if change is None:
        return "UNKNOWN"
    if abs(change) <= epsilon:
        return "FLAT"
    return "UP" if change > 0 else "DOWN"


def _series_change(store, symbol: str, *, periods: int) -> tuple[float | None, float | None]:
    rows = store.recent_series(symbol, "value", limit=max(periods + 1, 6))
    if len(rows) <= periods:
        return None, None
    latest = float(rows[-1][1])
    prior = float(rows[-1 - periods][1])
    if prior == 0:
        return None, None
    pct = (latest / prior) - 1.0
    return pct, latest - prior


def build_bcra_trader_snapshot(store) -> dict[str, Any]:
    specs = {
        spec["symbol"]: spec
        for spec in WATCHLIST
    }
    items: dict[str, Any] = {}
    for symbol, spec in specs.items():
        event_time, value = _safe_latest(store, symbol)
        change_5d_pct, change_5d_abs = _series_change(store, symbol, periods=5)
        change_20d_pct, change_20d_abs = _series_change(store, symbol, periods=20)
        unit = None
        metadata = None
        # The latest generic observation already carries semantics, but the
        # snapshot only exposes the compact trader-facing subset.
        rows = store.recent_series(symbol, "value", limit=1)
        if rows:
            # Units/description are not returned by recent_series, so leave
            # these internal labels deterministic from the semantic contract.
            unit = {
                "BCRA_RESERVAS_USD": "USD mm",
                "BCRA_WHOLESALE_FX": "ARS/USD",
                "BCRA_BASE_MONETARIA": "ARS mm",
                "BCRA_BADLAR_NA": "% n.a.",
                "BCRA_TAMAR_NA": "% n.a.",
            }.get(symbol)
        age_days = None
        if event_time:
            try:
                parsed = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                age_days = max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds() / 86400.0)
            except Exception:
                pass
        items[symbol] = {
            "value": value,
            "unit": unit,
            "asof": event_time,
            "age_days": round(age_days, 2) if age_days is not None else None,
            "change_5d_pct": change_5d_pct,
            "change_5d_abs": change_5d_abs,
            "change_20d_pct": change_20d_pct,
            "change_20d_abs": change_20d_abs,
            "direction_20d": _direction(change_20d_pct),
            "kind": spec["kind"],
        }

    available = sum(item["value"] is not None for item in items.values())
    freshness_values = [
        item["age_days"] for item in items.values() if item["age_days"] is not None
    ]
    return {
        "status": "READY" if available >= 3 else "PARTIAL",
        "available": available,
        "required": len(WATCHLIST),
        "source": "BCRA/MonetaryV4",
        "api_version": "v4.0",
        "asof_latest": min(freshness_values) if freshness_values else None,
        "items": items,
        "research_only": True,
        "no_execution_authority": True,
    }
