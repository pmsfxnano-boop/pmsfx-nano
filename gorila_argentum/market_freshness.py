"""Point-in-time freshness contract for market observations.

The market-data transport can succeed while the observation itself is delayed.
Gorila must distinguish transport health from the age of the market event.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

LIVE_MAX_AGE_SECONDS = max(
    30.0, float(os.getenv("GORILA_LIVE_EVENT_MAX_AGE_SECONDS", "90"))
)
DELAYED_MAX_AGE_SECONDS = max(
    LIVE_MAX_AGE_SECONDS + 1.0,
    float(os.getenv("GORILA_DELAYED_EVENT_MAX_AGE_SECONDS", "1800"))
)
FUTURE_TOLERANCE_SECONDS = max(
    1.0, float(os.getenv("GORILA_FUTURE_EVENT_TOLERANCE_SECONDS", "5"))
)


def parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601/epoch timestamp into timezone-aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            if text.isdigit():
                dt = datetime.fromtimestamp(float(text), tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_seconds(event_time: Any, now: datetime | None = None) -> float | None:
    dt = parse_timestamp(event_time)
    if dt is None:
        return None
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current.astimezone(timezone.utc) - dt).total_seconds()


def classify_age(age: float | None) -> str:
    """Classify observation age without conflating transport health."""
    if age is None:
        return "INVALID_TIMESTAMP"
    if age < -FUTURE_TOLERANCE_SECONDS:
        return "INVALID_TIMESTAMP"
    if age <= LIVE_MAX_AGE_SECONDS:
        return "LIVE"
    if age <= DELAYED_MAX_AGE_SECONDS:
        return "DELAYED"
    return "STALE"


def assess_observation(
    event_time: Any,
    received_time: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    event_age = age_seconds(event_time, current)
    received_age = age_seconds(received_time, current) if received_time is not None else None
    status = classify_age(event_age)
    return {
        "status": status,
        "is_live": status == "LIVE",
        "event_age_seconds": round(event_age, 3) if event_age is not None else None,
        "transport_age_seconds": round(received_age, 3) if received_age is not None else None,
        "event_time": parse_timestamp(event_time).isoformat() if parse_timestamp(event_time) else None,
        "received_time": parse_timestamp(received_time).isoformat() if parse_timestamp(received_time) else None,
    }


def aggregate_status(assessments: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [str(x.get("status") or "INVALID_TIMESTAMP") for x in assessments]
    live = sum(s == "LIVE" for s in statuses)
    delayed = sum(s == "DELAYED" for s in statuses)
    stale = sum(s == "STALE" for s in statuses)
    invalid = sum(s == "INVALID_TIMESTAMP" for s in statuses)
    ages = [
        float(x["event_age_seconds"])
        for x in assessments
        if x.get("event_age_seconds") is not None
    ]
    if invalid or not assessments:
        status = "DEGRADED"
    elif live == len(statuses):
        status = "HEALTHY"
    elif stale:
        status = "STALE"
    else:
        status = "DELAYED"
    return {
        "status": status,
        "live_symbols": live,
        "delayed_symbols": delayed,
        "stale_symbols": stale,
        "invalid_timestamp_symbols": invalid,
        "median_event_age_seconds": round(sorted(ages)[len(ages) // 2], 3) if ages else None,
        "max_event_age_seconds": round(max(ages), 3) if ages else None,
    }

def choose_fresher_observation(
    primary: dict[str, Any] | None,
    secondary: dict[str, Any] | None,
    now: datetime | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Select the freshest valid market observation without fabricating freshness.

    A LIVE observation always outranks DELAYED/STALE data; within the same
    validity class, the smallest event age wins. Ties preserve the primary
    source. INVALID_TIMESTAMP observations are rejected unless no valid
    candidate exists.
    """
    candidates: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for role, row in (("primary", primary), ("secondary", secondary)):
        if not row:
            continue
        assessment = assess_observation(
            row.get("event_time"),
            row.get("received_time"),
            now=now,
        )
        if assessment["status"] == "INVALID_TIMESTAMP":
            continue
        candidates.append((row, assessment, role))

    if not candidates:
        fallback = primary or secondary
        if fallback is None:
            return None, None, None
        assessment = assess_observation(
            fallback.get("event_time"),
            fallback.get("received_time"),
            now=now,
        )
        return fallback, assessment, "primary" if fallback is primary else "secondary"

    priority = {"LIVE": 0, "DELAYED": 1, "STALE": 2}
    candidates.sort(
        key=lambda item: (
            priority.get(item[1]["status"], 99),
            float(item[1]["event_age_seconds"] or 1e30),
            0 if item[2] == "primary" else 1,
        )
    )
    row, assessment, role = candidates[0]
    return row, assessment, role

