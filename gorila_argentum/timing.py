from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

@dataclass(frozen=True)
class TimingWindow:
    generated_at: str
    horizon_seconds: int
    validity_seconds: int
    entry_start: str
    entry_end: str
    expiry: str

def make_window(horizon_seconds: int, validity_seconds: int | None = None, now: datetime | None = None) -> TimingWindow:
    if horizon_seconds <= 0:
        raise ValueError("horizon_seconds_must_be_positive")
    now=now or datetime.now(timezone.utc)
    validity=max(1,int(validity_seconds if validity_seconds is not None else horizon_seconds))
    entry_end=now+timedelta(seconds=validity)
    expiry=entry_end
    target=now+timedelta(seconds=horizon_seconds)
    return TimingWindow(
        generated_at=now.isoformat(),
        horizon_seconds=horizon_seconds,
        validity_seconds=validity,
        entry_start=now.isoformat(),
        entry_end=entry_end.isoformat(),
        expiry=expiry.isoformat(),
    )
