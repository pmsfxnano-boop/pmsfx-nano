"""PMSF-X Nano — walk-forward, purge and embargo utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Sequence


@dataclass(frozen=True)
class TemporalSample:
    event_time: object
    label_end_time: object
    features: object
    label: int


@dataclass(frozen=True)
class TemporalFold:
    fold: int
    train: tuple[TemporalSample, ...]
    test: tuple[TemporalSample, ...]
    train_start: object | None
    train_end: object | None
    test_start: object | None
    test_end: object | None


def assert_point_in_time(samples: Sequence[TemporalSample]) -> None:
    for sample in samples:
        if sample.label_end_time <= sample.event_time:
            raise ValueError("LABEL_END_MUST_BE_AFTER_EVENT_TIME")


def _inside_any_embargo(event_time: object, embargo_windows: Sequence[tuple[object, object]]) -> bool:
    return any(start <= event_time < end for start, end in embargo_windows)


def walk_forward_splits(
    samples: Sequence[TemporalSample],
    *,
    train_size: int,
    test_size: int,
    purge: timedelta,
    embargo: timedelta,
) -> list[TemporalFold]:
    ordered = sorted(samples, key=lambda s: s.event_time)
    assert_point_in_time(ordered)
    folds: list[TemporalFold] = []
    embargo_windows: list[tuple[object, object]] = []
    cursor = train_size
    fold_no = 0

    while cursor + test_size <= len(ordered):
        raw_train = ordered[:cursor]
        test = ordered[cursor:cursor + test_size]
        test_start = test[0].event_time
        test_end = test[-1].label_end_time

        train = [
            s for s in raw_train
            if s.label_end_time <= test_start - purge
            and not _inside_any_embargo(s.event_time, embargo_windows)
        ]

        if train and test:
            embargo_end = test_end + embargo

            # Samples in the post-test embargo interval are intentionally
            # removed from future folds rather than merely skipped as tests.
            embargo_windows.append((test_end, embargo_end))

            next_cursor = cursor + test_size
            while (
                next_cursor < len(ordered)
                and ordered[next_cursor].event_time < embargo_end
            ):
                next_cursor += 1

            folds.append(TemporalFold(
                fold=fold_no,
                train=tuple(train),
                test=tuple(test),
                train_start=train[0].event_time,
                train_end=train[-1].event_time,
                test_start=test_start,
                test_end=test_end,
            ))

            cursor = next_cursor
        else:
            cursor += test_size

        fold_no += 1

    return folds
