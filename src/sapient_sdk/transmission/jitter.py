from __future__ import annotations

import random
from datetime import timedelta


def phase_offset(interval: timedelta, rng: random.Random | None = None) -> timedelta:
    r = rng if rng is not None else random.Random()
    return timedelta(seconds=r.uniform(0, interval.total_seconds()))


def jittered_interval(
    interval: timedelta, rng: random.Random | None = None, pct: float = 0.1
) -> timedelta:
    r = rng if rng is not None else random.Random()
    factor = r.uniform(1 - pct, 1 + pct)
    return timedelta(seconds=interval.total_seconds() * factor)


def registration_delay(
    window: timedelta, rng: random.Random | None = None
) -> timedelta:
    r = rng if rng is not None else random.Random()
    return timedelta(seconds=r.uniform(0, window.total_seconds()))
