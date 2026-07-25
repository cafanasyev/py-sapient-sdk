from __future__ import annotations

import random
from datetime import timedelta

from sapient_sdk.transmission.jitter import (
    jittered_interval,
    phase_offset,
    registration_delay,
)


def test_phase_offset_is_within_bounds() -> None:
    rng = random.Random(42)
    for _ in range(1000):
        offset = phase_offset(timedelta(seconds=10), rng)
        assert timedelta(0) <= offset < timedelta(seconds=10)


def test_phase_offset_of_zero_interval_is_zero() -> None:
    assert phase_offset(timedelta(0), random.Random(1)) == timedelta(0)


def test_jittered_interval_stays_within_ten_percent() -> None:
    rng = random.Random(7)
    interval = timedelta(seconds=10)
    for _ in range(1000):
        jittered = jittered_interval(interval, rng)
        assert timedelta(seconds=9) <= jittered <= timedelta(seconds=11)


def test_registration_delay_is_within_window() -> None:
    rng = random.Random(3)
    window = timedelta(seconds=2)
    for _ in range(1000):
        delay = registration_delay(window, rng)
        assert timedelta(0) <= delay < window


def test_registration_delay_of_zero_window_is_zero() -> None:
    assert registration_delay(timedelta(0), random.Random(2)) == timedelta(0)
