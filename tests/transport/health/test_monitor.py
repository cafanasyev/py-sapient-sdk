from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta

from sapient_sdk.transport.health.checks import HealthCheck
from sapient_sdk.transport.health.config import HealthCheckConfig, HealthCheckType
from sapient_sdk.transport.health.monitor import HealthMonitor
from tests.transport.conftest import wait_until

INTERVAL = timedelta(milliseconds=100)
TIMEOUT = timedelta(milliseconds=50)


def _config(threshold: int) -> HealthCheckConfig:
    return HealthCheckConfig(
        type=HealthCheckType.NETCAT,
        interval=INTERVAL,
        timeout=TIMEOUT,
        failure_threshold=threshold,
    )


class _FakeCheck(HealthCheck):
    def __init__(self) -> None:
        self.alive = True
        self.calls = 0
        self.inbound = 0

    async def check(self) -> bool:
        self.calls += 1
        return self.alive

    def on_inbound(self) -> None:
        self.inbound += 1


class _ThrowingCheck(_FakeCheck):
    async def check(self) -> bool:
        self.calls += 1
        raise RuntimeError("broken check")


class _HangingCheck(_FakeCheck):
    async def check(self) -> bool:
        self.calls += 1
        await asyncio.sleep(10)
        return True


def _start(
    check: HealthCheck, threshold: int, on_dead: Callable[[], None] = lambda: None
) -> tuple[HealthMonitor, asyncio.Task[None]]:
    monitor = HealthMonitor(check, _config(threshold), on_dead)
    return monitor, asyncio.ensure_future(monitor.run())


async def _stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_does_not_fire_before_the_first_interval() -> None:
    check = _FakeCheck()
    _, task = _start(check, 3)
    await asyncio.sleep(0.05)
    assert check.calls == 0
    await _stop(task)


async def test_declares_dead_only_after_threshold_consecutive_failures() -> None:
    check = _FakeCheck()
    check.alive = False
    dead = asyncio.Event()
    _, task = _start(check, 3, dead.set)

    await asyncio.sleep(0.26)
    assert not dead.is_set()

    await asyncio.wait_for(dead.wait(), timeout=3)
    assert check.calls == 3
    await _stop(task)


async def test_one_success_resets_the_failure_count() -> None:
    check = _FakeCheck()
    dead = asyncio.Event()
    _, task = _start(check, 3, dead.set)

    for _ in range(2):
        check.alive = False
        await asyncio.sleep(0.21)
        check.alive = True
        await asyncio.sleep(0.21)

    assert not dead.is_set()
    await _stop(task)


async def test_checks_are_one_interval_apart_even_after_a_failure() -> None:
    check = _FakeCheck()
    check.alive = False
    _, task = _start(check, 100)
    await asyncio.sleep(0.52)
    assert check.calls <= 7
    await _stop(task)


async def test_inbound_traffic_delays_the_next_check() -> None:
    check = _FakeCheck()
    monitor, task = _start(check, 3)
    for _ in range(12):
        monitor.on_inbound()
        await asyncio.sleep(0.04)
    assert check.calls == 0
    await _stop(task)


async def test_on_inbound_is_forwarded_to_the_check() -> None:
    check = _FakeCheck()
    monitor = HealthMonitor(check, _config(3), lambda: None)
    monitor.on_inbound()
    assert check.inbound == 1


async def test_throwing_check_counts_as_a_failure() -> None:
    check = _ThrowingCheck()
    dead = asyncio.Event()
    _, task = _start(check, 3, dead.set)
    await asyncio.wait_for(dead.wait(), timeout=3)
    assert check.calls == 3
    await _stop(task)


async def test_check_running_past_the_timeout_counts_as_a_failure() -> None:
    check = _HangingCheck()
    dead = asyncio.Event()
    _, task = _start(check, 3, dead.set)
    await wait_until(dead.is_set, timeout=3)
    assert check.calls == 3
    await _stop(task)
