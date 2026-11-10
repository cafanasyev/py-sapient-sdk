from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from sapient_sdk.transport.health.checks import HealthCheck
from sapient_sdk.transport.health.config import HealthCheckConfig

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Runs the check on a fixed cadence; declares the link dead after
    ``failure_threshold`` failures in a row. One monitor per connection.

    The next check starts one interval after the later of the last check's start
    and the last inbound frame. Anchoring on the start, not the end, keeps failed
    checks from running back to back.
    """

    def __init__(
        self,
        check: HealthCheck,
        config: HealthCheckConfig,
        on_dead: Callable[[], None],
    ) -> None:
        self._check = check
        self._config = config
        self._on_dead = on_dead
        now = asyncio.get_running_loop().time()
        self._last_inbound = now
        self._last_check_started = now

    def on_inbound(self) -> None:
        self._last_inbound = asyncio.get_running_loop().time()
        self._check.on_inbound()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        interval = self._config.interval.total_seconds()
        threshold = self._config.failure_threshold
        failures = 0
        while True:
            anchor = max(self._last_inbound, self._last_check_started)
            wait = anchor + interval - loop.time()
            if wait > 0:
                await asyncio.sleep(wait)
                continue

            self._last_check_started = loop.time()
            if await self._run_check():
                failures = 0
                continue

            failures += 1
            logger.warning("health check failed (%d of %d)", failures, threshold)
            if failures >= threshold:
                logger.error(
                    "health check failed %d times in a row, dropping the connection",
                    failures,
                )
                self._on_dead()
                return

    async def _run_check(self) -> bool:
        try:
            async with asyncio.timeout(self._config.timeout.total_seconds()):
                return await self._check.check()
        except TimeoutError:
            return False
        except Exception:
            # an exception escaping run() would stop the checks for good
            logger.exception("health check threw, counting it as a failed check")
            return False
