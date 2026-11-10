from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import sys
from collections.abc import Awaitable, Callable
from datetime import timedelta

logger = logging.getLogger(__name__)

PingRunner = Callable[[str, timedelta], Awaitable[bool]]
PrefixWriter = Callable[[int, timedelta], Awaitable[bool]]


class HealthCheck(abc.ABC):
    """One way to check that the peer is still alive."""

    @abc.abstractmethod
    async def check(self) -> bool:
        """Runs one check within the configured timeout. True if the peer is alive."""

    def on_inbound(self) -> None:  # noqa: B027 - optional hook, most checks ignore it
        """Told by the read path that a frame arrived.

        Only the in-band check uses it.
        """


async def tcp_connect_succeeds(host: str, port: int, timeout: timedelta) -> bool:
    """Opens and closes a plain TCP connection, like ``nc -z host port``."""
    try:
        async with asyncio.timeout(timeout.total_seconds()):
            _, writer = await asyncio.open_connection(host, port)
    # TimeoutError is an OSError; OverflowError comes from a port outside 0-65535
    except (OSError, ValueError, OverflowError):
        return False
    writer.close()
    return True


class NetcatHealthCheck(HealthCheck):
    """Proves the port accepts connections, not that the managed one is healthy."""

    def __init__(self, host: str, port: int, timeout: timedelta) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout

    async def check(self) -> bool:
        return await tcp_connect_succeeds(self._host, self._port, self._timeout)


def ping_command(platform: str, host: str, timeout: timedelta) -> list[str]:
    """The ``ping`` command line for one echo request on the given ``sys.platform``.

    Linux ``-W`` takes whole seconds, macOS ``-W`` takes milliseconds, Windows uses
    ``-n`` and ``-w`` in milliseconds. An unknown platform gets the Linux form.
    """
    millis = int(timeout / timedelta(milliseconds=1))
    if platform.startswith("win"):
        return ["ping", "-n", "1", "-w", str(millis), host]
    if platform == "darwin":
        return ["ping", "-c", "1", "-W", str(millis), host]
    # 0 means "no timeout" on some Linux builds, so round up and never go below 1
    seconds = max(1, (millis + 999) // 1000)
    return ["ping", "-c", "1", "-W", str(seconds), host]


async def run_command(command: list[str], timeout: timedelta) -> bool:
    """Runs a command; True if it exits with code 0 within the timeout."""
    line = " ".join(command)
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError:
        logger.exception("failed to run %s", line)
        return False
    try:
        async with asyncio.timeout(timeout.total_seconds()):
            return await process.wait() == 0
    except TimeoutError:
        logger.warning("%s did not finish in time, killing it", line)
        return False
    finally:
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()


async def run_system_ping(host: str, timeout: timedelta) -> bool:
    """One ICMP echo through the ``ping`` binary.

    The binary carries cap_net_raw or the setuid bit, so it works without root.
    """
    return await run_command(ping_command(sys.platform, host, timeout), timeout)


class IcmpHealthCheck(HealthCheck):
    """Proves the host is up, not that the SAPIENT port still accepts connections."""

    def __init__(
        self, host: str, timeout: timedelta, ping: PingRunner = run_system_ping
    ) -> None:
        self._host = host
        self._timeout = timeout
        self._ping = ping

    async def check(self) -> bool:
        return await self._ping(self._host, self._timeout)


class InBandHealthCheck(HealthCheck):
    """Keepalive on the connection itself: send a ping prefix, wait for any frame back.

    Any inbound frame counts as the answer, not only the pong. The client never
    answers a ping from the peer.
    """

    def __init__(
        self, ping_prefix: int, timeout: timedelta, write_prefix: PrefixWriter
    ) -> None:
        self._ping_prefix = ping_prefix
        self._timeout = timeout
        self._write_prefix = write_prefix
        self._answered = asyncio.Event()

    async def check(self) -> bool:
        deadline = asyncio.get_running_loop().time() + self._timeout.total_seconds()
        # a frame already here is at least one interval old: the monitor checks only
        # a full interval after the last inbound frame
        self._answered.clear()
        if not await self._write_prefix(self._ping_prefix, self._timeout):
            # the write path was blocked; still alive if the peer spoke meanwhile
            return self._answered.is_set()
        try:
            async with asyncio.timeout_at(deadline):
                await self._answered.wait()
        except TimeoutError:
            return False
        return True

    def on_inbound(self) -> None:
        self._answered.set()
