from __future__ import annotations

import asyncio
import shutil
import socket
import sys
from datetime import timedelta

import pytest

from sapient_sdk.transport.health.checks import (
    IcmpHealthCheck,
    InBandHealthCheck,
    NetcatHealthCheck,
    ping_command,
    run_command,
    run_system_ping,
)
from tests.transport.conftest import start_loopback_server

TIMEOUT = timedelta(milliseconds=300)


# --- TCP connect ---


async def test_listening_port_is_alive() -> None:
    server, host, port = await start_loopback_server()
    assert await NetcatHealthCheck(host, port, TIMEOUT).check() is True
    server.close()
    await server.wait_closed()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_closed_port_is_dead() -> None:
    assert await NetcatHealthCheck("127.0.0.1", _free_port(), TIMEOUT).check() is False


async def test_unresolvable_host_is_dead() -> None:
    check = NetcatHealthCheck("no-such-host.invalid", 5000, timedelta(seconds=2))
    assert await check.check() is False


async def test_port_out_of_range_is_dead() -> None:
    assert await NetcatHealthCheck("127.0.0.1", 70000, TIMEOUT).check() is False


# --- ICMP ---


async def test_icmp_alive_when_ping_succeeds() -> None:
    async def ping(host: str, timeout: timedelta) -> bool:
        return True

    assert await IcmpHealthCheck("10.0.0.1", TIMEOUT, ping=ping).check() is True


async def test_icmp_dead_when_ping_fails() -> None:
    async def ping(host: str, timeout: timedelta) -> bool:
        return False

    assert await IcmpHealthCheck("10.0.0.1", TIMEOUT, ping=ping).check() is False


async def test_icmp_passes_host_and_timeout_to_the_runner() -> None:
    calls: list[tuple[str, timedelta]] = []

    async def ping(host: str, timeout: timedelta) -> bool:
        calls.append((host, timeout))
        return True

    await IcmpHealthCheck("10.0.0.1", TIMEOUT, ping=ping).check()
    assert calls == [("10.0.0.1", TIMEOUT)]


def test_linux_command_uses_whole_seconds() -> None:
    assert ping_command("linux", "10.0.0.1", timedelta(milliseconds=2500)) == [
        "ping",
        "-c",
        "1",
        "-W",
        "3",
        "10.0.0.1",
    ]


def test_linux_timeout_rounds_up_to_at_least_one_second() -> None:
    assert ping_command("linux", "10.0.0.1", timedelta(milliseconds=200)) == [
        "ping",
        "-c",
        "1",
        "-W",
        "1",
        "10.0.0.1",
    ]


def test_mac_command_uses_millis() -> None:
    assert ping_command("darwin", "10.0.0.1", timedelta(milliseconds=2500)) == [
        "ping",
        "-c",
        "1",
        "-W",
        "2500",
        "10.0.0.1",
    ]


def test_windows_command_uses_n_and_w() -> None:
    assert ping_command("win32", "10.0.0.1", timedelta(milliseconds=2500)) == [
        "ping",
        "-n",
        "1",
        "-w",
        "2500",
        "10.0.0.1",
    ]


def test_unknown_os_falls_back_to_linux_flags() -> None:
    assert ping_command("plan9", "10.0.0.1", timedelta(seconds=2)) == [
        "ping",
        "-c",
        "1",
        "-W",
        "2",
        "10.0.0.1",
    ]


@pytest.mark.skipif(shutil.which("ping") is None, reason="no ping binary")
async def test_loopback_answers() -> None:
    assert await run_system_ping("127.0.0.1", timedelta(seconds=3)) is True


@pytest.mark.skipif(sys.platform == "win32", reason="needs the sleep binary")
async def test_hung_process_is_killed_at_the_timeout() -> None:
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await run_command(["sleep", "5"], TIMEOUT) is False
    assert loop.time() - started < 0.65


async def test_missing_binary_is_a_failure() -> None:
    assert await run_command(["no-such-binary-xyz"], TIMEOUT) is False


# --- in-band ---


async def test_alive_when_answer_arrives() -> None:
    check: InBandHealthCheck

    async def write(prefix: int, timeout: timedelta) -> bool:
        asyncio.get_running_loop().call_later(0.05, check.on_inbound)
        return True

    check = InBandHealthCheck(0, TIMEOUT, write)
    assert await check.check() is True


async def test_dead_when_no_answer_arrives() -> None:
    async def write(prefix: int, timeout: timedelta) -> bool:
        return True

    assert await InBandHealthCheck(0, TIMEOUT, write).check() is False


async def test_sends_the_configured_ping_prefix() -> None:
    sent: list[int] = []

    async def write(prefix: int, timeout: timedelta) -> bool:
        sent.append(prefix)
        return True

    await InBandHealthCheck(0, TIMEOUT, write).check()
    assert sent == [0]


async def test_blocked_write_path_is_a_failure() -> None:
    async def write(prefix: int, timeout: timedelta) -> bool:
        return False

    assert await InBandHealthCheck(0, TIMEOUT, write).check() is False


async def test_blocked_write_path_is_alive_if_the_peer_spoke_anyway() -> None:
    check: InBandHealthCheck

    async def write(prefix: int, timeout: timedelta) -> bool:
        check.on_inbound()
        return False

    check = InBandHealthCheck(0, TIMEOUT, write)
    assert await check.check() is True


async def test_answer_from_before_the_check_does_not_count() -> None:
    async def write(prefix: int, timeout: timedelta) -> bool:
        return True

    check = InBandHealthCheck(0, TIMEOUT, write)
    check.on_inbound()
    assert await check.check() is False


async def test_slow_write_and_the_answer_share_one_timeout() -> None:
    async def write(prefix: int, timeout: timedelta) -> bool:
        await asyncio.sleep(TIMEOUT.total_seconds() - 0.02)
        return True

    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await InBandHealthCheck(0, TIMEOUT, write).check() is False
    assert loop.time() - started < TIMEOUT.total_seconds() * 1.5
