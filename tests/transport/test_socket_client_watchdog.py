from __future__ import annotations

from datetime import timedelta

from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.transport.conftest import start_loopback_server, wait_until


class _AlwaysUnreachableSocketClient(SocketClient):
    async def probe_reachable(self, timeout: timedelta) -> bool:
        return False


async def test_watchdog_closes_connection_when_probe_fails() -> None:
    server, host, port = await start_loopback_server()
    client = _AlwaysUnreachableSocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        watchdog_interval=timedelta(milliseconds=50),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )

    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)
    await wait_until(lambda: client.state == ConnectionState.DISCONNECTED, timeout=1.0)

    await client.close()
    server.close()
    await server.wait_closed()


async def test_watchdog_leaves_healthy_connection_alone() -> None:
    server, host, port = await start_loopback_server()
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        watchdog_interval=timedelta(milliseconds=50),
    )
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    import asyncio

    await asyncio.sleep(0.2)  # several watchdog cycles
    assert client.state == ConnectionState.CONNECTED

    await client.close()
    server.close()
    await server.wait_closed()
