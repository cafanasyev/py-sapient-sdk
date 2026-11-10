from __future__ import annotations

import asyncio
import ssl
from datetime import timedelta

import pytest

from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.transport.conftest import start_loopback_server, wait_until


class _BrokenReaderProvider(PlainSocketProvider):
    """Real connection, but its reader fails with the given error on first read."""

    error_kind: str
    opens: int = 0

    async def open(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        _, writer = await super().open()
        self.opens += 1
        broken = asyncio.StreamReader()
        error: Exception = (
            ssl.SSLError("bad record mac")
            if self.error_kind == "ssl"
            else RuntimeError("boom")
        )
        broken.set_exception(error)
        return broken, writer


@pytest.mark.parametrize("error_kind", ["ssl", "runtime"])
async def test_read_error_drops_the_connection_and_reconnects(error_kind: str) -> None:
    server, host, port = await start_loopback_server()
    provider = _BrokenReaderProvider(host=host, port=port, error_kind=error_kind)
    client = SocketClient(
        socket_provider=provider, initial_reconnect_delay=timedelta(milliseconds=10)
    )

    await client.start()
    await wait_until(lambda: provider.opens >= 3)
    assert client._run_task is not None and not client._run_task.done()

    await client.close()
    server.close()
    await server.wait_closed()
