from __future__ import annotations

import asyncio
from collections.abc import Callable


async def wait_until(
    predicate: Callable[[], bool], timeout: float = 2.0, interval: float = 0.01
) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(interval)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def start_loopback_server() -> tuple[asyncio.Server, str, int]:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await reader.read()  # keep the connection open until the client closes it
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    return server, host, port
