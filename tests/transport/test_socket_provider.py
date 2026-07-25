from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from sapient_sdk.transport.socket_provider import PlainSocketProvider, SocketProvider


def test_abstract_provider_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="abstract"):
        SocketProvider(host="localhost", port=1234)  # type: ignore[abstract]


async def test_plain_provider_opens_a_real_connection() -> None:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write(b"hi")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    provider = PlainSocketProvider(host=host, port=port)
    reader, writer = await provider.open()
    data = await reader.read(2)
    assert data == b"hi"

    writer.close()
    server.close()
    await server.wait_closed()


def test_provider_requires_port_as_int() -> None:
    with pytest.raises(ValidationError):
        PlainSocketProvider(host="localhost", port="not-a-port")  # type: ignore[arg-type]
