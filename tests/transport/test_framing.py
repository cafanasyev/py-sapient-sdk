from __future__ import annotations

import asyncio

import pytest

from sapient_sdk.transport.framing import read_framed, write_framed


async def test_round_trips_through_a_pipe() -> None:
    server_reader = None
    server_writer = None
    keep_alive = asyncio.Event()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal server_reader, server_writer
        server_reader = reader
        server_writer = writer
        try:
            await asyncio.wait_for(keep_alive.wait(), timeout=30)
        except (TimeoutError, asyncio.CancelledError):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    _, client_writer = await asyncio.open_connection(host, port)
    await asyncio.sleep(0.05)  # let the server accept

    await write_framed(client_writer, b"hello world")
    assert server_reader is not None
    payload = await read_framed(server_reader)
    assert payload == b"hello world"

    client_writer.close()
    keep_alive.set()
    assert server_writer is not None
    server_writer.close()
    server.close()
    await server.wait_closed()


async def test_read_framed_raises_on_truncated_stream() -> None:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write(b"\x05\x00\x00\x00ab")  # claims 5 bytes, sends 2, then closes
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    reader, writer = await asyncio.open_connection(host, port)

    with pytest.raises(asyncio.IncompleteReadError):
        await read_framed(reader)

    writer.close()
    server.close()
    await server.wait_closed()
