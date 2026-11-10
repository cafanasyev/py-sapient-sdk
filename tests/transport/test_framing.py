from __future__ import annotations

import asyncio

import pytest

from sapient_sdk.transport.framing import (
    DEFAULT_MAX_FRAME_SIZE,
    FrameTooLargeError,
    check_frame_size,
    encode_frame,
    encode_prefix,
    read_framed,
    read_length,
    write_framed,
)


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


def test_zero_length_is_allowed() -> None:
    check_frame_size(0, DEFAULT_MAX_FRAME_SIZE)


def test_exactly_max_is_allowed() -> None:
    check_frame_size(DEFAULT_MAX_FRAME_SIZE, DEFAULT_MAX_FRAME_SIZE)


def test_one_byte_over_max_is_rejected() -> None:
    with pytest.raises(FrameTooLargeError):
        check_frame_size(DEFAULT_MAX_FRAME_SIZE + 1, DEFAULT_MAX_FRAME_SIZE)


@pytest.mark.parametrize("length", [0xFFFFFFFF, 0x7FFFFFFF, 0x80000000])
def test_huge_lengths_are_rejected(length: int) -> None:
    with pytest.raises(FrameTooLargeError):
        check_frame_size(length, DEFAULT_MAX_FRAME_SIZE)


def test_custom_limit_is_applied() -> None:
    check_frame_size(100, 100)
    with pytest.raises(FrameTooLargeError):
        check_frame_size(101, 100)


def test_default_limit_is_16_mib() -> None:
    assert DEFAULT_MAX_FRAME_SIZE == 16 * 1024 * 1024


def test_frame_is_little_endian_prefix_plus_body() -> None:
    assert encode_frame(b"ab") == b"\x02\x00\x00\x00ab"
    assert encode_prefix(0xFFFFFFFF) == b"\xff\xff\xff\xff"


async def test_read_length_reads_an_unsigned_prefix() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(b"\xff\xff\xff\xff")
    assert await read_length(reader) == 0xFFFFFFFF
