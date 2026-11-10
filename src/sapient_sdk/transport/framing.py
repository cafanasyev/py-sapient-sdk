from __future__ import annotations

import asyncio
import struct

_LENGTH_PREFIX = struct.Struct("<I")

# same limit as the SAPIENT server
DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024


class FrameTooLargeError(ValueError):
    """A length prefix announced a body larger than the reader accepts."""


def encode_prefix(value: int) -> bytes:
    return _LENGTH_PREFIX.pack(value)


def encode_frame(payload: bytes) -> bytes:
    return _LENGTH_PREFIX.pack(len(payload)) + payload


async def write_all(writer: asyncio.StreamWriter, data: bytes) -> None:
    writer.write(data)
    await writer.drain()


async def write_framed(writer: asyncio.StreamWriter, payload: bytes) -> None:
    await write_all(writer, encode_frame(payload))


async def read_length(reader: asyncio.StreamReader) -> int:
    header = await reader.readexactly(_LENGTH_PREFIX.size)
    (length,) = _LENGTH_PREFIX.unpack(header)
    return int(length)


def check_frame_size(length: int, max_frame_size: int) -> None:
    if length > max_frame_size:
        raise FrameTooLargeError(
            f"frame too large: {length} bytes, max {max_frame_size} bytes"
        )


async def read_framed(
    reader: asyncio.StreamReader, max_frame_size: int = DEFAULT_MAX_FRAME_SIZE
) -> bytes:
    length = await read_length(reader)
    check_frame_size(length, max_frame_size)
    return await reader.readexactly(length)
