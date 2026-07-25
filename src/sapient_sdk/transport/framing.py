from __future__ import annotations

import asyncio
import struct

_LENGTH_PREFIX = struct.Struct("<I")


async def write_framed(writer: asyncio.StreamWriter, payload: bytes) -> None:
    writer.write(_LENGTH_PREFIX.pack(len(payload)))
    writer.write(payload)
    await writer.drain()


async def read_framed(reader: asyncio.StreamReader) -> bytes:
    header = await reader.readexactly(_LENGTH_PREFIX.size)
    (length,) = _LENGTH_PREFIX.unpack(header)
    return await reader.readexactly(length)
