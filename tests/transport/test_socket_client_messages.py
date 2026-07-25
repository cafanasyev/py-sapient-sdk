from __future__ import annotations

import asyncio

from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import write_framed
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_sapient_message, make_status_report
from tests.transport.conftest import wait_until


async def test_messages_yields_decoded_incoming_messages() -> None:
    sent = make_sapient_message(status_report=make_status_report())

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await write_framed(writer, sent.to_pb2().SerializeToString())
        await reader.read()  # keep open

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    received = await asyncio.wait_for(anext(aiter(client.messages())), timeout=2.0)
    assert received.node_id == sent.node_id

    await client.close()
    server.close()
    await server.wait_closed()


async def test_messages_skips_a_malformed_frame_and_keeps_reading() -> None:
    valid = make_sapient_message(status_report=make_status_report())

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        # One garbage frame (correctly framed, but the content is not a valid
        # protobuf SapientMessage) followed by a real message. The reader must
        # drop the bad one and still deliver the good one.
        await write_framed(writer, b"not valid protobuf")
        await write_framed(writer, valid.to_pb2().SerializeToString())
        await reader.read()  # keep open

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    received = await asyncio.wait_for(anext(aiter(client.messages())), timeout=2.0)
    assert received.node_id == valid.node_id
    assert client.state == ConnectionState.CONNECTED  # connection survived

    await client.close()
    server.close()
    await server.wait_closed()


async def test_messages_backpressure_blocks_reader_when_queue_is_full() -> None:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        for _ in range(3):
            msg = make_sapient_message(status_report=make_status_report())
            await write_framed(writer, msg.to_pb2().SerializeToString())
        await reader.read()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port), queue_maxsize=1
    )
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    # Only consume one message; the reader loop must still be alive (not crashed)
    # blocked trying to push the second one into the full queue.
    stream = client.messages()
    first = await asyncio.wait_for(anext(stream), timeout=2.0)
    assert first is not None

    # Prove backpressure actually happened, not just that one message arrived:
    # with maxsize=1, the reader loop must be blocked on `queue.put(msg #2)`
    # right now — a fixed pause is long enough for the (buggy) alternative,
    # a reader that dropped or crashed, to have already surfaced.
    await asyncio.sleep(0.2)
    assert client.state == ConnectionState.CONNECTED  # reader task hasn't died

    # Draining now must unblock the reader and let it push #2 and #3 through —
    # if it had silently dropped them instead of blocking, these would hang.
    second = await asyncio.wait_for(anext(stream), timeout=2.0)
    third = await asyncio.wait_for(anext(stream), timeout=2.0)
    assert second is not None
    assert third is not None

    await client.close()
    server.close()
    await server.wait_closed()
