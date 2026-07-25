from __future__ import annotations

import asyncio
from datetime import timedelta

from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import read_framed
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_sapient_message, make_status_report
from tests.transport.conftest import wait_until


async def test_reconnects_after_server_restarts() -> None:
    accepted_writer: asyncio.StreamWriter | None = None

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal accepted_writer
        accepted_writer = writer
        try:
            await reader.read()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        initial_reconnect_delay=timedelta(milliseconds=20),
    )
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    # Explicitly close the server-side connection rather than relying on
    # server.close()/wait_closed() alone: on Python 3.11, Server.wait_closed()
    # returns as soon as the listening socket stops accepting new connections
    # — it does NOT wait for already-accepted connections to close too (that
    # semantic only arrived in 3.12). Without this, the client's read would
    # never actually break on 3.11 and the test would hang past its timeout.
    assert accepted_writer is not None
    accepted_writer.close()
    server.close()
    await server.wait_closed()
    await wait_until(lambda: client.state == ConnectionState.DISCONNECTED)

    server2 = await asyncio.start_server(_hold_open, host, port)
    await wait_until(lambda: client.state == ConnectionState.CONNECTED, timeout=3.0)

    await client.close()
    server2.close()
    await server2.wait_closed()


async def test_publish_succeeds_after_reconnect() -> None:
    # test_reconnects_after_server_restarts only checks that client.state
    # transitions back to CONNECTED. This proves the write path itself works
    # again too -- i.e. that _writer_ready gets re-armed on the *second*
    # connection episode, not just the first.
    accepted_writer: asyncio.StreamWriter | None = None

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal accepted_writer
        accepted_writer = writer
        try:
            await reader.read()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        initial_reconnect_delay=timedelta(milliseconds=20),
    )
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    assert accepted_writer is not None
    accepted_writer.close()
    server.close()
    await server.wait_closed()
    await wait_until(lambda: client.state == ConnectionState.DISCONNECTED)

    received: list[bytes] = []

    async def handle2(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        received.append(await read_framed(reader))

    server2 = await asyncio.start_server(handle2, host, port)
    await wait_until(lambda: client.state == ConnectionState.CONNECTED, timeout=3.0)

    msg = make_sapient_message(status_report=make_status_report())
    await client.publish(msg, timedelta(seconds=2))
    await wait_until(lambda: len(received) == 1)

    await client.close()
    server2.close()
    await server2.wait_closed()


async def test_close_during_backoff_stops_the_loop_without_another_attempt() -> None:
    # No server listening at all: every connection attempt fails immediately,
    # so the client sits in its backoff sleep. close() during that sleep must
    # win — no further transition should occur afterward.
    #
    # Note on what this actually proves: _run_loop's backoff sleep is
    # asyncio.wait_for(self._closed_event.wait(), timeout=delay), so close()
    # setting that event wakes the sleep directly and _run_loop observes
    # self._closed_event.is_set() on its very next loop check -- this is now
    # the primary mechanism, not a fallback. close()'s unconditional
    # task.cancel() remains a second, independent guarantee (matters if the
    # task is suspended anywhere other than this wait_for). This test
    # verifies the end-to-end guarantee callers actually depend on -- no
    # transition after close() -- not which specific mechanism produced it.
    client = SocketClient(
        socket_provider=PlainSocketProvider(host="127.0.0.1", port=1),
        initial_reconnect_delay=timedelta(seconds=5),
    )
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))

    await client.start()
    await wait_until(lambda: ConnectionState.DISCONNECTED in seen)
    await client.close()

    count_before = len(seen)
    await asyncio.sleep(0.2)
    assert len(seen) == count_before  # nothing further was emitted


async def _hold_open(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        await reader.read()
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
