from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

import sapient_sdk.transport.socket_client as socket_client_module
from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import read_framed
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_sapient_message, make_status_report
from tests.transport.conftest import start_loopback_server, wait_until


async def test_publish_writes_a_framed_message() -> None:
    received: list[bytes] = []

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        payload = await read_framed(reader)
        received.append(payload)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    msg = make_sapient_message(status_report=make_status_report())
    await client.publish(msg, timedelta(seconds=2))
    await wait_until(lambda: len(received) == 1)

    from sapient_msg.bsi_flex_335_v2_0.sapient_message_pb2 import (
        SapientMessage as SapientMessagePb2,
    )

    parsed = SapientMessagePb2()
    parsed.ParseFromString(received[0])
    assert parsed.node_id == msg.node_id

    await client.close()
    server.close()
    await server.wait_closed()


async def test_publish_before_connected_times_out() -> None:
    client = SocketClient(socket_provider=PlainSocketProvider(host="127.0.0.1", port=1))
    msg = make_sapient_message(status_report=make_status_report())
    with pytest.raises(TimeoutError):
        await client.publish(msg, timedelta(milliseconds=50))


async def test_publish_after_close_fails_fast_instead_of_waiting_out_the_timeout() -> (
    None
):
    client = SocketClient(socket_provider=PlainSocketProvider(host="127.0.0.1", port=1))
    await client.close()  # never started/connected; close() just marks it dead

    msg = make_sapient_message(status_report=make_status_report())
    loop = asyncio.get_running_loop()
    start = loop.time()
    with pytest.raises(TimeoutError):
        # A generous timeout: self._writer can never become non-None again
        # once the client is closed, so publish() must detect that and fail
        # immediately rather than busy-polling for the full 5 seconds.
        await client.publish(msg, timedelta(seconds=5))
    elapsed = loop.time() - start

    assert elapsed < 0.5


async def test_publish_timeout_does_not_poison_the_shared_closed_watcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # publish() shares one long-lived "closed" watcher task across every call
    # (see start()) rather than creating a fresh one each time. A timed-out
    # call's finally-block must only cancel its own write_task -- if it ever
    # cancelled the shared watcher instead, every later publish() call would
    # incorrectly report "SocketClient is closed" forever after.
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    async def _hang(writer: object, payload: bytes) -> None:
        await asyncio.sleep(999)

    monkeypatch.setattr(socket_client_module, "write_framed", _hang)

    msg = make_sapient_message(status_report=make_status_report())
    with pytest.raises(TimeoutError, match="timed out"):
        await client.publish(msg, timedelta(milliseconds=50))

    monkeypatch.undo()

    # Must succeed, not raise "SocketClient is closed".
    await client.publish(msg, timedelta(seconds=2))

    await client.close()
    server.close()
    await server.wait_closed()


async def test_publish_converts_connection_error_to_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    async def _boom(writer: object, payload: bytes) -> None:
        raise ConnectionResetError("boom")

    monkeypatch.setattr(socket_client_module, "write_framed", _boom)

    msg = make_sapient_message(status_report=make_status_report())
    with pytest.raises(TimeoutError, match="connection lost"):
        await client.publish(msg, timedelta(seconds=1))

    await client.close()
    server.close()
    await server.wait_closed()
