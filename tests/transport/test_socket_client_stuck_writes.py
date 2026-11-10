from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

import sapient_sdk.transport.framing as framing_module
import sapient_sdk.transport.socket_client as socket_client_module
from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import read_framed
from sapient_sdk.transport.health.config import HealthCheckConfig, HealthCheckType
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_sapient_message, make_status_report
from tests.transport.conftest import wait_until

ONE_MB = 1_000_000


def _big_message() -> SapientMessage:
    return make_sapient_message(status_report=make_status_report(mode="x" * ONE_MB))


class _NeverReads:
    """Accepts and never reads, so the client's send buffers fill up."""

    def __init__(self) -> None:
        self.stop = asyncio.Event()
        self.accepts = 0
        # the client's state events; accepts also count its plain TCP probes
        self.states: list[ConnectionState] = []

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.accepts += 1
        await self.stop.wait()
        writer.close()


async def _connected(
    handler: _NeverReads, health: HealthCheckConfig | None = None
) -> tuple[SocketClient, asyncio.Server]:
    srv = await asyncio.start_server(handler.handle, "127.0.0.1", 0)
    host, port = srv.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        health_check=health or HealthCheckConfig(),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    client.add_state_change_listener(lambda state, ts: handler.states.append(state))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)
    return client, srv


async def _stop(
    client: SocketClient, srv: asyncio.Server, handler: _NeverReads
) -> None:
    handler.stop.set()
    await client.close()
    srv.close()
    await srv.wait_closed()


async def test_publish_gives_up_by_its_deadline_while_the_write_is_stuck() -> None:
    handler = _NeverReads()
    client, srv = await _connected(handler)
    loop = asyncio.get_running_loop()
    msg = _big_message()

    with pytest.raises(TimeoutError, match="publish timeout"):
        for _ in range(20):
            started = loop.time()
            await client.publish(msg, timedelta(milliseconds=100))
    assert loop.time() - started < 0.4
    await _stop(client, srv, handler)


async def test_buffer_holds_at_most_one_stuck_message() -> None:
    handler = _NeverReads()
    client, srv = await _connected(handler)
    msg = _big_message()

    for _ in range(20):
        try:
            await client.publish(msg, timedelta(milliseconds=100))
        except TimeoutError:
            pass
    writer = client._writer
    assert writer is not None
    assert writer.transport.get_write_buffer_size() <= 2 * ONE_MB
    await _stop(client, srv, handler)


async def test_cancelling_a_stuck_publisher_keeps_the_connection() -> None:
    handler = _NeverReads()
    client, srv = await _connected(handler)
    msg = _big_message()
    for _ in range(10):
        try:
            await client.publish(msg, timedelta(milliseconds=50))
        except TimeoutError:
            pass

    publisher = asyncio.ensure_future(client.publish(msg, timedelta(seconds=5)))
    await asyncio.sleep(0.1)
    publisher.cancel()
    await asyncio.gather(publisher, return_exceptions=True)

    assert client.state == ConnectionState.CONNECTED
    assert ConnectionState.DISCONNECTED not in handler.states
    await _stop(client, srv, handler)


async def test_stuck_ping_still_drops_the_dead_connection() -> None:
    handler = _NeverReads()
    health = HealthCheckConfig(
        type=HealthCheckType.PINGPONG,
        interval=timedelta(milliseconds=100),
        timeout=timedelta(milliseconds=50),
        failure_threshold=3,
    )
    client, srv = await _connected(handler, health)
    msg = _big_message()
    for _ in range(10):
        try:
            await client.publish(msg, timedelta(milliseconds=50))
        except TimeoutError:
            pass

    await wait_until(
        lambda: ConnectionState.DISCONNECTED in handler.states, timeout=3.0
    )
    await _stop(client, srv, handler)


async def test_close_flushes_a_message_still_in_the_buffer() -> None:
    received: list[bytes] = []
    reading = asyncio.Event()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reading.wait()
        received.append(await read_framed(reader))
        writer.close()

    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = srv.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        health_check=HealthCheckConfig(timeout=timedelta(seconds=2)),
    )
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    msg = make_sapient_message(status_report=make_status_report(mode="y" * 8 * ONE_MB))
    with pytest.raises(TimeoutError):
        await client.publish(msg, timedelta(milliseconds=50))

    asyncio.get_running_loop().call_later(0.2, reading.set)
    await client.close()

    await wait_until(lambda: len(received) == 1, timeout=3.0)
    assert len(received[0]) > 8 * ONE_MB
    srv.close()
    await srv.wait_closed()


async def test_failed_write_is_retried_on_the_next_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[bytes] = []
    accepts = 0

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        nonlocal accepts
        accepts += 1
        try:
            received.append(await read_framed(reader))
            await reader.read()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = srv.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    real_write_all = framing_module.write_all
    failures = [ConnectionResetError("boom")]

    async def _fail_once(writer: asyncio.StreamWriter, data: bytes) -> None:
        if failures:
            raise failures.pop()
        await real_write_all(writer, data)

    monkeypatch.setattr(socket_client_module, "write_all", _fail_once)

    msg = make_sapient_message(status_report=make_status_report())
    await client.publish(msg, timedelta(seconds=3))
    await wait_until(lambda: len(received) == 1)
    assert seen.count(ConnectionState.CONNECTED) >= 2

    await client.close()
    srv.close()
    await srv.wait_closed()
