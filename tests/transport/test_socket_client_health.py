from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from pydantic import ValidationError

from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import encode_prefix, write_framed
from sapient_sdk.transport.health.config import HealthCheckConfig, HealthCheckType
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_sapient_message, make_status_report
from tests.transport.conftest import wait_until


class _Server:
    """Accepts connections, sends the scripted bytes, then holds the line open."""

    def __init__(self, script: bytes = b"") -> None:
        self.script = script
        self.accepts = 0

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.accepts += 1
        if self.script:
            writer.write(self.script)
            await writer.drain()
        try:
            await reader.read()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()


async def _start(server: _Server) -> tuple[asyncio.Server, str, int]:
    srv = await asyncio.start_server(server.handle, "127.0.0.1", 0)
    host, port = srv.sockets[0].getsockname()[:2]
    return srv, host, port


def _client(
    host: str, port: int, check_type: HealthCheckType
) -> tuple[SocketClient, list[ConnectionState]]:
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        health_check=HealthCheckConfig(type=check_type),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    # the server also counts the client's plain TCP probes, so connections are
    # counted from the client's state events instead
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))
    return client, seen


def _message_frame() -> bytes:
    msg = make_sapient_message(status_report=make_status_report())
    payload: bytes = msg.to_pb2().SerializeToString()
    return encode_prefix(len(payload)) + payload


async def _stop(client: SocketClient, srv: asyncio.Server) -> None:
    await client.close()
    srv.close()
    await srv.wait_closed()


def test_detection_delay_comes_from_the_health_check_config() -> None:
    client = SocketClient(
        socket_provider=PlainSocketProvider(host="127.0.0.1", port=1),
        health_check=HealthCheckConfig(
            interval=timedelta(seconds=4), timeout=timedelta(seconds=2)
        ),
    )
    assert client.connection_loss_detection_delay == timedelta(seconds=14)


def test_default_client_uses_the_default_health_check() -> None:
    client = SocketClient(socket_provider=PlainSocketProvider(host="127.0.0.1", port=1))
    assert client.health_check == HealthCheckConfig()
    assert client.connection_loss_detection_delay == timedelta(seconds=32)


def test_non_positive_max_frame_size_is_rejected() -> None:
    with pytest.raises(ValidationError):
        SocketClient(
            socket_provider=PlainSocketProvider(host="127.0.0.1", port=1),
            max_frame_size=0,
        )


@pytest.mark.parametrize("old_field", ["probe_timeout", "watchdog_interval"])
def test_old_fields_are_rejected(old_field: str) -> None:
    with pytest.raises(ValidationError, match="Extra inputs"):
        SocketClient(
            socket_provider=PlainSocketProvider(host="127.0.0.1", port=1),
            **{old_field: timedelta(seconds=1)},  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("check_type", "keepalive"),
    [
        (HealthCheckType.ECHO, encode_prefix(0)),
        (HealthCheckType.PINGPONG, encode_prefix(0xFFFFFFFF)),
    ],
)
async def test_keepalive_frame_is_consumed_and_not_delivered(
    check_type: HealthCheckType, keepalive: bytes
) -> None:
    server = _Server(keepalive + _message_frame())
    srv, host, port = await _start(server)
    client, seen = _client(host, port, check_type)
    await client.start()

    received = await asyncio.wait_for(anext(aiter(client.messages())), timeout=2.0)
    assert received.status_report is not None
    assert client.state == ConnectionState.CONNECTED
    assert ConnectionState.DISCONNECTED not in seen
    await _stop(client, srv)


@pytest.mark.parametrize(
    "header",
    [encode_prefix(0xFFFFFFFF), encode_prefix(16 * 1024 * 1024 + 1)],
)
async def test_bad_length_prefix_drops_the_connection_and_reconnects(
    header: bytes,
) -> None:
    server = _Server(header)
    srv, host, port = await _start(server)
    client, seen = _client(host, port, HealthCheckType.NETCAT)
    await client.start()

    await wait_until(lambda: seen.count(ConnectionState.CONNECTED) >= 2)
    assert client._run_task is not None and not client._run_task.done()
    await _stop(client, srv)


async def test_empty_frame_does_not_drop_the_connection() -> None:
    server = _Server(encode_prefix(0) + _message_frame())
    srv, host, port = await _start(server)
    client, seen = _client(host, port, HealthCheckType.NETCAT)
    await client.start()

    received = await asyncio.wait_for(anext(aiter(client.messages())), timeout=2.0)
    assert received.status_report is not None
    assert ConnectionState.DISCONNECTED not in seen
    await _stop(client, srv)


async def test_no_state_events_while_the_server_is_unreachable() -> None:
    client = SocketClient(
        socket_provider=PlainSocketProvider(host="127.0.0.1", port=1),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))

    await client.start()
    await asyncio.sleep(0.2)
    assert seen == []
    await client.close()
    assert seen == [ConnectionState.CLOSED]


async def test_same_state_is_not_reported_twice() -> None:
    client = SocketClient(socket_provider=PlainSocketProvider(host="127.0.0.1", port=1))
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))
    await client.close()
    await client.close()
    assert seen == [ConnectionState.CLOSED]


async def test_probe_reachable_is_plain_tcp() -> None:
    server = _Server()
    srv, host, port = await _start(server)
    client, _ = _client(host, port, HealthCheckType.NETCAT)
    assert await client.probe_reachable(timedelta(seconds=1)) is True
    srv.close()
    await srv.wait_closed()
    assert await client.probe_reachable(timedelta(milliseconds=200)) is False


async def test_write_framed_server_helper_still_works() -> None:
    received: list[bytes] = []

    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await write_framed(w, b"x")
        received.append(await r.read(1))
        w.close()

    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = srv.sockets[0].getsockname()[:2]
    reader, writer = await asyncio.open_connection(host, port)
    assert await reader.readexactly(5) == b"\x01\x00\x00\x00x"
    writer.close()
    srv.close()
    await srv.wait_closed()
