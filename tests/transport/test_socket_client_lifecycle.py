from __future__ import annotations

from datetime import timedelta

from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_sapient_message, make_status_report
from tests.transport.conftest import start_loopback_server, wait_until


async def test_starts_disconnected() -> None:
    client = SocketClient(socket_provider=PlainSocketProvider(host="localhost", port=1))
    assert client.state == ConnectionState.DISCONNECTED


async def test_start_transitions_to_connected() -> None:
    server, host, port = await start_loopback_server()
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    await client.close()
    server.close()
    await server.wait_closed()


async def test_close_transitions_to_closed_and_awaits_teardown() -> None:
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    await client.close()
    assert client.state == ConnectionState.CLOSED

    server.close()
    await server.wait_closed()


async def test_start_after_close_allows_publish_again() -> None:
    # start() resets _closed_event -- without that reset, publish() would
    # incorrectly keep reporting "SocketClient is closed" forever after any
    # restart, since the event set by the earlier close() would still be set.
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))

    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)
    await client.close()
    assert client.state == ConnectionState.CLOSED

    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)

    msg = make_sapient_message(status_report=make_status_report())
    await client.publish(msg, timedelta(seconds=2))  # must not raise

    await client.close()
    server.close()
    await server.wait_closed()


async def test_state_change_listener_is_notified() -> None:
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))

    await client.start()
    await wait_until(lambda: ConnectionState.CONNECTED in seen)
    await client.close()

    assert ConnectionState.CONNECTING in seen
    assert ConnectionState.CONNECTED in seen
    assert ConnectionState.CLOSED in seen

    server.close()
    await server.wait_closed()


async def test_state_change_listener_exception_does_not_break_state_machine() -> None:
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))

    def _raising_listener(state: ConnectionState, ts: object) -> None:
        raise RuntimeError("boom")

    client.add_state_change_listener(_raising_listener)

    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)
    await client.close()
    assert client.state == ConnectionState.CLOSED

    server.close()
    await server.wait_closed()


async def test_async_state_change_listener_exception_does_not_break_state_machine() -> (
    None
):
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))

    async def _raising_async_listener(state: ConnectionState, ts: object) -> None:
        raise RuntimeError("boom")

    client.add_state_change_listener(_raising_async_listener)

    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)
    await client.close()
    assert client.state == ConnectionState.CLOSED

    server.close()
    await server.wait_closed()
