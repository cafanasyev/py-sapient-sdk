from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta

import pytest
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk.transport.client import Client
from sapient_sdk.transport.connection_state import ConnectionState


class _StubClient(Client):
    _state: ConnectionState = ConnectionState.DISCONNECTED

    async def start(self) -> None:
        self._state = ConnectionState.CONNECTED

    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None:
        pass

    def messages(self) -> AsyncIterator[SapientMessage]:
        async def _empty() -> AsyncIterator[SapientMessage]:
            return
            yield  # pragma: no cover

        return _empty()

    @property
    def state(self) -> ConnectionState:
        return self._state

    def add_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        pass

    def remove_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        pass

    async def probe_reachable(self, timeout: timedelta) -> bool:
        return True

    async def close(self) -> None:
        self._state = ConnectionState.CLOSED


def test_abstract_client_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="abstract"):
        Client()  # type: ignore[abstract]


async def test_is_connected_reflects_state() -> None:
    client = _StubClient()
    assert client.is_connected is False
    await client.start()
    assert client.is_connected is True


async def test_async_context_manager_starts_and_closes() -> None:
    async with _StubClient() as client:
        state_during: ConnectionState = client.state
        assert state_during == ConnectionState.CONNECTED
    state_after: ConnectionState = client.state
    assert state_after == ConnectionState.CLOSED
