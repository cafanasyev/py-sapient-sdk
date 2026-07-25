from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta

from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk.transport.client import Client
from sapient_sdk.transport.connection_state import ConnectionState


class FakeClient(Client):
    _state: ConnectionState = ConnectionState.DISCONNECTED
    _listeners: list[Callable[[ConnectionState, datetime], object]] = []
    start_calls: int = 0
    close_calls: int = 0
    published: list[SapientMessage] = []
    fail_next_publish: bool = False

    def model_post_init(self, __context: object) -> None:
        self._listeners = []
        self.published = []

    def set_state(self, state: ConnectionState) -> None:
        self._state = state
        ts = datetime.now()
        for listener in list(self._listeners):
            listener(state, ts)

    async def start(self) -> None:
        self.start_calls += 1
        self._state = ConnectionState.CONNECTED

    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None:
        if self.fail_next_publish:
            self.fail_next_publish = False
            raise TimeoutError("simulated publish timeout")
        self.published.append(msg)

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
        self._listeners.append(listener)

    def remove_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def probe_reachable(self, timeout: timedelta) -> bool:
        return True

    async def close(self) -> None:
        self.close_calls += 1
        self._state = ConnectionState.CLOSED
