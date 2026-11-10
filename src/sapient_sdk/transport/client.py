from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta
from typing import Self

from pydantic import BaseModel, ConfigDict
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk.transport.connection_state import ConnectionState


class Client(BaseModel, abc.ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @abc.abstractmethod
    async def start(self) -> None: ...

    @abc.abstractmethod
    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None:
        """Sends one message to the server.

        ``timeout`` covers both the wait for the connection and the write. A
        ``TimeoutError`` raised during the write does not mean the message was not
        sent: a message already being written is finished and may still arrive.
        """

    @abc.abstractmethod
    def messages(self) -> AsyncIterator[SapientMessage]: ...

    @property
    @abc.abstractmethod
    def state(self) -> ConnectionState: ...

    @property
    @abc.abstractmethod
    def connection_loss_detection_delay(self) -> timedelta:
        """Worst case time between the network dying and this client reporting
        DISCONNECTED.

        The dispatcher adds it to the measured reconnect gap and takes it off the
        server retention window, so both re-registration checks follow the real
        outage rather than the moment the client happened to notice.
        """

    @property
    def is_connected(self) -> bool:
        return self.state == ConnectionState.CONNECTED

    @abc.abstractmethod
    def add_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None: ...

    @abc.abstractmethod
    def remove_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None: ...

    @abc.abstractmethod
    async def probe_reachable(self, timeout: timedelta) -> bool: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
