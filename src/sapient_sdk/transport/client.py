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
    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None: ...

    @abc.abstractmethod
    def messages(self) -> AsyncIterator[SapientMessage]: ...

    @property
    @abc.abstractmethod
    def state(self) -> ConnectionState: ...

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
