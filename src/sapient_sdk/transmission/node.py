from __future__ import annotations

import abc
import uuid

from pydantic import BaseModel, ConfigDict
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert_ack import AlertAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.error import Error
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task import Task


class Node(BaseModel, abc.ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    @abc.abstractmethod
    def node_id(self) -> uuid.UUID: ...

    @abc.abstractmethod
    async def is_online(self) -> bool: ...

    @abc.abstractmethod
    async def get_registration(self) -> Registration: ...

    @abc.abstractmethod
    async def get_status_report(self) -> StatusReport: ...

    @abc.abstractmethod
    async def on_registration_ack(self, ack: RegistrationAck) -> None: ...

    @abc.abstractmethod
    async def on_alert_ack(self, ack: AlertAck) -> None: ...

    @abc.abstractmethod
    async def on_task(self, task: Task) -> None: ...

    @abc.abstractmethod
    async def on_error(self, error: Error) -> None: ...
