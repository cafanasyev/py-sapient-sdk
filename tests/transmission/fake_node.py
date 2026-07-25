from __future__ import annotations

import uuid

from pydantic import Field, PrivateAttr
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert_ack import AlertAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.error import Error
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task import Task

from sapient_sdk.transmission.node import Node
from tests.fixtures import make_registration, make_status_report


class FakeNode(Node):
    node_id_: uuid.UUID

    _online: bool = PrivateAttr(default=False)
    registration_acks: list[RegistrationAck] = Field(default_factory=list)
    alert_acks: list[AlertAck] = Field(default_factory=list)
    tasks: list[Task] = Field(default_factory=list)
    errors: list[Error] = Field(default_factory=list)

    async def is_online(self) -> bool:
        return self._online

    @property
    def node_id(self) -> uuid.UUID:
        return self.node_id_

    def set_online(self, online: bool) -> None:
        self._online = online

    async def get_registration(self) -> Registration:
        return make_registration()

    async def get_status_report(self) -> StatusReport:
        return make_status_report()

    async def on_registration_ack(self, ack: RegistrationAck) -> None:
        self.registration_acks.append(ack)

    async def on_alert_ack(self, ack: AlertAck) -> None:
        self.alert_acks.append(ack)

    async def on_task(self, task: Task) -> None:
        self.tasks.append(task)

    async def on_error(self, error: Error) -> None:
        self.errors.append(error)
