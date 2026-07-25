from __future__ import annotations

import uuid
from datetime import timedelta

from sapient_msg_pydantic.bsi_flex_335_v2_0.alert import Alert
from sapient_msg_pydantic.bsi_flex_335_v2_0.detection_report import DetectionReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task_ack import TaskAck


class FakeDispatcher:
    def __init__(self) -> None:
        self.online_calls: list[uuid.UUID] = []
        self.offline_calls: list[uuid.UUID] = []
        self.published: list[object] = []
        self._epoch = 0
        self.fail_next_publish = False

    async def _on_node_online(self, node_id: uuid.UUID) -> None:
        self.online_calls.append(node_id)

    async def _on_node_offline(self, node_id: uuid.UUID) -> None:
        self.offline_calls.append(node_id)

    @property
    def reregistration_epoch(self) -> int:
        return self._epoch

    def bump_epoch(self) -> None:
        self._epoch += 1

    async def publish(
        self,
        msg: Registration | StatusReport | TaskAck | Alert | DetectionReport,
        node_id: uuid.UUID,
        timeout: timedelta,
    ) -> SapientMessage:
        if self.fail_next_publish:
            self.fail_next_publish = False
            raise TimeoutError("simulated publish timeout")
        self.published.append(msg)
        node_id_str = str(node_id)
        return SapientMessage(
            node_id=node_id_str,
            destination_id=node_id_str,
            registration=msg if isinstance(msg, Registration) else None,
            status_report=msg if isinstance(msg, StatusReport) else None,
        )
