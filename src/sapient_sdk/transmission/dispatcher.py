from __future__ import annotations

import abc
import asyncio
import functools
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import assert_never

from pydantic import BaseModel, PrivateAttr
from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2, task_ack_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert import Alert
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert_ack import AlertAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.detection_report import DetectionReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.error import Error
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import Info, StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task import Task
from sapient_msg_pydantic.bsi_flex_335_v2_0.task_ack import TaskAck
from ulid import ULID

from sapient_sdk._task_utils import cancel_and_wait, log_task_exception
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.node import Node
from sapient_sdk.transmission.node_wrapper import NodeWrapper
from sapient_sdk.transport.client import Client
from sapient_sdk.transport.connection_state import ConnectionState

logger = logging.getLogger(__name__)

_CONTENT_FIELDS = (
    "registration",
    "registration_ack",
    "status_report",
    "detection_report",
    "task",
    "task_ack",
    "alert",
    "alert_ack",
    "error",
)


def _content_type(message: SapientMessage) -> str:
    for field in _CONTENT_FIELDS:
        if getattr(message, field) is not None:
            return field
    return "unknown"


def _log_body(message: SapientMessage) -> None:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("message: %s", message.model_dump_json())


class NodeDispatcher(BaseModel, abc.ABC):
    @abc.abstractmethod
    async def register(self, node: Node) -> None: ...

    @abc.abstractmethod
    async def unregister(self, node: Node) -> None: ...

    @abc.abstractmethod
    async def publish(
        self,
        msg: Registration | StatusReport | TaskAck | Alert | DetectionReport,
        node_id: uuid.UUID,
        timeout: timedelta,
    ) -> SapientMessage: ...

    @abc.abstractmethod
    async def close(self) -> None: ...


class DefaultNodeDispatcher(NodeDispatcher):
    client: Client
    config: NodeDispatcherConfig

    _online: dict[uuid.UUID, NodeWrapper] = PrivateAttr(default_factory=dict)
    _offline: dict[uuid.UUID, NodeWrapper] = PrivateAttr(default_factory=dict)
    _client_running: bool = PrivateAttr(default=False)
    _disconnected_at: datetime | None = PrivateAttr(default=None)
    _reregistration_epoch: int = PrivateAttr(default=0)
    _last_status_report: dict[uuid.UUID, StatusReport] = PrivateAttr(
        default_factory=dict
    )
    _inbound_messages_task: asyncio.Task[None] | None = PrivateAttr(default=None)

    def model_post_init(self, __context: object) -> None:
        self.client.add_state_change_listener(self._on_client_state_change)
        self._inbound_messages_task = asyncio.ensure_future(self._consume_messages())
        self._inbound_messages_task.add_done_callback(
            functools.partial(
                log_task_exception,
                logger=logger,
                message="message-consumption task died",
            )
        )

    async def _consume_messages(self) -> None:
        async for msg in self.client.messages():
            await self._on_message(msg)

    def _find_wrapper(self, node_id: uuid.UUID) -> NodeWrapper | None:
        return self._online.get(node_id) or self._offline.get(node_id)

    async def _on_message(self, message: SapientMessage) -> None:
        try:
            await self._route_message(message)
        except Exception:
            logger.exception(
                "error handling incoming message for destination %s",
                message.destination_id,
            )

    async def _route_message(self, message: SapientMessage) -> None:
        destination_id = message.destination_id
        if destination_id is None:
            logger.warning("received message with no destination_id, dropping")
            return
        # destination_id is `is_uuid: true` in the ICD -- SapientMessage's own
        # validator already guarantees it parses, on every construction path.
        node_id = uuid.UUID(destination_id)
        logger.info("received %s for node: %s", _content_type(message), node_id)
        _log_body(message)
        node_wrapper = self._find_wrapper(node_id)

        match message:
            case SapientMessage(registration_ack=RegistrationAck() as ack):
                if node_wrapper is not None:
                    await self._handle_registration_ack(ack, node_wrapper)
            case SapientMessage(alert_ack=AlertAck() as ack):
                if node_wrapper is not None:
                    await node_wrapper.node.on_alert_ack(ack)
            case SapientMessage(error=Error() as err):
                if node_wrapper is not None:
                    await node_wrapper.node.on_error(err)
            case SapientMessage(task=Task() as task):
                await self._handle_task(task, node_id, node_wrapper)

    @staticmethod
    async def _handle_registration_ack(
        ack: RegistrationAck, wrapper: NodeWrapper
    ) -> None:
        await wrapper.node.on_registration_ack(ack)
        if wrapper.registered:
            # Status-report phase: nothing is polling the ack mailbox, so a
            # Task-driven re-registration ack is delivered straight to the
            # node above. A rejection must still force the wrapper back to
            # the register phase (CHANGELOG §6) rather than being dropped.
            if not ack.acceptance:
                wrapper.registered = False
        else:
            wrapper.offer_registration_ack(ack)

    async def _handle_task(
        self, task: Task, node_id: uuid.UUID, wrapper: NodeWrapper | None
    ) -> None:
        reject_reason: str | None = None
        if wrapper is None:
            reject_reason = f"node not registered: {node_id}"
        elif node_id not in self._online:
            reject_reason = "node offline"

        if reject_reason is not None:
            rejection = TaskAck(
                task_id=task.task_id,
                task_status=task_ack_pb2.TaskAck.TaskStatus.TASK_STATUS_REJECTED,
                reason=[reject_reason],
            )
            await self.publish(rejection, node_id, self.config.publish_timeout)
            return

        assert wrapper is not None
        is_registration_request = (
            task.command is not None
            and task.command.request is not None
            and task.command.request.lower() == "registration"
        )
        if is_registration_request:
            registration = await wrapper.node.get_registration()
            await self.publish(registration, node_id, self.config.publish_timeout)
            return

        await wrapper.node.on_task(task)

    def _on_client_state_change(self, state: ConnectionState, ts: datetime) -> None:
        if state == ConnectionState.DISCONNECTED:
            if self._disconnected_at is None:  # first-write-wins
                self._disconnected_at = ts
        elif state == ConnectionState.CONNECTED:
            lost = self._disconnected_at
            self._disconnected_at = None
            if lost is None:
                return
            gap = (ts - lost) + self.config.connection_loss_detection_delay
            if (
                gap.total_seconds()
                > self.config.reconnect_grace_period.total_seconds() * 0.95
            ):
                self._reregistration_epoch += 1

    @property
    def reregistration_epoch(self) -> int:
        return self._reregistration_epoch

    async def _on_node_online(self, node_id: uuid.UUID) -> None:
        wrapper = self._offline.pop(node_id, None) or self._online.get(node_id)
        if wrapper is None:
            return  # unregistered before it finished coming online
        self._online[node_id] = wrapper
        if len(self._online) == 1:
            await self.client.start()
            self._client_running = True

    async def _on_node_offline(self, node_id: uuid.UUID) -> None:
        wrapper = self._online.pop(node_id, None)
        if wrapper is not None:
            self._offline[node_id] = wrapper
        if not self._online:
            await self._close_client_if_needed()

    async def _close_client_if_needed(self) -> None:
        if not self._client_running:
            return
        self._client_running = False
        await self.client.close()

    async def register(self, node: Node) -> None:
        logger.info("registering node: %s", node.node_id)
        if node.node_id in self._online or node.node_id in self._offline:
            return
        wrapper = NodeWrapper(node=node, dispatcher=self, config=self.config)
        self._offline[node.node_id] = wrapper
        wrapper.start()

    async def unregister(self, node: Node) -> None:
        logger.info("unregistering node: %s", node.node_id)
        was_online = node.node_id in self._online
        wrapper = self._offline.pop(node.node_id, None) or self._online.pop(
            node.node_id, None
        )
        if wrapper is None:
            return
        await wrapper.close()
        self._last_status_report.pop(node.node_id, None)
        # If this was the last online node, close the shared client the same
        # way _on_node_offline would — unregister() removes the wrapper here
        # directly rather than through that callback, so it must not skip the
        # check, or the client would leak running with no online nodes left.
        if was_online and not self._online:
            await self._close_client_if_needed()

    def _compute_info(self, node_id: uuid.UUID, status: StatusReport) -> Info:
        previous = self._last_status_report.get(node_id)
        if previous is None:
            return (
                status.info
                if status.info is not None
                else status_report_pb2.StatusReport.Info.INFO_NEW
            )
        clear = {"report_id": None, "info": None}
        if previous.model_copy(update=clear) == status.model_copy(update=clear):
            return status_report_pb2.StatusReport.Info.INFO_UNCHANGED
        return (
            status.info
            if status.info is not None
            else status_report_pb2.StatusReport.Info.INFO_NEW
        )

    async def publish(
        self,
        msg: Registration | StatusReport | TaskAck | Alert | DetectionReport,
        node_id: uuid.UUID,
        timeout: timedelta,
    ) -> SapientMessage:
        now = datetime.now(UTC)
        destination_id = self.config.destination_id
        node_id_str = str(node_id)
        match msg:
            case Registration():
                envelope = SapientMessage(
                    node_id=node_id_str,
                    destination_id=destination_id,
                    timestamp=now,
                    registration=msg,
                )
            case StatusReport():
                # Mutates the caller's StatusReport in place with the computed
                # dedup info field -- callers must not reuse the same instance
                # across publish() calls expecting it to stay untouched.
                if msg.report_id is None:
                    msg.report_id = str(ULID())
                msg.info = self._compute_info(node_id, msg)
                envelope = SapientMessage(
                    node_id=node_id_str,
                    destination_id=destination_id,
                    timestamp=now,
                    status_report=msg,
                )
            case TaskAck():
                envelope = SapientMessage(
                    node_id=node_id_str,
                    destination_id=destination_id,
                    timestamp=now,
                    task_ack=msg,
                )
            case Alert():
                envelope = SapientMessage(
                    node_id=node_id_str,
                    destination_id=destination_id,
                    timestamp=now,
                    alert=msg,
                )
            case DetectionReport():
                if msg.report_id is None:
                    msg.report_id = str(ULID())
                envelope = SapientMessage(
                    node_id=node_id_str,
                    destination_id=destination_id,
                    timestamp=now,
                    detection_report=msg,
                )
            case _:
                assert_never(msg)
        logger.info("sending %s to %s", _content_type(envelope), node_id)
        _log_body(envelope)
        await self.client.publish(envelope, timeout)
        if isinstance(msg, StatusReport):
            # Commit the dedup baseline only after the send actually succeeded —
            # last published value must only be committed after a successful send.
            self._last_status_report[node_id] = msg
        return envelope

    async def close(self) -> None:
        wrappers = [*self._online.values(), *self._offline.values()]
        await asyncio.gather(*(w.close() for w in wrappers))
        self._online.clear()
        self._offline.clear()

        await cancel_and_wait(self._inbound_messages_task)
        self._inbound_messages_task = None

        await self._close_client_if_needed()
