from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from datetime import timedelta
from typing import Protocol

from sapient_msg.bsi_flex_335_v2_0 import registration_pb2, status_report_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert import Alert
from sapient_msg_pydantic.bsi_flex_335_v2_0.detection_report import DetectionReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Duration, Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task_ack import TaskAck

from sapient_sdk._task_utils import cancel_and_wait, log_task_exception
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.jitter import (
    jittered_interval,
    phase_offset,
    registration_delay,
)
from sapient_sdk.transmission.node import Node

logger = logging.getLogger(__name__)

_NANOS_PER_UNIT: dict[int, int] = {
    registration_pb2.Registration.TimeUnits.TIME_UNITS_NANOSECONDS: 1,
    registration_pb2.Registration.TimeUnits.TIME_UNITS_MICROSECONDS: 1_000,
    registration_pb2.Registration.TimeUnits.TIME_UNITS_MILLISECONDS: 1_000_000,
    registration_pb2.Registration.TimeUnits.TIME_UNITS_SECONDS: 1_000_000_000,
    registration_pb2.Registration.TimeUnits.TIME_UNITS_MINUTES: 60_000_000_000,
    registration_pb2.Registration.TimeUnits.TIME_UNITS_HOURS: 3_600_000_000_000,
    registration_pb2.Registration.TimeUnits.TIME_UNITS_DAYS: 86_400_000_000_000,
}


def to_timedelta(duration: Duration) -> timedelta:
    units = duration.units if duration.units is not None else 0
    value = duration.value if duration.value is not None else 0.0
    nanos_per_unit = _NANOS_PER_UNIT.get(units, 1_000_000_000)
    return timedelta(seconds=(value * nanos_per_unit) / 1_000_000_000)


class _DispatcherProtocol(Protocol):
    async def _on_node_online(self, node_id: uuid.UUID) -> None: ...
    async def _on_node_offline(self, node_id: uuid.UUID) -> None: ...

    @property
    def reregistration_epoch(self) -> int: ...

    async def publish(
        self,
        msg: Registration | StatusReport | TaskAck | Alert | DetectionReport,
        node_id: uuid.UUID,
        timeout: timedelta,
    ) -> SapientMessage: ...


class NodeWrapper:
    def __init__(
        self, node: Node, dispatcher: _DispatcherProtocol, config: NodeDispatcherConfig
    ) -> None:
        self.node = node
        self.dispatcher = dispatcher
        self.config = config
        self.registered = False
        self._registration_epoch = 0
        self._ack_mailbox: asyncio.Queue[RegistrationAck] = asyncio.Queue(maxsize=1)
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.ensure_future(self._run())
        self._task.add_done_callback(
            functools.partial(
                log_task_exception, logger=logger, message="node lifecycle task died"
            )
        )

    def offer_registration_ack(self, ack: RegistrationAck) -> None:
        if self.registered:
            return  # nothing is polling the mailbox during the status-report phase
        try:
            self._ack_mailbox.put_nowait(ack)
        except asyncio.QueueFull:
            pass  # drop and let the caller's timeout fire

    async def _run(self) -> None:
        await self._wait_until_online()
        await self.dispatcher._on_node_online(self.node.node_id)

        try:
            while await self.node.is_online():
                registration = await self._register()
                if registration is None:
                    continue
                await self._run_status_loop(registration)
        finally:
            await self._send_goodbye_if_registered()
            if not await self.node.is_online():
                await self.dispatcher._on_node_offline(self.node.node_id)

    async def _wait_until_online(self) -> None:
        while not await self.node.is_online():
            await asyncio.sleep(self.config.online_check_interval.total_seconds())

    async def _register(self) -> Registration | None:
        while not self._ack_mailbox.empty():
            self._ack_mailbox.get_nowait()  # drain any stale ack from a previous cycle

        delay = registration_delay(self.config.registration_jitter_window)
        await asyncio.sleep(delay.total_seconds())

        registration = await self.node.get_registration()
        try:
            await self.dispatcher.publish(
                registration, self.node.node_id, self.config.publish_timeout
            )
        except TimeoutError:
            logger.warning(
                "registration publish timeout for node: %s", self.node.node_id
            )
            return None

        try:
            ack = await asyncio.wait_for(
                self._ack_mailbox.get(),
                timeout=self.config.registration_ack_timeout.total_seconds(),
            )
        except TimeoutError:
            logger.warning("registration ack timeout for node: %s", self.node.node_id)
            return None

        if not ack.acceptance:
            return None

        self.registered = True
        self._registration_epoch = self.dispatcher.reregistration_epoch
        return registration

    async def _run_status_loop(self, registration: Registration) -> None:
        status_definition = registration.status_definition
        if status_definition is None or status_definition.status_interval is None:
            raise ValueError(
                "registration.status_definition.status_interval must be set"
            )
        status_interval = to_timedelta(status_definition.status_interval)

        grace = self.config.reconnect_grace_period
        server_retention = (
            status_interval * 3 + grace - self.config.connection_loss_detection_delay
        ) * 0.95
        last_sent_at = asyncio.get_running_loop().time()

        await asyncio.sleep(phase_offset(status_interval).total_seconds())

        while await self.node.is_online() and self.registered:
            if self.dispatcher.reregistration_epoch != self._registration_epoch:
                self.registered = False
                return

            elapsed = asyncio.get_running_loop().time() - last_sent_at
            if elapsed > server_retention.total_seconds():
                self.registered = False
                return

            try:
                status_report = await self.node.get_status_report()
                await self.dispatcher.publish(
                    status_report,
                    self.node.node_id,
                    self.config.publish_timeout,
                )
                last_sent_at = asyncio.get_running_loop().time()
            except TimeoutError:
                logger.warning(
                    "status report publish timeout for node: %s", self.node.node_id
                )

            await asyncio.sleep(jittered_interval(status_interval).total_seconds())

    async def _send_goodbye_if_registered(self) -> None:
        if not self.registered:
            return
        self.registered = False  # no await between check and clear: atomic here
        logger.info("sending GOOD BYE for node: %s", self.node.node_id)
        goodbye = await self.node.get_status_report()
        goodbye.system = status_report_pb2.StatusReport.System.SYSTEM_GOODBYE
        try:
            await self.dispatcher.publish(
                goodbye, self.node.node_id, self.config.publish_timeout
            )
        except TimeoutError:
            logger.warning("goodbye publish timeout for node: %s", self.node.node_id)

    async def close(self) -> None:
        task, self._task = self._task, None  # no await between claim and clear
        await cancel_and_wait(task)
        await self._send_goodbye_if_registered()
