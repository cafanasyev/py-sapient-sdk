from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sapient_msg.bsi_flex_335_v2_0 import registration_pb2, status_report_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert import Alert
from sapient_msg_pydantic.bsi_flex_335_v2_0.detection_report import DetectionReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Duration, Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.task_ack import TaskAck

from sapient_sdk._task_utils import log_task_exception
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.jitter import (
    jittered_interval,
    phase_offset,
    registration_delay,
)
from sapient_sdk.transmission.node import Node

logger = logging.getLogger(__name__)

_STOP_TIMEOUT = timedelta(seconds=5)

# fraction applied to period thresholds so scheduler jitter of a few milliseconds
# does not hide a real outage right at the boundary
THRESHOLD_TOLERANCE = 0.95


def _now() -> datetime:
    return datetime.now(UTC)


def server_retention(
    status_interval: timedelta, grace: timedelta, detection_delay: timedelta
) -> timedelta:
    """How long the server keeps the registration after the last confirmed report.

    3 x status_interval is when the server closes the connection, grace is how long
    it keeps the registration after that, and the detection delay is taken off
    because some reports went into a socket the client had not yet seen die.
    Clamped at zero: a negative window would mean re-registering on every tick.
    """
    return max(status_interval * 3 + grace - detection_delay, timedelta(0))


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

    @property
    def connection_loss_detection_delay(self) -> timedelta: ...

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
        self.last_status_report: StatusReport | None = None
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
        node_id = self.node.node_id
        while True:
            try:
                await self._run_once()
            except TimeoutError as exc:
                logger.error("lifecycle timeout for node: %s: %s", node_id, exc)
            except Exception:
                logger.exception("node lifecycle error: %s", node_id)
            finally:
                if not await self.node.is_online():
                    await self.dispatcher._on_node_offline(node_id)

    async def _run_once(self) -> None:
        await self._wait_until_online()
        await self.dispatcher._on_node_online(self.node.node_id)
        while await self.node.is_online():
            registration = await self._register()
            if registration is None:
                await asyncio.sleep(
                    self.config.registration_ack_timeout.total_seconds()
                )
                continue
            await self._run_status_loop(registration)
        await self._send_goodbye_if_registered()
        self.last_status_report = None

    async def _wait_until_online(self) -> None:
        while not await self.node.is_online():
            await asyncio.sleep(self.config.online_check_interval.total_seconds())

    async def _register(self) -> Registration | None:
        # acks go to the mailbox only while not registered, see offer_registration_ack
        self.registered = False
        while not self._ack_mailbox.empty():
            self._ack_mailbox.get_nowait()  # drain any stale ack from a previous cycle

        delay = registration_delay(self.config.registration_jitter_window)
        await asyncio.sleep(delay.total_seconds())

        registration = await self.node.get_registration()
        await self.dispatcher.publish(
            registration, self.node.node_id, self.config.publish_timeout
        )

        ack_timeout = self.config.registration_ack_timeout
        try:
            ack = await asyncio.wait_for(
                self._ack_mailbox.get(), timeout=ack_timeout.total_seconds()
            )
        except TimeoutError as exc:
            logger.error(
                "registration ack timeout for node: %s after %s",
                self.node.node_id,
                ack_timeout,
            )
            raise TimeoutError(
                f"registration ack timeout for node: {self.node.node_id}"
            ) from exc

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
        node_id = self.node.node_id

        detection_delay = self.dispatcher.connection_loss_detection_delay
        raw_retention = server_retention(
            status_interval, self.config.reconnect_grace_period, detection_delay
        )
        if raw_retention == timedelta(0):
            logger.warning(
                "connection loss detection delay %s eats the whole retention budget"
                " for node: %s - the node will re-register on every status tick."
                " Lower the health check interval or the failure threshold.",
                detection_delay,
                node_id,
            )
        retention = raw_retention * THRESHOLD_TOLERANCE
        # wall clock, like Java: the monotonic loop clock stops while the machine sleeps
        last_sent_at = _now()

        await asyncio.sleep(phase_offset(status_interval).total_seconds())

        while await self.node.is_online() and self.registered:
            if self.dispatcher.reregistration_epoch != self._registration_epoch:
                logger.info(
                    "reconnected after grace period, re-registering node: %s", node_id
                )
                self.registered = False
                return

            if _now() - last_sent_at > retention:
                logger.info(
                    "server retention expired for node: %s, re-registering", node_id
                )
                self.registered = False
                return

            try:
                status_report = await self.node.get_status_report()
                sent = await self.dispatcher.publish(
                    status_report, node_id, self.config.publish_timeout
                )
                last_sent_at = sent.timestamp if sent.timestamp is not None else _now()
            except TimeoutError:
                logger.error("status report publish timeout for node: %s", node_id)

            await asyncio.sleep(jittered_interval(status_interval).total_seconds())

    async def _send_goodbye_if_registered(self) -> None:
        if not self.registered:
            return
        self.registered = False  # no await between check and clear: atomic here
        logger.info("sending GOOD BYE for node: %s", self.node.node_id)
        report = await self.node.get_status_report()
        goodbye = report.model_copy(
            update={"system": status_report_pb2.StatusReport.System.SYSTEM_GOODBYE},
            deep=True,
        )
        try:
            await self.dispatcher.publish(
                goodbye, self.node.node_id, self.config.publish_timeout
            )
        except TimeoutError:
            logger.warning("goodbye publish timeout for node: %s", self.node.node_id)

    async def close(self) -> None:
        logger.info("stopping node: %s gracefully", self.node.node_id)
        task, self._task = self._task, None  # no await between claim and clear
        if task is not None:
            task.cancel()
            done, _ = await asyncio.wait({task}, timeout=_STOP_TIMEOUT.total_seconds())
            if not done:
                logger.error(
                    "node: %s did not stop within %s, it may still publish",
                    self.node.node_id,
                    _STOP_TIMEOUT,
                )
        await self._send_goodbye_if_registered()
        logger.info("node: %s gracefully stopped", self.node.node_id)
