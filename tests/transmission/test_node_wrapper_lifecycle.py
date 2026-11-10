from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import timedelta

import pytest
from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport

import sapient_sdk.transmission.node_wrapper as node_wrapper_module
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.node_wrapper import NodeWrapper
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_dispatcher import FakeDispatcher
from tests.transmission.fake_node import FakeNode

_SYSTEM_GOODBYE = status_report_pb2.StatusReport.System.SYSTEM_GOODBYE


class _FastNode(FakeNode):
    fail_status_reports: int = 0

    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.02)

    async def get_status_report(self) -> StatusReport:
        if self.fail_status_reports > 0:
            self.fail_status_reports -= 1
            raise RuntimeError("sensor read failed")
        return await super().get_status_report()


def _config(ack_timeout: timedelta = timedelta(seconds=1)) -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
    ).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": ack_timeout,
        }
    )


def _registrations(dispatcher: FakeDispatcher) -> int:
    return sum(isinstance(m, Registration) for m in dispatcher.published)


def _goodbyes(dispatcher: FakeDispatcher) -> int:
    return sum(
        isinstance(m, StatusReport) and m.system == _SYSTEM_GOODBYE
        for m in dispatcher.published
    )


async def _register_accepted(wrapper: NodeWrapper, dispatcher: FakeDispatcher) -> None:
    count = _registrations(dispatcher)
    await wait_until(lambda: _registrations(dispatcher) == count + 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)


async def test_node_registers_again_after_coming_back_online() -> None:
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())
    wrapper.start()
    await _register_accepted(wrapper, dispatcher)

    node.set_online(False)
    await wait_until(lambda: _goodbyes(dispatcher) == 1)
    await wait_until(lambda: dispatcher.offline_calls == [node.node_id])

    node.set_online(True)
    await _register_accepted(wrapper, dispatcher)
    assert dispatcher.online_calls == [node.node_id, node.node_id]

    await wrapper.close()


async def test_status_report_error_is_logged_and_node_registers_again(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR, logger="sapient_sdk.transmission.node_wrapper")
    node = _FastNode(node_id_=uuid.uuid4(), fail_status_reports=1)
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())
    wrapper.start()
    await wait_until(lambda: _registrations(dispatcher) == 1)
    # the failing report ends the registration within one interval, so the
    # registered flag is too short-lived to wait on
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))

    await wait_until(lambda: _registrations(dispatcher) == 2)
    assert any("node lifecycle error" in r.message for r in caplog.records)
    assert wrapper._task is not None and not wrapper._task.done()

    await wrapper.close()


async def test_rejected_registration_waits_ack_timeout_before_retry() -> None:
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    config = _config(ack_timeout=timedelta(milliseconds=300))
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=config)
    wrapper.start()
    await wait_until(lambda: _registrations(dispatcher) == 1)

    wrapper.offer_registration_ack(RegistrationAck(acceptance=False))
    await asyncio.sleep(0.15)
    assert _registrations(dispatcher) == 1

    await wait_until(lambda: _registrations(dispatcher) == 2)
    assert wrapper.registered is False

    await wrapper.close()


async def test_no_goodbye_when_node_goes_offline_after_rejected_registration() -> None:
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    config = _config(ack_timeout=timedelta(milliseconds=100))
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=config)
    wrapper.start()
    await wait_until(lambda: _registrations(dispatcher) == 1)

    wrapper.offer_registration_ack(RegistrationAck(acceptance=False))
    node.set_online(False)

    await wait_until(lambda: dispatcher.offline_calls == [node.node_id])
    assert _goodbyes(dispatcher) == 0

    await wrapper.close()


class _StubbornNode(_FastNode):
    """Swallows cancellation while building its registration."""

    swallow_cancel: bool = True

    async def get_registration(self) -> Registration:
        while True:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                if not self.swallow_cancel:
                    raise


async def test_close_gives_up_on_a_task_that_ignores_cancellation(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        node_wrapper_module, "_STOP_TIMEOUT", timedelta(milliseconds=100)
    )
    caplog.set_level(logging.ERROR, logger="sapient_sdk.transmission.node_wrapper")
    node = _StubbornNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())
    wrapper.start()
    task = wrapper._task
    assert task is not None
    await asyncio.sleep(0.05)

    await asyncio.wait_for(wrapper.close(), timeout=1.0)
    assert any("did not stop within" in r.message for r in caplog.records)

    node.swallow_cancel = False
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
