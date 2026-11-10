from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

import pytest
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck

import sapient_sdk.transmission.node_wrapper as node_wrapper_module
from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.node_wrapper import NodeWrapper, server_retention
from sapient_sdk.transport.connection_state import ConnectionState
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_client import FakeClient
from tests.transmission.fake_dispatcher import FakeDispatcher
from tests.transmission.fake_node import FakeNode

GRACE = timedelta(minutes=2)


def test_normal_budget_is_unchanged() -> None:
    retention = server_retention(timedelta(seconds=10), GRACE, timedelta(seconds=32))
    assert retention == timedelta(seconds=118)


def test_oversized_detection_delay_is_clamped_at_zero() -> None:
    retention = server_retention(timedelta(seconds=5), GRACE, timedelta(seconds=182))
    assert retention == timedelta(0)


def test_exactly_zero_budget_is_allowed() -> None:
    retention = server_retention(timedelta(seconds=10), GRACE, timedelta(seconds=150))
    assert retention == timedelta(0)


def test_result_is_never_negative() -> None:
    for delay_seconds in range(0, 1000, 7):
        retention = server_retention(
            timedelta(seconds=1), GRACE, timedelta(seconds=delay_seconds)
        )
        assert retention >= timedelta(0)


class _FastNode(FakeNode):
    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.02)


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(destination_id=random_uuid()).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )


def _registrations(dispatcher: FakeDispatcher) -> int:
    return sum(isinstance(m, Registration) for m in dispatcher.published)


async def _accepted(wrapper: NodeWrapper, dispatcher: FakeDispatcher) -> None:
    count = _registrations(dispatcher)
    await wait_until(lambda: _registrations(dispatcher) == count + 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)


async def test_zero_budget_warns_and_reregisters(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="sapient_sdk.transmission.node_wrapper")
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher(detection_delay=timedelta(hours=1))
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())
    wrapper.start()
    await wait_until(lambda: _registrations(dispatcher) == 1)
    # a zero window ends the registration within one interval
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))

    await wait_until(lambda: _registrations(dispatcher) == 2)
    assert any("eats the whole retention budget" in r.message for r in caplog.records)
    await wrapper.close()


async def test_wall_clock_jump_past_retention_triggers_reregistration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())
    wrapper.start()
    await _accepted(wrapper, dispatcher)

    real_now = node_wrapper_module._now

    def _after_an_hour_of_sleep() -> datetime:
        return real_now() + timedelta(hours=1)

    monkeypatch.setattr(node_wrapper_module, "_now", _after_an_hour_of_sleep)
    await wait_until(lambda: _registrations(dispatcher) == 2)
    await wrapper.close()


async def test_dispatcher_adds_the_client_detection_delay_to_the_gap() -> None:
    client = FakeClient(detection_delay=timedelta(minutes=2))
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())

    client.set_state(ConnectionState.DISCONNECTED)
    client.set_state(ConnectionState.CONNECTED)

    assert dispatcher.reregistration_epoch == 1
    await dispatcher.close()


async def test_short_gap_with_small_detection_delay_keeps_the_epoch() -> None:
    client = FakeClient(detection_delay=timedelta(seconds=1))
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())

    client.set_state(ConnectionState.DISCONNECTED)
    client.set_state(ConnectionState.CONNECTED)

    assert dispatcher.reregistration_epoch == 0
    await dispatcher.close()
