from __future__ import annotations

import uuid
from datetime import timedelta

import ulid
from sapient_msg.bsi_flex_335_v2_0 import alert_ack_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.alert_ack import AlertAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.error import Error
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.task import Command, Task

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from tests.fixtures import random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_client import FakeClient
from tests.transmission.fake_node import FakeNode


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    ).model_copy(update={"registration_jitter_window": timedelta(0)})


async def test_registration_ack_is_always_delivered_to_the_node() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    ack = RegistrationAck(acceptance=True)
    node_id_str = str(node.node_id)
    await dispatcher._on_message(
        SapientMessage(
            node_id=node_id_str, destination_id=node_id_str, registration_ack=ack
        )
    )
    await wait_until(lambda: node.registration_acks == [ack])

    await dispatcher.close()


async def test_alert_ack_is_delivered() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    ack = AlertAck(
        alert_id=str(ulid.ULID()),
        alert_ack_status=alert_ack_pb2.AlertAck.AlertAckStatus.ALERT_ACK_STATUS_ACCEPTED,
    )
    node_id_str = str(node.node_id)
    await dispatcher._on_message(
        SapientMessage(node_id=node_id_str, destination_id=node_id_str, alert_ack=ack)
    )
    await wait_until(lambda: node.alert_acks == [ack])

    await dispatcher.close()


async def test_error_is_delivered() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    error = Error(error_message=["bad field"])
    node_id_str = str(node.node_id)
    await dispatcher._on_message(
        SapientMessage(node_id=node_id_str, destination_id=node_id_str, error=error)
    )
    await wait_until(lambda: node.errors == [error])

    await dispatcher.close()


async def test_task_for_unknown_node_is_rejected_with_task_ack() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    unknown_id = random_uuid()

    task = Task(command=Command(request="detection_threshold"))
    await dispatcher._on_message(
        SapientMessage(node_id=unknown_id, destination_id=unknown_id, task=task)
    )
    await wait_until(lambda: len(client.published) == 1)
    rejection = client.published[0].task_ack
    assert rejection is not None
    assert rejection.task_status == 2  # TASK_STATUS_REJECTED

    await dispatcher.close()


async def test_task_delivered_to_online_node() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    node.set_online(True)
    await dispatcher.register(node)
    await wait_until(lambda: node.node_id in dispatcher._online)

    task = Task(command=Command(mode_change="patrol"))
    node_id_str = str(node.node_id)
    await dispatcher._on_message(
        SapientMessage(node_id=node_id_str, destination_id=node_id_str, task=task)
    )
    await wait_until(lambda: node.tasks == [task])

    await dispatcher.close()


class _RaisingNode(FakeNode):
    async def on_error(self, error: Error) -> None:
        raise RuntimeError("boom in node callback")


async def test_on_message_survives_one_callback_raising() -> None:
    # A single node's callback raising must not tear down message routing for
    # every other node sharing the connection: _on_message isolates it, logs,
    # and keeps going, so a well-behaved node's message still gets routed.
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    bad = _RaisingNode(node_id_=uuid.uuid4())
    good = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(bad)
    await dispatcher.register(good)

    error = Error(error_message=["bad field"])
    bad_id_str = str(bad.node_id)
    await dispatcher._on_message(
        SapientMessage(node_id=bad_id_str, destination_id=bad_id_str, error=error)
    )

    good_error = Error(error_message=["other node"])
    good_id_str = str(good.node_id)
    await dispatcher._on_message(
        SapientMessage(
            node_id=good_id_str, destination_id=good_id_str, error=good_error
        )
    )
    await wait_until(lambda: good.errors == [good_error])

    await dispatcher.close()


async def test_rejected_ack_during_status_phase_forces_reregistration() -> None:
    # CHANGELOG §6 parity: a Task-driven re-registration that gets rejected
    # must clear `registered` so the wrapper falls back to the register phase,
    # not be silently dropped because "nothing is polling the ack mailbox".
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    node.set_online(True)
    await dispatcher.register(node)
    await wait_until(lambda: node.node_id in dispatcher._online)

    wrapper = dispatcher._online[node.node_id]
    wrapper.registered = True  # simulate an already-established registration

    ack = RegistrationAck(acceptance=False)
    node_id_str = str(node.node_id)
    await dispatcher._on_message(
        SapientMessage(
            node_id=node_id_str, destination_id=node_id_str, registration_ack=ack
        )
    )

    assert wrapper.registered is False
    assert node.registration_acks == [ack]  # still delivered to the node either way

    await dispatcher.close()


async def test_message_with_no_destination_id_is_dropped_without_raising() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())

    # A message with no destination_id (e.g. broadcast/unaddressed) must be
    # dropped gracefully, not raise an AssertionError that _on_message then
    # has to log as an unexpected error.
    await dispatcher._on_message(
        SapientMessage(node_id=random_uuid(), error=Error(error_message=["stray"]))
    )

    await dispatcher.close()
