from __future__ import annotations

import uuid
from datetime import timedelta

from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport

from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.node_wrapper import NodeWrapper
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_dispatcher import FakeDispatcher
from tests.transmission.fake_node import FakeNode


class FastNode(FakeNode):
    """A node that advertises a tiny status interval.

    The wrapper only re-checks ``is_online`` between status-loop sleeps, so a
    short interval makes the natural-offline transition deterministic instead
    of racing a multi-second sleep.
    """

    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.02)


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    ).model_copy(
        update={
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )


def _goodbyes(dispatcher: FakeDispatcher) -> list[StatusReport]:
    from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2

    return [
        m
        for m in dispatcher.published
        if isinstance(m, StatusReport)
        and m.system == status_report_pb2.StatusReport.System.SYSTEM_GOODBYE
    ]


async def test_going_offline_sends_exactly_one_goodbye() -> None:
    node = FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    await wait_until(lambda: len(dispatcher.published) == 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)

    node.set_online(False)
    await wait_until(lambda: len(_goodbyes(dispatcher)) == 1, timeout=2.0)

    await wrapper.close()
    assert len(_goodbyes(dispatcher)) == 1  # close() must not send a second one


async def test_close_while_registered_sends_goodbye() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    await wait_until(lambda: len(dispatcher.published) == 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)

    await wrapper.close()
    assert len(_goodbyes(dispatcher)) == 1


async def test_close_while_not_registered_sends_no_goodbye() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    await wrapper.close()
    assert _goodbyes(dispatcher) == []
