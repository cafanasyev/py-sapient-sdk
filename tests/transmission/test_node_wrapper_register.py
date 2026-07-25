from __future__ import annotations

import asyncio
from datetime import timedelta

from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck

from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.node_wrapper import NodeWrapper
from tests.fixtures import random_node_id, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_dispatcher import FakeDispatcher
from tests.transmission.fake_node import FakeNode


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    ).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )


async def test_wrapper_waits_offline_then_registers_once_online() -> None:
    node = FakeNode(node_id_=random_node_id())
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    await asyncio.sleep(0.05)
    assert dispatcher.online_calls == []  # still offline, no registration attempted

    node.set_online(True)
    await wait_until(lambda: dispatcher.online_calls == [node.node_id])
    await wait_until(lambda: len(dispatcher.published) == 1)
    assert isinstance(dispatcher.published[0], Registration)

    await wrapper.close()


async def test_registration_ack_acceptance_unblocks_register() -> None:
    node = FakeNode(node_id_=random_node_id())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    await wait_until(lambda: len(dispatcher.published) == 1)

    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)

    await wrapper.close()


async def test_registration_rejection_retries() -> None:
    node = FakeNode(node_id_=random_node_id())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    await wait_until(lambda: len(dispatcher.published) == 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=False))

    await wait_until(lambda: len(dispatcher.published) == 2)
    assert wrapper.registered is False

    await wrapper.close()


async def test_registration_publish_timeout_is_retried_not_fatal() -> None:
    node = FakeNode(node_id_=random_node_id())
    dispatcher = FakeDispatcher()
    dispatcher.fail_next_publish = True  # the very first publish() call times out
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=_config())

    wrapper.start()
    node.set_online(True)

    # The first attempt's publish() raises TimeoutError (simulated connection
    # loss). The wrapper must log and retry -- not let the exception kill its
    # background lifecycle task -- so a second attempt succeeds.
    await wait_until(lambda: len(dispatcher.published) == 1)
    assert wrapper._task is not None
    assert not wrapper._task.done()

    await wrapper.close()
