from __future__ import annotations

import uuid
from datetime import timedelta

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
    ).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            # Short ack timeout so a wrapper whose (never-acked) registration
            # cycles quickly notices its node going offline well within the
            # 2s wait_until budget (see test_client_closes_when_last_node...).
            "registration_ack_timeout": timedelta(milliseconds=20),
        }
    )


async def test_register_creates_a_node_and_starts_client_once_online() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())

    await dispatcher.register(node)
    assert client.start_calls == 0  # offline: client not started yet

    node.set_online(True)
    await wait_until(lambda: client.start_calls == 1)

    await dispatcher.close()


async def test_client_starts_only_once_for_two_online_nodes() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_a = FakeNode(node_id_=uuid.uuid4())
    node_b = FakeNode(node_id_=uuid.uuid4())
    node_a.set_online(True)
    node_b.set_online(True)

    await dispatcher.register(node_a)
    await dispatcher.register(node_b)
    await wait_until(lambda: client.start_calls >= 1)
    import asyncio

    await asyncio.sleep(0.1)
    assert client.start_calls == 1

    await dispatcher.close()


async def test_client_closes_when_last_node_goes_offline() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    node.set_online(True)

    await dispatcher.register(node)
    await wait_until(lambda: client.start_calls == 1)

    node.set_online(False)
    await wait_until(lambda: client.close_calls == 1)

    await dispatcher.close()


async def test_unregister_of_last_online_node_closes_the_client() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    node.set_online(True)

    await dispatcher.register(node)
    await wait_until(lambda: client.start_calls == 1)

    await dispatcher.unregister(node)
    assert client.close_calls == 1

    await dispatcher.close()


async def test_dispatcher_close_is_safe_with_multiple_nodes() -> None:
    # Regression test for the Java double-close hazard: N registered nodes
    # must never cause client.close() to be called more than once.
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    nodes = [FakeNode(node_id_=uuid.uuid4()) for _ in range(3)]
    for node in nodes:
        node.set_online(True)
        await dispatcher.register(node)

    await wait_until(lambda: client.start_calls == 1)
    await dispatcher.close()

    assert client.close_calls == 1
