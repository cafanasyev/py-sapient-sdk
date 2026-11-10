from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_client import FakeClient
from tests.transmission.fake_node import FakeNode

_SYSTEM_GOODBYE = status_report_pb2.StatusReport.System.SYSTEM_GOODBYE


class _FastNode(FakeNode):
    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.02)


class _RecordingClient(FakeClient):
    published_before_close: int = -1

    async def close(self) -> None:
        self.published_before_close = len(self.published)
        await super().close()


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
    ).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )


async def _registered(dispatcher: DefaultNodeDispatcher, node: FakeNode) -> None:
    await wait_until(lambda: node.node_id in dispatcher._online)
    wrapper = dispatcher._online[node.node_id]
    await asyncio.sleep(0.05)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)


async def test_client_closed_once_when_nodes_go_offline_then_dispatcher_closes() -> (
    None
):
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    nodes = [_FastNode(node_id_=uuid.uuid4()) for _ in range(2)]
    for node in nodes:
        node.set_online(True)
        await dispatcher.register(node)
    await wait_until(lambda: client.start_calls == 1)

    for node in nodes:
        node.set_online(False)
    await wait_until(lambda: client.close_calls == 1)

    await dispatcher.close()
    assert client.close_calls == 1


async def test_goodbyes_go_out_before_the_client_closes() -> None:
    client = _RecordingClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    nodes = [_FastNode(node_id_=uuid.uuid4()) for _ in range(2)]
    for node in nodes:
        node.set_online(True)
        await dispatcher.register(node)
        await _registered(dispatcher, node)

    await dispatcher.close()

    goodbyes = [
        i
        for i, m in enumerate(client.published)
        if m.status_report is not None and m.status_report.system == _SYSTEM_GOODBYE
    ]
    assert len(goodbyes) == 2
    assert all(i < client.published_before_close for i in goodbyes)


async def test_node_does_not_publish_after_dispatcher_closed() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)
    await dispatcher.register(node)
    await _registered(dispatcher, node)

    await dispatcher.close()
    count = len(client.published)
    await asyncio.sleep(0.1)
    assert len(client.published) == count
