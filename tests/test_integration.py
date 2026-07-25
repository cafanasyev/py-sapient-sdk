from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2
from sapient_msg.bsi_flex_335_v2_0.sapient_message_pb2 import (
    SapientMessage as SapientMessagePb2,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transport.framing import read_framed, write_framed
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import PlainSocketProvider
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_node import FakeNode


class _FastFakeNode(FakeNode):
    """A FakeNode that reports on a short interval so the status loop reacts
    promptly to going offline (the default 5s interval would delay the goodbye
    by up to a full interval, longer than the test's wait windows)."""

    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.1)


class _FakeFusionNode:
    def __init__(self) -> None:
        self.received: list[SapientMessage] = []
        self._writer: asyncio.StreamWriter | None = None
        self.node_id = random_uuid()

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writer = writer
        try:
            while True:
                payload = await read_framed(reader)
                pb2_msg = SapientMessagePb2()
                pb2_msg.ParseFromString(payload)
                msg = SapientMessage.from_pb2(pb2_msg)
                self.received.append(msg)
                if msg.registration is not None:
                    ack = SapientMessage(
                        node_id=self.node_id,
                        destination_id=msg.node_id,
                        timestamp=datetime.now(UTC),
                        registration_ack=RegistrationAck(acceptance=True),
                    )
                    await write_framed(writer, ack.to_pb2().SerializeToString())
        except (asyncio.IncompleteReadError, ConnectionError):
            pass

    def goodbyes(self) -> list[SapientMessage]:
        return [
            m
            for m in self.received
            if m.status_report is not None
            and m.status_report.system
            == status_report_pb2.StatusReport.System.SYSTEM_GOODBYE
        ]


async def test_full_lifecycle_against_a_fake_fusion_node() -> None:
    fake_server = _FakeFusionNode()
    server = await asyncio.start_server(fake_server.handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))
    config = NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=200),
    ).model_copy(
        update={
            "registration_jitter_window": timedelta(0),
            "online_check_interval": timedelta(milliseconds=20),
        }
    )
    dispatcher = DefaultNodeDispatcher(client=client, config=config)

    node = _FastFakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    node.set_online(True)
    await wait_until(
        lambda: any(m.registration is not None for m in fake_server.received),
        timeout=3.0,
    )
    await wait_until(
        lambda: any(m.status_report is not None for m in fake_server.received),
        timeout=5.0,
    )

    node.set_online(False)
    await wait_until(lambda: len(fake_server.goodbyes()) == 1, timeout=3.0)

    await dispatcher.close()
    # dispatcher.close() must not send a second goodbye
    assert len(fake_server.goodbyes()) == 1

    server.close()
    await server.wait_closed()
