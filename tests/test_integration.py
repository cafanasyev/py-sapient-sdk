from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from datetime import UTC, datetime, timedelta

import pytest
from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2
from sapient_msg.bsi_flex_335_v2_0.sapient_message_pb2 import (
    SapientMessage as SapientMessagePb2,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import encode_prefix, read_length, write_framed
from sapient_sdk.transport.health.config import HealthCheckConfig, HealthCheckType
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
    def __init__(self, keepalive: HealthCheckType | None = None) -> None:
        self.received: list[SapientMessage] = []
        self._writer: asyncio.StreamWriter | None = None
        self.node_id = random_uuid()
        self.keepalive = keepalive
        self.answer_pings = True
        self.pings = 0

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._writer = writer
        try:
            while True:
                length = await read_length(reader)
                if length == 0 and self.keepalive is not None:
                    self.pings += 1
                    if self.answer_pings:
                        pong = (
                            0xFFFFFFFF
                            if self.keepalive == HealthCheckType.PINGPONG
                            else 0
                        )
                        writer.write(encode_prefix(pong))
                        await writer.drain()
                    continue
                payload = await reader.readexactly(length)
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
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()

    def registrations(self) -> int:
        return sum(m.registration is not None for m in self.received)

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


def _fast_health(check_type: HealthCheckType) -> HealthCheckConfig:
    return HealthCheckConfig(
        type=check_type,
        interval=timedelta(milliseconds=100),
        timeout=timedelta(milliseconds=50),
        failure_threshold=3,
    )


def _config(grace: timedelta = timedelta(minutes=2)) -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(destination_id=random_uuid()).model_copy(
        update={
            "registration_jitter_window": timedelta(0),
            "online_check_interval": timedelta(milliseconds=20),
            "reconnect_grace_period": grace,
        }
    )


@pytest.mark.parametrize("check_type", [HealthCheckType.ECHO, HealthCheckType.PINGPONG])
async def test_full_lifecycle_with_in_band_keepalive(
    check_type: HealthCheckType,
) -> None:
    fake = _FakeFusionNode(keepalive=check_type)
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        health_check=_fast_health(check_type),
    )
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = _FastFakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)
    node.set_online(True)

    await wait_until(lambda: fake.registrations() == 1, timeout=3.0)
    await wait_until(lambda: fake.pings >= 2, timeout=3.0)
    assert client.state == ConnectionState.CONNECTED

    node.set_online(False)
    await wait_until(lambda: len(fake.goodbyes()) == 1, timeout=3.0)
    await dispatcher.close()
    server.close()
    await server.wait_closed()


async def test_silent_server_is_dropped_and_the_node_registers_again() -> None:
    fake = _FakeFusionNode(keepalive=HealthCheckType.PINGPONG)
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        health_check=_fast_health(HealthCheckType.PINGPONG),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    seen: list[ConnectionState] = []
    client.add_state_change_listener(lambda state, ts: seen.append(state))
    # detection delay 0.35s > 0.95 x 0.3s grace, so any reconnect re-registers;
    # the retention window 3 x 0.1 + 0.3 - 0.35 = 0.25s still holds between reports
    dispatcher = DefaultNodeDispatcher(
        client=client, config=_config(grace=timedelta(milliseconds=300))
    )
    node = _FastFakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)
    node.set_online(True)
    await wait_until(lambda: fake.registrations() == 1, timeout=3.0)

    fake.answer_pings = False
    loop = asyncio.get_running_loop()
    silent_at = loop.time()
    await wait_until(lambda: ConnectionState.DISCONNECTED in seen, timeout=3.0)
    # 3 x 100ms + 50ms, plus scheduling slack
    assert loop.time() - silent_at < 1.0

    fake.answer_pings = True
    await wait_until(lambda: fake.registrations() == 2, timeout=3.0)

    await dispatcher.close()
    server.close()
    await server.wait_closed()


async def test_node_offline_and_back_end_to_end() -> None:
    fake = _FakeFusionNode()
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    client = SocketClient(
        socket_provider=PlainSocketProvider(host=host, port=port),
        initial_reconnect_delay=timedelta(milliseconds=10),
    )
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = _FastFakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    node.set_online(True)
    await wait_until(lambda: fake.registrations() == 1, timeout=3.0)
    node.set_online(False)
    await wait_until(lambda: len(fake.goodbyes()) == 1, timeout=3.0)
    node.set_online(True)
    await wait_until(lambda: fake.registrations() == 2, timeout=3.0)

    await dispatcher.close()
    server.close()
    await server.wait_closed()
