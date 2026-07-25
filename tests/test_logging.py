from __future__ import annotations

import logging
import uuid
from datetime import timedelta

import pytest
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transport.connection_state import ConnectionState
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_client import FakeClient
from tests.transmission.fake_node import FakeNode


class _FastNode(FakeNode):
    """A node with a short status interval so the wrapper notices going
    offline promptly (the default 5s interval would delay this well beyond
    any reasonable test timeout — see Tasks 17/18 for the same fix)."""

    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.02)


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    ).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(milliseconds=50),
        }
    )


async def test_register_and_unregister_log_at_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="sapient_sdk.transmission.dispatcher")
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    node_id_str = str(node.node_id)

    await dispatcher.register(node)
    await dispatcher.unregister(node)

    assert any(
        "registering node" in r.message and node_id_str in r.message
        for r in caplog.records
    )
    assert any(
        "unregistering node" in r.message and node_id_str in r.message
        for r in caplog.records
    )


async def test_publish_logs_sending_at_info_and_body_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="sapient_sdk.transmission.dispatcher")
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    await dispatcher.publish(make_registration(), node_id, timedelta(seconds=1))

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]

    assert any(
        "sending registration to" in r.message and str(node_id) in r.message
        for r in info_records
    )
    # The DEBUG body log renders the full message as JSON — confirm it
    # actually contains real content, not just a placeholder line.
    assert any("registration" in r.message.lower() for r in debug_records)


async def test_publish_does_not_log_body_below_debug_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="sapient_sdk.transmission.dispatcher")
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    await dispatcher.publish(make_registration(), node_id, timedelta(seconds=1))

    # At INFO (not DEBUG), the isEnabledFor(DEBUG) guard must skip the body
    # entirely — the serialization cost should never even run.
    assert all(r.levelno != logging.DEBUG for r in caplog.records)


async def test_received_message_logs_at_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="sapient_sdk.transmission.dispatcher")
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    from sapient_msg_pydantic.bsi_flex_335_v2_0.error import Error
    from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

    node_id_str = str(node.node_id)
    await dispatcher._on_message(
        SapientMessage(
            node_id=node_id_str,
            destination_id=node_id_str,
            error=Error(error_message=["boom"]),
        )
    )

    assert any(
        "received error for node" in r.message and node_id_str in r.message
        for r in caplog.records
    )

    await dispatcher.close()


async def test_goodbye_logs_at_info(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="sapient_sdk.transmission.node_wrapper")
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = _FastNode(node_id_=uuid.uuid4())
    node.set_online(True)

    from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck

    await dispatcher.register(node)
    await wait_until(lambda: node.node_id in dispatcher._online)
    wrapper = dispatcher._online[node.node_id]
    await wait_until(lambda: len(client.published) >= 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)

    node.set_online(False)
    await wait_until(
        lambda: any("sending GOOD BYE" in r.message for r in caplog.records),
        timeout=2.0,
    )

    await dispatcher.close()


async def test_connection_state_transitions_log_at_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from sapient_sdk.transport.socket_client import SocketClient
    from sapient_sdk.transport.socket_provider import PlainSocketProvider
    from tests.transport.conftest import start_loopback_server

    caplog.set_level(logging.INFO, logger="sapient_sdk.transport.socket_client")
    server, host, port = await start_loopback_server()
    client = SocketClient(socket_provider=PlainSocketProvider(host=host, port=port))

    await client.start()
    await wait_until(lambda: client.state == ConnectionState.CONNECTED)
    await client.close()

    messages = [r.message for r in caplog.records]
    assert any("connecting" in m for m in messages)
    assert any("connected" in m for m in messages)
    assert any("closed" in m for m in messages)

    server.close()
    await server.wait_closed()
