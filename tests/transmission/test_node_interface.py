from __future__ import annotations

import uuid

import pytest

from sapient_sdk.transmission.node import Node
from tests.fixtures import make_registration, make_status_report
from tests.transmission.fake_node import FakeNode


def test_abstract_node_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="abstract"):
        Node()  # type: ignore[abstract]


def test_node_id_is_a_uuid() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    assert isinstance(node.node_id, uuid.UUID)


async def test_fake_node_starts_offline_by_default() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    assert await node.is_online() is False


async def test_fake_node_can_be_toggled_online() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    node.set_online(True)
    assert await node.is_online() is True


async def test_fake_node_returns_configured_registration_and_status() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    assert await node.get_registration() == make_registration()
    status_report = await node.get_status_report()
    assert status_report.system == make_status_report().system


async def test_fake_node_records_callbacks() -> None:
    node = FakeNode(node_id_=uuid.uuid4())
    from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck

    ack = RegistrationAck(acceptance=True)
    await node.on_registration_ack(ack)
    assert node.registration_acks == [ack]
