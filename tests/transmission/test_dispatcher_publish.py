from __future__ import annotations

import uuid
from datetime import timedelta

from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.detection_report import DetectionReport
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport
from ulid import ULID

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from tests.fixtures import make_registration, random_uuid
from tests.transmission.fake_client import FakeClient
from tests.transmission.fake_node import FakeNode

_SYSTEM_OK = status_report_pb2.StatusReport.System.SYSTEM_OK
_SYSTEM_GOODBYE = status_report_pb2.StatusReport.System.SYSTEM_GOODBYE
_INFO_NEW = status_report_pb2.StatusReport.Info.INFO_NEW
_INFO_UNCHANGED = status_report_pb2.StatusReport.Info.INFO_UNCHANGED


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    )


async def test_publish_registration_wraps_in_sapient_message() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    sent = await dispatcher.publish(make_registration(), node_id, timedelta(seconds=1))

    assert sent.node_id == str(node_id)
    assert sent.registration is not None
    assert client.published == [sent]


async def test_publish_accepts_uuid_node_id_and_stringifies_it_on_the_wire() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    sent = await dispatcher.publish(make_registration(), node_id, timedelta(seconds=1))

    assert sent.node_id == str(node_id)


async def test_publish_sets_destination_id_to_the_configured_fusion_node() -> None:
    config = _config()
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=config)
    node_id = uuid.uuid4()

    sent = await dispatcher.publish(make_registration(), node_id, timedelta(seconds=1))

    assert sent.destination_id == config.destination_id


async def test_goodbye_does_not_pollute_the_dedup_baseline() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    goodbye = StatusReport(system=_SYSTEM_GOODBYE, mode="idle")
    await dispatcher.publish(goodbye, node_id, timedelta(seconds=1))

    # A real status report with the same content published right after must
    # still go out as INFO_NEW -- the GOOD BYE must not have become the
    # stored baseline a future report gets compared against.
    report = StatusReport(system=_SYSTEM_OK, info=_INFO_NEW, mode="idle")
    sent = await dispatcher.publish(report, node_id, timedelta(seconds=1))
    assert sent.status_report is not None
    assert sent.status_report.info == _INFO_NEW


async def test_unregister_evicts_the_dedup_baseline() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)

    report = StatusReport(system=_SYSTEM_OK, info=_INFO_NEW, mode="idle")
    await dispatcher.publish(report, node.node_id, timedelta(seconds=1))
    assert node.node_id in dispatcher._last_status_report

    await dispatcher.unregister(node)
    assert node.node_id not in dispatcher._last_status_report

    await dispatcher.close()


async def test_second_identical_status_report_is_downgraded_to_unchanged() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    first = StatusReport(system=_SYSTEM_OK, info=_INFO_NEW, mode="idle")
    sent_first = await dispatcher.publish(first, node_id, timedelta(seconds=1))
    assert sent_first.status_report is not None
    assert sent_first.status_report.info == _INFO_NEW

    second = StatusReport(system=_SYSTEM_OK, info=_INFO_NEW, mode="idle")
    sent_second = await dispatcher.publish(second, node_id, timedelta(seconds=1))
    assert sent_second.status_report is not None
    assert sent_second.status_report.info == _INFO_UNCHANGED


async def test_failed_publish_does_not_commit_the_dedup_baseline() -> None:
    client = FakeClient()
    client.fail_next_publish = True
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    report = StatusReport(system=_SYSTEM_OK, info=_INFO_NEW, mode="idle")
    try:
        await dispatcher.publish(report, node_id, timedelta(seconds=1))
    except TimeoutError:
        pass

    # A second identical report must still go out as INFO_NEW: the first
    # attempt never actually reached the server (fail_next_publish consumed
    # itself and raised), so nothing was confirmed.
    report_again = StatusReport(system=_SYSTEM_OK, info=_INFO_NEW, mode="idle")
    sent = await dispatcher.publish(report_again, node_id, timedelta(seconds=1))
    assert sent.status_report is not None
    assert sent.status_report.info == _INFO_NEW


async def test_publish_auto_populates_status_report_report_id_when_none() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    report = StatusReport(system=_SYSTEM_OK, mode="idle", report_id=None)
    sent = await dispatcher.publish(report, node_id, timedelta(seconds=1))

    assert sent.status_report is not None
    assert sent.status_report.report_id is not None
    ULID.from_str(sent.status_report.report_id)


async def test_publish_preserves_existing_status_report_report_id() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()
    existing_id = str(ULID())

    report = StatusReport(system=_SYSTEM_OK, mode="idle", report_id=existing_id)
    sent = await dispatcher.publish(report, node_id, timedelta(seconds=1))

    assert sent.status_report is not None
    assert sent.status_report.report_id == existing_id


async def test_publish_auto_populates_detection_report_report_id_when_none() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()

    detection = DetectionReport(report_id=None)
    sent = await dispatcher.publish(detection, node_id, timedelta(seconds=1))

    assert sent.detection_report is not None
    assert sent.detection_report.report_id is not None
    ULID.from_str(sent.detection_report.report_id)


async def test_publish_preserves_existing_detection_report_report_id() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()
    existing_id = str(ULID())

    detection = DetectionReport(report_id=existing_id)
    sent = await dispatcher.publish(detection, node_id, timedelta(seconds=1))

    assert sent.detection_report is not None
    assert sent.detection_report.report_id == existing_id
