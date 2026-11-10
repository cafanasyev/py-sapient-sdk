from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from pydantic import PrivateAttr
from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_client import FakeClient
from tests.transmission.fake_node import FakeNode

_R = status_report_pb2.StatusReport
_OK = _R.System.SYSTEM_OK
_GOODBYE = _R.System.SYSTEM_GOODBYE
_NEW = _R.Info.INFO_NEW
_UNCHANGED = _R.Info.INFO_UNCHANGED
_UNSPECIFIED = _R.Info.INFO_UNSPECIFIED
_TIMEOUT = timedelta(seconds=1)


def _config() -> NodeDispatcherConfig:
    return NodeDispatcherConfig.defaults(destination_id=random_uuid()).model_copy(
        update={
            "online_check_interval": timedelta(milliseconds=20),
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )


async def _registered_dispatcher() -> tuple[
    DefaultNodeDispatcher, FakeNode, FakeClient
]:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)
    return dispatcher, node, client


def _info(sent: SapientMessage) -> int | None:
    assert sent.status_report is not None
    return sent.status_report.info


async def test_goodbye_is_always_info_new() -> None:
    dispatcher, node, _ = await _registered_dispatcher()
    await dispatcher.publish(
        StatusReport(system=_OK, mode="idle"), node.node_id, _TIMEOUT
    )
    goodbye = StatusReport(system=_GOODBYE, mode="idle", info=_UNCHANGED)
    assert _info(await dispatcher.publish(goodbye, node.node_id, _TIMEOUT)) == _NEW
    await dispatcher.close()


async def test_info_unspecified_counts_as_unset() -> None:
    dispatcher, node, _ = await _registered_dispatcher()
    first = StatusReport(system=_OK, mode="idle", info=_UNSPECIFIED)
    second = StatusReport(system=_OK, mode="idle", info=_UNSPECIFIED)
    assert _info(await dispatcher.publish(first, node.node_id, _TIMEOUT)) == _NEW
    assert _info(await dispatcher.publish(second, node.node_id, _TIMEOUT)) == _UNCHANGED
    await dispatcher.close()


async def test_explicit_unchanged_is_sent_as_is() -> None:
    dispatcher, node, _ = await _registered_dispatcher()
    report = StatusReport(system=_OK, mode="idle", info=_UNCHANGED)
    assert _info(await dispatcher.publish(report, node.node_id, _TIMEOUT)) == _UNCHANGED
    await dispatcher.close()


async def test_unknown_node_is_not_stored() -> None:
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node_id = uuid.uuid4()
    for _ in range(2):
        report = StatusReport(system=_OK, mode="idle")
        assert _info(await dispatcher.publish(report, node_id, _TIMEOUT)) == _NEW
    await dispatcher.close()


async def test_caller_report_is_not_changed() -> None:
    dispatcher, node, _ = await _registered_dispatcher()
    report = StatusReport(system=_OK, mode="idle", report_id=None, info=None)
    sent = await dispatcher.publish(report, node.node_id, _TIMEOUT)
    assert report.report_id is None and report.info is None
    assert sent.status_report is not None and sent.status_report.report_id is not None
    await dispatcher.close()


async def test_reused_report_object_reports_its_change_as_new() -> None:
    dispatcher, node, _ = await _registered_dispatcher()
    cached = StatusReport(system=_OK, mode="idle")
    await dispatcher.publish(cached, node.node_id, _TIMEOUT)
    cached.mode = "search"
    assert _info(await dispatcher.publish(cached, node.node_id, _TIMEOUT)) == _NEW
    assert _info(await dispatcher.publish(cached, node.node_id, _TIMEOUT)) == _UNCHANGED
    await dispatcher.close()


class _GatedClient(FakeClient):
    """Holds the first publish until the test opens the gate."""

    held: bool = False
    _gate: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)

    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None:
        if not self.held:
            self.held = True
            await self._gate.wait()
        await super().publish(msg, timeout)


async def test_late_send_does_not_overwrite_a_newer_report() -> None:
    client = _GatedClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = FakeNode(node_id_=uuid.uuid4())
    await dispatcher.register(node)
    wrapper = dispatcher._offline[node.node_id]

    late = asyncio.ensure_future(
        dispatcher.publish(StatusReport(system=_OK, mode="old"), node.node_id, _TIMEOUT)
    )
    await asyncio.sleep(0.01)
    await dispatcher.publish(
        StatusReport(system=_OK, mode="new"), node.node_id, _TIMEOUT
    )
    client._gate.set()
    await late

    assert wrapper.last_status_report is not None
    assert wrapper.last_status_report.mode == "new"
    await dispatcher.close()


class _CachingNode(FakeNode):
    cached: StatusReport = StatusReport(system=_OK, mode="idle")

    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.02)

    async def get_status_report(self) -> StatusReport:
        return self.cached


async def test_goodbye_does_not_change_the_node_report_and_clears_the_baseline() -> (
    None
):
    client = FakeClient()
    dispatcher = DefaultNodeDispatcher(client=client, config=_config())
    node = _CachingNode(node_id_=uuid.uuid4())
    node.set_online(True)
    await dispatcher.register(node)
    await wait_until(lambda: node.node_id in dispatcher._online)
    wrapper = dispatcher._online[node.node_id]
    await wait_until(lambda: len(client.published) >= 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.last_status_report is not None)

    node.set_online(False)
    await wait_until(
        lambda: any(
            m.status_report is not None and m.status_report.system == _GOODBYE
            for m in client.published
        )
    )
    await wait_until(lambda: wrapper.last_status_report is None)
    assert node.cached.system == _OK
    await dispatcher.close()
