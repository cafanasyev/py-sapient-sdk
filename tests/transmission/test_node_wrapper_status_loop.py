from __future__ import annotations

import uuid
from datetime import timedelta

from sapient_msg_pydantic.bsi_flex_335_v2_0.registration import Registration
from sapient_msg_pydantic.bsi_flex_335_v2_0.registration_ack import RegistrationAck
from sapient_msg_pydantic.bsi_flex_335_v2_0.status_report import StatusReport

from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transmission.node_wrapper import NodeWrapper, to_timedelta
from tests.fixtures import make_registration, random_uuid
from tests.transmission.conftest import wait_until
from tests.transmission.fake_dispatcher import FakeDispatcher
from tests.transmission.fake_node import FakeNode


class _FastStatusNode(FakeNode):
    """A node with a tiny status interval so timing-based tests stay fast and
    deterministic (phase_offset/jittered_interval scale with the interval)."""

    async def get_registration(self) -> Registration:
        return make_registration(status_interval_seconds=0.05)


def test_to_timedelta_converts_seconds() -> None:
    reg = make_registration(status_interval_seconds=2.5)
    assert reg.status_definition is not None
    duration = reg.status_definition.status_interval
    assert duration is not None
    assert to_timedelta(duration) == timedelta(seconds=2.5)


async def test_status_reports_are_sent_periodically() -> None:
    node = _FastStatusNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    config = NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    ).model_copy(
        update={
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=config)

    wrapper.start()
    await wait_until(lambda: len(dispatcher.published) == 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)

    await wait_until(
        lambda: any(isinstance(m, StatusReport) for m in dispatcher.published),
        timeout=3.0,
    )

    await wrapper.close()


async def test_epoch_mismatch_triggers_reregistration() -> None:
    node = _FastStatusNode(node_id_=uuid.uuid4())
    node.set_online(True)
    dispatcher = FakeDispatcher()
    config = NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(milliseconds=10),
    ).model_copy(
        update={
            "registration_jitter_window": timedelta(0),
            "registration_ack_timeout": timedelta(seconds=1),
        }
    )
    wrapper = NodeWrapper(node=node, dispatcher=dispatcher, config=config)

    wrapper.start()
    await wait_until(lambda: len(dispatcher.published) == 1)
    wrapper.offer_registration_ack(RegistrationAck(acceptance=True))
    await wait_until(lambda: wrapper.registered is True)

    dispatcher.bump_epoch()  # simulate a grace-period-exceeding reconnect

    # The epoch mismatch must trigger a re-registration: a *second* Registration
    # is published. Status reports may be interleaved (they publish on the same
    # channel), so count Registrations rather than assume a fixed index.
    def _registration_count() -> int:
        return sum(isinstance(m, Registration) for m in dispatcher.published)

    await wait_until(lambda: _registration_count() >= 2, timeout=3.0)

    await wrapper.close()
