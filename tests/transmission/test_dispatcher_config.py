from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from tests.fixtures import random_uuid


def test_defaults_factory_fills_in_standard_intervals() -> None:
    config = NodeDispatcherConfig.defaults(
        destination_id=random_uuid(),
        connection_loss_detection_delay=timedelta(seconds=12),
    )
    assert config.online_check_interval == timedelta(seconds=5)
    assert config.publish_timeout == timedelta(seconds=5)
    assert config.registration_ack_timeout == timedelta(seconds=5)
    assert config.reconnect_grace_period == timedelta(minutes=2)
    assert config.registration_jitter_window == timedelta(seconds=2)
    assert config.connection_loss_detection_delay == timedelta(seconds=12)


def test_connection_loss_detection_delay_has_no_default() -> None:
    with pytest.raises(ValidationError):
        NodeDispatcherConfig(destination_id=random_uuid())  # type: ignore[call-arg]


def test_destination_id_must_be_a_valid_uuid() -> None:
    with pytest.raises(ValidationError):
        NodeDispatcherConfig(
            destination_id="not-a-uuid",
            connection_loss_detection_delay=timedelta(seconds=1),
        )
