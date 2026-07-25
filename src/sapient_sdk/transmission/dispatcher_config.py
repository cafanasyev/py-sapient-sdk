from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Self

from pydantic import BaseModel, field_validator


class NodeDispatcherConfig(BaseModel):
    online_check_interval: timedelta = timedelta(seconds=5)
    publish_timeout: timedelta = timedelta(seconds=5)
    registration_ack_timeout: timedelta = timedelta(seconds=5)
    reconnect_grace_period: timedelta = timedelta(minutes=2)
    connection_loss_detection_delay: timedelta
    destination_id: str
    registration_jitter_window: timedelta = timedelta(seconds=2)

    @field_validator("destination_id")
    @classmethod
    def _validate_destination_id(cls, value: str) -> str:
        try:
            uuid.UUID(value)
        except ValueError as exc:
            raise ValueError(f"destination_id: {value!r} is not a valid UUID") from exc
        return value

    @classmethod
    def defaults(
        cls, destination_id: str, connection_loss_detection_delay: timedelta
    ) -> Self:
        return cls(
            destination_id=destination_id,
            connection_loss_detection_delay=connection_loss_detection_delay,
        )
