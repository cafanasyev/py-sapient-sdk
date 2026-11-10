from __future__ import annotations

import enum
from datetime import timedelta
from typing import NamedTuple, Self, assert_never

from pydantic import BaseModel, ConfigDict, model_validator


class HealthCheckType(enum.StrEnum):
    """How a client checks that the remote endpoint is still alive."""

    NETCAT = "netcat"
    """Opens and closes a throwaway TCP connection, like ``nc -z host port``."""

    ICMP = "icmp"
    """Runs one ICMP echo through the system ``ping`` binary."""

    ECHO = "echo"
    """Sends a zero-length frame, the server echoes it. Not compatible with PINGPONG."""

    PINGPONG = "pingpong"
    """Sends a zero-length frame, the server answers 0xFFFFFFFF.

    Not compatible with ECHO.
    """


class HealthCheckConfig(BaseModel):
    """Health check settings of one client.

    ``interval`` is how often a check starts. ``timeout`` is how long one check may
    take, never above ``interval``. ``failure_threshold`` is how many checks in a row
    must fail before the connection is dropped; any success resets the count.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: HealthCheckType = HealthCheckType.NETCAT
    interval: timedelta = timedelta(seconds=10)
    timeout: timedelta = timedelta(seconds=2)
    failure_threshold: int = 3

    @model_validator(mode="after")
    def _check_bounds(self) -> Self:
        if self.interval <= timedelta(0):
            raise ValueError(f"interval must be positive, got {self.interval}")
        if self.timeout <= timedelta(0):
            raise ValueError(f"timeout must be positive, got {self.timeout}")
        if self.timeout > self.interval:
            raise ValueError(
                f"timeout {self.timeout} must not exceed interval {self.interval}"
            )
        if self.failure_threshold < 1:
            raise ValueError(
                f"failure_threshold must be at least 1, got {self.failure_threshold}"
            )
        return self

    @property
    def connection_loss_detection_delay(self) -> timedelta:
        """Worst case time between the network dying and the client noticing.

        The link dies right after a check passed, so ``failure_threshold`` checks
        still have to run and the last one has to time out.
        """
        return self.interval * self.failure_threshold + self.timeout


class KeepalivePrefixes(NamedTuple):
    """Length-prefix values that carry a keepalive instead of a message."""

    ping: int
    pong: int


def keepalive_prefixes(check_type: HealthCheckType) -> KeepalivePrefixes | None:
    """The prefixes of an in-band type, or None for a type with nothing on the wire."""
    match check_type:
        case HealthCheckType.NETCAT | HealthCheckType.ICMP:
            return None
        case HealthCheckType.ECHO:
            return KeepalivePrefixes(ping=0x00000000, pong=0x00000000)
        case HealthCheckType.PINGPONG:
            return KeepalivePrefixes(ping=0x00000000, pong=0xFFFFFFFF)
        case _:
            assert_never(check_type)
