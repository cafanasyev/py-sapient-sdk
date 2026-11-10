from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from sapient_sdk.transport.health.config import (
    HealthCheckConfig,
    HealthCheckType,
    KeepalivePrefixes,
    keepalive_prefixes,
)


def _config(
    interval: float = 10, timeout: float = 2, threshold: int = 3
) -> HealthCheckConfig:
    return HealthCheckConfig(
        type=HealthCheckType.NETCAT,
        interval=timedelta(seconds=interval),
        timeout=timedelta(seconds=timeout),
        failure_threshold=threshold,
    )


def test_detection_delay_is_threshold_times_interval_plus_timeout() -> None:
    assert _config(10, 2, 3).connection_loss_detection_delay == timedelta(seconds=32)


def test_detection_delay_with_threshold_one() -> None:
    assert _config(4, 2, 1).connection_loss_detection_delay == timedelta(seconds=6)


def test_defaults_match_the_documented_values() -> None:
    config = HealthCheckConfig()
    assert config.type == HealthCheckType.NETCAT
    assert config.interval == timedelta(seconds=10)
    assert config.timeout == timedelta(seconds=2)
    assert config.failure_threshold == 3


def test_timeout_equal_to_interval_is_allowed() -> None:
    _config(5, 5, 3)


def test_timeout_above_interval_is_rejected() -> None:
    with pytest.raises(ValidationError, match="must not exceed interval"):
        _config(1, 2, 3)


def test_threshold_below_one_is_rejected() -> None:
    with pytest.raises(ValidationError, match="failure_threshold"):
        _config(10, 2, 0)


def test_zero_interval_is_rejected() -> None:
    with pytest.raises(ValidationError, match="interval must be positive"):
        _config(0, 0, 3)


def test_zero_timeout_is_rejected() -> None:
    with pytest.raises(ValidationError, match="timeout must be positive"):
        _config(10, 0, 3)


def test_unknown_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        HealthCheckConfig(type="carrier-pigeon")  # type: ignore[arg-type]


def test_config_is_frozen() -> None:
    config = HealthCheckConfig()
    with pytest.raises(ValidationError):
        config.failure_threshold = 5


@pytest.mark.parametrize("check_type", [HealthCheckType.NETCAT, HealthCheckType.ICMP])
def test_out_of_band_types_have_no_prefixes(check_type: HealthCheckType) -> None:
    assert keepalive_prefixes(check_type) is None


def test_echo_ping_and_pong_are_both_zero() -> None:
    assert keepalive_prefixes(HealthCheckType.ECHO) == KeepalivePrefixes(0, 0)


def test_pingpong_answers_with_all_ones() -> None:
    assert keepalive_prefixes(HealthCheckType.PINGPONG) == KeepalivePrefixes(
        0, 0xFFFFFFFF
    )
