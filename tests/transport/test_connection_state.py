from __future__ import annotations

from sapient_sdk.transport.connection_state import ConnectionState


def test_has_four_states() -> None:
    assert {s.value for s in ConnectionState} == {
        "disconnected",
        "connecting",
        "connected",
        "closed",
    }


def test_is_a_str_enum() -> None:
    assert ConnectionState.CONNECTED == "connected"  # type: ignore[comparison-overlap]
