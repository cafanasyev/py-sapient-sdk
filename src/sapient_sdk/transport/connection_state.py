from __future__ import annotations

import enum


class ConnectionState(enum.StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CLOSED = "closed"
