"""Environment-driven configuration: connection settings and timeouts."""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from sapient_sdk.transmission.dispatcher_config import NodeDispatcherConfig
from sapient_sdk.transport.socket_client import SocketClient
from sapient_sdk.transport.socket_provider import (
    PlainSocketProvider,
    SocketProvider,
    TlsSocketProvider,
    build_client_ssl_context,
)


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


def _timedelta_seconds_env(name: str, default_seconds: float) -> timedelta:
    return timedelta(seconds=float(os.environ.get(name, str(default_seconds))))


def _build_socket_provider(host: str, port: int) -> SocketProvider:
    # TLS turns on when FUSION_NODE_TLS_CA_CERT is set; otherwise plain TCP.
    ca_cert = os.environ.get("FUSION_NODE_TLS_CA_CERT")
    if ca_cert is None:
        return PlainSocketProvider(host=host, port=port)

    # These are passed through as-is: the SDK accepts a file path or inline
    # PEM for each, so no reading/parsing is needed here.
    ssl_context = build_client_ssl_context(
        ca_cert=ca_cert,
        client_cert=os.environ.get("FUSION_NODE_TLS_CLIENT_CERT"),
        client_key=os.environ.get("FUSION_NODE_TLS_CLIENT_KEY"),
    )
    return TlsSocketProvider(host=host, port=port, ssl_context=ssl_context)


def build_socket_client() -> SocketClient:
    host = os.environ["FUSION_NODE_HOST"]
    port = int(os.environ["FUSION_NODE_PORT"])
    socket_provider = _build_socket_provider(host, port)

    # These three match SocketClient's own built-in defaults, and are
    # readable from .env so you can tune them per-deployment (e.g. a laggier
    # network) without touching code.
    probe_timeout = _timedelta_seconds_env("SOCKET_PROBE_TIMEOUT_SECONDS", 2)
    initial_reconnect_delay = _timedelta_seconds_env(
        "SOCKET_INITIAL_RECONNECT_DELAY_SECONDS", 1
    )
    watchdog_interval = _timedelta_seconds_env("SOCKET_WATCHDOG_INTERVAL_SECONDS", 10)

    return SocketClient(
        socket_provider=socket_provider,
        probe_timeout=probe_timeout,
        initial_reconnect_delay=initial_reconnect_delay,
        watchdog_interval=watchdog_interval,
    )


def build_dispatcher_config() -> NodeDispatcherConfig:
    destination_id = os.environ["FUSION_NODE_DESTINATION_ID"]
    return NodeDispatcherConfig.defaults(destination_id=destination_id)
