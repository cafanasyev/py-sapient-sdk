"""Copy-paste starting point for a SAPIENT ASM node.

Copy this whole directory out of the SDK repo, `cd` into it, copy .env.example
to .env and fill in your fusion node's real address, then:

    uv sync
    uv run python main.py

See README.md in this directory for what each setting means.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from datetime import datetime

from config import build_dispatcher_config, build_socket_client, setup_logging
from dotenv import load_dotenv
from node import QuickstartNode

from sapient_sdk.transmission.dispatcher import DefaultNodeDispatcher
from sapient_sdk.transport.connection_state import ConnectionState

logger = logging.getLogger("sapient_quickstart")


def _log_connection_state_change(state: ConnectionState, ts: datetime) -> None:
    # Example of client.add_state_change_listener() -- swap this for real
    # alerting/metrics if you need to react to prolonged disconnects.
    logger.info("connection state changed to %s at %s", state.name, ts)


async def main() -> None:
    load_dotenv()
    setup_logging()

    client_settings = build_socket_client()
    client_settings.client.add_state_change_listener(_log_connection_state_change)
    dispatcher_config = build_dispatcher_config(
        client_settings.connection_loss_detection_delay
    )
    dispatcher = DefaultNodeDispatcher(
        client=client_settings.client, config=dispatcher_config
    )
    node = QuickstartNode(dispatcher=dispatcher)
    await dispatcher.register(node)
    detection_task = asyncio.create_task(node.send_detection_reports_periodically())

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    logger.info("node %s running -- press Ctrl+C to stop", node.node_id)
    await stop.wait()

    logger.info("shutting down")
    detection_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await detection_task
    await dispatcher.close()


if __name__ == "__main__":
    asyncio.run(main())
