from __future__ import annotations

import asyncio
import logging


def log_task_exception(
    task: asyncio.Task[object], logger: logging.Logger, message: str
) -> None:
    """Done-callback for a fire-and-forget task: log its exception, if any.

    Without this, an exception raised by a task nobody awaits only surfaces
    as an easy-to-miss "Task exception was never retrieved" warning at GC time.
    """
    if not task.cancelled() and task.exception() is not None:
        logger.error(message, exc_info=task.exception())


async def cancel_and_wait(task: asyncio.Task[object] | None) -> None:
    """Cancel a possibly-None task and await it, swallowing CancelledError."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
