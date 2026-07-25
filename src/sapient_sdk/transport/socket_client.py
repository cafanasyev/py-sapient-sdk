from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import logging
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta

from pydantic import PrivateAttr
from sapient_msg.bsi_flex_335_v2_0.sapient_message_pb2 import (
    SapientMessage as SapientMessagePb2,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk._task_utils import cancel_and_wait, log_task_exception
from sapient_sdk.transport.client import Client
from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import read_framed, write_framed
from sapient_sdk.transport.socket_provider import SocketProvider

logger = logging.getLogger(__name__)


class SocketClient(Client):
    socket_provider: SocketProvider
    probe_timeout: timedelta = timedelta(seconds=2)
    initial_reconnect_delay: timedelta = timedelta(seconds=1)
    watchdog_interval: timedelta = timedelta(seconds=10)
    queue_maxsize: int = 256

    _state: ConnectionState = PrivateAttr(default=ConnectionState.DISCONNECTED)
    _listeners: list[Callable[[ConnectionState, datetime], object]] = PrivateAttr(
        default_factory=list
    )
    _writer: asyncio.StreamWriter | None = PrivateAttr(default=None)
    _writer_ready: asyncio.Event = PrivateAttr()
    _closed_event: asyncio.Event = PrivateAttr()
    _closed_task: asyncio.Task[None] | None = PrivateAttr(default=None)
    _run_task: asyncio.Task[None] | None = PrivateAttr(default=None)
    _write_lock: asyncio.Lock = PrivateAttr()
    _queue: asyncio.Queue[SapientMessage] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        self._write_lock = asyncio.Lock()
        self._queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self._writer_ready = asyncio.Event()
        self._closed_event = asyncio.Event()

    def _set_state(self, next_state: ConnectionState) -> None:
        logger.info("connection state: %s -> %s", self._state, next_state)
        self._state = next_state
        ts = datetime.now(UTC)
        for listener in list(self._listeners):
            try:
                result = listener(next_state, ts)
                if inspect.isawaitable(result):
                    task: asyncio.Task[object] = asyncio.ensure_future(result)
                    task.add_done_callback(
                        functools.partial(
                            log_task_exception,
                            logger=logger,
                            message="async state-change listener raised",
                        )
                    )
            except Exception:
                logger.exception("state-change listener raised")

    @property
    def state(self) -> ConnectionState:
        return self._state

    def add_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        self._listeners.append(listener)

    def remove_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def _reader_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            payload = await read_framed(reader)
            try:
                pb2_msg = SapientMessagePb2()
                pb2_msg.ParseFromString(payload)
                msg = SapientMessage.from_pb2(pb2_msg)
            except Exception:
                # Broad catch is deliberate: this is a network-input boundary, and
                # a single malformed/ICD-invalid message must not kill the whole
                # connection when framing itself (the 4-byte length prefix) is
                # still intact — only the content of this one message is bad.
                logger.exception("dropping malformed incoming message")
                continue
            await self._queue.put(msg)

    async def _watchdog_loop(self, writer: asyncio.StreamWriter) -> None:
        while True:
            await asyncio.sleep(self.watchdog_interval.total_seconds())
            if not await self.probe_reachable(self.probe_timeout):
                writer.close()
                return

    async def probe_reachable(self, timeout: timedelta) -> bool:
        try:
            _, writer = await asyncio.wait_for(
                self.socket_provider.open(), timeout=timeout.total_seconds()
            )
        except (OSError, TimeoutError):
            return False
        with contextlib.closing(writer):
            return True

    async def _wait_closed(self) -> None:
        await self._closed_event.wait()

    async def start(self) -> None:
        if self._run_task is not None:
            return
        self._closed_event.clear()
        self._closed_task = asyncio.ensure_future(self._wait_closed())
        self._run_task = asyncio.ensure_future(self._run_loop())

    async def _backoff(self, attempt: int) -> bool:
        """Sleep for the backoff delay, interruptible by close(). Returns True
        if close() won the race (caller should stop retrying)."""
        delay = min(attempt, 10) * self.initial_reconnect_delay.total_seconds()
        try:
            await asyncio.wait_for(self._closed_event.wait(), timeout=delay)
            return True
        except TimeoutError:
            return False

    async def _run_loop(self) -> None:
        attempt = 0
        while not self._closed_event.is_set():
            self._set_state(ConnectionState.CONNECTING)
            try:
                reader, writer = await self.socket_provider.open()
            except OSError:
                self._set_state(ConnectionState.DISCONNECTED)
                attempt += 1
                if await self._backoff(attempt):
                    break
                continue

            attempt = 0
            self._writer = writer
            self._writer_ready.set()
            self._set_state(ConnectionState.CONNECTED)

            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self._reader_loop(reader))
                    tg.create_task(self._watchdog_loop(writer))
            except* (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                self._writer = None
                self._writer_ready.clear()
                self._set_state(ConnectionState.DISCONNECTED)

            attempt += 1
            if await self._backoff(attempt):
                break

    async def close(self) -> None:
        self._closed_event.set()
        await cancel_and_wait(self._run_task)
        self._run_task = None
        self._set_state(ConnectionState.CLOSED)

    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None:
        # A three-way select: whichever of "write completed", "client closed",
        # or "deadline elapsed" happens first decides the outcome. _write()
        # itself needs to know nothing about timeouts or shutdown -- that
        # concern lives here, in exactly one place.
        #
        # self._closed_task is a single long-lived task shared across every
        # publish() call in a session (see start()) -- it only ever fires once
        # (when close() sets self._closed_event), so recreating it per call
        # would be pure overhead. It must NEVER be cancelled here: cancelling
        # a task shared by other in-flight/future publish() calls would
        # permanently poison them into reporting "closed" forever after.
        async def _write() -> None:
            await self._writer_ready.wait()
            payload = msg.to_pb2().SerializeToString()
            async with self._write_lock:
                writer = self._writer
                if writer is None:
                    raise ConnectionError("SocketClient is not connected")
                await write_framed(writer, payload)

        closed_task = self._closed_task
        if closed_task is None:
            closed_task = asyncio.ensure_future(self._wait_closed())
            self._closed_task = closed_task

        write_task = asyncio.ensure_future(_write())
        try:
            async with asyncio.timeout(timeout.total_seconds()):
                done, _pending = await asyncio.wait(
                    {write_task, closed_task}, return_when=asyncio.FIRST_COMPLETED
                )
        except TimeoutError as exc:
            raise TimeoutError("SocketClient publish timed out") from exc
        finally:
            if not write_task.done():
                write_task.cancel()

        if closed_task in done:
            raise TimeoutError("SocketClient is closed")

        try:
            write_task.result()
        except OSError as exc:
            # Callers (NodeWrapper) only catch TimeoutError to mean "publish
            # failed, retry next cycle" -- a raw ConnectionResetError/
            # BrokenPipeError from drain() must not bypass that handling.
            raise TimeoutError("SocketClient publish failed: connection lost") from exc

    async def messages(self) -> AsyncIterator[SapientMessage]:
        while True:
            yield await self._queue.get()
