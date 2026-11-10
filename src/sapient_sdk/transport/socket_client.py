from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import assert_never

from pydantic import ConfigDict, Field, PrivateAttr
from sapient_msg.bsi_flex_335_v2_0.sapient_message_pb2 import (
    SapientMessage as SapientMessagePb2,
)
from sapient_msg_pydantic.bsi_flex_335_v2_0.sapient_message import SapientMessage

from sapient_sdk._task_utils import cancel_and_wait, log_task_exception
from sapient_sdk.transport.client import Client
from sapient_sdk.transport.connection_state import ConnectionState
from sapient_sdk.transport.framing import (
    DEFAULT_MAX_FRAME_SIZE,
    check_frame_size,
    encode_frame,
    encode_prefix,
    read_length,
    write_all,
)
from sapient_sdk.transport.health.checks import (
    HealthCheck,
    IcmpHealthCheck,
    InBandHealthCheck,
    NetcatHealthCheck,
    tcp_connect_succeeds,
)
from sapient_sdk.transport.health.config import (
    HealthCheckConfig,
    HealthCheckType,
    keepalive_prefixes,
)
from sapient_sdk.transport.health.monitor import HealthMonitor
from sapient_sdk.transport.socket_provider import SocketProvider

logger = logging.getLogger(__name__)


class _ClosedError(Exception):
    """close() was called while a writer waited for the connection."""


class SocketClient(Client):
    """TCP client with reconnection, a health check and serialized publishing.

    One write lock serves publishers in arrival order. A publish never blocks
    past its timeout. A message already being written is still finished, so a
    timeout during the write does not mean the message was not sent.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    socket_provider: SocketProvider
    health_check: HealthCheckConfig = Field(default_factory=HealthCheckConfig)
    initial_reconnect_delay: timedelta = timedelta(seconds=1)
    max_frame_size: int = Field(default=DEFAULT_MAX_FRAME_SIZE, gt=0)
    queue_maxsize: int = 256

    _state: ConnectionState = PrivateAttr(default=ConnectionState.DISCONNECTED)
    _listeners: list[Callable[[ConnectionState, datetime], object]] = PrivateAttr(
        default_factory=list
    )
    # set exactly while _writer holds a live connection
    _writer: asyncio.StreamWriter | None = PrivateAttr(default=None)
    _writer_ready: asyncio.Event = PrivateAttr()
    _closed_event: asyncio.Event = PrivateAttr()
    _closed_task: asyncio.Task[None] | None = PrivateAttr(default=None)
    _run_task: asyncio.Task[None] | None = PrivateAttr(default=None)
    _write_lock: asyncio.Lock = PrivateAttr()
    _write_tasks: set[asyncio.Task[bool]] = PrivateAttr(default_factory=set)
    _queue: asyncio.Queue[SapientMessage] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        self._write_lock = asyncio.Lock()
        self._queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self._writer_ready = asyncio.Event()
        self._closed_event = asyncio.Event()

    def _set_state(self, next_state: ConnectionState) -> None:
        if next_state == self._state:
            return
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

    @property
    def connection_loss_detection_delay(self) -> timedelta:
        return self.health_check.connection_loss_detection_delay

    def add_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        self._listeners.append(listener)

    def remove_state_change_listener(
        self, listener: Callable[[ConnectionState, datetime], object]
    ) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    async def probe_reachable(self, timeout: timedelta) -> bool:
        return await tcp_connect_succeeds(
            self.socket_provider.host, self.socket_provider.port, timeout
        )

    async def _wait_closed(self) -> None:
        await self._closed_event.wait()

    def _closed_watcher(self) -> asyncio.Task[None]:
        # one long-lived task shared by every waiter; cancelling it would make
        # every later waiter believe the client is closed
        if self._closed_task is None:
            self._closed_task = asyncio.ensure_future(self._wait_closed())
        return self._closed_task

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
        logger.info("sleep %ss before reconnecting to the server", delay)
        try:
            await asyncio.wait_for(self._closed_event.wait(), timeout=delay)
            return True
        except TimeoutError:
            return False

    async def _run_loop(self) -> None:
        attempt = 0
        while not self._closed_event.is_set():
            # an unreachable server costs no state events, so the dispatcher keeps
            # the moment the connection really died
            if await self.probe_reachable(self.health_check.timeout):
                if await self._connect_and_serve():
                    attempt = 0
            attempt += 1
            if await self._backoff(attempt):
                break

    async def _connect_and_serve(self) -> bool:
        """Returns True if a connection was established, whatever ended it."""
        logger.info("initializing new server connection")
        self._set_state(ConnectionState.CONNECTING)
        try:
            reader, writer = await self.socket_provider.open()
        except OSError:
            logger.exception("server connection failure")
            self._set_state(ConnectionState.DISCONNECTED)
            return False
        except Exception:
            logger.exception("unexpected failure on the connection")
            self._set_state(ConnectionState.DISCONNECTED)
            return False
        await self._serve(reader, writer)
        return True

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        monitor = HealthMonitor(
            self._new_health_check(),
            self.health_check,
            functools.partial(self._abort, writer),
        )
        self._writer = writer
        self._writer_ready.set()
        self._set_state(ConnectionState.CONNECTED)
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._reader_loop(reader, monitor))
                tg.create_task(monitor.run())
        except* asyncio.IncompleteReadError:
            logger.info("server closed the connection")
        except* OSError:
            logger.exception("server connection failure")
        except* Exception:
            logger.exception("unexpected failure on the connection")
        finally:
            self._forget(writer)
            await self._drop(writer, graceful=self._closed_event.is_set())
            self._set_state(ConnectionState.DISCONNECTED)

    def _new_health_check(self) -> HealthCheck:
        host = self.socket_provider.host
        timeout = self.health_check.timeout
        check_type = self.health_check.type
        match check_type:
            case HealthCheckType.NETCAT:
                return NetcatHealthCheck(host, self.socket_provider.port, timeout)
            case HealthCheckType.ICMP:
                return IcmpHealthCheck(host, timeout)
            case HealthCheckType.ECHO | HealthCheckType.PINGPONG:
                prefixes = keepalive_prefixes(check_type)
                assert prefixes is not None
                return InBandHealthCheck(prefixes.ping, timeout, self._write_prefix)
            case _:
                assert_never(check_type)

    async def _reader_loop(
        self, reader: asyncio.StreamReader, monitor: HealthMonitor
    ) -> None:
        prefixes = keepalive_prefixes(self.health_check.type)
        while True:
            length = await read_length(reader)
            if prefixes is not None and length in (prefixes.ping, prefixes.pong):
                # a keepalive has no body; the client never answers a peer's ping
                monitor.on_inbound()
                continue
            check_frame_size(length, self.max_frame_size)
            payload = await reader.readexactly(length)
            # any frame from the peer proves the link is alive, not only a pong
            monitor.on_inbound()
            try:
                pb2_msg = SapientMessagePb2()
                pb2_msg.ParseFromString(payload)
                msg = SapientMessage.from_pb2(pb2_msg)
            except Exception:
                # framing is intact, only this one message is bad
                logger.exception("dropping malformed incoming message")
                continue
            await self._queue.put(msg)

    def _forget(self, writer: asyncio.StreamWriter) -> None:
        if self._writer is writer:
            self._writer = None
            self._writer_ready.clear()

    def _abort(self, writer: asyncio.StreamWriter) -> None:
        self._forget(writer)
        writer.transport.abort()

    async def _drop(self, writer: asyncio.StreamWriter, graceful: bool) -> None:
        if graceful:
            # close() lets bytes already buffered, such as a goodbye, go out first
            writer.close()
            try:
                async with asyncio.timeout(self.health_check.timeout.total_seconds()):
                    await writer.wait_closed()
                return
            except (TimeoutError, OSError):
                pass
        # writer.close() never closes a socket whose buffer the peer does not drain
        writer.transport.abort()

    async def close(self) -> None:
        self._closed_event.set()
        await cancel_and_wait(self._run_task)
        self._run_task = None
        self._set_state(ConnectionState.CLOSED)

    async def _wait_for_connection(self) -> None:
        if self._closed_event.is_set():
            raise _ClosedError
        if not self._writer_ready.is_set():
            ready = asyncio.ensure_future(self._writer_ready.wait())
            try:
                await asyncio.wait(
                    {ready, self._closed_watcher()},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                ready.cancel()
        if self._closed_event.is_set():
            raise _ClosedError

    async def _take_writer(self, deadline: float) -> asyncio.StreamWriter:
        """Waits for a live connection and the write lock until the deadline.
        Returns with the lock held."""
        try:
            async with asyncio.timeout_at(deadline):
                while True:
                    await self._wait_for_connection()
                    await self._write_lock.acquire()
                    writer = self._writer
                    if writer is not None and not writer.is_closing():
                        return writer
                    if writer is not None:
                        self._abort(writer)  # clears _writer_ready, so the wait blocks
                    self._write_lock.release()
        except TimeoutError as exc:
            raise TimeoutError("publish timeout") from exc

    async def _write(
        self, writer: asyncio.StreamWriter, data: bytes, deadline: float
    ) -> bool:
        """Writes on a task of its own, which releases the write lock once the data
        is out, so no other frame can start in the middle of this one. Returns True
        once written, False if the connection died during the write. Raises
        TimeoutError at the deadline; the write goes on."""
        task = asyncio.ensure_future(self._write_and_release(writer, data))
        self._write_tasks.add(task)
        task.add_done_callback(self._write_tasks.discard)
        remaining = max(0.0, deadline - asyncio.get_running_loop().time())
        done, _ = await asyncio.wait({task}, timeout=remaining)
        if not done:
            raise TimeoutError("publish timeout: message still being written")
        return task.result()

    async def _write_and_release(
        self, writer: asyncio.StreamWriter, data: bytes
    ) -> bool:
        try:
            await write_all(writer, data)
            return True
        except OSError:
            logger.exception("failed to write to the server")
            self._abort(writer)
            return False
        finally:
            self._write_lock.release()

    async def publish(self, msg: SapientMessage, timeout: timedelta) -> None:
        deadline = asyncio.get_running_loop().time() + timeout.total_seconds()
        frame = encode_frame(msg.to_pb2().SerializeToString())
        while True:
            try:
                writer = await self._take_writer(deadline)
            except _ClosedError:
                raise TimeoutError("SocketClient is closed") from None
            if await self._write(writer, frame, deadline):
                return
            # the connection died during the write; wait for the next one

    async def _write_prefix(self, prefix: int, timeout: timedelta) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout.total_seconds()
        try:
            writer = await self._take_writer(deadline)
            return await self._write(writer, encode_prefix(prefix), deadline)
        except (TimeoutError, _ClosedError):
            return False

    async def messages(self) -> AsyncIterator[SapientMessage]:
        while True:
            yield await self._queue.get()
