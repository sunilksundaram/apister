"""In-process pub/sub bus.

One writer appends each payload to the ledger once and is the only task that
mutates the topic trie. Subscriber queues hold integer offsets, so fan-out
stays small when connections or message bodies do not. Shutdown enqueues a
sentinel on every subscriber queue.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from apister.brokers.base import BrokerAdapter, Incoming
from apister.dedup import DedupLog
from apister.framing import decode_frame, encode_frame
from apister.ledger import Ledger
from apister.reconnect import ReconnectingBroker
from apister.topics import TopicTrie, parse_topic, topic_matches
from apister.types import (
    SHUTDOWN,
    BrokerUnavailable,
    BusClosed,
    BusStats,
    Envelope,
    OverflowPolicy,
    PayloadTooLarge,
    PublishTimeout,
    ReconnectPolicy,
    SubscriptionClosed,
    _ShutdownSentinel,
    unreachable,
)

logger = logging.getLogger("apister.bus")

_CURSOR_NAME = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass(slots=True)
class _Publish:
    topic: str
    payload: bytes
    from_broker: bool
    message_id: bytes
    future: asyncio.Future[Envelope | None]
    deadline: float | None = None
    claimed: bool = False
    cancelled: bool = False


@dataclass(slots=True)
class _Subscribe:
    pattern: str
    queue_size: int
    replay_from: int | None
    name: str | None
    inflight_limit: int | None
    future: asyncio.Future[Subscription]


@dataclass(slots=True)
class _Unsubscribe:
    subscription: Subscription
    future: asyncio.Future[None]


@dataclass(slots=True)
class _Ack:
    subscription: Subscription
    offset: int
    next_offset: int
    future: asyncio.Future[None]


@dataclass(slots=True)
class _Fill:
    subscription: Subscription


@dataclass(slots=True)
class _SetLimit:
    topic: str
    limit: int
    future: asyncio.Future[None]


@dataclass(slots=True)
class _Shutdown:
    future: asyncio.Future[None]


_Command = _Publish | _Subscribe | _Unsubscribe | _Ack | _Fill | _SetLimit | _Shutdown


class Subscription:
    """Async iterator of envelopes for one pattern.

    ``ack`` advances a contiguous cursor. With ``name``, that cursor is reloaded
    on the next subscribe, so a restart redelivers anything not yet acked.
    ``inflight_limit`` is how many delivered messages may stay unacked.
    """

    __slots__ = (
        "pattern",
        "name",
        "queue",
        "min_live_offset",
        "inflight_limit",
        "cursor",
        "_bus",
        "_closed",
        "_explicit_limit",
        "_outstanding",
        "_acked",
        "_pending",
        "_replay_pos",
        "_cursor_path",
        "_cursor_dirty",
        "_fill_scheduled",
    )

    def __init__(
        self,
        bus: Bus,
        pattern: str,
        queue_size: int,
        *,
        name: str | None,
        inflight_limit: int | None,
        explicit_limit: bool,
    ) -> None:
        self._bus = bus
        self.pattern = pattern
        self.name = name
        self.queue: asyncio.Queue[int | _ShutdownSentinel] = asyncio.Queue(maxsize=queue_size)
        self.min_live_offset = 0
        self.inflight_limit = inflight_limit
        self.cursor = 0
        self._explicit_limit = explicit_limit
        self._closed = False
        self._outstanding: deque[tuple[int, int]] = deque()
        self._acked: set[int] = set()
        self._pending: deque[tuple[int, int, str]] = deque()
        self._replay_pos: int | None = None
        self._cursor_path: Path | None = None
        self._cursor_dirty = False
        self._fill_scheduled = False

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Envelope:
        try:
            return await self.get()
        except SubscriptionClosed:
            raise StopAsyncIteration from None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self._bus.unsubscribe(self)

    async def get(self) -> Envelope:
        """Return the next envelope. Raises ``SubscriptionClosed`` on shutdown."""
        if self._closed and self.queue.empty():
            raise SubscriptionClosed()
        item = await self.queue.get()
        self._bus._nudge(self)
        if item is SHUTDOWN:
            self._closed = True
            raise SubscriptionClosed()
        return await self._bus._materialize(item)

    async def ack(self, envelope: Envelope) -> None:
        """Mark ``envelope`` done. The cursor advances through a contiguous prefix."""
        if self._closed:
            raise SubscriptionClosed()
        await self._bus._ack(self, envelope.offset, envelope.next_offset)


class _TailCache:
    """Hot tail so live subscribers do not re-read a payload just written."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._bytes = 0
        self._data: OrderedDict[int, Envelope] = OrderedDict()

    def put(self, envelope: Envelope) -> None:
        size = len(envelope.payload) + len(envelope.topic)
        if self._max <= 0 or size > self._max:
            return
        self._data[envelope.offset] = envelope
        self._bytes += size
        while self._bytes > self._max and self._data:
            _offset, old = self._data.popitem(last=False)
            self._bytes -= len(old.payload) + len(old.topic)

    def get(self, offset: int) -> Envelope | None:
        return self._data.get(offset)


@dataclass(slots=True)
class _Bridge:
    name: str
    session: ReconnectingBroker
    task: asyncio.Task[None]


class Bus:
    """Durable fan-out bus. Open with ``await Bus.open(path)``."""

    def __init__(
        self,
        directory: Path,
        *,
        max_queue_size: int,
        command_queue_size: int,
        overflow: OverflowPolicy,
        max_payload_bytes: int,
        segment_bytes: int,
        commit_every: int,
        retain_bytes: int | None,
        tail_cache_bytes: int,
        dedup_ids: int,
        shutdown_timeout_s: float,
        batch_max: int,
        fsync_interval_s: float,
    ) -> None:
        if max_queue_size < 1 or command_queue_size < 1:
            raise ValueError("queue sizes must be >= 1")
        if max_payload_bytes < 1:
            raise ValueError("max_payload_bytes must be >= 1")
        if batch_max < 1:
            raise ValueError("batch_max must be >= 1")
        if fsync_interval_s <= 0:
            raise ValueError("fsync_interval_s must be > 0")
        self._dir = directory
        self._max_queue_size = max_queue_size
        self._overflow = overflow
        self._max_payload_bytes = max_payload_bytes
        self._shutdown_timeout_s = shutdown_timeout_s
        self._batch_max = batch_max
        self._fsync_interval_s = fsync_interval_s
        self._commit_every = commit_every
        self._accepting = True
        self._draining = False
        self._trie = TopicTrie()
        self._subscribers: set[Subscription] = set()
        self._ledger = Ledger(
            directory / "ledger",
            segment_bytes=segment_bytes,
            commit_every=commit_every,
            retain_bytes=retain_bytes,
        )
        self._dedup = DedupLog(directory / "dedup", max_ids=dedup_ids)
        self._cache = _TailCache(tail_cache_bytes)
        self._instance_id = _load_instance_id(directory)
        self._commands: asyncio.Queue[_Command] = asyncio.Queue(maxsize=command_queue_size)
        self._topic_limits: dict[str, int] = {}
        self._cursor_names: set[str] = set()
        self._retired_cursors: dict[str, int] = {}
        self._cursor_high: dict[Path, int] = {}
        self._pending_cursor_writes: list[tuple[Path, int]] = []
        self._cursors_frozen = False
        self._cursor_lock = threading.Lock()
        self._space = asyncio.Event()
        self._held: list[_Publish] = []
        self._writer = asyncio.create_task(self._run_writer(), name="apister-writer")
        self._fsync_task = asyncio.create_task(self._fsync_loop(), name="apister-fsync")
        self._bridges: list[_Bridge] = []
        self._bridge_patterns: dict[str, tuple[str, ...]] = {}
        self._append_gen = 0
        self._appended = asyncio.Event()
        self._shutdown_waiter: asyncio.Task[None] | None = None
        self._current: _Command | None = None
        self.published = 0
        self.delivered = 0
        self.dropped = 0
        self.broker_forwards = 0
        self.broker_ingests = 0
        self.duplicates = 0

    @classmethod
    async def open(
        cls,
        path: str | Path,
        *,
        max_queue_size: int = 1024,
        command_queue_size: int = 4096,
        overflow: OverflowPolicy = OverflowPolicy.BLOCK,
        max_payload_bytes: int = 8 * 1024 * 1024,
        segment_bytes: int = 64 * 1024 * 1024,
        commit_every: int = 128,
        retain_bytes: int | None = None,
        tail_cache_bytes: int = 32 * 1024 * 1024,
        dedup_ids: int = 200_000,
        shutdown_timeout_s: float = 5.0,
        batch_max: int = 128,
        fsync_interval_s: float = 0.01,
    ) -> Bus:
        """Create the ledger directory, recover sealed segments, and start the writer.

        Appends are batched. The ledger is fsynced on ``fsync_interval_s`` and also
        every ``commit_every`` records, whichever comes first. Shutdown fsyncs too.
        """
        return cls(
            Path(path),
            max_queue_size=max_queue_size,
            command_queue_size=command_queue_size,
            overflow=overflow,
            max_payload_bytes=max_payload_bytes,
            segment_bytes=segment_bytes,
            commit_every=commit_every,
            retain_bytes=retain_bytes,
            tail_cache_bytes=tail_cache_bytes,
            dedup_ids=dedup_ids,
            shutdown_timeout_s=shutdown_timeout_s,
            batch_max=batch_max,
            fsync_interval_s=fsync_interval_s,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.shutdown()

    def stats(self) -> BusStats:
        reconnects = sum(bridge.session.reconnects for bridge in self._bridges)
        return BusStats(
            published=self.published,
            delivered=self.delivered,
            dropped=self.dropped,
            broker_forwards=self.broker_forwards,
            broker_ingests=self.broker_ingests,
            duplicates=self.duplicates,
            reconnects=reconnects,
            subscribers=len(self._subscribers),
            ledger_bytes=self._ledger.bytes_retained,
            inflight=sum(len(sub._outstanding) for sub in self._subscribers),
            parked=sum(len(sub._pending) for sub in self._subscribers),
        )

    async def publish(
        self,
        topic: str,
        payload: bytes | bytearray | memoryview,
        *,
        from_broker: bool = False,
        message_id: bytes | None = None,
        timeout: float | None = None,
    ) -> Envelope | None:
        """Append one message and fan it out. Returns ``None`` if it was a duplicate.

        ``timeout`` bounds how long the writer may wait for subscriber queue space
        before the append. Once the append has started, this waits for that result.
        ``timeout`` raises ``PublishTimeout`` and does not write the ledger.
        """
        if not self._accepting:
            raise BusClosed()
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0")
        parse_topic(topic, pattern=False)
        body = _as_bytes(payload)
        if len(body) > self._max_payload_bytes:
            raise PayloadTooLarge(f"payload is {len(body)} bytes; limit is {self._max_payload_bytes}")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Envelope | None] = loop.create_future()
        deadline = None if timeout is None else time.monotonic() + timeout
        command = _Publish(
            topic=topic,
            payload=body,
            from_broker=from_broker,
            message_id=message_id if message_id is not None else _new_message_id(),
            future=future,
            deadline=deadline,
        )
        await self._commands.put(command)
        if self._writer.done() and not future.done():
            future.set_exception(BusClosed())
        return await future

    async def subscribe(
        self,
        pattern: str,
        *,
        queue_size: int | None = None,
        replay_from: int | None = None,
        name: str | None = None,
        inflight_limit: int | None = None,
    ) -> Subscription:
        """Subscribe to ``pattern``.

        ``replay_from`` is an absolute ledger offset. ``name`` persists an ack
        cursor under that name. A new name starts at the live tail. ``inflight_limit``
        is how many delivered messages may stay unacked before later ones park.
        """
        if not self._accepting:
            raise BusClosed()
        parse_topic(pattern, pattern=True)
        size = self._max_queue_size if queue_size is None else queue_size
        if size < 1:
            raise ValueError("queue_size must be >= 1")
        if replay_from is not None and replay_from < 0:
            raise ValueError("replay_from must be >= 0")
        if name is not None and _CURSOR_NAME.fullmatch(name) is None:
            raise ValueError("cursor name must be 1-64 characters of A-Z, a-z, 0-9, ., _, -")
        if inflight_limit is not None and inflight_limit < 1:
            raise ValueError("inflight_limit must be >= 1")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Subscription] = loop.create_future()
        command = _Subscribe(pattern, size, replay_from, name, inflight_limit, future)
        await self._commands.put(command)
        if self._writer.done() and not future.done():
            future.set_exception(BusClosed())
        return await future

    async def limit_inflight(self, pattern: str, limit: int) -> None:
        """Default unacked cap for subscribers of this exact pattern."""
        if not self._accepting:
            raise BusClosed()
        parse_topic(pattern, pattern=True)
        if limit < 1:
            raise ValueError("inflight limit must be >= 1")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        await self._commands.put(_SetLimit(pattern, limit, future))
        if self._writer.done() and not future.done():
            future.set_exception(BusClosed())
        await future

    async def _ack(self, subscription: Subscription, offset: int, next_offset: int) -> None:
        if self._writer.done():
            raise BusClosed()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        await self._commands.put(_Ack(subscription, offset, next_offset, future))
        if self._writer.done() and not future.done():
            future.set_exception(BusClosed())
        await future

    async def unsubscribe(self, subscription: Subscription) -> None:
        if subscription._closed and subscription not in self._subscribers:
            return
        if self._writer.done():
            self._seal(subscription)
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        await self._commands.put(_Unsubscribe(subscription, future))
        if self._writer.done() and not future.done():
            future.set_exception(BusClosed())
        with suppress(BusClosed):
            await future

    async def attach_broker(
        self,
        adapter: BrokerAdapter,
        *,
        patterns: Sequence[str],
        name: str | None = None,
        policy: ReconnectPolicy | None = None,
    ) -> None:
        """Forward the local ledger to ``adapter`` and ingest matching remote messages.

        Remote records are stored locally and are not forwarded again, so brokers
        are not bridged to each other. Delivery is at-least-once; redelivery is
        dropped by the message-id ledger.
        """
        if not self._accepting:
            raise BusClosed()
        if not patterns:
            raise ValueError("at least one pattern is required")
        for pattern in patterns:
            parse_topic(pattern, pattern=True)
        bridge_name = name or adapter.name
        if bridge_name in self._bridge_patterns:
            raise ValueError(f"broker {bridge_name} is already attached")
        stored = tuple(patterns)
        self._bridge_patterns[bridge_name] = stored
        session = ReconnectingBroker(
            adapter,
            policy or ReconnectPolicy(),
            stored,
            lambda incoming: self._ingest(bridge_name, incoming),
        )
        await session.start()
        cursor_path = self._dir / "bridges" / f"{bridge_name}.cursor"
        task = asyncio.create_task(
            self._pump(session, cursor_path),
            name=f"apister-{bridge_name}-outbox",
        )
        self._bridges.append(_Bridge(bridge_name, session, task))

    async def shutdown(self) -> None:
        """Stop publishers, seal subscriber queues with ``SHUTDOWN``, and fsync."""
        if self._shutdown_waiter is not None:
            await self._shutdown_waiter
            return
        self._accepting = False
        self._shutdown_waiter = asyncio.create_task(self._shutdown_body(), name="apister-shutdown")
        await self._shutdown_waiter

    async def _shutdown_body(self) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        try:
            await asyncio.wait_for(
                self._commands.put(_Shutdown(future)),
                timeout=self._shutdown_timeout_s,
            )
            await asyncio.wait_for(future, timeout=self._shutdown_timeout_s)
        except TimeoutError:
            logger.warning("writer did not drain in time; cancelling")
            self._writer.cancel()
            with suppress(asyncio.CancelledError):
                await self._writer
            if not future.done():
                future.set_result(None)
        self._draining = True
        self._appended.set()
        pumps = [bridge.task for bridge in self._bridges]
        if pumps:
            _done, pending = await asyncio.wait(pumps, timeout=self._shutdown_timeout_s)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        for bridge in self._bridges:
            await bridge.session.stop()
        await self._stop_fsync()
        self._ledger.close()
        self._dedup.close()

    async def _stop_fsync(self) -> None:
        if self._fsync_task.done():
            return
        self._fsync_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._fsync_task

    async def _run_writer(self) -> None:
        try:
            while True:
                self._service_fills()
                command = await self._commands.get()
                if await self._consume(command):
                    return
        except asyncio.CancelledError:
            for pending in self._held:
                _reject(pending)
                self._commands.task_done()
            self._held.clear()
            self._reject_pending()
            self._freeze_and_write_cursors()
            self._seal_all()
            self._ledger.commit()
            self._dedup.flush()
            raise

    async def _consume(self, command: _Command) -> bool:
        if not isinstance(command, _Publish):
            return await self._dispatch_one(command)
        batch: list[_Publish] = [command]
        while len(batch) < self._batch_max:
            try:
                nxt = self._commands.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not isinstance(nxt, _Publish):
                await self._run_publish_batch(batch)
                return await self._dispatch_one(nxt)
            batch.append(nxt)
        await self._run_publish_batch(batch)
        return False

    async def _run_publish_batch(self, batch: list[_Publish]) -> None:
        self._held = batch
        while self._held:
            command = self._held.pop(0)
            await self._dispatch_one(command)
        self._held = []
        self._service_fills()

    async def _dispatch_one(self, command: _Command) -> bool:
        self._current = command
        try:
            try:
                return await self._dispatch(command)
            except Exception as exc:
                logger.exception("writer failed a command")
                _fail_command(command, exc)
                return False
        finally:
            if not _is_finished(command):
                _reject(command)
            self._current = None
            self._commands.task_done()

    async def _dispatch(self, command: _Command) -> bool:
        match command:
            case _Publish() as publish:
                if await self._admit(publish):
                    self._append_and_fanout(publish)
                return False
            case _Subscribe() as subscribe:
                self._on_subscribe(subscribe)
                return False
            case _Unsubscribe() as unsubscribe:
                self._on_unsubscribe(unsubscribe)
                return False
            case _Ack() as ack:
                self._on_ack(ack)
                return False
            case _Fill() as fill:
                fill.subscription._fill_scheduled = False
                self._fill(fill.subscription)
                return False
            case _SetLimit() as limit:
                self._on_set_limit(limit)
                return False
            case _Shutdown() as shutdown:
                await self._on_shutdown(shutdown)
                return True
            case _ as other:
                unreachable(other)

    async def _admit(self, command: _Publish) -> bool:
        """Wait until every blocking subscriber can take or park this publish."""
        while True:
            if command.cancelled:
                return False
            if self._can_admit(command.topic):
                command.claimed = True
                return True
            if command.deadline is not None and time.monotonic() >= command.deadline:
                command.cancelled = True
                _fail(command.future, PublishTimeout(command.topic))
                return False
            self._space.clear()
            self._service_fills()
            if self._can_admit(command.topic):
                command.claimed = True
                return True
            remaining = None
            if command.deadline is not None:
                remaining = max(0.0, command.deadline - time.monotonic())
            try:
                await asyncio.wait_for(self._space.wait(), remaining)
            except TimeoutError:
                continue

    def _can_admit(self, topic: str) -> bool:
        if self._overflow is not OverflowPolicy.BLOCK:
            return True
        for subscription in self._trie.match(topic):
            if subscription._closed or self._must_park(subscription):
                continue
            if subscription.queue.full():
                return False
        return True

    def _must_park(self, subscription: Subscription) -> bool:
        if subscription._replay_pos is not None and subscription._replay_pos < subscription.min_live_offset:
            return True
        if subscription._pending:
            return True
        limit = subscription.inflight_limit
        return limit is not None and len(subscription._outstanding) >= limit

    def _append_and_fanout(self, command: _Publish) -> None:
        if command.cancelled:
            return
        if not self._dedup.remember(command.message_id):
            self.duplicates += 1
            _ok(command.future, None)
            return
        envelope = self._ledger.append(
            command.topic,
            command.payload,
            command.message_id,
            time.time_ns(),
            from_broker=command.from_broker,
            commit=False,
        )
        self._ledger.commit_if_due()
        self._cache.put(envelope)
        self.published += 1
        if command.from_broker:
            self.broker_ingests += 1
        self._append_gen += 1
        self._appended.set()
        targets = [
            sub
            for sub in self._trie.match(command.topic)
            if envelope.offset >= sub.min_live_offset and not sub._closed
        ]
        for subscription in targets:
            self._route(subscription, envelope)
        _ok(command.future, envelope)

    def _route(self, subscription: Subscription, envelope: Envelope) -> None:
        if self._must_park(subscription):
            subscription._pending.append((envelope.offset, envelope.next_offset, envelope.topic))
            return
        queued = self._offer(subscription, envelope.offset, envelope.next_offset)
        if not queued:
            subscription._pending.append((envelope.offset, envelope.next_offset, envelope.topic))

    def _offer(self, subscription: Subscription, offset: int, next_offset: int) -> bool:
        """Queue one offset. False means a blocking queue is full and nothing was consumed."""
        queue = subscription.queue
        match self._overflow:
            case OverflowPolicy.BLOCK:
                if queue.full():
                    return False
                queue.put_nowait(offset)
            case OverflowPolicy.DROP_NEWEST:
                try:
                    queue.put_nowait(offset)
                except asyncio.QueueFull:
                    self.dropped += 1
                    return True
            case OverflowPolicy.DROP_OLDEST:
                while True:
                    try:
                        queue.put_nowait(offset)
                        break
                    except asyncio.QueueFull:
                        try:
                            dropped = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            continue
                        self.dropped += 1
                        if isinstance(dropped, int):
                            self._forget_queued(subscription, dropped)
            case _ as other:
                unreachable(other)
        self._track(subscription, offset, next_offset)
        self.delivered += 1
        return True

    def _track(self, subscription: Subscription, offset: int, next_offset: int) -> None:
        if subscription.name is None and subscription.inflight_limit is None:
            return
        subscription._outstanding.append((offset, next_offset))

    def _forget_queued(self, subscription: Subscription, offset: int) -> None:
        if not subscription._outstanding:
            return
        kept = deque(item for item in subscription._outstanding if item[0] != offset)
        if len(kept) == len(subscription._outstanding):
            return
        subscription._outstanding = kept
        subscription._acked.discard(offset)

    def _on_subscribe(self, command: _Subscribe) -> None:
        if command.name is not None and command.name in self._cursor_names:
            _fail(command.future, ValueError(f"cursor {command.name!r} is already subscribed"))
            return
        explicit = command.inflight_limit is not None
        limit = command.inflight_limit
        if not explicit and command.pattern in self._topic_limits:
            limit = self._topic_limits[command.pattern]
        subscription = Subscription(
            self,
            command.pattern,
            command.queue_size,
            name=command.name,
            inflight_limit=limit,
            explicit_limit=explicit,
        )
        end = self._ledger.end_offset
        subscription.min_live_offset = end
        if command.name is not None:
            path = self._dir / "cursors" / command.name
            subscription._cursor_path = path
            stored = self._stored_cursor(path, command.name)
            if command.replay_from is not None:
                subscription.cursor = command.replay_from if stored is None else stored
                subscription._replay_pos = command.replay_from
            elif stored is None:
                subscription.cursor = end
                self._cursor_high[path] = end
                _write_cursor(path, end)
            else:
                subscription.cursor = stored
                if stored < end:
                    subscription._replay_pos = stored
            self._cursor_names.add(command.name)
        elif command.replay_from is not None:
            subscription.cursor = command.replay_from
            subscription._replay_pos = command.replay_from
        self._trie.add(command.pattern, subscription)
        self._subscribers.add(subscription)
        self._fill(subscription)
        _ok(command.future, subscription)

    def _on_unsubscribe(self, command: _Unsubscribe) -> None:
        subscription = command.subscription
        self._retire_cursor(subscription)
        self._trie.discard(subscription.pattern, subscription)
        self._subscribers.discard(subscription)
        self._seal(subscription)
        self._space.set()
        _ok(command.future, None)

    def _retire_cursor(self, subscription: Subscription) -> None:
        if subscription.name is None:
            return
        self._cursor_names.discard(subscription.name)
        self._retired_cursors[subscription.name] = subscription.cursor
        subscription._cursor_dirty = False

    def _on_ack(self, command: _Ack) -> None:
        subscription = command.subscription
        if subscription._closed or subscription not in self._subscribers:
            _fail(command.future, SubscriptionClosed())
            return
        self._apply_ack(subscription, command.offset, command.next_offset)
        self._space.set()
        self._fill(subscription)
        _ok(command.future, None)

    def _apply_ack(self, subscription: Subscription, offset: int, next_offset: int) -> None:
        if subscription.name is None and subscription.inflight_limit is None:
            return
        if not subscription._outstanding:
            return
        if subscription._outstanding[0][0] != offset:
            for queued, _follow in subscription._outstanding:
                if queued == offset:
                    subscription._acked.add(offset)
                    return
            return
        subscription._outstanding.popleft()
        advanced = next_offset
        while subscription._outstanding and subscription._outstanding[0][0] in subscription._acked:
            acked_offset, acked_next = subscription._outstanding.popleft()
            subscription._acked.discard(acked_offset)
            advanced = acked_next
        if advanced > subscription.cursor and subscription._cursor_path is not None:
            subscription.cursor = advanced
            subscription._cursor_dirty = True
        elif advanced > subscription.cursor:
            subscription.cursor = advanced

    def _on_set_limit(self, command: _SetLimit) -> None:
        self._topic_limits[command.topic] = command.limit
        for subscription in self._subscribers:
            if subscription.pattern != command.topic or subscription._explicit_limit:
                continue
            subscription.inflight_limit = command.limit
            self._fill(subscription)
        self._space.set()
        _ok(command.future, None)

    async def _on_shutdown(self, command: _Shutdown) -> None:
        self._draining = True
        self._reject_pending()
        await self._stop_fsync()
        self._freeze_and_write_cursors()
        self._seal_all()
        self._ledger.commit()
        self._dedup.flush()
        self._appended.set()
        _ok(command.future, None)

    def _service_fills(self) -> None:
        for subscription in list(self._subscribers):
            replay_left = (
                subscription._replay_pos is not None
                and subscription._replay_pos < subscription.min_live_offset
            )
            if subscription._closed or not (replay_left or subscription._pending):
                continue
            self._fill(subscription)

    def _fill(self, subscription: Subscription) -> None:
        subscription._fill_scheduled = False
        if subscription._closed:
            return
        while True:
            limit = subscription.inflight_limit
            if limit is not None and len(subscription._outstanding) >= limit:
                return
            replay_at = subscription._replay_pos
            if replay_at is not None and replay_at < subscription.min_live_offset:
                record = self._ledger.read_next(replay_at, subscription.min_live_offset)
                if record is None:
                    subscription._replay_pos = None
                    continue
                if not topic_matches(subscription.pattern, record.topic):
                    subscription._replay_pos = record.next_offset
                    continue
                if not self._offer(subscription, record.offset, record.next_offset):
                    return
                subscription._replay_pos = record.next_offset
                continue
            subscription._replay_pos = None
            if not subscription._pending:
                return
            offset, next_offset, _topic = subscription._pending[0]
            if not self._offer(subscription, offset, next_offset):
                return
            subscription._pending.popleft()

    def _nudge(self, subscription: Subscription) -> None:
        """Wake a writer blocked on queue space and continue replay or parked mail."""
        self._space.set()
        if subscription._fill_scheduled or subscription._closed:
            return
        replay_left = (
            subscription._replay_pos is not None
            and subscription._replay_pos < subscription.min_live_offset
        )
        if not replay_left and not subscription._pending:
            return
        subscription._fill_scheduled = True
        try:
            self._commands.put_nowait(_Fill(subscription))
        except asyncio.QueueFull:
            subscription._fill_scheduled = False

    async def _fsync_loop(self) -> None:
        try:
            while not self._draining and not self._cursors_frozen:
                await asyncio.sleep(self._fsync_interval_s)
                if self._draining or self._cursors_frozen:
                    return
                await self._fsync_once()
        except asyncio.CancelledError:
            raise

    async def _fsync_once(self) -> None:
        cursors = self._snapshot_dirty_cursors()
        blob = self._dedup.steal_pending()
        if not cursors and not blob and self._ledger.uncommitted == 0:
            return
        try:
            await asyncio.to_thread(self._sync_durable, cursors, blob)
        except Exception:
            logger.exception("timed fsync failed")
            self._pending_cursor_writes.extend(cursors)

    def _snapshot_dirty_cursors(self) -> list[tuple[Path, int]]:
        merged: dict[Path, int] = {}
        pending = self._pending_cursor_writes
        self._pending_cursor_writes = []
        for path, offset in pending:
            self._keep_cursor(merged, path, offset)
        for subscription in self._subscribers:
            path = subscription._cursor_path
            if path is None or not subscription._cursor_dirty:
                continue
            self._keep_cursor(merged, path, subscription.cursor)
            subscription._cursor_dirty = False
        retired = self._retired_cursors
        self._retired_cursors = {}
        for name, offset in retired.items():
            self._keep_cursor(merged, self._dir / "cursors" / name, offset)
        return list(merged.items())

    def _freeze_and_write_cursors(self) -> None:
        with self._cursor_lock:
            self._cursors_frozen = True
            for path, offset in self._all_cursors():
                _write_cursor(path, offset)

    def _all_cursors(self) -> list[tuple[Path, int]]:
        merged: dict[Path, int] = {}
        pending = self._pending_cursor_writes
        self._pending_cursor_writes = []
        for path, offset in pending:
            self._keep_cursor(merged, path, offset)
        for subscription in self._subscribers:
            path = subscription._cursor_path
            if path is None:
                continue
            self._keep_cursor(merged, path, subscription.cursor)
            subscription._cursor_dirty = False
        retired = self._retired_cursors
        self._retired_cursors = {}
        for name, offset in retired.items():
            self._keep_cursor(merged, self._dir / "cursors" / name, offset)
        return list(merged.items())

    def _keep_cursor(self, merged: dict[Path, int], path: Path, offset: int) -> None:
        previous = self._cursor_high.get(path, -1)
        chosen = offset if offset >= previous else previous
        self._cursor_high[path] = chosen
        if chosen > merged.get(path, -1):
            merged[path] = chosen

    def _stored_cursor(self, path: Path, name: str) -> int | None:
        retired = self._retired_cursors.pop(name, None)
        candidates = [
            value
            for value in (retired, self._cursor_high.get(path), _read_cursor_optional(path))
            if value is not None
        ]
        if not candidates:
            return None
        best = max(candidates)
        previous = self._cursor_high.get(path, -1)
        self._cursor_high[path] = best if best >= previous else previous
        return self._cursor_high[path]

    def _sync_durable(self, cursors: list[tuple[Path, int]], dedup_blob: bytes) -> None:
        if dedup_blob:
            self._dedup.write_pending(dedup_blob)
        with self._cursor_lock:
            if not self._cursors_frozen:
                for path, offset in cursors:
                    _write_cursor(path, offset)
        self._ledger.commit()

    def _seal(self, subscription: Subscription) -> None:
        subscription._closed = True
        try:
            subscription.queue.put_nowait(SHUTDOWN)
            return
        except asyncio.QueueFull:
            logger.warning("dropping one queued offset to deliver the shutdown sentinel")
        with suppress(asyncio.QueueEmpty):
            subscription.queue.get_nowait()
            self.dropped += 1
        with suppress(asyncio.QueueFull):
            subscription.queue.put_nowait(SHUTDOWN)

    def _seal_all(self) -> None:
        for subscription in list(self._subscribers):
            self._trie.discard(subscription.pattern, subscription)
            self._seal(subscription)
        self._subscribers.clear()

    def _reject_pending(self) -> None:
        while True:
            try:
                command = self._commands.get_nowait()
            except asyncio.QueueEmpty:
                return
            _reject(command)
            self._commands.task_done()

    async def _ingest(self, bridge_name: str, incoming: Incoming) -> None:
        patterns = self._bridge_patterns.get(bridge_name, ())
        if not any(topic_matches(pattern, incoming.topic) for pattern in patterns):
            await incoming.ack()
            return
        framed = decode_frame(incoming.payload)
        if framed is not None:
            instance_id, message_id, payload = framed
            if instance_id == self._instance_id:
                await incoming.ack()
                return
        else:
            message_id = _new_message_id()
            payload = incoming.payload
        await self.publish(
            incoming.topic,
            payload,
            from_broker=True,
            message_id=message_id,
        )
        await incoming.ack()

    async def _pump(self, session: ReconnectingBroker, cursor_path: Path) -> None:
        cursor = _read_cursor(cursor_path)
        pending = 0
        while True:
            record = self._ledger.read_next(cursor)
            if record is None:
                if self._draining:
                    _write_cursor(cursor_path, cursor)
                    return
                await self._wait_for_append()
                continue
            if not record.from_broker:
                framed = encode_frame(self._instance_id, record.message_id, record.payload)
                try:
                    await session.publish(record.topic, framed)
                except BrokerUnavailable:
                    if self._draining:
                        return
                    await asyncio.sleep(0.05)
                    continue
                except asyncio.CancelledError:
                    raise
                self.broker_forwards += 1
            cursor = record.next_offset
            pending += 1
            if pending >= 32:
                _write_cursor(cursor_path, cursor)
                pending = 0

    async def _wait_for_append(self) -> None:
        seen = self._append_gen
        if self._draining:
            return
        self._appended.clear()
        if self._append_gen != seen or self._draining:
            return
        await self._appended.wait()

    async def _materialize(self, offset: int) -> Envelope:
        cached = self._cache.get(offset)
        if cached is not None:
            return cached
        return await asyncio.to_thread(self._ledger.read, offset)


def _ok(future: asyncio.Future[object], value: object) -> None:
    if not future.done():
        future.set_result(value)


def _fail(future: asyncio.Future[object], exc: BaseException) -> None:
    if not future.done():
        future.set_exception(exc)


def _reject(command: _Command) -> None:
    match command:
        case _Shutdown(future=future):
            _ok(future, None)
        case _Fill():
            return
        case _Publish() | _Subscribe() | _Unsubscribe() | _Ack() | _SetLimit():
            _fail_command(command, BusClosed())
        case _ as other:
            unreachable(other)


def _fail_command(command: _Command, exc: BaseException) -> None:
    match command:
        case (
            _Publish(future=future)
            | _Subscribe(future=future)
            | _Unsubscribe(future=future)
            | _Ack(future=future)
            | _SetLimit(future=future)
            | _Shutdown(future=future)
        ):
            _fail(future, exc)
        case _Fill():
            return
        case _ as other:
            unreachable(other)


def _is_finished(command: _Command) -> bool:
    match command:
        case _Fill():
            return True
        case (
            _Publish(future=future)
            | _Subscribe(future=future)
            | _Unsubscribe(future=future)
            | _Ack(future=future)
            | _SetLimit(future=future)
            | _Shutdown(future=future)
        ):
            return future.done()
        case _ as other:
            unreachable(other)


def _as_bytes(payload: bytes | bytearray | memoryview) -> bytes:
    if isinstance(payload, str):
        raise TypeError("encode str payloads to bytes before publish")
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray | memoryview):
        return bytes(payload)
    raise TypeError("payload must be bytes")


def _new_message_id() -> bytes:
    millis = time.time_ns() // 1_000_000
    return millis.to_bytes(6, "big") + os.urandom(10)


def _load_instance_id(directory: Path) -> bytes:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "instance-id"
    if path.exists():
        data = path.read_bytes()
        if len(data) == 16:
            return data
    data = os.urandom(16)
    path.write_bytes(data)
    return data


def _read_cursor(path: Path) -> int:
    stored = _read_cursor_optional(path)
    return 0 if stored is None else stored


def _read_cursor_optional(path: Path) -> int | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    return int(text)


def _write_cursor(path: Path, offset: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f"{path.name}.tmp"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(str(offset))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
