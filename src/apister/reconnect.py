"""Reconnect supervisor for broker adapters."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress

from apister.brokers.base import BrokerAdapter, Incoming
from apister.types import BrokerUnavailable, ReconnectPolicy

logger = logging.getLogger("apister.reconnect")

_TRANSIENT = (ConnectionError, OSError, EOFError, asyncio.IncompleteReadError, TimeoutError)


class ReconnectingBroker:
    """Connect, subscribe, and retry with exponential backoff and jitter."""

    def __init__(
        self,
        adapter: BrokerAdapter,
        policy: ReconnectPolicy,
        patterns: Sequence[str],
        on_message: Callable[[Incoming], Awaitable[None]],
    ) -> None:
        self._adapter = adapter
        self._policy = policy
        self._patterns = tuple(patterns)
        self._on_message = on_message
        self._ready = False
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._rng = random.Random()
        self._reader: asyncio.Task[None] | None = None
        self.connects = 0
        self.reconnects = 0

    @property
    def name(self) -> str:
        return self._adapter.name

    async def start(self) -> None:
        self._reader = asyncio.create_task(self._read_loop(), name=f"apister-{self.name}-read")

    async def publish(self, topic: str, payload: bytes) -> None:
        attempt = 0
        delay = self._policy.initial_delay_s
        while not self._stop.is_set():
            try:
                await self._ensure()
                await self._adapter.publish(topic, payload)
                return
            except asyncio.CancelledError:
                raise
            except _TRANSIENT as exc:
                logger.warning("%s publish failed: %s", self.name, exc)
                await self._drop()
                if self._give_up(attempt):
                    raise BrokerUnavailable(self.name) from exc
                await self._pause(delay)
                attempt += 1
                delay = self._grow(delay)
        raise BrokerUnavailable(self.name)

    async def stop(self) -> None:
        self._stop.set()
        if self._reader is not None:
            self._reader.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader
        await self._drop()

    async def _read_loop(self) -> None:
        attempt = 0
        delay = self._policy.initial_delay_s
        while not self._stop.is_set():
            try:
                await self._ensure()
                incoming = await self._adapter.pull()
                attempt = 0
                delay = self._policy.initial_delay_s
                await self._on_message(incoming)
            except asyncio.CancelledError:
                raise
            except _TRANSIENT as exc:
                if self._stop.is_set():
                    break
                logger.warning("%s read failed: %s", self.name, exc)
                await self._drop()
                if self._give_up(attempt):
                    logger.error("%s gave up after %s failures", self.name, attempt + 1)
                    return
                await self._pause(delay)
                attempt += 1
                delay = self._grow(delay)

    async def _ensure(self) -> None:
        async with self._lock:
            if self._ready:
                return
            await self._adapter.connect()
            await self._adapter.subscribe(self._patterns)
            self._ready = True
            self.connects += 1
            if self.connects > 1:
                self.reconnects += 1
                logger.info("%s reconnected (attempt %s)", self.name, self.connects)

    async def _drop(self) -> None:
        async with self._lock:
            self._ready = False
            with suppress(Exception):
                await self._adapter.close()

    def _give_up(self, attempt: int) -> bool:
        limit = self._policy.max_attempts
        return limit is not None and attempt + 1 >= limit

    def _grow(self, delay: float) -> float:
        return min(delay * self._policy.multiplier, self._policy.max_delay_s)

    async def _pause(self, delay: float) -> None:
        spread = delay * self._policy.jitter_ratio
        wait = delay if spread == 0 else max(0.0, delay + self._rng.uniform(-spread, spread))
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=wait)
