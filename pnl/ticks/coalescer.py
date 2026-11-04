"""
Per-symbol tick coalescing (design doc §5).

If ticks arrive faster than the fan-out can drain them, queueing every tick
backs up faster than it drains — memory blowup, growing latency, collapse.
Instead keep only the *latest* tick per symbol and overwrite stale ones, so
the dispatcher always works with the freshest price.

asyncio shape of "a buffered channel of size 1": `_latest[symbol]` holds
the newest tick, an `asyncio.Event` per symbol signals "something new", and
one long-lived consumer task per symbol drains it. The consumer re-reads
`_latest[symbol]` each time it wakes, so anything that arrived while it was
busy is coalesced away for free.

After handing a tick to the dispatcher, the consumer waits (bounded by
`max_drain_wait_seconds`) for that tick's fan-out to finish before taking
the next value. Without this, `handle_tick` returning right after spawning
tasks would let every tick add another full round of waiting per-client
tasks — the queue would just move one layer down.
"""
from __future__ import annotations

import asyncio
import logging

from pnl.common.events import Tick

logger = logging.getLogger(__name__)


class SymbolCoalescer:
    def __init__(
        self,
        dispatcher,
        watermark_lateness_seconds: float = 2.0,
        max_drain_wait_seconds: float = 1.0,
    ) -> None:
        self.dispatcher = dispatcher
        self.watermark_lateness_seconds = watermark_lateness_seconds
        self.max_drain_wait_seconds = max_drain_wait_seconds
        self._latest: dict[str, Tick] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._consumers: dict[str, asyncio.Task] = {}

    async def submit(self, tick: Tick) -> None:
        self._latest[tick.symbol] = tick
        self._events.setdefault(tick.symbol, asyncio.Event()).set()
        self._ensure_consumer(tick.symbol)

    def _ensure_consumer(self, symbol: str) -> None:
        task = self._consumers.get(symbol)
        if task is None or task.done():
            self._consumers[symbol] = asyncio.create_task(self._consume(symbol))

    async def _consume(self, symbol: str) -> None:
        event = self._events[symbol]
        while True:
            await event.wait()
            event.clear()
            tick = self._latest[symbol]
            try:
                await self.dispatcher.handle_tick(tick)
                wait_idle = getattr(self.dispatcher, "wait_idle", None)
                if wait_idle is not None:
                    await wait_idle(symbol, self.max_drain_wait_seconds)
            except Exception:
                # Keep the loop alive: a dead consumer would silently freeze
                # this symbol's live PnL. The next tick tries again.
                logger.exception("coalesced dispatch failed for symbol=%s", symbol)

    async def close(self) -> None:
        for task in self._consumers.values():
            task.cancel()
        await asyncio.gather(*self._consumers.values(), return_exceptions=True)
        self._consumers.clear()
