"""
Per-client fan-out for one tick (design doc §3, §4).

A tick can affect tens of thousands of clients. Processing them in one task
that loops over every client means one slow operation (a hung push, a slow
Redis call) blocks everyone behind it. Instead each affected client gets its
own `asyncio.Task`, and a shared `asyncio.Semaphore` bounds how many run at
once — the asyncio form of "worker pool + buffered channel". A slow client
only delays itself, and one huge tick can't open 50,000 Redis
connections/pushes at the same moment.

The Bloom filter gate (§3) is checked once per tick, before the index
lookup: there's no point asking who holds a symbol nobody holds.

`handle_tick` returns as soon as the tasks are spawned. `wait_idle(symbol)`
lets the coalescer (§5) wait — boundedly — for a symbol's previous fan-out
to drain before starting the next one, so ticks arriving faster than
clients drain can't pile up an unbounded backlog of waiting tasks. Each
per-client task also has a timeout, so one hung client can't hold its slot
(or its symbol) forever.

Crash mid-fan-out (§9) needs no recovery code: PnL is recomputed from
scratch on every tick, so a client whose task failed or was cancelled is
simply correct again on the next tick. The counters below make that
observable (`GET /metrics`); they don't drive any retry.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from decimal import Decimal

import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter
from pnl.cache.position_cache import PositionCache
from pnl.cdc.symbol_index_projector import index_key
from pnl.common.events import Tick
from pnl.fanout.pnl_calculator import compute_pnl

logger = logging.getLogger(__name__)

# (client_id, symbol, pnl, price, quantity)
PushCallback = Callable[[str, str, Decimal, Decimal, Decimal], Awaitable[None]]

# How many index members to pull per SSCAN round trip.
_SCAN_BATCH = 1000


class FanoutDispatcher:
    def __init__(
        self,
        redis_client: redis.Redis,
        position_cache: PositionCache,
        bloom: BloomFilter,
        push_callback: PushCallback,
        max_concurrency: int = 500,
        client_timeout_seconds: float | None = 5.0,
    ) -> None:
        self.redis = redis_client
        self.position_cache = position_cache
        self.bloom = bloom
        self.push_callback = push_callback
        self.max_concurrency = self._cap_to_redis_pool(redis_client, max_concurrency)
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.client_timeout_seconds = client_timeout_seconds
        # asyncio keeps only weak references to tasks, so a fire-and-forget
        # task with no other reference can be garbage-collected mid-flight.
        # Hold every in-flight task here until it finishes.
        self._in_flight_tasks: set[asyncio.Task] = set()
        self._in_flight_by_symbol: dict[str, set[asyncio.Task]] = {}
        self.tasks_started = 0
        self.tasks_completed = 0
        self.tasks_failed = 0

    @property
    def tasks_in_flight(self) -> int:
        return len(self._in_flight_tasks)

    @staticmethod
    def _cap_to_redis_pool(redis_client: redis.Redis, max_concurrency: int) -> int:
        """Each per-client task holds at most one Redis connection at a time,
        so running more of them than the pool can serve only produces
        connection errors. Keep a reserve for other callers of the same
        client (the index scan, WebSocket snapshots)."""
        pool_max = getattr(redis_client.connection_pool, "max_connections", None)
        if not pool_max:
            return max_concurrency
        usable = max(1, pool_max - max(1, pool_max // 10))
        if max_concurrency > usable:
            logger.warning(
                "fan-out concurrency %d exceeds what the Redis pool (%d connections) can "
                "serve; capping at %d", max_concurrency, pool_max, usable,
            )
            return usable
        return max_concurrency

    async def handle_tick(self, tick: Tick) -> None:
        if not await self.bloom.might_contain(tick.symbol):
            return

        # SSCAN, not SMEMBERS: a 50k-member SMEMBERS is one O(n) command
        # that blocks Redis while it runs.
        async for client_id in self.redis.sscan_iter(index_key(tick.symbol), count=_SCAN_BATCH):
            task = asyncio.create_task(self._process_client(tick, client_id))
            self.tasks_started += 1
            self._track(tick.symbol, task)

    async def wait_idle(self, symbol: str, timeout: float | None) -> bool:
        """Wait until every in-flight task for `symbol` has finished, or
        `timeout` seconds pass. Returns True if the symbol went idle."""
        tasks = self._in_flight_by_symbol.get(symbol)
        if not tasks:
            return True
        _, pending = await asyncio.wait(set(tasks), timeout=timeout)
        return not pending

    def _track(self, symbol: str, task: asyncio.Task) -> None:
        self._in_flight_tasks.add(task)
        per_symbol = self._in_flight_by_symbol.setdefault(symbol, set())
        per_symbol.add(task)

        def _untrack(done: asyncio.Task) -> None:
            self._in_flight_tasks.discard(done)
            remaining = self._in_flight_by_symbol.get(symbol)
            if remaining is not None:
                remaining.discard(done)
                if not remaining:
                    del self._in_flight_by_symbol[symbol]

        task.add_done_callback(_untrack)

    async def _process_client(self, tick: Tick, client_id: str) -> None:
        try:
            async with self.semaphore:
                async with asyncio.timeout(self.client_timeout_seconds):
                    position = await self.position_cache.get(client_id, tick.symbol)
                    if position is not None:
                        pnl = compute_pnl(tick.price, position.avg_buy_price, position.quantity)
                        await self.push_callback(
                            client_id, tick.symbol, pnl, tick.price, position.quantity
                        )
        except asyncio.CancelledError:
            # A crash/cancel mid-fan-out. Count it and let cancellation
            # propagate; the next tick recomputes this client from scratch.
            self.tasks_failed += 1
            raise
        except Exception:
            # Fire-and-forget: nobody awaits this task, so log here or the
            # error vanishes. No retry needed — the next tick recomputes.
            self.tasks_failed += 1
            logger.exception("fan-out failed for client=%s symbol=%s", client_id, tick.symbol)
        else:
            self.tasks_completed += 1
