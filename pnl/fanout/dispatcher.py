"""
STAGE 4 EXERCISE — implement FanoutDispatcher.

Design doc §4: a single tick can affect thousands of clients. Processing
them as one task that loops over all affected client_ids means one slow
operation (a hung push, a slow Redis call) blocks everyone queued behind it
-- unbounded blast radius from a single misbehaving client.

The Python analogue of "worker pool + buffered channel" for this kind of
I/O-bound fan-out is: one `asyncio.Task` per affected client, bounded by a
shared `asyncio.Semaphore`. Tasks are independent -- a slow one only delays
itself, never the others -- while the semaphore caps how many run at once so
one huge tick (50,000 affected clients) doesn't fire 50,000 simultaneous
Redis connections/pushes.

This also carries the Bloom filter gate from §3: "is this symbol held by
anyone at all?" must be checked *before* the index lookup (`SMEMBERS`), not
per client afterward -- there's no point looking up who holds a symbol you
already know nobody holds.

What to implement:
- `__init__(redis_client, position_cache, bloom, push_callback,
  max_concurrency=500)`: store everything, create
  `asyncio.Semaphore(max_concurrency)`.
- `handle_tick(tick)`:
  1. `if not await self.bloom.might_contain(tick.symbol): return` — the
     gate, checked once per tick, before anything else.
  2. `SMEMBERS symbol_index:{tick.symbol}` to find affected client_ids.
  3. `asyncio.create_task(self._process_client(tick, client_id))` for each
     one. Do NOT `await` them in the loop before moving to the next — that
     would serialize them and defeat the point. You don't need to await the
     created tasks at all here; fire-and-forget is correct per §9 — a
     dropped/slow task self-heals on the next tick.
- `_process_client(tick, client_id)`: `async with self.semaphore:` guarding
  the body. Inside: `await self.position_cache.get(client_id, tick.symbol)`
  (return early if `None` — position closed/never existed); compute PnL via
  `compute_pnl`; `await self.push_callback(client_id, tick.symbol, pnl,
  tick.price, position.quantity)` (see the Stage 10 note below for why the
  callback takes price/quantity too).

STAGE 9 ADDITION — observability for the "crash mid-fan-out is fine"
property (§9). Add three counters as instance attributes, initialized to 0
in `__init__`: `tasks_started`, `tasks_completed`, `tasks_failed`. Also keep
a `set[asyncio.Task]` of in-flight tasks (`self._in_flight_tasks`) so a
chaos test can reach in and cancel a random subset mid-tick — add each
task to the set when created, and remove it via
`task.add_done_callback(self._in_flight_tasks.discard)`.
- Increment `tasks_started` once per task spawned in `handle_tick`.
- In `_process_client`, wrap the body (after acquiring the semaphore) so
  that a clean finish increments `tasks_completed`, and any exception
  (including `asyncio.CancelledError` from a chaos-injected cancellation)
  increments `tasks_failed` — log it, don't let it propagate as an
  unhandled task exception, and don't re-raise `CancelledError` either
  (swallow it here; that's the whole point of this stage — a cancelled or
  failed per-client task should not need any special recovery, since the
  next tick recomputes everything from scratch anyway).

STAGE 10 ADDITION — `push_callback`'s signature widens from
`(client_id, symbol, pnl)` to `(client_id, symbol, pnl, price, quantity)`.
Design doc §10's reconnect cache stores `{"pnl": ..., "price": ...,
"qty": ...}` per symbol, and `price`/`quantity` are already sitting right
here in `_process_client` (from the tick and the position lookup) — passing
them through avoids a second lookup in the push implementation.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from decimal import Decimal

import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter
from pnl.cache.position_cache import PositionCache
from pnl.common.events import Tick

PushCallback = Callable[[str, str, Decimal, Decimal, Decimal], Awaitable[None]]


class FanoutDispatcher:
    def __init__(
        self,
        redis_client: redis.Redis,
        position_cache: PositionCache,
        bloom: BloomFilter,
        push_callback: PushCallback,
        max_concurrency: int = 500,
    ) -> None:
        raise NotImplementedError("Stage 4: implement FanoutDispatcher.__init__")

    async def handle_tick(self, tick: Tick) -> None:
        raise NotImplementedError("Stage 4: implement FanoutDispatcher.handle_tick")

    async def _process_client(self, tick: Tick, client_id: str) -> None:
        raise NotImplementedError("Stage 4: implement FanoutDispatcher._process_client")
