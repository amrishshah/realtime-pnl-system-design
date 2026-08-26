"""
STAGE 5 EXERCISE — implement SymbolCoalescer.

Design doc §5: if ticks arrive faster than the fan-out pool can drain them
(e.g., every 100ms, each spawning thousands of tasks), an unbounded queue
backs up faster than it drains — memory blowup, growing latency, eventual
collapse.

The fix is to coalesce, not queue: keep only the *latest* price per symbol,
overwriting stale intermediate values, so the dispatcher always works with
the freshest tick instead of grinding through a stale backlog. The asyncio
analogue of a "buffered channel of size 1" is: a `dict[symbol -> latest
tick]` plus one `asyncio.Event` per symbol, drained by one long-lived
consumer task per symbol. A new `submit()` call for a symbol overwrites the
dict entry and sets the event; the consumer task wakes, clears the event,
and processes whatever is *currently* in the dict — which may already be
newer than what woke it up, and that's fine, that's the point.

What to implement:
- `__init__(dispatcher)`: store the dispatcher (anything with an
  `async handle_tick(tick)` method). Initialize `_latest: dict[str, Tick]`,
  `_events: dict[str, asyncio.Event]`, `_consumers: dict[str, asyncio.Task]`.
- `submit(tick)`:
  1. `self._latest[tick.symbol] = tick`.
  2. Get-or-create `self._events[tick.symbol]` (an `asyncio.Event`), then
     `.set()` it.
  3. Get-or-create `self._consumers[tick.symbol]`: if there's no
     consumer task registered for this symbol yet, spawn one via
     `asyncio.create_task(self._consume(tick.symbol))` and store it.
- `_consume(symbol)`: loop forever — `await event.wait()`, `event.clear()`,
  then `await self.dispatcher.handle_tick(self._latest[symbol])`. Because
  `_latest[symbol]` is read fresh each iteration (not captured when the
  task was scheduled), any ticks that arrived while the previous
  `handle_tick` call was still running get coalesced away automatically —
  you never process a backlog, just whatever's newest right now.

STAGE 12 ADDITION — event-time comparison and watermarking (§12). Network
jitter or multi-source feeds can deliver a tick generated *later* upstream
(event-time) before one generated *earlier*. `submit()` must compare
event-time, not arrival order:

    if incoming.event_time <= stored.event_time: don't overwrite

This alone fixes simple reordering: a stale tick arriving late just gets
ignored instead of regressing the coalesced value backward.

The harder case is a genuinely late straggler — one that arrives long after
much newer ticks were already processed. Waiting indefinitely isn't viable,
so a **watermark** bounds it: "accept late data up to N seconds behind the
newest event-time seen for this symbol, then give up on it." Since
`self._latest[symbol].event_time` IS the newest event-time ever accepted
for that symbol (by the invariant above — you only overwrite on strictly
newer), you don't need a second tracking dict: when a tick fails the
"newer than stored" check, classify *why* using
`(stored.event_time - incoming.event_time).total_seconds()`:
  - `<= watermark_lateness_seconds`: a nearby, harmless reordering —
    count it in `self.dropped_out_of_order`.
  - `> watermark_lateness_seconds`: a genuine straggler — count it in
    `self.dropped_late`.
Both cases still result in the tick being dropped for the live path — the
distinction is for observability, not different handling. (§12 also notes
the ClickHouse raw-tick path must NEVER apply this logic — every tick lands
there regardless of lateness, ordered by event-time in the table itself.
That's `pnl/history/clickhouse_sink.py`, already built in Stage 6 and
untouched by this stage.)

Add two counters in `__init__`: `self.dropped_out_of_order = 0`,
`self.dropped_late = 0`. Add a `watermark_lateness_seconds: float = 2.0`
constructor parameter.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from pnl.common.events import Tick

DispatcherLike = Callable[[Tick], Awaitable[None]]


class SymbolCoalescer:
    def __init__(self, dispatcher, watermark_lateness_seconds: float = 2.0) -> None:
        raise NotImplementedError("Stage 5: implement SymbolCoalescer.__init__")

    async def submit(self, tick: Tick) -> None:
        raise NotImplementedError("Stage 5: implement SymbolCoalescer.submit")

    async def _consume(self, symbol: str) -> None:
        raise NotImplementedError("Stage 5: implement SymbolCoalescer._consume")
