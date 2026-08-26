"""
STAGE 3 EXERCISE — implement PositionCacheProjector.handle().

Design doc §7 (previewed here, fully justified there): this projector must
stay a simple, stateless, math-free propagator forever — it never computes
a weighted average or any other business logic, it just mirrors whatever
MySQL already decided into Redis. That decision-making happens in the trade
service (Stage 7), not here. Keep it that way even as later stages change
what MySQL computes.

For Stage 3 alone: pull `client_id`, `symbol`, `quantity`, `avg_buy_price`
out of the Debezium envelope's `after` (guard against `after` being None).
`HSET position:{client_id}:{symbol}` with those two fields. Nothing else —
no reads, no math, no conditionals on the values. `bloom` can be left
`None` and ignored for Stage 3's tests.

STAGE 11 ADDITION — clean up on a full exit (§11). When an update's `after`
shows `quantity == 0` (a full sell), this client no longer holds the
position:
    await self.redis.srem(f"symbol_index:{symbol}", client_id)
    if self.bloom is not None:
        await self.bloom.remove(symbol)
Call `bloom.remove()` unconditionally on every such event — once per
(client, symbol) exit, matching `SymbolIndexProjector`'s one `bloom.add()`
per (client, symbol) *entry* (guarded by SADD's return value there). This
symmetry is what keeps the counting Bloom filter's per-slot counters
correct: a symbol still held by other clients keeps a positive count from
their still-standing `add()`s even after this one client's `remove()`;
only the last holder's exit actually brings a slot to zero. Don't try to
special-case "is this the last holder" yourself — the counter does that
for you as long as every entry and exit calls `add`/`remove` exactly once.

What to implement:
- `__init__(redis_client, bloom=None)` — store both.
- `handle(event)`: mirror fields as before; if `after["quantity"]` is
  (numerically) `0`, also do the symbol-index/bloom cleanup above.
"""
from __future__ import annotations

import redis.asyncio as redis

from pnl.cache.bloom import CountingBloomFilter


class PositionCacheProjector:
    def __init__(
        self, redis_client: redis.Redis, bloom: CountingBloomFilter | None = None
    ) -> None:
        raise NotImplementedError("Stage 3: implement PositionCacheProjector.__init__")

    async def handle(self, event: dict) -> None:
        raise NotImplementedError("Stage 3: implement PositionCacheProjector.handle")
