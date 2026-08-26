"""
STAGE 2 EXERCISE — implement SymbolIndexProjector.handle().

Design doc §2: CDC delivery is at-least-once, not exactly-once. If the same
trade change event is delivered twice, naive "append client_id to a list"
logic creates duplicates. Back the index with a Redis SET instead — SADD is
idempotent by construction, so redelivery is a free no-op with zero dedup
logic of your own.

For Stage 2 alone, `bloom` can be left as `None` and ignored entirely —
`__init__(redis_client)` with no second argument is a valid way to construct
this for Stage 2's tests.

STAGE 3 ADDITION — also feed the Bloom filter (§3). Once you've implemented
`pnl/cache/bloom.py::BloomFilter`, construct this projector with a real
`bloom` instance and, in `handle()`, also call `await bloom.add(symbol)`
every time a client is recorded as holding a symbol — otherwise a
newly-bought symbol would never pass the "is this held by anyone?" check
later in the pipeline.

STAGE 11 REFINEMENT — once `bloom` is a `CountingBloomFilter` (real
per-slot counters, not just bits), calling `bloom.add()` unconditionally on
every trade event is no longer safe: a client buying the same symbol a
second time isn't a new membership, and incrementing the counters again for
it means they'd never fully return to zero when that client eventually
exits (see `CountingBloomFilter`'s docstring). Redis's `SADD` return value
tells you whether the member was actually new — it returns the number of
elements actually added (0 if already a member, 1 if newly added). Use
that to guard the `bloom.add()` call:
    added = await self.redis.sadd(f"symbol_index:{symbol}", client_id)
    if added and self.bloom is not None:
        await self.bloom.add(symbol)
This same guard works fine whether `bloom` is a plain `BloomFilter` (where
it's a harmless optimization — repeat adds were already free) or a
`CountingBloomFilter` (where it's required for correctness).

What to implement:
- `__init__(redis_client, bloom=None)` — store both.
- `handle(event)` — receives one decoded Debezium envelope (see
  `pnl/cdc/consumer.py`'s docstring for its shape) for a row change on
  `trades`. Extract `client_id` and `symbol` from `event["after"]` (guard
  against `after` being None). `SADD symbol_index:{symbol} {client_id}`,
  then conditionally `bloom.add(symbol)` per the guard above.
"""
from __future__ import annotations

import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter


class SymbolIndexProjector:
    def __init__(self, redis_client: redis.Redis, bloom: BloomFilter | None = None) -> None:
        raise NotImplementedError("Stage 2: implement SymbolIndexProjector.__init__")

    async def handle(self, event: dict) -> None:
        raise NotImplementedError("Stage 2: implement SymbolIndexProjector.handle")
