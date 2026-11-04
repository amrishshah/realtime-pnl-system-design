"""
Bloom filters gating per-tick lookups (design doc §3, §11).

"Is this symbol held by anyone at all?" answered in O(1), with no false
negatives: a Bloom filter may say "maybe held" for a symbol nobody holds (a
false positive, costing one wasted lookup) but never "not held" for one
that is. Only symbols that pass proceed to the index lookup and Redis
read-through, so ticks for unheld symbols never touch Redis/MySQL.

The filter lives in Redis so every app instance and the CDC worker share it.
"""
from __future__ import annotations

import mmh3
import redis.asyncio as redis


class BloomFilter:
    """Standard Bloom filter over a Redis bit array (SETBIT/GETBIT).

    k bit positions per value come from `mmh3` seeded 0..k-1. Sizing: at
    size_bits=1_000_000 and num_hashes=7, a few thousand distinct symbols
    give a false-positive rate well under 1%.
    """

    def __init__(
        self, redis_client: redis.Redis, key: str, size_bits: int, num_hashes: int
    ) -> None:
        if size_bits <= 0 or num_hashes <= 0:
            raise ValueError("size_bits and num_hashes must be positive")
        self.redis = redis_client
        self.key = key
        self.size_bits = size_bits
        self.num_hashes = num_hashes

    def _bit_positions(self, value: str) -> list[int]:
        return [
            mmh3.hash(value, seed=i, signed=False) % self.size_bits
            for i in range(self.num_hashes)
        ]

    async def add(self, value: str) -> None:
        pipe = self.redis.pipeline(transaction=False)
        for pos in self._bit_positions(value):
            pipe.setbit(self.key, pos, 1)
        await pipe.execute()

    async def might_contain(self, value: str) -> bool:
        pipe = self.redis.pipeline(transaction=False)
        for pos in self._bit_positions(value):
            pipe.getbit(self.key, pos)
        return all(await pipe.execute())


class CountingBloomFilter(BloomFilter):
    """
    STAGE 11 EXERCISE — implement CountingBloomFilter.

    Design doc §11: a standard Bloom filter has no deletion — it's just a
    bit array, set to 1 on insert. If a client fully exits a position and
    you "delete" by clearing bits, any *other* symbol that happens to hash
    to the same bit positions (a collision) gets silently unset too — its
    ticks would stop triggering fan-out with no visible failure.

    The fix: each slot is a small counter instead of a single bit. Insert
    increments the relevant counters; delete decrements them. A collision
    means a counter goes from 2 to 1 rather than being cleared to 0, so it
    correctly stays "positive" for whichever symbol still needs it. Only
    when a counter reaches 0 does that slot genuinely mean "nothing hashes
    here anymore."

    Correctness depends on `add`/`remove` being called exactly once per
    logical (client, symbol) membership joining/leaving — NOT once per
    trade event. If the same client buys the same symbol twice, that's
    still one membership; calling `add()` a second time would inflate the
    counters and they'd never fully return to 0 on exit. See the updated
    `pnl/cdc/symbol_index_projector.py` docstring for how to guard this
    using Redis SADD's return value (it tells you whether the member was
    actually new).

    This class inherits `_bit_positions` from `BloomFilter` for hashing, but
    MUST override `add` and `might_contain` below — the inherited
    SETBIT/GETBIT versions operate on a bit array, which is the wrong
    storage for a counter-per-slot structure. Re-implement both here.

    What to implement:
    - `add(value)`: `HINCRBY <key> <slot> 1` for each of the k slots.
    - `might_contain(value)`: `HMGET` the k slots; True only if every one
      has a count > 0.
    - `remove(value)`: `HINCRBY <key> <slot> -1` for each of the k slots.
    - `rebuild_from_positions(pool)`: the EOD safety-net checkpoint (used
      again in Stage 13). Clear this filter's Redis key entirely, then
      `SELECT DISTINCT symbol FROM positions WHERE quantity > 0` and
      `add()` each one — rebuilding truth from MySQL rather than trusting
      accumulated increment/decrement history forever.
    """

    async def add(self, value: str) -> None:
        raise NotImplementedError("Stage 11: implement CountingBloomFilter.add")

    async def might_contain(self, value: str) -> bool:
        raise NotImplementedError("Stage 11: implement CountingBloomFilter.might_contain")

    async def remove(self, value: str) -> None:
        raise NotImplementedError("Stage 11: implement CountingBloomFilter.remove")

    async def rebuild_from_positions(self, mysql_pool) -> None:
        raise NotImplementedError(
            "Stage 11: implement CountingBloomFilter.rebuild_from_positions"
        )
