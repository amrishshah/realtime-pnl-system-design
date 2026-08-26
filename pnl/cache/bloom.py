"""
STAGE 3 EXERCISE — implement BloomFilter.

Design doc §3: read-through lookups (Redis, then MySQL on miss) for every
incoming tick's symbol -- even symbols nobody holds -- would hammer MySQL at
tick-rate volume. Gate lookups behind a Bloom filter: "is this symbol held
by anyone at all?" answered in O(1), with no false negatives (a Bloom filter
may say "maybe held" for something nobody holds -- a false positive, costing
one wasted lookup -- but it will never say "not held" for something that
*is* held).

Implementation notes:
- Back the bit array with Redis SETBIT/GETBIT under one key (e.g.
  "bloom:symbols") so the filter is shared across all app instances/workers.
- Use `k` independent hash functions: `mmh3.hash(value, seed=i) % size_bits`
  for i in range(k) is a standard, simple way to get k "independent enough"
  hashes from one fast hash function with different seeds.
- `add(symbol)`: SETBIT to 1 at each of the k computed positions.
- `might_contain(symbol)`: GETBIT at each of the k positions; return True
  only if ALL of them are 1. Pipeline the GETBITs into one round trip.

Suggested sizing for the exercise: size_bits=1_000_000, num_hashes=7
(~1% false-positive rate at a few thousand distinct symbols).

(Stage 11 adds `CountingBloomFilter` below, which supports deletion —
`BloomFilter` itself never needs to change.)
"""
from __future__ import annotations

import redis.asyncio as redis


class BloomFilter:
    def __init__(
        self, redis_client: redis.Redis, key: str, size_bits: int, num_hashes: int
    ) -> None:
        raise NotImplementedError("Stage 3: implement BloomFilter.__init__")

    def _bit_positions(self, value: str) -> list[int]:
        raise NotImplementedError("Stage 3: implement BloomFilter._bit_positions")

    async def add(self, value: str) -> None:
        raise NotImplementedError("Stage 3: implement BloomFilter.add")

    async def might_contain(self, value: str) -> bool:
        raise NotImplementedError("Stage 3: implement BloomFilter.might_contain")


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

    Reuses `_bit_positions` from `BloomFilter` — same hashing, different
    storage (a Redis HASH of per-slot counters instead of a bit array).

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
