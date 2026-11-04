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

import logging
import uuid

import mmh3
import redis.asyncio as redis

from pnl.config import settings

logger = logging.getLogger(__name__)


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

    async def join(self, set_key: str, member: str, value: str) -> bool:
        """Add `member` to the index SET `set_key` and record `value` here.
        Returns whether `member` was new."""
        added = await self.redis.sadd(set_key, member)
        await self.add(value)  # idempotent for a plain bit array
        return bool(added)

    async def leave(self, set_key: str, member: str, value: str) -> bool:
        """Remove `member` from the index SET. A plain Bloom filter can't
        forget `value` safely (§11), so its bits stay set: at worst a false
        positive. Returns whether `member` was present."""
        return bool(await self.redis.srem(set_key, member))


# KEYS[1] = index SET, KEYS[2] = counter HASH; ARGV[1] = member, ARGV[2..] = slots.
# The SET change and the counter change happen in one atomic step, and the
# counters move only if membership actually changed. That keeps the invariant
# "each slot counts the index members that hash to it" exact under crashes
# and at-least-once redelivery (a redelivered join is a no-op, not a second
# increment). Single-node Redis; on Redis Cluster both keys would need a
# shared hash tag.
_JOIN = """
if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then
    return 0
end
for i = 2, #ARGV do
    redis.call('HINCRBY', KEYS[2], ARGV[i], 1)
end
return 1
"""

_LEAVE = """
if redis.call('SREM', KEYS[1], ARGV[1]) == 0 then
    return {0, 0}
end
local underflow = 0
for i = 2, #ARGV do
    local n = tonumber(redis.call('HGET', KEYS[2], ARGV[i]) or '0')
    if n <= 0 then
        underflow = underflow + 1
    elseif n == 1 then
        redis.call('HDEL', KEYS[2], ARGV[i])
    else
        redis.call('HINCRBY', KEYS[2], ARGV[i], -1)
    end
end
return {1, underflow}
"""

# ARGV = slots. Decrement without going below zero; report underflows.
_REMOVE = """
local underflow = 0
for i = 1, #ARGV do
    local n = tonumber(redis.call('HGET', KEYS[1], ARGV[i]) or '0')
    if n <= 0 then
        underflow = underflow + 1
    elseif n == 1 then
        redis.call('HDEL', KEYS[1], ARGV[i])
    else
        redis.call('HINCRBY', KEYS[1], ARGV[i], -1)
    end
end
return underflow
"""


class CountingBloomFilter(BloomFilter):
    """Bloom filter that supports deletion (design doc §11).

    A plain Bloom filter can't delete: clearing the bits of a symbol whose
    last holder exited also clears them for any other symbol that collides
    on those bits, and that symbol's ticks silently stop fanning out. Here
    each slot is a counter in a Redis HASH: insert increments, delete
    decrements, so a shared slot goes 2 -> 1 instead of being cleared, and
    only a count of 0 means "nothing hashes here anymore". Empty slots are
    deleted, keeping the hash proportional to what's held.

    Counts are per *membership* — one per (client, symbol) holding — not per
    symbol, so a symbol stays positive until its last holder exits. `join` /
    `leave` change the symbol index and these counters atomically, and only
    when membership actually changes; that's what keeps the counts exact.

    `rebuild*` replace the counters wholesale. They build into a temporary
    key and RENAME it into place, so readers never see a half-built (empty)
    filter. Run them while CDC projection is paused (startup, or EOD with
    trading closed): a membership change applied during a rebuild could be
    lost when the new counters replace the old ones.
    """

    def __init__(
        self, redis_client: redis.Redis, key: str, size_bits: int, num_hashes: int
    ) -> None:
        super().__init__(redis_client, key, size_bits, num_hashes)
        self._join = redis_client.register_script(_JOIN)
        self._leave = redis_client.register_script(_LEAVE)
        self._remove = redis_client.register_script(_REMOVE)

    async def add(self, value: str) -> None:
        pipe = self.redis.pipeline(transaction=True)
        for slot in self._bit_positions(value):
            pipe.hincrby(self.key, slot, 1)
        await pipe.execute()

    async def might_contain(self, value: str) -> bool:
        counts = await self.redis.hmget(self.key, self._bit_positions(value))
        return all(c is not None and int(c) > 0 for c in counts)

    async def remove(self, value: str) -> None:
        underflow = await self._remove(keys=[self.key], args=self._bit_positions(value))
        self._warn_on_underflow(underflow, value)

    async def join(self, set_key: str, member: str, value: str) -> bool:
        joined = await self._join(
            keys=[set_key, self.key], args=[member, *self._bit_positions(value)]
        )
        return bool(joined)

    async def leave(self, set_key: str, member: str, value: str) -> bool:
        left, underflow = await self._leave(
            keys=[set_key, self.key], args=[member, *self._bit_positions(value)]
        )
        self._warn_on_underflow(underflow, value)
        return bool(left)

    async def rebuild(self, holder_counts: dict[str, int]) -> None:
        """Replace all counters with `holder_counts` ({symbol: number of holders})."""
        totals: dict[int, int] = {}
        for value, holders in holder_counts.items():
            if holders <= 0:
                continue
            for slot in self._bit_positions(value):
                totals[slot] = totals.get(slot, 0) + holders

        if not totals:
            await self.redis.delete(self.key)
            return
        staging = f"{self.key}:rebuild:{uuid.uuid4().hex}"
        await self.redis.hset(staging, mapping=totals)
        await self.redis.rename(staging, self.key)

    async def rebuild_from_positions(self, mysql_pool) -> None:
        """EOD safety net: rebuild from MySQL, the source of truth (§11, §13)."""
        async with mysql_pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT symbol, COUNT(*) FROM positions WHERE quantity > 0 GROUP BY symbol"
                    )
                    rows = await cur.fetchall()
            finally:
                await conn.rollback()  # end the read snapshot before pooling
        await self.rebuild({symbol: int(count) for symbol, count in rows})

    def _warn_on_underflow(self, underflow: int, value: str) -> None:
        if underflow:
            # A decrement with nothing to decrement means the counters
            # drifted from the index; the EOD rebuild corrects it.
            logger.warning(
                "counting bloom underflow on %d slot(s) for %r — counters out of sync", underflow, value
            )


def symbols_bloom(redis_client: redis.Redis) -> CountingBloomFilter:
    """The shared "is this symbol held by anyone?" filter."""
    return CountingBloomFilter(
        redis_client, settings.bloom_key, settings.bloom_size_bits, settings.bloom_num_hashes
    )
