"""
Projects `positions` change events into the `symbol -> {client_ids}` index
(design doc §2, §3, §11).

Membership follows the position's quantity: `quantity > 0` means the client
holds the symbol; `quantity == 0` (a full sell) or a deleted row means it
doesn't. Each event carries the whole truth for its (client, symbol), so
redelivery is harmless: adding an existing member or removing an absent
one is a no-op.

Why the `positions` topic and not `trades`: entry and exit must come from
the same ordered stream. Kafka orders events within a partition, and
Debezium keys `positions` events by primary key, so one (client, symbol)'s
events stay in order. Feeding entries from `trades` and exits from
`positions` would let a lagging exit (buy, sell-all, buy again) land after
the re-entry and silently drop a client who still holds the position.

The index and the Bloom filter change together through `bloom.join` /
`bloom.leave`, atomically, and only when membership actually changes —
which is what keeps a counting Bloom filter's counters exact.

Key: `symbol_index:{symbol}` -> SET of client_ids.
"""
from __future__ import annotations

import logging
from decimal import Decimal

import redis.asyncio as redis
from asyncmy.cursors import SSCursor

from pnl.cache.bloom import BloomFilter

logger = logging.getLogger(__name__)

INDEX_KEY_PREFIX = "symbol_index:"


def index_key(symbol: str) -> str:
    return f"{INDEX_KEY_PREFIX}{symbol}"


async def index_holder_counts(redis_client: redis.Redis) -> dict[str, int]:
    """{symbol: number of index members}, for rebuilding the Bloom filter
    from the index itself."""
    counts: dict[str, int] = {}
    async for key in redis_client.scan_iter(match=f"{INDEX_KEY_PREFIX}*", count=1000):
        counts[key[len(INDEX_KEY_PREFIX):]] = await redis_client.scard(key)
    return counts


class SymbolIndexProjector:
    def __init__(
        self,
        redis_client: redis.Redis,
        bloom: BloomFilter | None = None,
        reconnect_cache=None,
    ) -> None:
        self.redis = redis_client
        self.bloom = bloom
        self.reconnect_cache = reconnect_cache

    async def handle(self, event: dict) -> None:
        after = event.get("after")
        before = event.get("before")
        if after:
            client_id, symbol = after["client_id"], after["symbol"]
            quantity = Decimal(after["quantity"])
        elif before:
            # The row was deleted: the client no longer holds anything.
            client_id, symbol = before["client_id"], before["symbol"]
            quantity = Decimal(0)
        else:
            logger.debug("skipping event with no row image: op=%s", event.get("op"))
            return

        if quantity > 0:
            await self._join(symbol, client_id)
        elif await self._leave(symbol, client_id) and self.reconnect_cache is not None:
            # Drop the closed position from the client's reconnect snapshot.
            await self.reconnect_cache.forget(client_id, symbol)

    async def _join(self, symbol: str, client_id: str) -> bool:
        if self.bloom is None:
            return bool(await self.redis.sadd(index_key(symbol), client_id))
        return await self.bloom.join(index_key(symbol), client_id, symbol)

    async def _leave(self, symbol: str, client_id: str) -> bool:
        if self.bloom is None:
            return bool(await self.redis.srem(index_key(symbol), client_id))
        return await self.bloom.leave(index_key(symbol), client_id, symbol)


async def rebuild_index_and_bloom(redis_client: redis.Redis, mysql_pool, bloom) -> dict[str, int]:
    """Rebuild every `symbol_index:*` SET and the Bloom filter from MySQL
    positions, the source of truth (EOD safety net, §11/§13).

    Both are rebuilt from the same read, so the counting filter's invariant
    (counters == index membership) holds afterwards. Each symbol's SET is
    built in a temp key and swapped in with RENAME; index keys for symbols
    nobody holds anymore are deleted. Run with CDC projection paused (EOD,
    trading closed): a membership change applied mid-rebuild can be lost.

    Returns {symbol: holder count}.
    """
    counts: dict[str, int] = {}
    staged: dict[str, str] = {}

    async def stage(symbol: str, members: list[str]) -> None:
        # Outside the symbol_index:* namespace, so a crashed rebuild can't
        # leave keys that look like real symbols.
        temp = f"rebuild:{index_key(symbol)}"
        await redis_client.delete(temp)
        for i in range(0, len(members), 1000):
            await redis_client.sadd(temp, *members[i : i + 1000])
        staged[symbol] = temp
        counts[symbol] = len(members)

    async with mysql_pool.acquire() as conn:
        try:
            async with conn.cursor(SSCursor) as cur:
                # Ordered by symbol so one symbol's holders are contiguous:
                # memory stays bounded by the largest single symbol.
                await cur.execute(
                    "SELECT symbol, client_id FROM positions WHERE quantity > 0 ORDER BY symbol"
                )
                current, members = None, []
                while rows := await cur.fetchmany(10_000):
                    for symbol, client_id in rows:
                        if symbol != current:
                            if current is not None:
                                await stage(current, members)
                            current, members = symbol, []
                        members.append(client_id)
                if current is not None:
                    await stage(current, members)
        finally:
            await conn.rollback()

    for symbol, temp in staged.items():
        await redis_client.rename(temp, index_key(symbol))
    async for key in redis_client.scan_iter(match=f"{INDEX_KEY_PREFIX}*", count=1000):
        symbol = key[len(INDEX_KEY_PREFIX):]
        if symbol not in staged:
            await redis_client.delete(key)

    await bloom.rebuild(counts)
    return counts
