"""
Read-through cache for a client's position (design doc §3).

Quantity and average buy price come from Redis (`position:{client}:{symbol}`,
kept fresh by the CDC position projector). On a miss, fall back to MySQL —
the source of truth — and backfill Redis so the next lookup is a hit.
Current price is not this cache's concern: it's already in the tick.

Callers only reach this for symbols that passed the Bloom filter gate, but
the cache is correct on its own either way.
"""
from __future__ import annotations

from decimal import Decimal

import asyncmy
import redis.asyncio as redis

from pnl.common.events import PositionSnapshot

# Backfill only if the key is still absent. If the CDC projector wrote a
# value between our MySQL read and this write, that value is at least as new
# as ours — never overwrite it with what may be a staler read.
_BACKFILL_IF_ABSENT = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1], 'quantity', ARGV[1], 'avg_buy_price', ARGV[2])
    return 1
end
return 0
"""


def position_key(client_id: str, symbol: str) -> str:
    return f"position:{client_id}:{symbol}"


class PositionCache:
    def __init__(self, redis_client: redis.Redis, mysql_pool: asyncmy.Pool) -> None:
        self.redis = redis_client
        self.pool = mysql_pool
        self._backfill = redis_client.register_script(_BACKFILL_IF_ABSENT)

    async def get(self, client_id: str, symbol: str) -> PositionSnapshot | None:
        key = position_key(client_id, symbol)
        cached = await self.redis.hgetall(key)
        if "quantity" in cached and "avg_buy_price" in cached:
            return PositionSnapshot(
                client_id=client_id,
                symbol=symbol,
                quantity=Decimal(cached["quantity"]),
                avg_buy_price=Decimal(cached["avg_buy_price"]),
            )

        row = await self._load_from_mysql(client_id, symbol)
        if row is None:
            return None

        quantity, avg_buy_price = Decimal(row[0]), Decimal(row[1])
        await self._backfill(keys=[key], args=[str(quantity), str(avg_buy_price)])
        return PositionSnapshot(
            client_id=client_id, symbol=symbol, quantity=quantity, avg_buy_price=avg_buy_price
        )

    async def _load_from_mysql(self, client_id: str, symbol: str):
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT quantity, avg_buy_price FROM positions "
                        "WHERE client_id = %s AND symbol = %s",
                        (client_id, symbol),
                    )
                    return await cur.fetchone()
            finally:
                # The pool runs with autocommit off, so this SELECT opened a
                # REPEATABLE READ snapshot. End it before the connection goes
                # back to the pool, or later reads on it would see stale data.
                await conn.rollback()
