"""
STAGE 3 EXERCISE — implement PositionCache.get().

Design doc §3: buy price and quantity come from Redis via read-through — on
a cache miss, fall back to MySQL and populate Redis so the next lookup is
fast. Current price is NOT this cache's job (it's free, already in the tick
payload, handled entirely in the fan-out path from Stage 4 onward).

What to implement:
1. `HGETALL position:{client_id}:{symbol}` on Redis. If both `quantity` and
   `avg_buy_price` fields are present, build and return a PositionSnapshot
   from them — cache hit, no MySQL involved.
2. On a miss, `SELECT quantity, avg_buy_price FROM positions WHERE
   client_id=%s AND symbol=%s`. If no row, the client doesn't hold this
   position — return None.
3. If a row was found, backfill Redis with `HSET position:{client_id}:{symbol}`
   (so the next call is a cache hit), then return the PositionSnapshot.

This function should only ever be called for a symbol that already passed
the Bloom filter check (that gating happens in the fan-out dispatcher,
Stage 4) — but it doesn't need to know or enforce that; it's a correct,
self-contained read-through cache either way.
"""
from __future__ import annotations

import asyncmy
import redis.asyncio as redis

from pnl.common.events import PositionSnapshot


class PositionCache:
    def __init__(self, redis_client: redis.Redis, mysql_pool: asyncmy.Pool) -> None:
        raise NotImplementedError("Stage 3: implement PositionCache.__init__")

    async def get(self, client_id: str, symbol: str) -> PositionSnapshot | None:
        raise NotImplementedError("Stage 3: implement PositionCache.get")
