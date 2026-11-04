"""
Mirrors `positions` change events into the Redis position cache
(design doc §3, §7).

This projector is a pure, stateless propagator: it never computes a
weighted average or any other business logic. It copies whatever MySQL
already decided — the trade service owns cost-basis math (Stage 7). Keep it
that way: a static test guards against arithmetic creeping in here.
"""
from __future__ import annotations

import logging

import redis.asyncio as redis

from pnl.cache.bloom import CountingBloomFilter
from pnl.cache.position_cache import position_key

logger = logging.getLogger(__name__)


class PositionCacheProjector:
    def __init__(
        self, redis_client: redis.Redis, bloom: CountingBloomFilter | None = None
    ) -> None:
        self.redis = redis_client
        self.bloom = bloom

    async def handle(self, event: dict) -> None:
        after = event.get("after")
        if not after:
            before = event.get("before")
            if before:
                # A deleted position row: drop the mirrored cache entry too.
                await self.redis.delete(position_key(before["client_id"], before["symbol"]))
            return

        await self.redis.hset(
            position_key(after["client_id"], after["symbol"]),
            mapping={
                "quantity": after["quantity"],
                "avg_buy_price": after["avg_buy_price"],
            },
        )
