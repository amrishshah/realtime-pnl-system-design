"""
Projects `trades` change events into the `symbol -> {client_ids}` index
(design doc §2).

CDC delivery is at-least-once, not exactly-once: the same trade event can
arrive twice. The index is a Redis SET, so `SADD` makes redelivery a free
no-op — no "have I seen this event" bookkeeping needed.

Key: `symbol_index:{symbol}` -> SET of client_ids.
"""
from __future__ import annotations

import logging

import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter

logger = logging.getLogger(__name__)


def index_key(symbol: str) -> str:
    return f"symbol_index:{symbol}"


class SymbolIndexProjector:
    def __init__(self, redis_client: redis.Redis, bloom: BloomFilter | None = None) -> None:
        self.redis = redis_client
        self.bloom = bloom

    async def handle(self, event: dict) -> None:
        after = event.get("after")
        if not after:
            # `trades` is append-only; a delete (or tombstone) has nothing to add.
            logger.debug("skipping trades event with no after-image: op=%s", event.get("op"))
            return

        client_id = after["client_id"]
        symbol = after["symbol"]
        await self.redis.sadd(index_key(symbol), client_id)
