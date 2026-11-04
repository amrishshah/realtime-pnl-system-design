"""Entrypoint that runs every CDC consumer as one process."""
from __future__ import annotations

import asyncio
import logging

from pnl.cache.bloom import BloomFilter
from pnl.cache.redis_client import get_redis
from pnl.cdc.consumer import CDCConsumer
from pnl.cdc.position_cache_projector import PositionCacheProjector
from pnl.cdc.symbol_index_projector import SymbolIndexProjector
from pnl.config import settings

logging.basicConfig(level=logging.INFO)

BLOOM_KEY = "bloom:symbols"
BLOOM_SIZE_BITS = 1_000_000
BLOOM_NUM_HASHES = 7


async def main() -> None:
    redis_client = get_redis()
    bloom = BloomFilter(redis_client, BLOOM_KEY, BLOOM_SIZE_BITS, BLOOM_NUM_HASHES)
    symbol_index_projector = SymbolIndexProjector(redis_client, bloom)
    position_cache_projector = PositionCacheProjector(redis_client)

    consumers = [
        CDCConsumer(
            topic="pnl.pnl.trades",
            group_id="pnl-symbol-index-projector",
            handler=symbol_index_projector.handle,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        ),
        CDCConsumer(
            topic="pnl.pnl.positions",
            group_id="pnl-position-cache-projector",
            handler=position_cache_projector.handle,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        ),
    ]
    for consumer in consumers:
        await consumer.start()
    try:
        await asyncio.gather(*(consumer.run_forever() for consumer in consumers))
    finally:
        for consumer in consumers:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
