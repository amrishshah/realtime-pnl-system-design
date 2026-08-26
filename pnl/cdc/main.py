"""Entrypoint that runs every CDC consumer as one process. Provided wiring."""
from __future__ import annotations

import asyncio
import logging

from pnl.cache.bloom import CountingBloomFilter
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
    bloom = CountingBloomFilter(redis_client, BLOOM_KEY, BLOOM_SIZE_BITS, BLOOM_NUM_HASHES)
    symbol_index_projector = SymbolIndexProjector(redis_client, bloom)
    position_cache_projector = PositionCacheProjector(redis_client, bloom)

    trades_consumer = CDCConsumer(
        topic="pnl.pnl.trades",
        group_id="pnl-symbol-index-projector",
        handler=symbol_index_projector.handle,
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )
    positions_consumer = CDCConsumer(
        topic="pnl.pnl.positions",
        group_id="pnl-position-cache-projector",
        handler=position_cache_projector.handle,
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    await trades_consumer.start()
    await positions_consumer.start()
    try:
        await asyncio.gather(
            trades_consumer.run_forever(),
            positions_consumer.run_forever(),
        )
    finally:
        await trades_consumer.stop()
        await positions_consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
