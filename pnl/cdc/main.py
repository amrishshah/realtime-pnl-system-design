"""Entrypoint that runs every CDC consumer as one process."""
from __future__ import annotations

import asyncio
import logging

from pnl.cache.bloom import symbols_bloom
from pnl.cache.reconnect_cache import ReconnectCache
from pnl.cache.redis_client import get_redis
from pnl.cdc.consumer import CDCConsumer
from pnl.cdc.position_cache_projector import PositionCacheProjector
from pnl.cdc.symbol_index_projector import SymbolIndexProjector, index_holder_counts
from pnl.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pnl.cdc")

POSITIONS_TOPIC = "pnl.pnl.positions"


async def main() -> None:
    redis_client = get_redis()
    bloom = symbols_bloom(redis_client)

    # The counting filter must agree with the index it gates. On first start
    # (or after its key was lost) seed it from the index before consuming,
    # while nothing else is changing either.
    if not await redis_client.exists(bloom.key):
        counts = await index_holder_counts(redis_client)
        await bloom.rebuild(counts)
        logger.info("seeded counting bloom filter from %d indexed symbols", len(counts))

    symbol_index_projector = SymbolIndexProjector(
        redis_client, bloom, reconnect_cache=ReconnectCache(redis_client)
    )
    position_cache_projector = PositionCacheProjector(redis_client)

    # Both projectors read the positions stream; each has its own group, so
    # each sees every event and tracks its own offsets.
    consumers = [
        CDCConsumer(
            topic=POSITIONS_TOPIC,
            group_id="pnl-symbol-index-projector",
            handler=symbol_index_projector.handle,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        ),
        CDCConsumer(
            topic=POSITIONS_TOPIC,
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
