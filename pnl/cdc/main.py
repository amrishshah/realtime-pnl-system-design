"""Entrypoint that runs every CDC consumer as one process."""
from __future__ import annotations

import asyncio
import logging

from pnl.cache.redis_client import get_redis
from pnl.cdc.consumer import CDCConsumer
from pnl.cdc.symbol_index_projector import SymbolIndexProjector
from pnl.config import settings

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    redis_client = get_redis()
    symbol_index_projector = SymbolIndexProjector(redis_client)

    trades_consumer = CDCConsumer(
        topic="pnl.pnl.trades",
        group_id="pnl-symbol-index-projector",
        handler=symbol_index_projector.handle,
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    consumers = [trades_consumer]
    for consumer in consumers:
        await consumer.start()
    try:
        await asyncio.gather(*(consumer.run_forever() for consumer in consumers))
    finally:
        for consumer in consumers:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
