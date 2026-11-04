from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, HTTPException

from pnl.cache.bloom import BloomFilter
from pnl.cache.position_cache import PositionCache
from pnl.cache.redis_client import get_redis
from pnl.config import settings
from pnl.db.mysql import close_pool, get_pool
from pnl.fanout.dispatcher import FanoutDispatcher
from pnl.ticks.coalescer import SymbolCoalescer
from pnl.trade_service.api import router as trade_router
from pnl.ws.server import router as ws_router
from pnl.ws.server import set_tick_handler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("pnl")

BLOOM_KEY = "bloom:symbols"
BLOOM_SIZE_BITS = 1_000_000
BLOOM_NUM_HASHES = 7

_dispatcher = None


async def log_push(
    client_id: str, symbol: str, pnl: Decimal, price: Decimal, quantity: Decimal
) -> None:
    """Placeholder push — replaced by the reconnect cache + WebSocket push in Stage 10."""
    logger.info("pnl client=%s symbol=%s pnl=%s price=%s qty=%s", client_id, symbol, pnl, price, quantity)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = get_redis()
    pool = await get_pool()

    bloom = BloomFilter(redis_client, BLOOM_KEY, BLOOM_SIZE_BITS, BLOOM_NUM_HASHES)
    position_cache = PositionCache(redis_client, pool)
    dispatcher = FanoutDispatcher(
        redis_client, position_cache, bloom, log_push,
        max_concurrency=settings.fanout_max_concurrency,
        client_timeout_seconds=settings.fanout_client_timeout_seconds,
    )
    coalescer = SymbolCoalescer(
        dispatcher, max_drain_wait_seconds=settings.coalescer_max_drain_wait_seconds
    )
    set_tick_handler(coalescer.submit)

    yield
    await coalescer.close()
    await close_pool()


app = FastAPI(title="Real-Time PnL Service", lifespan=lifespan)
app.include_router(trade_router)
app.include_router(ws_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> dict[str, int]:
    """Fan-out dispatcher task counters (Stage 9)."""
    if _dispatcher is None:
        raise HTTPException(status_code=503, detail="fan-out dispatcher not wired yet")
    return {
        "tasks_started": _dispatcher.tasks_started,
        "tasks_completed": _dispatcher.tasks_completed,
        "tasks_failed": _dispatcher.tasks_failed,
    }
