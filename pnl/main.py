from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from decimal import Decimal

import clickhouse_connect
from fastapi import FastAPI, HTTPException

from pnl.cache.bloom import symbols_bloom
from pnl.cache.position_cache import PositionCache
from pnl.cache.reconnect_cache import ReconnectCache
from pnl.cache.redis_client import get_redis
from pnl.common.events import Tick
from pnl.config import settings
from pnl.db.mysql import close_pool, get_pool
from pnl.fanout.dispatcher import FanoutDispatcher
from pnl.history.clickhouse_sink import RawTickSink
from pnl.ticks.coalescer import SymbolCoalescer
from pnl.trade_service.api import router as trade_router
from pnl.ws.connection_manager import ConnectionManager
from pnl.ws.server import init_reconnect
from pnl.ws.server import router as ws_router
from pnl.ws.server import set_tick_handler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("pnl")

_dispatcher = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _dispatcher

    redis_client = get_redis()
    pool = await get_pool()
    ch_client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port
    )

    bloom = symbols_bloom(redis_client)
    position_cache = PositionCache(redis_client, pool)
    reconnect_cache = ReconnectCache(redis_client)
    connection_manager = ConnectionManager()
    init_reconnect(reconnect_cache, connection_manager)

    async def push(
        client_id: str, symbol: str, pnl: Decimal, price: Decimal, quantity: Decimal
    ) -> None:
        # Persist first (§10: a reconnect is a cheap read), then fan out to
        # whatever WebSocket connections this client has open right now.
        await reconnect_cache.record(client_id, symbol, pnl, price, quantity)
        await connection_manager.send(
            client_id,
            {"type": "pnl", "symbol": symbol, "pnl": str(pnl),
             "price": str(price), "qty": str(quantity)},
        )

    dispatcher = FanoutDispatcher(
        redis_client, position_cache, bloom, push,
        max_concurrency=settings.fanout_max_concurrency,
        client_timeout_seconds=settings.fanout_client_timeout_seconds,
    )
    _dispatcher = dispatcher
    coalescer = SymbolCoalescer(
        dispatcher, max_drain_wait_seconds=settings.coalescer_max_drain_wait_seconds
    )
    raw_sink = RawTickSink(ch_client)

    async def ingest(tick: Tick) -> None:
        # Branch before coalescing (§6): every tick reaches ClickHouse,
        # in parallel with the coalesced live-dashboard path.
        await asyncio.gather(raw_sink.write(tick), coalescer.submit(tick))

    set_tick_handler(ingest)

    yield
    await coalescer.close()
    await raw_sink.close()
    await ch_client.close()
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
        "tasks_in_flight": _dispatcher.tasks_in_flight,
    }
