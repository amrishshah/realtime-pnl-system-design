from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from decimal import Decimal

import clickhouse_connect
from fastapi import FastAPI

from pnl.audit.audit_writer import AuditWriter
from pnl.cache.bloom import CountingBloomFilter
from pnl.cache.position_cache import PositionCache
from pnl.cache.reconnect_cache import ReconnectCache
from pnl.cache.redis_client import get_redis
from pnl.common.events import Tick
from pnl.config import settings
from pnl.db.mysql import get_pool
from pnl.fanout.dispatcher import FanoutDispatcher
from pnl.history.clickhouse_sink import RawTickSink
from pnl.ticks.coalescer import SymbolCoalescer
from pnl.trade_service.api import router as trade_router
from pnl.ws.connection_manager import ConnectionManager
from pnl.ws.server import init_reconnect
from pnl.ws.server import router as ws_router
from pnl.ws.server import set_tick_handler

logger = logging.getLogger("pnl")

BLOOM_KEY = "bloom:symbols"
BLOOM_SIZE_BITS = 1_000_000
BLOOM_NUM_HASHES = 7

_dispatcher: FanoutDispatcher | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _dispatcher

    redis_client = get_redis()
    pool = await get_pool()
    ch_client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port
    )

    # The live pipeline is built from components you implement stage by
    # stage. Until they exist they raise NotImplementedError — don't take the
    # whole app (and the stage-specific routes like /trades and /health) down.
    try:
        bloom = CountingBloomFilter(redis_client, BLOOM_KEY, BLOOM_SIZE_BITS, BLOOM_NUM_HASHES)
        position_cache = PositionCache(redis_client, pool)
        reconnect_cache = ReconnectCache(redis_client)
        connection_manager = ConnectionManager()
        audit_writer = AuditWriter(ch_client)

        async def push(
            client_id: str, symbol: str, pnl: Decimal, price: Decimal, quantity: Decimal
        ) -> None:
            # Persist for cheap reconnect reads (§10) and a durable audit trail
            # for EOD reconciliation (§13), then fan out to whatever WebSocket
            # connections are open right now for this client.
            await reconnect_cache.record(client_id, symbol, pnl, price, quantity)
            await audit_writer.write(client_id, symbol, pnl, price)
            await connection_manager.send(
                client_id,
                {
                    "type": "pnl",
                    "symbol": symbol,
                    "pnl": str(pnl),
                    "price": str(price),
                    "qty": str(quantity),
                },
            )

        dispatcher = FanoutDispatcher(redis_client, position_cache, bloom, push)
        _dispatcher = dispatcher
        coalescer = SymbolCoalescer(dispatcher, watermark_lateness_seconds=settings.watermark_lateness_seconds)
        raw_sink = RawTickSink(ch_client)
        init_reconnect(reconnect_cache, connection_manager)

        async def ingest(tick: Tick) -> None:
            # Branch before coalescing (§6): every tick reaches ClickHouse
            # unconditionally, in parallel with the coalesced live-dashboard path.
            await asyncio.gather(raw_sink.write(tick), coalescer.submit(tick))

        set_tick_handler(ingest)
    except NotImplementedError as exc:
        logger.warning("live pipeline not fully wired yet: %s", exc)
    yield
    await ch_client.close()


app = FastAPI(title="Real-Time PnL Service", lifespan=lifespan)
app.include_router(trade_router)
app.include_router(ws_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> dict[str, int]:
    """Stage 9: exposes the fan-out dispatcher's task counters, used by the
    chaos test to observe that failed/cancelled tasks don't need recovery."""
    assert _dispatcher is not None
    return {
        "tasks_started": _dispatcher.tasks_started,
        "tasks_completed": _dispatcher.tasks_completed,
        "tasks_failed": _dispatcher.tasks_failed,
    }
