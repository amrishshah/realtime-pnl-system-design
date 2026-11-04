"""The point of §5 end to end: with a real dispatcher whose per-client
pushes are slow, a burst of ticks must not pile up an unbounded backlog of
per-client tasks. Coalescing has to hold the *work* down, not just the tick
count — the dispatcher spawns tasks and returns immediately, so without the
coalescer waiting for each fan-out to drain, every tick would add another
full round of waiting tasks."""
import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter
from pnl.cache.position_cache import PositionCache
from pnl.common.events import Tick
from pnl.fanout.dispatcher import FanoutDispatcher
from pnl.ticks.coalescer import SymbolCoalescer


@pytest.mark.asyncio
async def test_tick_burst_does_not_grow_task_backlog():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    bloom_key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    bloom = BloomFilter(r, bloom_key, 100_000, 7)
    # Every lookup below is a cache hit, so MySQL is never touched.
    position_cache = PositionCache(r, mysql_pool=None)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    num_clients = 20
    client_ids = [f"client-{i}-{uuid.uuid4().hex[:6]}" for i in range(num_clients)]
    index = f"symbol_index:{symbol}"
    for cid in client_ids:
        await r.sadd(index, cid)
        await r.hset(f"position:{cid}:{symbol}", mapping={"quantity": "1", "avg_buy_price": "1"})
    await bloom.add(symbol)

    pushed_prices: dict[str, Decimal] = {}

    async def slow_push(cid, sym, pnl, price, qty):
        await asyncio.sleep(0.2)
        pushed_prices[cid] = price

    dispatcher = FanoutDispatcher(r, position_cache, bloom, slow_push, max_concurrency=50)
    coalescer = SymbolCoalescer(dispatcher, max_drain_wait_seconds=1.0)

    max_backlog = 0
    try:
        for i in range(50):  # 50 ticks, far faster than a 0.2s fan-out drains
            await coalescer.submit(
                Tick(symbol=symbol, price=Decimal(100 + i), event_time=datetime.now(timezone.utc))
            )
            max_backlog = max(max_backlog, len(dispatcher._in_flight_tasks))
            await asyncio.sleep(0.01)
        await asyncio.sleep(1.0)
        max_backlog = max(max_backlog, len(dispatcher._in_flight_tasks))

        # At most one tick's worth of tasks in flight at once; without the
        # drain wait this reaches ~50 ticks x 20 clients.
        assert max_backlog <= num_clients, (
            f"{max_backlog} per-client tasks were in flight at once for "
            f"{num_clients} clients — ticks are piling up work instead of coalescing"
        )
        # And everyone still converges on the freshest price.
        assert set(pushed_prices.values()) == {Decimal(149)}
    finally:
        await coalescer.close()
        await r.delete(index, bloom_key, *(f"position:{cid}:{symbol}" for cid in client_ids))
        await r.aclose()
