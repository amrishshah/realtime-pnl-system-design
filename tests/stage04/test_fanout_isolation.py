"""One artificially slow client among many fast ones for the same symbol
must not delay the fast ones — that's the whole point of per-client tasks
instead of a loop-per-tick."""
import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import asyncmy
import pytest
import redis.asyncio as redis

from pnl.cache.bloom import BloomFilter
from pnl.cache.position_cache import PositionCache
from pnl.common.events import Tick
from pnl.fanout.dispatcher import FanoutDispatcher


async def _seed(r: redis.Redis, bloom: BloomFilter, symbol: str, client_ids: list[str]) -> None:
    key = f"symbol_index:{symbol}"
    await r.delete(key)
    for cid in client_ids:
        await r.sadd(key, cid)
        await r.hset(f"position:{cid}:{symbol}", mapping={"quantity": "10", "avg_buy_price": "100"})
    await bloom.add(symbol)


async def _cleanup(r: redis.Redis, symbol: str, client_ids: list[str]) -> None:
    await r.delete(f"symbol_index:{symbol}")
    for cid in client_ids:
        await r.delete(f"position:{cid}:{symbol}")


@pytest.mark.asyncio
async def test_one_slow_client_does_not_block_the_rest():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=5,
    )
    bloom_key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    bloom = BloomFilter(r, bloom_key, 100_000, 7)
    position_cache = PositionCache(r, pool)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    slow_client = "client-slow"
    fast_clients = [f"client-fast-{i}" for i in range(199)]
    all_clients = [slow_client] + fast_clients
    await _seed(r, bloom, symbol, all_clients)

    fast_done = asyncio.Event()
    fast_completed: list[str] = []
    slow_completed: list[str] = []

    async def push(client_id: str, sym: str, pnl: Decimal, price: Decimal, quantity: Decimal) -> None:
        if client_id == slow_client:
            await asyncio.sleep(3)
            slow_completed.append(client_id)
        else:
            fast_completed.append(client_id)
            if len(fast_completed) == len(fast_clients):
                fast_done.set()

    dispatcher = FanoutDispatcher(r, position_cache, bloom, push, max_concurrency=500)
    tick = Tick(symbol=symbol, price=Decimal("105"), event_time=datetime.now(timezone.utc))

    try:
        start = asyncio.get_event_loop().time()
        await dispatcher.handle_tick(tick)
        await asyncio.wait_for(fast_done.wait(), timeout=1.0)
        elapsed = asyncio.get_event_loop().time() - start

        assert elapsed < 1.0, "fast clients were blocked behind the slow one"
        assert len(slow_completed) == 0, (
            "slow client already finished — test setup issue, it should "
            "still be sleeping at this point"
        )
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(bloom_key)
        await _cleanup(r, symbol, all_clients)
        await r.aclose()
