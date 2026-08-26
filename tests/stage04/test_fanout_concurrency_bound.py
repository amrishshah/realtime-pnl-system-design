"""In-flight fan-out tasks must never exceed the configured semaphore
bound, no matter how many clients a tick affects."""
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


@pytest.mark.asyncio
async def test_concurrency_never_exceeds_semaphore_bound():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=10,
    )
    bloom_key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    bloom = BloomFilter(r, bloom_key, 100_000, 7)
    position_cache = PositionCache(r, pool)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    max_concurrency = 10
    num_clients = 100
    client_ids = [f"client-{i}-{uuid.uuid4().hex[:6]}" for i in range(num_clients)]

    key = f"symbol_index:{symbol}"
    await r.delete(key)
    for cid in client_ids:
        await r.sadd(key, cid)
        await r.hset(f"position:{cid}:{symbol}", mapping={"quantity": "10", "avg_buy_price": "100"})
    await bloom.add(symbol)

    in_flight = 0
    max_observed = 0
    done_count = 0
    lock = asyncio.Lock()
    all_done = asyncio.Event()

    async def push(client_id: str, sym: str, pnl: Decimal, price: Decimal, quantity: Decimal) -> None:
        nonlocal in_flight, max_observed, done_count
        async with lock:
            in_flight += 1
            max_observed = max(max_observed, in_flight)
        await asyncio.sleep(0.05)
        async with lock:
            in_flight -= 1
            done_count += 1
            if done_count == num_clients:
                all_done.set()

    dispatcher = FanoutDispatcher(r, position_cache, bloom, push, max_concurrency=max_concurrency)
    tick = Tick(symbol=symbol, price=Decimal("50"), event_time=datetime.now(timezone.utc))

    try:
        await dispatcher.handle_tick(tick)
        await asyncio.wait_for(all_done.wait(), timeout=10)

        assert max_observed <= max_concurrency, (
            f"observed {max_observed} concurrent in-flight tasks, exceeding "
            f"the configured bound of {max_concurrency}"
        )
        assert max_observed > 1, "test didn't actually exercise concurrency"
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(key, bloom_key)
        for cid in client_ids:
            await r.delete(f"position:{cid}:{symbol}")
        await r.aclose()


@pytest.mark.asyncio
async def test_unheld_symbol_never_reaches_index_lookup():
    """The Bloom filter gate is checked once per tick, before SMEMBERS —
    for a symbol nobody holds, handle_tick must not even look up the
    index, let alone touch the position cache."""
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=2,
    )
    bloom_key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    bloom = BloomFilter(r, bloom_key, 1_000_000, 7)  # large + few adds => negligible FP risk
    for s in (f"HELD{i}" for i in range(20)):
        await bloom.add(s)

    unheld_symbol = f"UNHELD{uuid.uuid4().hex[:6].upper()}"
    # Deliberately leave symbol_index populated with a stray client to prove
    # the dispatcher never even gets far enough to read it.
    key = f"symbol_index:{unheld_symbol}"
    await r.delete(key)
    await r.sadd(key, "client-should-not-be-processed")

    push_called = False

    async def push(client_id: str, sym: str, pnl: Decimal, price: Decimal, quantity: Decimal) -> None:
        nonlocal push_called
        push_called = True

    position_cache = PositionCache(r, pool)
    dispatcher = FanoutDispatcher(r, position_cache, bloom, push, max_concurrency=10)
    tick = Tick(symbol=unheld_symbol, price=Decimal("1"), event_time=datetime.now(timezone.utc))

    try:
        await dispatcher.handle_tick(tick)
        await asyncio.sleep(0.2)  # let any (incorrectly) spawned tasks run
        assert not push_called, (
            "push_callback was invoked for a symbol that failed the Bloom "
            "filter check — the gate must short-circuit before the index "
            "lookup, not after"
        )
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(key, bloom_key)
        await r.aclose()
