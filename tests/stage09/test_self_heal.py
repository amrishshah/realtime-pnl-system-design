"""Cancels in-flight fan-out tasks mid-tick (simulating a crash) and
confirms the very next tick brings every client back to a correct,
current PnL value with zero special-case recovery code — the property
that makes at-least-once delivery on the push itself unnecessary."""
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
async def test_crash_mid_fanout_self_heals_on_next_tick():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=10,
    )
    bloom_key = f"bloom:test:{uuid.uuid4().hex[:8]}"
    bloom = BloomFilter(r, bloom_key, 100_000, 7)
    position_cache = PositionCache(r, pool)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    num_clients = 50
    client_ids = [f"client-{i}-{uuid.uuid4().hex[:6]}" for i in range(num_clients)]

    key = f"symbol_index:{symbol}"
    await r.delete(key)
    for cid in client_ids:
        await r.sadd(key, cid)
        await r.hset(
            f"position:{cid}:{symbol}", mapping={"quantity": "10", "avg_buy_price": "100"}
        )
    await bloom.add(symbol)

    received: dict[str, Decimal] = {}
    processed_tick1: set[str] = set()

    async def slow_push(
        client_id: str, sym: str, pnl: Decimal, price: Decimal, quantity: Decimal
    ) -> None:
        await asyncio.sleep(0.3)  # keep the task in-flight long enough to cancel
        processed_tick1.add(client_id)
        received[client_id] = pnl

    async def push_ok(
        client_id: str, sym: str, pnl: Decimal, price: Decimal, quantity: Decimal
    ) -> None:
        received[client_id] = pnl

    try:
        # --- Tick 1: simulate a crash mid-fan-out by cancelling in-flight tasks ---
        crashy_dispatcher = FanoutDispatcher(
            r, position_cache, bloom, slow_push, max_concurrency=500
        )
        tick1 = Tick(symbol=symbol, price=Decimal("110"), event_time=datetime.now(timezone.utc))

        await crashy_dispatcher.handle_tick(tick1)
        await asyncio.sleep(0.05)  # let tasks spawn and reach the sleep in slow_push

        in_flight = list(crashy_dispatcher._in_flight_tasks)
        assert in_flight, "no in-flight tasks found to cancel — test timing issue"
        for t in in_flight:
            t.cancel()
        await asyncio.sleep(0.5)

        assert not processed_tick1, (
            "expected tick 1's tasks to be cancelled before completing — "
            "if they all finished anyway, this didn't simulate a crash"
        )
        assert crashy_dispatcher.tasks_failed >= 1, (
            "cancelled tasks must be reflected in tasks_failed — see the "
            "Stage 9 docstring addition in dispatcher.py"
        )

        # --- Tick 2: no recovery code runs here, just an ordinary next tick ---
        healthy_dispatcher = FanoutDispatcher(r, position_cache, bloom, push_ok, max_concurrency=500)
        tick2 = Tick(symbol=symbol, price=Decimal("120"), event_time=datetime.now(timezone.utc))

        await healthy_dispatcher.handle_tick(tick2)
        await asyncio.sleep(1.0)

        missing = set(client_ids) - set(received.keys())
        assert not missing, (
            f"clients {missing} never got a PnL update even after a "
            "subsequent, uninterrupted tick"
        )
        for cid in client_ids:
            assert received[cid] == Decimal("200"), (
                f"client {cid} has stale/incorrect PnL after the "
                "self-healing tick (expected (120-100)*10 = 200)"
            )
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(key, bloom_key)
        for cid in client_ids:
            await r.delete(f"position:{cid}:{symbol}")
        await r.aclose()
