"""Read-through: cache hit avoids MySQL entirely; a miss falls back to
MySQL and backfills Redis so the next read is a hit."""
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

import asyncmy
import pytest
import redis.asyncio as redis

from pnl.cache.position_cache import PositionCache


@pytest.mark.asyncio
async def test_cache_hit_skips_mysql():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id, symbol = f"client-{uuid.uuid4().hex[:8]}", "AAPL"
    key = f"position:{client_id}:{symbol}"
    await r.delete(key)
    await r.hset(key, mapping={"quantity": "15", "avg_buy_price": "150.50"})

    fake_pool = AsyncMock()  # if get() touches this at all on a hit, tests fail
    cache = PositionCache(r, fake_pool)

    snapshot = await cache.get(client_id, symbol)
    assert snapshot is not None
    assert snapshot.quantity == Decimal("15")
    assert snapshot.avg_buy_price == Decimal("150.50")
    fake_pool.acquire.assert_not_called()

    await r.delete(key)
    await r.aclose()


@pytest.mark.asyncio
async def test_cache_miss_falls_back_to_mysql_and_backfills():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id, symbol = f"client-{uuid.uuid4().hex[:8]}", "GOOG"
    key = f"position:{client_id}:{symbol}"
    await r.delete(key)

    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=2,
    )
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE quantity=VALUES(quantity), "
                    "avg_buy_price=VALUES(avg_buy_price)",
                    (client_id, symbol, "20", "2500.00"),
                )
            await conn.commit()

        cache = PositionCache(r, pool)
        snapshot = await cache.get(client_id, symbol)
        assert snapshot is not None
        assert snapshot.quantity == Decimal("20")
        assert snapshot.avg_buy_price == Decimal("2500.00")

        # Backfilled — a second read should now be servable from Redis alone.
        cached = await r.hgetall(key)
        assert cached.get("quantity") is not None
        assert cached.get("avg_buy_price") is not None
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(key)
        await r.aclose()


@pytest.mark.asyncio
async def test_unheld_symbol_returns_none():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id, symbol = f"client-{uuid.uuid4().hex[:8]}", "NOPOSITION"
    await r.delete(f"position:{client_id}:{symbol}")

    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=2,
    )
    try:
        cache = PositionCache(r, pool)
        assert await cache.get(client_id, symbol) is None
    finally:
        pool.close()
        await pool.wait_closed()
        await r.aclose()
