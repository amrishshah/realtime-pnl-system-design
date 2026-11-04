"""EOD safety-net checkpoint: rebuilding the Counting Bloom Filter directly
from MySQL positions must match exactly the symbols with a nonzero held
quantity — independent of whatever incremental add/remove history led up
to it."""
import uuid

import asyncmy
import pytest
import redis.asyncio as redis

from pnl.cache.bloom import CountingBloomFilter


@pytest.mark.asyncio
async def test_rebuild_from_positions_matches_held_symbols():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"cbloom:rebuild-test:{uuid.uuid4().hex[:8]}"
    await r.delete(key)

    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=2,
    )
    held_symbol = f"HELD{uuid.uuid4().hex[:6].upper()}"
    closed_symbol = f"CLOSED{uuid.uuid4().hex[:6].upper()}"
    client_id = f"client-{uuid.uuid4().hex[:8]}"

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) "
                    "VALUES (%s, %s, %s, %s)",
                    (client_id, held_symbol, "5", "100"),
                )
                await cur.execute(
                    "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) "
                    "VALUES (%s, %s, %s, %s)",
                    (client_id, closed_symbol, "0", "100"),
                )
            await conn.commit()

        bloom = CountingBloomFilter(r, key, size_bits=1_000_000, num_hashes=7)
        await bloom.rebuild_from_positions(pool)

        assert await bloom.might_contain(held_symbol)
        assert not await bloom.might_contain(closed_symbol), (
            "rebuild must only include symbols with a nonzero held quantity, "
            "not every symbol that ever appeared in positions"
        )
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(key)
        await r.aclose()


@pytest.mark.asyncio
async def test_rebuild_counts_each_holder_so_one_exit_keeps_symbol():
    """Rebuild must count holders, not distinct symbols: with two holders
    rebuilt as a single count, the first exit would zero the slots while the
    second holder still holds the symbol."""
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"cbloom:rebuild-test:{uuid.uuid4().hex[:8]}"
    symbol = f"TWO{uuid.uuid4().hex[:6].upper()}"
    index = f"symbol_index:{symbol}"
    await r.delete(key, index)

    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=2,
    )
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for client in ("holder-a", "holder-b"):
                    await cur.execute(
                        "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) "
                        "VALUES (%s, %s, %s, %s)",
                        (f"{client}-{uuid.uuid4().hex[:6]}", symbol, "5", "100"),
                    )
            await conn.commit()

        bloom = CountingBloomFilter(r, key, size_bits=1_000_000, num_hashes=7)
        await bloom.rebuild_from_positions(pool)
        await r.sadd(index, "holder-a", "holder-b")

        await bloom.leave(index, "holder-a", symbol)
        assert await bloom.might_contain(symbol), "holder-b still holds it"
        await bloom.leave(index, "holder-b", symbol)
        assert not await bloom.might_contain(symbol)
    finally:
        pool.close()
        await pool.wait_closed()
        await r.delete(key, index)
        await r.aclose()
