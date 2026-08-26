"""A full sell (quantity -> 0) must remove that client from the symbol
index and decrement the Counting Bloom Filter by exactly one membership --
correctly leaving the filter positive as long as any other client still
holds the symbol."""
import uuid

import pytest
import redis.asyncio as redis

from pnl.cache.bloom import CountingBloomFilter
from pnl.cdc.position_cache_projector import PositionCacheProjector


def _sell_to_zero_event(client_id: str, symbol: str) -> dict:
    return {
        "before": {"client_id": client_id, "symbol": symbol, "quantity": "5.00000000",
                   "avg_buy_price": "100.00000000"},
        "after": {"client_id": client_id, "symbol": symbol, "quantity": "0.00000000",
                  "avg_buy_price": "100.00000000"},
        "op": "u",
        "source": {"table": "positions"},
        "ts_ms": 0,
    }


@pytest.mark.asyncio
async def test_full_sell_removes_sole_holder_from_index_and_bloom():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    bloom_key = f"cbloom:test:{uuid.uuid4().hex[:8]}"
    bloom = CountingBloomFilter(r, bloom_key, size_bits=100_000, num_hashes=7)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    index_key = f"symbol_index:{symbol}"

    await r.delete(index_key)
    await r.sadd(index_key, client_id)
    await bloom.add(symbol)
    assert await bloom.might_contain(symbol)

    try:
        projector = PositionCacheProjector(r, bloom)
        await projector.handle(_sell_to_zero_event(client_id, symbol))

        assert await r.scard(index_key) == 0, (
            "client should be removed from the symbol index on full sell"
        )
        assert not await bloom.might_contain(symbol), (
            "bloom filter should no longer report this symbol once its "
            "only holder exits"
        )
    finally:
        await r.delete(index_key, bloom_key)
        await r.aclose()


@pytest.mark.asyncio
async def test_full_sell_keeps_bloom_entry_if_another_client_still_holds():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    bloom_key = f"cbloom:test:{uuid.uuid4().hex[:8]}"
    bloom = CountingBloomFilter(r, bloom_key, size_bits=100_000, num_hashes=7)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    exiting_client = f"client-exit-{uuid.uuid4().hex[:8]}"
    remaining_client = f"client-stay-{uuid.uuid4().hex[:8]}"
    index_key = f"symbol_index:{symbol}"

    await r.delete(index_key)
    await r.sadd(index_key, exiting_client, remaining_client)
    # Each holder's membership contributed one add() in the real system.
    await bloom.add(symbol)
    await bloom.add(symbol)

    try:
        projector = PositionCacheProjector(r, bloom)
        await projector.handle(_sell_to_zero_event(exiting_client, symbol))

        assert await r.smembers(index_key) == {remaining_client}
        assert await bloom.might_contain(symbol), (
            "symbol is still held by another client — one exit's decrement "
            "must not zero out a slot that another holder's add() still "
            "keeps positive"
        )
    finally:
        await r.delete(index_key, bloom_key)
        await r.aclose()
