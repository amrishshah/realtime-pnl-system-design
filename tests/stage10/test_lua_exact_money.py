"""The portfolio total is money: Redis Lua numbers are doubles, so summing
"0.1" + "0.2" as floats would store 0.30000000000000004. total_pnl.lua sums
in exact fixed-point units of 10^-4 instead."""
import uuid
from decimal import Decimal

import pytest
import redis.asyncio as redis

from pnl.cache.reconnect_cache import ReconnectCache


@pytest.fixture
async def cache_and_client():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    yield ReconnectCache(r), client_id
    await r.delete(f"client:{client_id}")
    await r.aclose()


async def _total(cache: ReconnectCache, client_id: str) -> str:
    return (await cache.snapshot(client_id))["total"]


@pytest.mark.asyncio
async def test_no_float_drift(cache_and_client):
    cache, cid = cache_and_client
    await cache.record(cid, "A", Decimal("0.1"), Decimal(1), Decimal(1))
    await cache.record(cid, "B", Decimal("0.2"), Decimal(1), Decimal(1))
    assert await _total(cache, cid) == "0.3000"


@pytest.mark.asyncio
async def test_negative_values_and_rounding(cache_and_client):
    cache, cid = cache_and_client
    await cache.record(cid, "A", Decimal("10"), Decimal(1), Decimal(1))
    await cache.record(cid, "B", Decimal("-125.00005"), Decimal(1), Decimal(1))  # -> -125.0001
    await cache.record(cid, "C", Decimal("125.0000000000000000"), Decimal(1), Decimal(1))
    assert await _total(cache, cid) == "9.9999"


@pytest.mark.asyncio
async def test_large_values_stay_exact(cache_and_client):
    cache, cid = cache_and_client
    await cache.record(cid, "A", Decimal("123456789012.3456"), Decimal(1), Decimal(1))
    await cache.record(cid, "B", Decimal("0.0001"), Decimal(1), Decimal(1))
    assert await _total(cache, cid) == "123456789012.3457"


@pytest.mark.asyncio
async def test_forget_drops_symbol_and_recomputes(cache_and_client):
    cache, cid = cache_and_client
    await cache.record(cid, "A", Decimal("5"), Decimal(1), Decimal(1))
    await cache.record(cid, "B", Decimal("7"), Decimal(1), Decimal(1))
    await cache.forget(cid, "B")
    snapshot = await cache.snapshot(cid)
    assert "B" not in snapshot
    assert snapshot["total"] == "5.0000"
