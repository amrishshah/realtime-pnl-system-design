"""ReconnectCache.record() must write per-symbol fields correctly and keep
the aggregate `total` field consistent with the sum of all per-symbol pnl
values, even after many concurrent updates across different symbols for
the same client."""
import asyncio
import json
import uuid
from decimal import Decimal

import pytest
import redis.asyncio as redis

from pnl.cache.reconnect_cache import ReconnectCache


@pytest.mark.asyncio
async def test_record_writes_per_symbol_field_as_json():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    key = f"client:{client_id}"
    await r.delete(key)

    cache = ReconnectCache(r)
    await cache.record(client_id, "AAPL", Decimal("150.00"), Decimal("180.00"), Decimal("10"))

    raw = await r.hget(key, "AAPL")
    assert raw is not None
    payload = json.loads(raw)
    assert payload["pnl"] == "150.00"
    assert payload["price"] == "180.00"
    assert payload["qty"] == "10"

    await r.delete(key)
    await r.aclose()


@pytest.mark.asyncio
async def test_total_matches_sum_of_symbols_after_concurrent_updates():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    key = f"client:{client_id}"
    await r.delete(key)

    cache = ReconnectCache(r)
    symbols = [f"SYM{i}" for i in range(20)]

    async def record_many(symbol: str) -> None:
        for i in range(10):
            await cache.record(
                client_id, symbol, Decimal(str(i)), Decimal("100"), Decimal("1")
            )

    await asyncio.gather(*[record_many(s) for s in symbols])

    data = await r.hgetall(key)
    expected_total = sum(
        Decimal(json.loads(v)["pnl"]) for k, v in data.items() if k != "total"
    )
    assert "total" in data, "record() never wrote the aggregate total field"
    assert Decimal(data["total"]) == expected_total, (
        "total does not match the sum of per-symbol pnl values — check that "
        "total_pnl.lua sums every field except 'total' itself"
    )

    await r.delete(key)
    await r.aclose()
