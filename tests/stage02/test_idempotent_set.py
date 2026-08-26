"""Feeding the same Debezium-shaped event N times must be a no-op after the
first — that's the whole point of backing the index with a SET."""
import uuid

import pytest
import redis.asyncio as redis

from pnl.cdc.symbol_index_projector import SymbolIndexProjector


def make_event(client_id: str, symbol: str) -> dict:
    return {
        "before": None,
        "after": {
            "id": 1,
            "client_id": client_id,
            "symbol": symbol,
            "side": "buy",
            "quantity": "10.00000000",
            "price": "100.00000000",
        },
        "op": "c",
        "source": {"table": "trades"},
        "ts_ms": 0,
    }


@pytest.mark.asyncio
async def test_duplicate_delivery_is_a_noop():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    key = f"symbol_index:{symbol}"

    await r.delete(key)
    projector = SymbolIndexProjector(r)

    event = make_event(client_id, symbol)
    for _ in range(5):
        await projector.handle(event)

    members = await r.smembers(key)
    assert members == {client_id}
    assert await r.scard(key) == 1

    await r.delete(key)
    await r.aclose()


@pytest.mark.asyncio
async def test_multiple_distinct_clients_accumulate():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    key = f"symbol_index:{symbol}"
    await r.delete(key)
    projector = SymbolIndexProjector(r)

    client_ids = {f"client-{i}-{uuid.uuid4().hex[:6]}" for i in range(10)}
    for cid in client_ids:
        await projector.handle(make_event(cid, symbol))
        await projector.handle(make_event(cid, symbol))  # duplicate each

    members = await r.smembers(key)
    assert members == client_ids

    await r.delete(key)
    await r.aclose()
