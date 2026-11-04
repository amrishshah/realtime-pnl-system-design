"""Index membership follows `positions`: quantity > 0 joins, a full sell
(quantity 0) leaves. Joining and leaving change the index and the counting
Bloom filter together, only on a real membership change — so counts stay
exact under redelivery, and a symbol stays in the filter while anyone
still holds it."""
import json
import uuid
from decimal import Decimal

import pytest
import redis.asyncio as redis

from pnl.cache.bloom import CountingBloomFilter
from pnl.cache.reconnect_cache import ReconnectCache
from pnl.cdc.symbol_index_projector import SymbolIndexProjector


def position_event(client_id: str, symbol: str, quantity: str) -> dict:
    return {
        "before": None,
        "after": {"client_id": client_id, "symbol": symbol, "quantity": quantity,
                  "avg_buy_price": "100.00000000"},
        "op": "u",
        "source": {"table": "positions"},
        "ts_ms": 0,
    }


@pytest.fixture
async def env():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    bloom_key = f"cbloom:test:{uuid.uuid4().hex[:8]}"
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    bloom = CountingBloomFilter(r, bloom_key, size_bits=100_000, num_hashes=7)
    yield r, bloom, symbol
    await r.delete(bloom_key, f"symbol_index:{symbol}")
    await r.aclose()


async def _slot_counts(r, bloom, symbol) -> list[int]:
    return [int(c or 0) for c in await r.hmget(bloom.key, bloom._bit_positions(symbol))]


@pytest.mark.asyncio
async def test_full_sell_removes_sole_holder_from_index_and_bloom(env):
    r, bloom, symbol = env
    projector = SymbolIndexProjector(r, bloom)
    await projector.handle(position_event("client-a", symbol, "5.00000000"))
    assert await bloom.might_contain(symbol)

    await projector.handle(position_event("client-a", symbol, "0.00000000"))
    assert await r.scard(f"symbol_index:{symbol}") == 0
    assert not await bloom.might_contain(symbol), (
        "the symbol's last holder exited — the filter must no longer report it"
    )


@pytest.mark.asyncio
async def test_full_sell_keeps_bloom_entry_if_another_client_still_holds(env):
    r, bloom, symbol = env
    projector = SymbolIndexProjector(r, bloom)
    await projector.handle(position_event("client-exit", symbol, "5"))
    await projector.handle(position_event("client-stay", symbol, "3"))

    await projector.handle(position_event("client-exit", symbol, "0"))
    assert await r.smembers(f"symbol_index:{symbol}") == {"client-stay"}
    assert await bloom.might_contain(symbol), (
        "another client still holds the symbol — one exit must not zero its slots"
    )


@pytest.mark.asyncio
async def test_redelivered_events_do_not_skew_counts(env):
    """At-least-once delivery: the same join or exit can arrive twice."""
    r, bloom, symbol = env
    projector = SymbolIndexProjector(r, bloom)
    for _ in range(3):
        await projector.handle(position_event("client-a", symbol, "5"))  # repeat buys/redelivery
    await projector.handle(position_event("client-b", symbol, "1"))
    assert set(await _slot_counts(r, bloom, symbol)) == {2}, "one count per holder, not per event"

    for _ in range(3):
        await projector.handle(position_event("client-a", symbol, "0"))  # redelivered exit
    assert set(await _slot_counts(r, bloom, symbol)) == {1}
    assert await bloom.might_contain(symbol), "client-b still holds it"


@pytest.mark.asyncio
async def test_exit_then_reentry_keeps_client_indexed(env):
    """buy -> sell everything -> buy again, as one ordered stream: the client
    must end up indexed. (With entries from `trades` and exits from
    `positions`, a lagging exit could land after the re-entry and drop them.)"""
    r, bloom, symbol = env
    projector = SymbolIndexProjector(r, bloom)
    await projector.handle(position_event("client-a", symbol, "5"))
    await projector.handle(position_event("client-a", symbol, "0"))
    await projector.handle(position_event("client-a", symbol, "2"))

    assert await r.sismember(f"symbol_index:{symbol}", "client-a")
    assert await bloom.might_contain(symbol)


@pytest.mark.asyncio
async def test_deleted_position_row_counts_as_exit(env):
    r, bloom, symbol = env
    projector = SymbolIndexProjector(r, bloom)
    await projector.handle(position_event("client-a", symbol, "5"))
    await projector.handle({
        "before": {"client_id": "client-a", "symbol": symbol, "quantity": "5"},
        "after": None, "op": "d", "source": {"table": "positions"}, "ts_ms": 0,
    })
    assert await r.scard(f"symbol_index:{symbol}") == 0
    assert not await bloom.might_contain(symbol)


@pytest.mark.asyncio
async def test_full_sell_drops_symbol_from_reconnect_snapshot(env):
    r, bloom, symbol = env
    reconnect = ReconnectCache(r)
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    await reconnect.record(client_id, symbol, Decimal("50"), Decimal("105"), Decimal("10"))
    await reconnect.record(client_id, "OTHER", Decimal("7"), Decimal("1"), Decimal("1"))

    projector = SymbolIndexProjector(r, bloom, reconnect_cache=reconnect)
    await projector.handle(position_event(client_id, symbol, "10"))
    await projector.handle(position_event(client_id, symbol, "0"))

    snapshot = await reconnect.snapshot(client_id)
    assert symbol not in snapshot, "a closed position shouldn't render on reconnect"
    assert snapshot["total"] == "7.0000"
    assert json.loads(snapshot["OTHER"])["pnl"] == "7"
    await r.delete(f"client:{client_id}")
