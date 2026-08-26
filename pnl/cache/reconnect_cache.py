"""
STAGE 10 EXERCISE — implement ReconnectCache.

Design doc §10: persist last-known PnL per client so a reconnect is a cheap
read instead of a wait for the next tick.

Key shape: one Redis HASH per client_id (not per client_id+symbol) —
`client:{id}` with one field per symbol holding a small JSON blob
(`{"pnl": ..., "price": ..., "qty": ...}`). This lets a full portfolio
render in a single HGETALL instead of N round trips for N positions.

Where each concurrency tool lives (the design answer this stage builds):
- Per-symbol writes: plain HSET. Different fields in the same hash never
  collide — two fan-out tasks updating AAPL and TSLA for the same client
  concurrently need zero coordination.
- The aggregate `total` field: recomputing it needs an atomic
  read-sum-write, since two symbols updating concurrently could both read
  a stale total and clobber each other's write. That's
  `pnl/cache/lua/total_pnl.lua`, invoked via a registered script — the one
  place in this whole design that actually needs a Lua script.

What to implement:
- `__init__(redis_client)`: store the client; load and register
  `pnl/cache/lua/total_pnl.lua` (redis.asyncio's `Redis.register_script(...)`
  returns a callable `Script` object — store it).
- `record(client_id, symbol, pnl, price, quantity)`:
  1. `HSET client:{client_id} {symbol} <JSON string with pnl/price/qty>`
     (`json.dumps({"pnl": str(pnl), "price": str(price), "qty": str(quantity)})`).
  2. Invoke the registered script with `keys=[f"client:{client_id}"]` to
     recompute `total` atomically.
- `snapshot(client_id)`: `HGETALL client:{client_id}` — used on WebSocket
  reconnect to render the whole portfolio in one round trip.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import redis.asyncio as redis

LUA_SCRIPT_PATH = Path(__file__).parent / "lua" / "total_pnl.lua"


class ReconnectCache:
    def __init__(self, redis_client: redis.Redis) -> None:
        raise NotImplementedError("Stage 10: implement ReconnectCache.__init__")

    async def record(
        self, client_id: str, symbol: str, pnl: Decimal, price: Decimal, quantity: Decimal
    ) -> None:
        raise NotImplementedError("Stage 10: implement ReconnectCache.record")

    async def snapshot(self, client_id: str) -> dict:
        raise NotImplementedError("Stage 10: implement ReconnectCache.snapshot")
