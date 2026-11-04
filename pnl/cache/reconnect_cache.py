"""
Last-known PnL per client, for cheap reconnects (design doc §10).

A client that was disconnected for ten minutes doesn't need market history
replayed — it needs the current values, now. Every computed PnL is
persisted here, so a reconnect is one HGETALL instead of a wait for the next
tick.

Key shape: one HASH per client — `client:{id}` — with one field per symbol
holding `{"pnl": ..., "price": ..., "qty": ...}`, plus a `total` field. One
hash per client (not per client+symbol) means a whole portfolio renders in
a single round trip.

Concurrency: per-symbol fields never collide, but `total` is a
read-sum-write. `lua/total_pnl.lua` writes the symbol's field and recomputes
`total` in one atomic script — exactly summed, never torn.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import redis.asyncio as redis

LUA_SCRIPT_PATH = Path(__file__).parent / "lua" / "total_pnl.lua"


def client_key(client_id: str) -> str:
    return f"client:{client_id}"


class ReconnectCache:
    def __init__(self, redis_client: redis.Redis) -> None:
        self.redis = redis_client
        self._write_and_total = redis_client.register_script(LUA_SCRIPT_PATH.read_text())

    async def record(
        self, client_id: str, symbol: str, pnl: Decimal, price: Decimal, quantity: Decimal
    ) -> None:
        blob = json.dumps({"pnl": str(pnl), "price": str(price), "qty": str(quantity)})
        await self._write_and_total(keys=[client_key(client_id)], args=[symbol, blob])

    async def forget(self, client_id: str, symbol: str) -> None:
        """Drop a symbol the client no longer holds and recompute `total`."""
        await self._write_and_total(keys=[client_key(client_id)], args=[symbol, ""])

    async def snapshot(self, client_id: str) -> dict[str, str]:
        return await self.redis.hgetall(client_key(client_id))
