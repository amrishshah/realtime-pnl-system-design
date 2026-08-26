"""Connecting a WebSocket must yield an immediate snapshot of last-known
PnL — a reconnect is a cheap read, not a wait for the next tick."""
import asyncio
import json
import uuid

import pytest
import redis.asyncio as redis
import websockets

WS_URL = "ws://localhost:8000/ws/"


@pytest.mark.asyncio
async def test_websocket_sends_immediate_snapshot_on_connect():
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    key = f"client:{client_id}"
    await r.delete(key)
    seeded_aapl = json.dumps({"pnl": "150.00", "price": "180.00", "qty": "10"})
    await r.hset(key, mapping={"AAPL": seeded_aapl, "total": "150.00"})

    try:
        async with websockets.connect(f"{WS_URL}{client_id}") as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
        message = json.loads(raw)
        assert message["type"] == "snapshot"
        assert message["data"]["AAPL"] == seeded_aapl
        assert message["data"]["total"] == "150.00"
    finally:
        await r.delete(key)
        await r.aclose()
