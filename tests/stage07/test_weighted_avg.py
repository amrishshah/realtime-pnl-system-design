"""Repeat buys must recompute a weighted-average cost basis at write time
in MySQL, propagating unchanged (math-free) to Redis via CDC. Sells reduce
quantity without touching the average, and overselling is rejected."""
import asyncio
import datetime
import decimal
import uuid

import asyncmy
import httpx
import pytest
import redis.asyncio as redis

from pnl.common.events import Side, Trade
from pnl.trade_service.position_writer import record_trade

BASE_URL = "http://localhost:8000"


@pytest.mark.asyncio
async def test_sequential_buys_produce_correct_weighted_average():
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    symbol = "AAPL"

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
        for qty, price in [("10", "100.00"), ("10", "120.00")]:
            resp = await client.post(
                "/trades",
                json={
                    "client_id": client_id, "symbol": symbol, "side": "buy",
                    "quantity": qty, "price": price,
                    "event_time": datetime.datetime.utcnow().isoformat(),
                },
            )
            assert resp.status_code == 201

    conn = await asyncmy.connect(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl"
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT quantity, avg_buy_price FROM positions WHERE client_id=%s AND symbol=%s",
                (client_id, symbol),
            )
            row = await cur.fetchone()
            assert row is not None
            assert decimal.Decimal(row[0]) == decimal.Decimal("20")
            assert decimal.Decimal(row[1]) == decimal.Decimal("110.00"), (
                "weighted average of (10@100, 10@120) must be 110, not the "
                "latest trade price or a plain sum"
            )
    finally:
        conn.close()

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    try:
        cached: dict = {}
        deadline = asyncio.get_event_loop().time() + 15
        while asyncio.get_event_loop().time() < deadline:
            cached = await r.hgetall(f"position:{client_id}:{symbol}")
            if cached.get("avg_buy_price") and decimal.Decimal(
                cached["avg_buy_price"]
            ) == decimal.Decimal("110.00"):
                break
            await asyncio.sleep(0.5)
        assert cached.get("avg_buy_price") is not None, (
            "CDC never propagated the updated position to Redis"
        )
        assert decimal.Decimal(cached["avg_buy_price"]) == decimal.Decimal("110.00")
        assert decimal.Decimal(cached["quantity"]) == decimal.Decimal("20")
    finally:
        await r.delete(f"position:{client_id}:{symbol}")
        await r.aclose()


@pytest.mark.asyncio
async def test_sell_reduces_quantity_without_changing_avg_price():
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    symbol = "MSFT"
    now = datetime.datetime.utcnow()

    await record_trade(Trade(
        client_id=client_id, symbol=symbol, side=Side.BUY,
        quantity=decimal.Decimal("10"), price=decimal.Decimal("50"), event_time=now,
    ))
    await record_trade(Trade(
        client_id=client_id, symbol=symbol, side=Side.SELL,
        quantity=decimal.Decimal("4"), price=decimal.Decimal("60"), event_time=now,
    ))

    conn = await asyncmy.connect(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl"
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT quantity, avg_buy_price FROM positions WHERE client_id=%s AND symbol=%s",
                (client_id, symbol),
            )
            row = await cur.fetchone()
            assert decimal.Decimal(row[0]) == decimal.Decimal("6")
            assert decimal.Decimal(row[1]) == decimal.Decimal("50.00"), (
                "selling must not change the cost basis of remaining shares"
            )
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_overselling_is_rejected():
    client_id = f"client-{uuid.uuid4().hex[:8]}"
    symbol = "TSLA"
    now = datetime.datetime.utcnow()

    await record_trade(Trade(
        client_id=client_id, symbol=symbol, side=Side.BUY,
        quantity=decimal.Decimal("5"), price=decimal.Decimal("200"), event_time=now,
    ))
    with pytest.raises(Exception):
        await record_trade(Trade(
            client_id=client_id, symbol=symbol, side=Side.SELL,
            quantity=decimal.Decimal("10"), price=decimal.Decimal("210"), event_time=now,
        ))
