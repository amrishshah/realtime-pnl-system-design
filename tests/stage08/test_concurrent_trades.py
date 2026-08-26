"""Fires many concurrent buys for the same (client_id, symbol) and checks
the final quantity is exactly the sum of all trade quantities — a lost
update would under-count. Repeated across several rounds to rule out
flakiness from a race that doesn't always trigger."""
import asyncio
import datetime
import decimal
import uuid

import asyncmy
import httpx
import pytest

BASE_URL = "http://localhost:8000"


async def _post_trade(
    client: httpx.AsyncClient, client_id: str, symbol: str, qty: str, price: str
) -> None:
    resp = await client.post(
        "/trades",
        json={
            "client_id": client_id, "symbol": symbol, "side": "buy",
            "quantity": qty, "price": price,
            "event_time": datetime.datetime.utcnow().isoformat(),
        },
    )
    assert resp.status_code == 201


async def _get_quantity(client_id: str, symbol: str) -> decimal.Decimal:
    conn = await asyncmy.connect(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl"
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT quantity FROM positions WHERE client_id=%s AND symbol=%s",
                (client_id, symbol),
            )
            row = await cur.fetchone()
            return decimal.Decimal(row[0]) if row else decimal.Decimal("0")
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("round_num", range(5))
async def test_concurrent_buys_never_lose_an_update(round_num):
    client_id = f"client-concurrent-{uuid.uuid4().hex[:8]}"
    symbol = "NFLX"
    num_trades = 20
    qty_each = decimal.Decimal("1")

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=15) as client:
        await asyncio.gather(*[
            _post_trade(client, client_id, symbol, str(qty_each), "500.00")
            for _ in range(num_trades)
        ])

    final_qty = await _get_quantity(client_id, symbol)
    assert final_qty == qty_each * num_trades, (
        f"expected quantity {qty_each * num_trades} after {num_trades} "
        f"concurrent buys, got {final_qty} — a concurrent read-modify-write "
        "lost an update (see Stage 8: SELECT ... FOR UPDATE)"
    )
