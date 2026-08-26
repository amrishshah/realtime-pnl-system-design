"""Verifies record_trade() writes MySQL correctly and stays a single-writer
path (no second write path to any index — that's the whole point of §1)."""
import ast
import datetime
import decimal
from pathlib import Path

import httpx
import pytest

BASE_URL = "http://localhost:8000"

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_trade_service_has_no_redis_or_kafka_producer_imports():
    """Static guard: the trade-write path must not perform a second,
    application-level write to any index. Only MySQL should be touched here
    — everything else is derived via CDC starting in Stage 2."""
    source = (REPO_ROOT / "pnl" / "trade_service" / "position_writer.py").read_text()
    tree = ast.parse(source)
    imported_modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module.split(".")[0])
    forbidden = {"redis", "aiokafka", "kafka"}
    assert not (imported_modules & forbidden), (
        f"position_writer.py imports {imported_modules & forbidden} — "
        "the trade-write path must only write MySQL; the index/cache must "
        "be derived via CDC, not written here directly."
    )


@pytest.mark.asyncio
async def test_post_trade_persists_to_mysql():
    import asyncmy

    client_id = "client-stage1-test"
    symbol = "AAPL"

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
        resp = await client.post(
            "/trades",
            json={
                "client_id": client_id,
                "symbol": symbol,
                "side": "buy",
                "quantity": "10",
                "price": "100.00",
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
                "SELECT quantity, price FROM trades WHERE client_id=%s AND symbol=%s "
                "ORDER BY id DESC LIMIT 1",
                (client_id, symbol),
            )
            row = await cur.fetchone()
            assert row is not None, "trade row was not written"
            assert decimal.Decimal(row[0]) == decimal.Decimal("10")
            assert decimal.Decimal(row[1]) == decimal.Decimal("100.00")

            await cur.execute(
                "SELECT quantity FROM positions WHERE client_id=%s AND symbol=%s",
                (client_id, symbol),
            )
            pos_row = await cur.fetchone()
            assert pos_row is not None, "position row was not upserted"
    finally:
        conn.close()
