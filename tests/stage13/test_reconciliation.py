"""Seeds known positions, independent closing prices, and a mix of correct
and deliberately-drifted audit rows; runs the EOD reconciler; and checks it
flags exactly the drifted client/symbol pairs and rebuilds the Bloom filter
to match exactly the symbols with a nonzero held quantity."""
import uuid
from datetime import date
from decimal import Decimal

import asyncmy
import clickhouse_connect
import pytest
import redis.asyncio as redis

from pnl.cache.bloom import CountingBloomFilter
from pnl.eod.reconciliation import EODReconciler


@pytest.mark.asyncio
async def test_reconciliation_flags_only_drifted_pairs_and_rebuilds_bloom():
    close_date = date(2024, 1, 15)
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    bloom_key = f"cbloom:eod-test:{uuid.uuid4().hex[:8]}"
    await r.delete(bloom_key)

    pool = await asyncmy.create_pool(
        host="localhost", port=3306, user="pnl", password="pnl", db="pnl",
        minsize=1, maxsize=5,
    )
    ch_client = await clickhouse_connect.get_async_client(host="localhost", port=8123)

    correct_symbol = f"OK{uuid.uuid4().hex[:6].upper()}"
    drifted_symbol = f"BAD{uuid.uuid4().hex[:6].upper()}"
    missing_audit_symbol = f"MISS{uuid.uuid4().hex[:6].upper()}"
    closed_symbol = f"CLOSED{uuid.uuid4().hex[:6].upper()}"
    client_id = f"client-{uuid.uuid4().hex[:8]}"

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                for symbol, qty, avg_price in [
                    (correct_symbol, "10", "100"),
                    (drifted_symbol, "10", "100"),
                    (missing_audit_symbol, "10", "100"),
                    (closed_symbol, "0", "100"),
                ]:
                    await cur.execute(
                        "INSERT INTO positions (client_id, symbol, quantity, avg_buy_price) "
                        "VALUES (%s, %s, %s, %s)",
                        (client_id, symbol, qty, avg_price),
                    )
                for symbol, close_price in [
                    (correct_symbol, "150"),
                    (drifted_symbol, "150"),
                    (missing_audit_symbol, "150"),
                ]:
                    await cur.execute(
                        "INSERT INTO eod_closing_prices (symbol, close_date, close_price) "
                        "VALUES (%s, %s, %s)",
                        (symbol, close_date, close_price),
                    )
            await conn.commit()

        # official_pnl for correct_symbol/drifted_symbol = (150-100)*10 = 500
        await ch_client.insert(
            "pnl_audit", [[client_id, correct_symbol, "500.0000", "150.0000"]],
            column_names=["client_id", "symbol", "pnl", "price"],
        )
        # Deliberately wrong: audit says 300, official says 500 -- drift.
        await ch_client.insert(
            "pnl_audit", [[client_id, drifted_symbol, "300.0000", "150.0000"]],
            column_names=["client_id", "symbol", "pnl", "price"],
        )
        # missing_audit_symbol: no audit row at all -- should also flag.

        bloom = CountingBloomFilter(r, bloom_key, size_bits=1_000_000, num_hashes=7)
        reconciler = EODReconciler(pool, ch_client, bloom)
        drifts = await reconciler.run(close_date, tolerance=Decimal("0.01"))

        drifted_symbols_found = {d["symbol"] for d in drifts}
        assert drifted_symbols_found == {drifted_symbol, missing_audit_symbol}, (
            f"expected exactly {{'{drifted_symbol}', '{missing_audit_symbol}'}} "
            f"to be flagged, got {drifted_symbols_found}"
        )

        assert await bloom.might_contain(correct_symbol)
        assert await bloom.might_contain(drifted_symbol)
        assert not await bloom.might_contain(closed_symbol), (
            "rebuild must exclude symbols with a fully-closed (zero) position"
        )
    finally:
        pool.close()
        await pool.wait_closed()
        await ch_client.command(
            f"ALTER TABLE pnl_audit DELETE WHERE client_id = '{client_id}'"
        )
        await ch_client.close()
        await r.delete(bloom_key)
        await r.aclose()
