"""The ClickHouse raw-tick path must never apply lateness/reordering
logic — every tick lands, and querying by event_time returns them in the
correct historical order regardless of arrival order."""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import clickhouse_connect
import pytest

from pnl.common.events import Tick
from pnl.history.clickhouse_sink import RawTickSink


@pytest.mark.asyncio
async def test_out_of_order_ticks_all_land_and_are_correctly_ordered():
    client = await clickhouse_connect.get_async_client(host="localhost", port=8123)
    sink = RawTickSink(client)
    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    try:
        # Arrival order deliberately scrambled relative to event-time.
        for seconds in [5, 3, 8, 1, 7]:
            await sink.write(
                Tick(
                    symbol=symbol, price=Decimal(str(100 + seconds)),
                    event_time=base + timedelta(seconds=seconds),
                )
            )

        result = await client.query(
            "SELECT event_time FROM ticks WHERE symbol = {symbol:String} ORDER BY event_time",
            parameters={"symbol": symbol},
        )
        rows = [r[0] for r in result.result_rows]
        assert len(rows) == 5, f"expected all 5 ticks to land regardless of lateness, got {len(rows)}"

        expected_order = [base + timedelta(seconds=s) for s in [1, 3, 5, 7, 8]]
        # ClickHouse DateTime64 has millisecond precision; compare with a
        # small tolerance rather than exact equality.
        for actual, expected in zip(rows, expected_order):
            assert abs((actual.replace(tzinfo=timezone.utc) - expected).total_seconds()) < 0.01, (
                f"expected {expected}, got {actual} — ticks must be queryable "
                "in correct event-time order regardless of insertion order"
            )
    finally:
        await client.command(f"ALTER TABLE ticks DELETE WHERE symbol = '{symbol}'")
        await client.close()
