"""ClickHouse must retain every raw tick (chart/history path) while the
coalesced live path processes far fewer — proving the branch happens
before coalescing, not after."""
import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect
import pytest

from pnl.common.events import Tick
from pnl.history.clickhouse_sink import RawTickSink
from pnl.ticks.coalescer import SymbolCoalescer


class FakeDispatcher:
    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.calls: list[Tick] = []

    async def handle_tick(self, tick: Tick) -> None:
        await asyncio.sleep(self.delay)
        self.calls.append(tick)


@pytest.mark.asyncio
async def test_clickhouse_retains_everything_while_coalesced_path_drops_most():
    client = await clickhouse_connect.get_async_client(host="localhost", port=8123)
    sink = RawTickSink(client)
    fake_dispatcher = FakeDispatcher(delay=0.05)
    coalescer = SymbolCoalescer(fake_dispatcher)

    symbol = f"SYM{uuid.uuid4().hex[:6].upper()}"
    num_ticks = 200

    async def ingest(tick: Tick) -> None:
        await asyncio.gather(sink.write(tick), coalescer.submit(tick))

    try:
        for i in range(num_ticks):
            await ingest(
                Tick(
                    symbol=symbol, price=Decimal(str(100 + i)),
                    event_time=datetime.now(timezone.utc),
                )
            )

        await asyncio.sleep(1.5)

        result = await client.query(
            "SELECT count() FROM ticks WHERE symbol = {symbol:String}",
            parameters={"symbol": symbol},
        )
        ch_count = result.result_rows[0][0]

        assert ch_count == num_ticks, (
            f"expected all {num_ticks} raw ticks in ClickHouse, got {ch_count} "
            "— the raw-tick branch must never drop for volume/coalescing reasons"
        )
        assert len(fake_dispatcher.calls) < num_ticks, (
            "the coalesced live path should have processed far fewer values "
            "than the raw count — if it processed all of them, coalescing "
            "isn't happening (check Stage 5)"
        )
    finally:
        await client.command(f"ALTER TABLE ticks DELETE WHERE symbol = '{symbol}'")
        await client.close()
