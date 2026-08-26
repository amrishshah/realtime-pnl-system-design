"""
STAGE 6 EXERCISE — implement RawTickSink.

Design doc §6: coalescing (Stage 5) is correct for the live dashboard
number — it only ever needs the latest price. But a PnL chart/history view
needs every meaningful price movement, or the line looks wrong/jumpy.
Applying coalescing to both consumers would silently drop data the chart
needs.

The fix is to branch the pipeline at ingestion, *before* coalescing: raw
ticks get written to ClickHouse in parallel with (not instead of) the
coalesced path — append-only, high-volume, exactly the shape ClickHouse is
built for. (`pnl/main.py` wires this branch via `asyncio.gather` — that
part is provided; this file is the ClickHouse-writing half of it.)

What to implement:
- `__init__(client)`: store a `clickhouse_connect` async client (constructed
  via `clickhouse_connect.get_async_client(...)` in `pnl/main.py`).
- `write(tick)`: insert one row into `ticks` — `await self.client.insert(
  "ticks", [[tick.symbol, tick.price, tick.event_time]],
  column_names=["symbol", "price", "event_time"])`.
"""
from __future__ import annotations

from pnl.common.events import Tick


class RawTickSink:
    def __init__(self, client) -> None:
        raise NotImplementedError("Stage 6: implement RawTickSink.__init__")

    async def write(self, tick: Tick) -> None:
        raise NotImplementedError("Stage 6: implement RawTickSink.write")
